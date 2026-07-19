#include "trino_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

/* Forward declaration */
int trino_get_result_metadata(argus_backend_conn_t raw_conn,
                               argus_backend_op_t raw_op,
                               argus_column_desc_t *columns,
                               int *num_cols);

/* ── Parse column metadata from Trino JSON ───────────────────── */

int trino_parse_columns(JsonNode *columns_node,
                        argus_column_desc_t *columns,
                        int *num_cols)
{
    if (!columns_node || !columns || !num_cols) return -1;

    JsonArray *arr = json_node_get_array(columns_node);
    if (!arr) return -1;

    int ncols = (int)json_array_get_length(arr);
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;

    for (int i = 0; i < ncols; i++) {
        JsonObject *col_obj = json_array_get_object_element(arr, (guint)i);
        if (!col_obj) continue;

        argus_column_desc_t *col = &columns[i];
        memset(col, 0, sizeof(*col));

        /* Column name */
        if (json_object_has_member(col_obj, "name")) {
            const char *name = json_object_get_string_member(col_obj, "name");
            if (name) {
                strncpy((char *)col->name, name, ARGUS_MAX_COLUMN_NAME - 1);
                col->name_len = (SQLSMALLINT)strlen(name);
            }
        }

        /* Column type */
        const char *type_name = "varchar";
        if (json_object_has_member(col_obj, "type")) {
            type_name = json_object_get_string_member(col_obj, "type");
        }

        col->sql_type       = trino_type_to_sql_type(type_name);
        col->column_size    = trino_type_column_size(col->sql_type);
        col->decimal_digits = trino_type_decimal_digits(col->sql_type);
        col->nullable       = SQL_NULLABLE_UNKNOWN;
    }

    *num_cols = ncols;
    return 0;
}

/* ── Parse data rows from Trino JSON into row cache ──────────── */

int trino_parse_data(JsonNode *data_node,
                     argus_row_cache_t *cache,
                     int num_cols)
{
    if (!data_node || !cache) return -1;

    JsonArray *rows_arr = json_node_get_array(data_node);
    if (!rows_arr) return -1;

    int nrows = (int)json_array_get_length(rows_arr);
    if (nrows == 0) {
        cache->num_rows = 0;
        return 0;
    }

    cache->rows = calloc((size_t)nrows, sizeof(argus_row_t));
    if (!cache->rows) return -1;
    cache->num_rows = (size_t)nrows;
    cache->capacity = (size_t)nrows;
    cache->num_cols = num_cols;

    for (int r = 0; r < nrows; r++) {
        JsonArray *row_arr = json_array_get_array_element(rows_arr, (guint)r);
        if (!row_arr) continue;

        cache->rows[r].cells = calloc((size_t)num_cols, sizeof(argus_cell_t));
        if (!cache->rows[r].cells) return -1;

        int row_len = (int)json_array_get_length(row_arr);

        for (int c = 0; c < num_cols && c < row_len; c++) {
            argus_cell_t *cell = &cache->rows[r].cells[c];
            JsonNode *val_node = json_array_get_element(row_arr, (guint)c);

            if (!val_node || json_node_is_null(val_node)) {
                cell->is_null = true;
                cell->data = NULL;
                cell->data_len = 0;
                continue;
            }

            cell->is_null = false;

            /* Convert all JSON values to string representation */
            GType vtype = json_node_get_value_type(val_node);

            if (vtype == G_TYPE_STRING) {
                /* Copy in a single pass: strdup would walk the string to size
                 * the allocation and then strlen walks it again. At millions of
                 * string cells that second walk is pure waste. */
                const char *s = json_node_get_string(val_node);
                size_t len = s ? strlen(s) : 0;
                cell->data = malloc(len + 1);
                if (cell->data) {
                    if (len) memcpy(cell->data, s, len);
                    cell->data[len] = '\0';
                    cell->data_len = len;
                }
            } else if (vtype == G_TYPE_INT64) {
                /* Native typed value: no text round-trip on SQLGetData. */
                cell->native_kind = ARGUS_NATIVE_I64;
                cell->native.i64 = json_node_get_int(val_node);
            } else if (vtype == G_TYPE_DOUBLE) {
                cell->native_kind = ARGUS_NATIVE_F64;
                cell->native.f64 = json_node_get_double(val_node);
            } else if (vtype == G_TYPE_BOOLEAN) {
                gboolean v = json_node_get_boolean(val_node);
                cell->data = strdup(v ? "true" : "false");
                cell->data_len = v ? 4 : 5;
            } else {
                /* For complex types (arrays, objects), serialize to JSON string */
                JsonGenerator *gen = json_generator_new();
                json_generator_set_root(gen, val_node);
                cell->data = json_generator_to_data(gen, &cell->data_len);
                g_object_unref(gen);
            }
        }
    }

    return 0;
}

/* ── Fast JSON scanner for the Trino `data` array ────────────────
 *
 * Building a full json-glib DOM for a result page is ~half of fetch time (a
 * JsonNode/JsonValue object per cell, millions of them, then copied out and
 * freed). This scans the `data` array text straight into cells, matching
 * trino_parse_data exactly (int64/double stay native; strings are unescaped;
 * complex values are kept as raw JSON). The small envelope (columns, nextUri,
 * error) still goes through json-glib. Disable with ARGUS_TRINO_NOFASTJSON. */

static inline const char *sj_ws(const char *p, const char *e)
{
    while (p < e && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

/* p points at the opening quote; returns the pointer just past the closing
 * quote, or NULL if unterminated. */
static const char *sj_skip_string(const char *p, const char *e)
{
    p++;   /* opening quote */
    while (p < e) {
        char c = *p++;
        if (c == '\\') { if (p < e) p++; }
        else if (c == '"') return p;
    }
    return NULL;
}

/* Skip a complete JSON value; returns the pointer just past it, or NULL. */
static const char *sj_skip_value(const char *p, const char *e)
{
    p = sj_ws(p, e);
    if (p >= e) return NULL;
    char c = *p;
    if (c == '"') return sj_skip_string(p, e);
    if (c == '[' || c == '{') {
        char open = c, close = (c == '[') ? ']' : '}';
        int depth = 0;
        while (p < e) {
            char d = *p;
            if (d == '"') { p = sj_skip_string(p, e); if (!p) return NULL; continue; }
            if (d == open) depth++;
            else if (d == close) { depth--; if (depth == 0) return p + 1; }
            p++;
        }
        return NULL;
    }
    while (p < e && *p != ',' && *p != ']' && *p != '}' &&
           *p != ' ' && *p != '\t' && *p != '\n' && *p != '\r') p++;
    return p;
}

static inline int sj_hex(char h)
{
    if (h >= '0' && h <= '9') return h - '0';
    if (h >= 'a' && h <= 'f') return h - 'a' + 10;
    if (h >= 'A' && h <= 'F') return h - 'A' + 10;
    return -1;
}

/* Parse a JSON string (p at opening quote) into a fresh UTF-8 buffer. */
static const char *sj_parse_string(const char *p, const char *e,
                                   char **out, size_t *out_len)
{
    const char *end = sj_skip_string(p, e);
    if (!end) { *out = NULL; *out_len = 0; return NULL; }
    const char *s = p + 1, *last = end - 1;     /* content is [s, last) */
    char *buf = malloc((size_t)(last - s) + 1);  /* unescaping only shrinks */
    if (!buf) { *out = NULL; *out_len = 0; return end; }
    size_t o = 0;
    for (const char *r = s; r < last; ) {
        char c = *r++;
        if (c != '\\') { buf[o++] = c; continue; }
        if (r >= last) break;
        char x = *r++;
        switch (x) {
        case '"': buf[o++] = '"'; break;
        case '\\': buf[o++] = '\\'; break;
        case '/': buf[o++] = '/'; break;
        case 'b': buf[o++] = '\b'; break;
        case 'f': buf[o++] = '\f'; break;
        case 'n': buf[o++] = '\n'; break;
        case 'r': buf[o++] = '\r'; break;
        case 't': buf[o++] = '\t'; break;
        case 'u': {
            if (r + 4 > last) break;
            unsigned cp = 0;
            int ok = 1;
            for (int k = 0; k < 4; k++) { int hv = sj_hex(*r++); if (hv < 0) ok = 0; cp = (cp << 4) | (unsigned)(hv < 0 ? 0 : hv); }
            if (!ok) break;
            /* Combine a UTF-16 surrogate pair if present. */
            if (cp >= 0xD800 && cp <= 0xDBFF && r + 6 <= last &&
                r[0] == '\\' && r[1] == 'u') {
                unsigned lo = 0; int ok2 = 1;
                const char *rr = r + 2;
                for (int k = 0; k < 4; k++) { int hv = sj_hex(*rr++); if (hv < 0) ok2 = 0; lo = (lo << 4) | (unsigned)(hv < 0 ? 0 : hv); }
                if (ok2 && lo >= 0xDC00 && lo <= 0xDFFF) {
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                    r = rr;
                }
            }
            if (cp < 0x80) buf[o++] = (char)cp;
            else if (cp < 0x800) { buf[o++] = (char)(0xC0 | (cp >> 6)); buf[o++] = (char)(0x80 | (cp & 0x3F)); }
            else if (cp < 0x10000) { buf[o++] = (char)(0xE0 | (cp >> 12)); buf[o++] = (char)(0x80 | ((cp >> 6) & 0x3F)); buf[o++] = (char)(0x80 | (cp & 0x3F)); }
            else { buf[o++] = (char)(0xF0 | (cp >> 18)); buf[o++] = (char)(0x80 | ((cp >> 12) & 0x3F)); buf[o++] = (char)(0x80 | ((cp >> 6) & 0x3F)); buf[o++] = (char)(0x80 | (cp & 0x3F)); }
            break;
        }
        default: buf[o++] = x; break;
        }
    }
    buf[o] = '\0';
    *out = buf; *out_len = o;
    return end;
}

/* Parse one JSON value at p into a cell, mirroring trino_parse_data. */
static const char *sj_value_to_cell(const char *p, const char *e, argus_cell_t *cell)
{
    p = sj_ws(p, e);
    if (p >= e) return NULL;
    char c = *p;
    if (c == 'n') { cell->is_null = true; return (p + 4 <= e) ? p + 4 : e; }
    if (c == 't') { cell->data = strdup("true");  cell->data_len = 4; return (p + 4 <= e) ? p + 4 : e; }
    if (c == 'f') { cell->data = strdup("false"); cell->data_len = 5; return (p + 5 <= e) ? p + 5 : e; }
    if (c == '"') {
        char *s = NULL; size_t sl = 0;
        const char *q = sj_parse_string(p, e, &s, &sl);
        cell->data = s; cell->data_len = sl;
        return q ? q : e;
    }
    if (c == '[' || c == '{') {
        const char *ve = sj_skip_value(p, e);
        if (!ve) return NULL;
        size_t rl = (size_t)(ve - p);
        char *buf = malloc(rl + 1);
        if (buf) { memcpy(buf, p, rl); buf[rl] = '\0'; cell->data = buf; cell->data_len = rl; }
        return ve;
    }
    /* number */
    const char *ns = p;
    int is_float = 0;
    while (p < e && *p != ',' && *p != ']' && *p != '}' &&
           *p != ' ' && *p != '\t' && *p != '\n' && *p != '\r') {
        if (*p == '.' || *p == 'e' || *p == 'E') is_float = 1;
        p++;
    }
    char tmp[64];
    size_t nl = (size_t)(p - ns);
    if (nl >= sizeof(tmp)) nl = sizeof(tmp) - 1;
    memcpy(tmp, ns, nl); tmp[nl] = '\0';
    if (is_float) { cell->native_kind = ARGUS_NATIVE_F64; cell->native.f64 = strtod(tmp, NULL); }
    else {
        errno = 0;
        long long v = strtoll(tmp, NULL, 10);
        if (errno == ERANGE) { cell->native_kind = ARGUS_NATIVE_F64; cell->native.f64 = strtod(tmp, NULL); }
        else { cell->native_kind = ARGUS_NATIVE_I64; cell->native.i64 = v; }
    }
    return p;
}

/* Scan a `data` array [ds,de) (array-of-arrays) straight into the row cache.
 * Returns 0 on success, -1 to signal the caller should fall back to json-glib. */
static int sj_scan_data(const char *ds, const char *de,
                        argus_row_cache_t *cache, int num_cols)
{
    const char *p = sj_ws(ds, de);
    if (p >= de || *p != '[') return -1;
    p++;
    size_t cap = 256, n = 0;
    argus_row_t *rows = calloc(cap, sizeof(argus_row_t));
    if (!rows) return -1;
    p = sj_ws(p, de);
    if (p < de && *p == ']') {
        cache->rows = rows; cache->num_rows = 0;
        cache->capacity = cap; cache->num_cols = num_cols;
        return 0;
    }
    while (p < de) {
        p = sj_ws(p, de);
        if (p >= de || *p != '[') goto fail;
        p++;
        if (n == cap) {
            cap *= 2;
            argus_row_t *nr = realloc(rows, cap * sizeof(argus_row_t));
            if (!nr) goto fail;
            rows = nr;
        }
        rows[n].cells = calloc((size_t)num_cols, sizeof(argus_cell_t));
        if (!rows[n].cells) goto fail;
        for (int col = 0; col < num_cols; col++) {
            p = sj_ws(p, de);
            if (p < de && *p == ']') break;   /* short row */
            const char *ve = sj_value_to_cell(p, de, &rows[n].cells[col]);
            if (!ve) { n++; goto fail; }
            p = sj_ws(ve, de);
            if (p < de && *p == ',') p++;
        }
        p = sj_ws(p, de);
        while (p < de && *p != ']') p++;   /* tolerate extra cells */
        if (p < de) p++;                    /* past row ']' */
        n++;
        p = sj_ws(p, de);
        if (p < de && *p == ',') { p++; continue; }
        break;   /* end of data array */
    }
    cache->rows = rows; cache->num_rows = n;
    cache->capacity = cap; cache->num_cols = num_cols;
    return 0;

fail:
    for (size_t i = 0; i < n; i++) {
        if (rows[i].cells) {
            for (int c = 0; c < num_cols; c++) free(rows[i].cells[c].data);
            free(rows[i].cells);
        }
    }
    free(rows);
    return -1;
}

/* Locate the value bounds of a named member in the top-level object. Returns 0
 * (found, *vs/*ve set), 1 (absent), or -1 (malformed). */
static int sj_find_member(const char *text, size_t len, const char *key,
                          const char **vs, const char **ve)
{
    const char *p = text, *e = text + len;
    p = sj_ws(p, e);
    if (p >= e || *p != '{') return -1;
    p++;
    size_t keylen = strlen(key);
    while (1) {
        p = sj_ws(p, e);
        if (p >= e) return -1;
        if (*p == '}') return 1;
        if (*p != '"') return -1;
        const char *kstart = p + 1;
        const char *kend = sj_skip_string(p, e);
        if (!kend) return -1;
        size_t klen = (size_t)(kend - 1 - kstart);
        p = sj_ws(kend, e);
        if (p >= e || *p != ':') return -1;
        p++;
        const char *vstart = sj_ws(p, e);
        const char *vend = sj_skip_value(vstart, e);
        if (!vend) return -1;
        if (klen == keylen && memcmp(kstart, key, keylen) == 0) {
            *vs = vstart; *ve = vend;
            return 0;
        }
        p = sj_ws(vend, e);
        if (p < e && *p == ',') { p++; continue; }
        return 1;
    }
}

/* ── FetchResults via Trino REST API ─────────────────────────── */

int trino_fetch_results(argus_backend_conn_t raw_conn,
                        argus_backend_op_t raw_op,
                        int max_rows,
                        argus_row_cache_t *cache,
                        argus_column_desc_t *columns,
                        int *num_cols)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    trino_operation_t *op = (trino_operation_t *)raw_op;
    if (!conn || !op) return -1;

    (void)max_rows;

    /* Return metadata if available */
    if (!op->metadata_fetched && columns && num_cols) {
        trino_get_result_metadata(raw_conn, raw_op, columns, num_cols);
    } else if (op->metadata_fetched && columns && num_cols) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    }

    /* Deliver any rows that get_result_metadata had to read off the stream
     * (Trino interleaves columns and data in one response for small results). */
    if (op->prefetch) {
        cache->rows = op->prefetch->rows;
        cache->num_rows = op->prefetch->num_rows;
        cache->capacity = op->prefetch->capacity;
        cache->num_cols = op->prefetch->num_cols;
        cache->current_row = 0;
        free(op->prefetch);   /* rows ownership transferred to cache */
        op->prefetch = NULL;
        if (!op->next_uri)
            cache->exhausted = true;
        return 0;
    }

    /* No more data to fetch */
    if (!op->next_uri) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    /* Poll nextUri until we get data or query finishes */
    while (op->next_uri) {
        trino_response_t resp = {0};
        if (trino_http_get(conn, op->next_uri, &resp) != 0) {
            free(resp.data);
            return -1;
        }

        if (!resp.data) return -1;

        /* Fast path: scan the `data` array straight into cells without building
         * a json-glib DOM for it (that DOM is ~half of fetch time). The small
         * envelope (columns/nextUri) is parsed on a copy with `data` excised.
         * Any error member or scan difficulty falls through to the DOM path. */
        if (!getenv("ARGUS_TRINO_NOFASTJSON")) {
            size_t rlen = strlen(resp.data);
            const char *ds, *de, *es, *ee;
            int has_err = (sj_find_member(resp.data, rlen, "error", &es, &ee) == 0);
            if (!has_err && sj_find_member(resp.data, rlen, "data", &ds, &de) == 0 &&
                sj_ws(ds, resp.data + rlen) < resp.data + rlen &&
                *sj_ws(ds, resp.data + rlen) == '[') {
                size_t head = (size_t)(ds - resp.data);
                size_t tail = rlen - (size_t)(de - resp.data);
                char *env = malloc(head + 2 + tail + 1);
                if (env) {
                    memcpy(env, resp.data, head);
                    env[head] = '['; env[head + 1] = ']';
                    memcpy(env + head + 2, de, tail);
                    env[head + 2 + tail] = '\0';

                    JsonParser *ep = json_parser_new();
                    int ok = json_parser_load_from_data(ep, env, -1, NULL);
                    JsonObject *eo = ok ? json_node_get_object(json_parser_get_root(ep)) : NULL;
                    if (eo) {
                        if (!op->metadata_fetched && json_object_has_member(eo, "columns")) {
                            JsonNode *cn = json_object_get_member(eo, "columns");
                            op->columns = calloc(ARGUS_MAX_COLUMNS, sizeof(argus_column_desc_t));
                            if (op->columns) {
                                trino_parse_columns(cn, op->columns, &op->num_cols);
                                op->metadata_fetched = true;
                                if (columns && num_cols) {
                                    memcpy(columns, op->columns,
                                           (size_t)op->num_cols * sizeof(argus_column_desc_t));
                                    *num_cols = op->num_cols;
                                }
                            }
                        }
                        free(op->next_uri);
                        op->next_uri = NULL;
                        if (json_object_has_member(eo, "nextUri"))
                            op->next_uri = strdup(json_object_get_string_member(eo, "nextUri"));
                        else
                            op->finished = true;

                        int ncols = op->num_cols > 0 ? op->num_cols : 1;
                        int sr = sj_scan_data(ds, de, cache, ncols);
                        if (sr == 0) {
                            if (!op->next_uri) cache->exhausted = true;
                            g_object_unref(ep);
                            free(env);
                            free(resp.data);
                            return 0;
                        }
                    }
                    g_object_unref(ep);
                    free(env);
                    /* fall through to the DOM path on any failure */
                }
            }
        }

        JsonParser *parser = json_parser_new();
        if (!json_parser_load_from_data(parser, resp.data, -1, NULL)) {
            g_object_unref(parser);
            free(resp.data);
            return -1;
        }

        JsonNode *root = json_parser_get_root(parser);
        JsonObject *obj = json_node_get_object(root);

        /* Check for error */
        if (json_object_has_member(obj, "error")) {
            trino_capture_error(conn, obj);
            g_object_unref(parser);
            free(resp.data);
            return -1;
        }

        /* Parse column metadata if not yet fetched */
        if (!op->metadata_fetched && json_object_has_member(obj, "columns")) {
            JsonNode *columns_node = json_object_get_member(obj, "columns");
            op->columns = calloc(ARGUS_MAX_COLUMNS, sizeof(argus_column_desc_t));
            if (op->columns) {
                trino_parse_columns(columns_node, op->columns, &op->num_cols);
                op->metadata_fetched = true;
                if (columns && num_cols) {
                    memcpy(columns, op->columns,
                           (size_t)op->num_cols * sizeof(argus_column_desc_t));
                    *num_cols = op->num_cols;
                }
            }
        }

        /* Update nextUri */
        free(op->next_uri);
        op->next_uri = NULL;
        if (json_object_has_member(obj, "nextUri")) {
            op->next_uri = strdup(
                json_object_get_string_member(obj, "nextUri"));
        } else {
            op->finished = true;
        }

        /* Parse data if present */
        if (json_object_has_member(obj, "data")) {
            JsonNode *data_node = json_object_get_member(obj, "data");
            int ncols = op->num_cols > 0 ? op->num_cols : 1;

            if (JSON_NODE_HOLDS_ARRAY(data_node)) {
                /* v1 format: flat array of arrays */
                trino_parse_data(data_node, cache, ncols);
            } else if (JSON_NODE_HOLDS_OBJECT(data_node)) {
                /* v2 format: spooled segments object */
                JsonObject *data_obj = json_node_get_object(data_node);
                op->spooling_active = true;
                trino_parse_spooled_data(conn, data_obj, cache, ncols);
            }

            if (!op->next_uri)
                cache->exhausted = true;

            g_object_unref(parser);
            free(resp.data);
            return 0;
        }

        g_object_unref(parser);
        free(resp.data);

        /* If no data and no nextUri, we're done */
        if (!op->next_uri) {
            cache->num_rows = 0;
            cache->exhausted = true;
            return 0;
        }
    }

    cache->num_rows = 0;
    cache->exhausted = true;
    return 0;
}

/* ── Get result set metadata ──────────────────────────────────── */

int trino_get_result_metadata(argus_backend_conn_t raw_conn,
                               argus_backend_op_t raw_op,
                               argus_column_desc_t *columns,
                               int *num_cols)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    trino_operation_t *op = (trino_operation_t *)raw_op;
    if (!conn || !op) return -1;

    /* Return cached metadata if available */
    if (op->metadata_fetched && op->columns && op->num_cols > 0) {
        if (columns && num_cols) {
            memcpy(columns, op->columns,
                   (size_t)op->num_cols * sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        }
        return 0;
    }

    /* Poll nextUri until we get columns metadata */
    while (op->next_uri && !op->metadata_fetched) {
        trino_response_t resp = {0};
        if (trino_http_get(conn, op->next_uri, &resp) != 0) {
            free(resp.data);
            return -1;
        }

        if (!resp.data) return -1;

        JsonParser *parser = json_parser_new();
        if (!json_parser_load_from_data(parser, resp.data, -1, NULL)) {
            g_object_unref(parser);
            free(resp.data);
            return -1;
        }

        JsonNode *root = json_parser_get_root(parser);
        JsonObject *obj = json_node_get_object(root);

        /* The query may have failed during planning/execution; the error
         * surfaces here while polling for the result. */
        if (json_object_has_member(obj, "error")) {
            trino_capture_error(conn, obj);
            g_object_unref(parser);
            free(resp.data);
            return -1;
        }

        /* Parse column metadata */
        if (json_object_has_member(obj, "columns")) {
            if (!op->columns)
                op->columns = calloc(ARGUS_MAX_COLUMNS,
                                     sizeof(argus_column_desc_t));
            if (op->columns && op->num_cols == 0) {
                JsonNode *columns_node = json_object_get_member(obj, "columns");
                trino_parse_columns(columns_node, op->columns, &op->num_cols);
            }
            /* A SELECT stops here and fetches data lazily. An update statement
             * (INSERT/UPDATE/DELETE/...) carries "updateType" and only applies
             * its write once the client drains the query to FINISHED, so keep
             * polling rather than abandoning it mid-flight. */
            if (op->columns && !json_object_has_member(obj, "updateType"))
                op->metadata_fetched = true;
        }

        /* Update nextUri */
        free(op->next_uri);
        op->next_uri = NULL;
        if (json_object_has_member(obj, "nextUri")) {
            op->next_uri = strdup(
                json_object_get_string_member(obj, "nextUri"));
        } else {
            op->finished = true;
        }

        /* Trino can return columns and the first data rows in the same
         * response. Since this response is now consumed, stash that data for
         * the next fetch_results rather than dropping it (small results would
         * otherwise come back with zero rows). */
        if (op->metadata_fetched && !op->prefetch &&
            json_object_has_member(obj, "data")) {
            JsonNode *data_node = json_object_get_member(obj, "data");
            if (JSON_NODE_HOLDS_ARRAY(data_node)) {
                op->prefetch = calloc(1, sizeof(argus_row_cache_t));
                if (op->prefetch) {
                    argus_row_cache_init(op->prefetch);
                    trino_parse_data(data_node, op->prefetch,
                                     op->num_cols > 0 ? op->num_cols : 1);
                }
            }
        }

        g_object_unref(parser);
        free(resp.data);
    }

    if (op->metadata_fetched && columns && num_cols) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    }

    return op->metadata_fetched ? 0 : -1;
}
