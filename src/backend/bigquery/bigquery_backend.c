#include "bigquery_internal.h"
#include "argus/handle.h"
#include "argus/log.h"
#include "argus/compat.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

/* ── CURL helpers ────────────────────────────────────────────── */

size_t argus_bq_write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t total = size * nmemb;
    bq_response_t *resp = (bq_response_t *)userp;
    char *p = realloc(resp->data, resp->size + total + 1);
    if (!p) return 0;
    resp->data = p;
    memcpy(resp->data + resp->size, contents, total);
    resp->size += total;
    resp->data[resp->size] = '\0';
    return total;
}

int bq_http(bq_conn_t *conn, const char *url, const char *post_body,
            bq_response_t *resp, long *http_code)
{
    if (bq_auth_ensure(conn) != 0) return -1;

    CURL *curl = conn->curl;
    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->headers);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, conn->ssl_verify ? 1L : 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, conn->ssl_verify ? 2L : 0L);
    if (conn->connect_timeout_sec > 0)
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                         (long)conn->connect_timeout_sec);
    if (post_body) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body);
    } else {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    }
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, argus_bq_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
    resp->data = NULL;
    resp->size = 0;

    CURLcode cc = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    if (http_code) *http_code = code;

    if (cc != CURLE_OK) {
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] %s", curl_easy_strerror(cc));
        return -1;
    }
    if (code >= 400) {
        /* Surface the API error message when the body carries one. */
        if (resp->data) {
            JsonParser *p = json_parser_new();
            if (json_parser_load_from_data(p, resp->data, -1, NULL)) {
                JsonObject *o = json_node_get_object(json_parser_get_root(p));
                JsonObject *e = (o && json_object_has_member(o, "error"))
                    ? json_object_get_object_member(o, "error") : NULL;
                const char *msg = (e && json_object_has_member(e, "message"))
                    ? json_object_get_string_member(e, "message") : NULL;
                if (msg)
                    snprintf(conn->last_error, sizeof(conn->last_error),
                             "[Argus][BigQuery] %s", msg);
            }
            g_object_unref(p);
        }
        if (!conn->last_error[0])
            snprintf(conn->last_error, sizeof(conn->last_error),
                     "[Argus][BigQuery] HTTP %ld from %s", code, url);
        return -1;
    }
    return 0;
}

/* Parse a JSON document; returns the parser (caller unrefs) or NULL. */
static JsonParser *bq_parse(const char *data)
{
    if (!data) return NULL;
    JsonParser *p = json_parser_new();
    if (!json_parser_load_from_data(p, data, -1, NULL)) {
        g_object_unref(p);
        return NULL;
    }
    return p;
}

/* ── SQL LIKE pattern matching for catalog filters ───────────── */

static bool bq_like(const char *pattern, const char *s)
{
    if (!pattern || !*pattern || strcmp(pattern, "%") == 0) return true;
    if (!s) return false;
    /* iterative wildcard match, case-insensitive, %/_ semantics */
    const char *star = NULL, *ss = NULL;
    while (*s) {
        if (*pattern == '%') {
            star = pattern++;
            ss = s;
        } else if (*pattern == '_' ||
                   g_ascii_tolower(*pattern) == g_ascii_tolower(*s)) {
            pattern++;
            s++;
        } else if (star) {
            pattern = star + 1;
            s = ++ss;
        } else {
            return false;
        }
    }
    while (*pattern == '%') pattern++;
    return *pattern == '\0';
}

/* True when the filter selects one concrete object (no wildcards). */
static bool bq_is_concrete(const char *pattern)
{
    return pattern && *pattern && !strchr(pattern, '%') && !strchr(pattern, '_');
}

/* ── Op helpers ──────────────────────────────────────────────── */

static bq_op_t *bq_op_new(void)
{
    bq_op_t *op = calloc(1, sizeof(*op));
    if (op) argus_row_cache_init(&op->cache);
    return op;
}

static void bq_op_free(bq_op_t *op)
{
    if (!op) return;
    argus_row_cache_free(&op->cache);
    free(op->columns);
    free(op->job_id);
    free(op->location);
    free(op->page_token);
    free(op);
}

/* Fixed VARCHAR result shape for catalog functions. */
static int bq_op_set_meta_columns(bq_op_t *op, const char **names, int n,
                                  int numeric_col /* -1 = none */)
{
    op->num_cols = n;
    op->columns = calloc((size_t)n, sizeof(argus_column_desc_t));
    if (!op->columns) { op->num_cols = 0; return -1; }
    for (int i = 0; i < n; i++) {
        strncpy((char *)op->columns[i].name, names[i],
                ARGUS_MAX_COLUMN_NAME - 1);
        op->columns[i].name_len = (SQLSMALLINT)strlen(names[i]);
        op->columns[i].sql_type = (i == numeric_col) ? SQL_SMALLINT
                                                     : SQL_VARCHAR;
        op->columns[i].column_size = 128;
        op->columns[i].nullable = SQL_NULLABLE;
    }
    return 0;
}

/* Append one row of text cells (NULL entry -> SQL NULL). */
static int bq_cache_append(argus_row_cache_t *cache, int ncols,
                           const char **values)
{
    if (cache->num_rows >= cache->capacity) {
        size_t ncap = cache->capacity ? cache->capacity * 2 : 32;
        argus_row_t *nr = realloc(cache->rows, ncap * sizeof(argus_row_t));
        if (!nr) return -1;
        memset(nr + cache->capacity, 0,
               (ncap - cache->capacity) * sizeof(argus_row_t));
        cache->rows = nr;
        cache->capacity = ncap;
    }
    argus_cell_t *cells = calloc((size_t)ncols, sizeof(argus_cell_t));
    if (!cells) return -1;
    for (int i = 0; i < ncols; i++) {
        if (!values[i]) { cells[i].is_null = true; continue; }
        cells[i].data = strdup(values[i]);
        cells[i].data_len = cells[i].data ? strlen(cells[i].data) : 0;
        cells[i].is_null = (cells[i].data == NULL);
    }
    cache->rows[cache->num_rows].cells = cells;
    cache->num_rows++;
    cache->num_cols = ncols;
    return 0;
}

/* ── Result parsing (jobs.query / getQueryResults share a shape) ── */

static void bq_parse_schema(bq_op_t *op, JsonObject *schema)
{
    JsonArray *fields = (schema && json_object_has_member(schema, "fields"))
        ? json_object_get_array_member(schema, "fields") : NULL;
    int ncols = fields ? (int)json_array_get_length(fields) : 0;
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;

    op->num_cols = ncols;
    op->columns = calloc((size_t)(ncols > 0 ? ncols : 1),
                         sizeof(argus_column_desc_t));
    if (!op->columns) { op->num_cols = 0; return; }

    for (int c = 0; c < ncols; c++) {
        JsonObject *f = json_array_get_object_element(fields, (guint)c);
        const char *nm = (f && json_object_has_member(f, "name"))
            ? json_object_get_string_member(f, "name") : "";
        const char *ty = (f && json_object_has_member(f, "type"))
            ? json_object_get_string_member(f, "type") : "STRING";
        const char *mode = (f && json_object_has_member(f, "mode"))
            ? json_object_get_string_member(f, "mode") : "NULLABLE";

        argus_column_desc_t *col = &op->columns[c];
        strncpy((char *)col->name, nm, ARGUS_MAX_COLUMN_NAME - 1);
        col->name_len = (SQLSMALLINT)strlen((char *)col->name);
        if (strcasecmp(mode, "REPEATED") == 0) {
            col->sql_type = SQL_VARCHAR;     /* serialized JSON array */
            col->column_size = 65535;
        } else {
            col->sql_type = bq_type_to_sql_type(ty);
            col->column_size = bq_type_column_size(ty);
            col->decimal_digits = bq_type_decimal_digits(ty);
        }
        col->nullable = (strcasecmp(mode, "REQUIRED") == 0)
            ? SQL_NO_NULLS : SQL_NULLABLE;
    }
}

/* Remember the REST type per column for cell conversion. */
static char **bq_schema_types(JsonObject *schema, int ncols)
{
    JsonArray *fields = (schema && json_object_has_member(schema, "fields"))
        ? json_object_get_array_member(schema, "fields") : NULL;
    if (!fields) return NULL;
    char **types = calloc((size_t)ncols, sizeof(char *));
    if (!types) return NULL;
    for (int c = 0; c < ncols && c < (int)json_array_get_length(fields); c++) {
        JsonObject *f = json_array_get_object_element(fields, (guint)c);
        const char *ty = (f && json_object_has_member(f, "type"))
            ? json_object_get_string_member(f, "type") : "STRING";
        const char *mode = (f && json_object_has_member(f, "mode"))
            ? json_object_get_string_member(f, "mode") : "NULLABLE";
        /* repeated/record values are serialized as JSON text */
        types[c] = strdup(strcasecmp(mode, "REPEATED") == 0 ? "STRING" : ty);
    }
    return types;
}

static void bq_free_types(char **types, int ncols)
{
    if (!types) return;
    for (int c = 0; c < ncols; c++) free(types[c]);
    free(types);
}

static void bq_parse_rows(bq_op_t *op, JsonObject *root, char **types)
{
    JsonArray *rows = json_object_has_member(root, "rows")
        ? json_object_get_array_member(root, "rows") : NULL;
    int nrows = rows ? (int)json_array_get_length(rows) : 0;
    int ncols = op->num_cols;
    if (nrows <= 0 || ncols <= 0) return;

    op->cache.rows = calloc((size_t)nrows, sizeof(argus_row_t));
    if (!op->cache.rows) return;
    op->cache.capacity = (size_t)nrows;
    op->cache.num_cols = ncols;

    size_t r = 0;
    for (int i = 0; i < nrows; i++) {
        JsonObject *row = json_array_get_object_element(rows, (guint)i);
        JsonArray *f = (row && json_object_has_member(row, "f"))
            ? json_object_get_array_member(row, "f") : NULL;
        if (!f) continue;
        argus_cell_t *cells = calloc((size_t)ncols, sizeof(argus_cell_t));
        if (!cells) break;
        int nf = (int)json_array_get_length(f);
        for (int c = 0; c < ncols && c < nf; c++) {
            argus_cell_t *cell = &cells[c];
            JsonObject *fo = json_array_get_object_element(f, (guint)c);
            JsonNode *v = (fo && json_object_has_member(fo, "v"))
                ? json_object_get_member(fo, "v") : NULL;
            if (!v || json_node_is_null(v)) { cell->is_null = true; continue; }
            if (JSON_NODE_HOLDS_VALUE(v) &&
                json_node_get_value_type(v) == G_TYPE_STRING) {
                bq_fill_cell(cell, types ? types[c] : NULL,
                             json_node_get_string(v));
            } else {
                /* RECORD / REPEATED: serialize the JSON subtree */
                JsonGenerator *gen = json_generator_new();
                json_generator_set_root(gen, v);
                gsize len = 0;
                char *txt = json_generator_to_data(gen, &len);
                g_object_unref(gen);
                cell->data = txt ? strdup(txt) : NULL;
                cell->data_len = cell->data ? len : 0;
                cell->is_null = (cell->data == NULL);
                g_free(txt);
            }
        }
        op->cache.rows[r].cells = cells;
        r++;
    }
    op->cache.num_rows = r;
    op->page_ready = (r > 0);
}

/* Digest one jobs.query / getQueryResults response into the op. */
static int bq_ingest_result(bq_conn_t *conn, bq_op_t *op, JsonObject *root,
                            bool *complete)
{
    if (json_object_has_member(root, "error")) {
        JsonObject *e = json_object_get_object_member(root, "error");
        const char *msg = (e && json_object_has_member(e, "message"))
            ? json_object_get_string_member(e, "message") : "query failed";
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] %s", msg);
        return -1;
    }

    *complete = !json_object_has_member(root, "jobComplete") ||
                json_object_get_boolean_member(root, "jobComplete");

    if (json_object_has_member(root, "jobReference")) {
        JsonObject *jr = json_object_get_object_member(root, "jobReference");
        if (jr && json_object_has_member(jr, "jobId") && !op->job_id)
            op->job_id = strdup(json_object_get_string_member(jr, "jobId"));
        if (jr && json_object_has_member(jr, "location") && !op->location)
            op->location = strdup(json_object_get_string_member(jr, "location"));
    }

    free(op->page_token);
    op->page_token = json_object_has_member(root, "pageToken")
        ? strdup(json_object_get_string_member(root, "pageToken")) : NULL;

    if (!*complete) return 0;

    JsonObject *schema = json_object_has_member(root, "schema")
        ? json_object_get_object_member(root, "schema") : NULL;
    if (op->num_cols == 0 && schema)
        bq_parse_schema(op, schema);

    char **types = bq_schema_types(schema, op->num_cols);
    bq_parse_rows(op, root, types);
    bq_free_types(types, op->num_cols);
    return 0;
}

/* GET getQueryResults for job polling and pagination. */
static int bq_get_query_results(bq_conn_t *conn, bq_op_t *op,
                                const char *page_token, bool *complete)
{
    char *e_job = g_uri_escape_string(op->job_id, NULL, FALSE);
    GString *url = g_string_new(NULL);
    g_string_printf(url, "%s/bigquery/v2/projects/%s/queries/%s"
                         "?timeoutMs=10000&maxResults=%d",
                    conn->base_url, conn->project, e_job,
                    conn->fetch_buffer_size);
    g_free(e_job);
    if (op->location && *op->location) {
        char *e_loc = g_uri_escape_string(op->location, NULL, FALSE);
        g_string_append_printf(url, "&location=%s", e_loc);
        g_free(e_loc);
    }
    if (page_token && *page_token) {
        char *e_tok = g_uri_escape_string(page_token, NULL, FALSE);
        g_string_append_printf(url, "&pageToken=%s", e_tok);
        g_free(e_tok);
    }

    bq_response_t resp = {0};
    int rc = bq_http(conn, url->str, NULL, &resp, NULL);
    g_string_free(url, TRUE);
    if (rc != 0) { free(resp.data); return -1; }

    JsonParser *p = bq_parse(resp.data);
    free(resp.data);
    if (!p) return -1;
    rc = bq_ingest_result(conn, op,
                          json_node_get_object(json_parser_get_root(p)),
                          complete);
    g_object_unref(p);
    return rc;
}

/* ── Connection lifecycle ────────────────────────────────────── */

static int bq_load_key_file(bq_conn_t *conn, const char *path)
{
    JsonParser *p = json_parser_new();
    GError *err = NULL;
    if (!json_parser_load_from_file(p, path, &err)) {
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] Cannot read key file %s: %s",
                 path, err ? err->message : "unknown error");
        if (err) g_error_free(err);
        g_object_unref(p);
        return -1;
    }
    JsonObject *o = json_node_get_object(json_parser_get_root(p));
    const char *email = (o && json_object_has_member(o, "client_email"))
        ? json_object_get_string_member(o, "client_email") : NULL;
    const char *key = (o && json_object_has_member(o, "private_key"))
        ? json_object_get_string_member(o, "private_key") : NULL;
    const char *token_uri = (o && json_object_has_member(o, "token_uri"))
        ? json_object_get_string_member(o, "token_uri") : NULL;
    if (!email || !key) {
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] Key file %s lacks client_email/private_key",
                 path);
        g_object_unref(p);
        return -1;
    }
    conn->sa_email = strdup(email);
    conn->sa_private_key = strdup(key);
    /* The key's token_uri is the default; an explicit BQTokenEndpoint
     * (S3NS and friends) takes precedence and is set by the caller. */
    if (!conn->token_url && token_uri)
        conn->token_url = strdup(token_uri);
    g_object_unref(p);
    return 0;
}

static void bq_conn_free(bq_conn_t *conn)
{
    if (!conn) return;
    if (conn->headers) curl_slist_free_all(conn->headers);
    if (conn->curl) curl_easy_cleanup(conn->curl);
    free(conn->base_url);
    free(conn->project);
    free(conn->dataset);
    free(conn->location);
    argus_secure_free(conn->access_token);
    free(conn->token_url);
    free(conn->audience);
    free(conn->scope);
    free(conn->sa_email);
    argus_secure_free(conn->sa_private_key);
    free(conn);
}

static int bq_connect(argus_dbc_t *dbc,
                      const char *host, int port,
                      const char *username, const char *password,
                      const char *database, const char *auth_mechanism,
                      argus_backend_conn_t *out_conn)
{
    (void)host; (void)port; (void)username; (void)password;
    (void)auth_mechanism;
    if (!out_conn || !dbc) return -1;

    if (!dbc->bq_project || !*dbc->bq_project) {
        argus_set_error(&dbc->diag, "08001",
                        "[Argus][BigQuery] PROJECT= is required", 0);
        return -1;
    }

    bq_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) return -1;
    conn->curl = curl_easy_init();
    if (!conn->curl) { free(conn); return -1; }

    conn->project = strdup(dbc->bq_project);
    if (database && *database) conn->dataset = strdup(database);
    if (dbc->bq_location) conn->location = strdup(dbc->bq_location);

    /* Every Google URL is overridable (sovereign clouds, emulator). */
    const char *ep = (dbc->bq_endpoint && *dbc->bq_endpoint)
        ? dbc->bq_endpoint : "https://bigquery.googleapis.com";
    size_t eplen = strlen(ep);
    while (eplen > 1 && ep[eplen - 1] == '/') eplen--;
    conn->base_url = strndup(ep, eplen);

    if (dbc->bq_token_url && *dbc->bq_token_url)
        conn->token_url = strdup(dbc->bq_token_url);
    if (dbc->bq_audience && *dbc->bq_audience)
        conn->audience = strdup(dbc->bq_audience);
    conn->scope = strdup((dbc->bq_scope && *dbc->bq_scope)
                         ? dbc->bq_scope
                         : "https://www.googleapis.com/auth/bigquery");

    conn->ssl_verify = dbc->ssl_verify;
    conn->connect_timeout_sec = dbc->connect_timeout_sec;
    conn->query_timeout_sec = dbc->query_timeout_sec;
    conn->fetch_buffer_size = dbc->fetch_buffer_size > 0
        ? dbc->fetch_buffer_size : 1000;

    if (dbc->bq_access_token && *dbc->bq_access_token) {
        conn->access_token = strdup(dbc->bq_access_token);
        conn->token_expiry = 0;    /* static */
    } else if (dbc->bq_key_file && *dbc->bq_key_file) {
        if (bq_load_key_file(conn, dbc->bq_key_file) != 0) {
            argus_set_error(&dbc->diag, "08001", conn->last_error, 0);
            bq_conn_free(conn);
            return -1;
        }
        if (!conn->token_url)
            conn->token_url = strdup("https://oauth2.googleapis.com/token");
    }
    bq_auth_set_headers(conn);

    /* Connectivity check: one-item dataset listing. */
    char url[1024];
    snprintf(url, sizeof(url),
             "%s/bigquery/v2/projects/%s/datasets?maxResults=1",
             conn->base_url, conn->project);
    bq_response_t resp = {0};
    if (bq_http(conn, url, NULL, &resp, NULL) != 0) {
        argus_set_error(&dbc->diag, "08001",
                        conn->last_error[0] ? conn->last_error
                        : "[Argus][BigQuery] Connectivity check failed", 0);
        free(resp.data);
        bq_conn_free(conn);
        return -1;
    }
    free(resp.data);

    *out_conn = conn;
    return 0;
}

static void bq_disconnect(argus_backend_conn_t raw)
{
    bq_conn_free((bq_conn_t *)raw);
}

static bool bq_is_alive(argus_backend_conn_t raw)
{
    return raw != NULL;
}

/* ── Execute ─────────────────────────────────────────────────── */

static int bq_execute(argus_backend_conn_t raw, const char *query,
                      argus_backend_op_t *out_op)
{
    bq_conn_t *conn = (bq_conn_t *)raw;
    if (!conn || !query || !out_op) return -1;
    conn->last_error[0] = '\0';

    JsonBuilder *b = json_builder_new();
    json_builder_begin_object(b);
    json_builder_set_member_name(b, "query");
    json_builder_add_string_value(b, query);
    json_builder_set_member_name(b, "useLegacySql");
    json_builder_add_boolean_value(b, FALSE);
    json_builder_set_member_name(b, "timeoutMs");
    json_builder_add_int_value(b, 10000);
    json_builder_set_member_name(b, "maxResults");
    json_builder_add_int_value(b, conn->fetch_buffer_size);
    if (conn->dataset && *conn->dataset) {
        json_builder_set_member_name(b, "defaultDataset");
        json_builder_begin_object(b);
        json_builder_set_member_name(b, "projectId");
        json_builder_add_string_value(b, conn->project);
        json_builder_set_member_name(b, "datasetId");
        json_builder_add_string_value(b, conn->dataset);
        json_builder_end_object(b);
    }
    if (conn->location && *conn->location) {
        json_builder_set_member_name(b, "location");
        json_builder_add_string_value(b, conn->location);
    }
    json_builder_end_object(b);

    JsonGenerator *gen = json_generator_new();
    json_generator_set_root(gen, json_builder_get_root(b));
    char *body = json_generator_to_data(gen, NULL);
    g_object_unref(gen);
    g_object_unref(b);

    char url[1024];
    snprintf(url, sizeof(url), "%s/bigquery/v2/projects/%s/queries",
             conn->base_url, conn->project);

    bq_response_t resp = {0};
    int rc = bq_http(conn, url, body, &resp, NULL);
    g_free(body);
    if (rc != 0) { free(resp.data); return -1; }

    bq_op_t *op = bq_op_new();
    if (!op) { free(resp.data); return -1; }

    JsonParser *p = bq_parse(resp.data);
    free(resp.data);
    if (!p) { bq_op_free(op); return -1; }
    bool complete = false;
    rc = bq_ingest_result(conn, op,
                          json_node_get_object(json_parser_get_root(p)),
                          &complete);
    g_object_unref(p);
    if (rc != 0) { bq_op_free(op); return -1; }

    /* Long-poll until the job completes (each call blocks <= 10 s). */
    time_t start = time(NULL);
    while (!complete) {
        if (!op->job_id) {
            snprintf(conn->last_error, sizeof(conn->last_error),
                     "[Argus][BigQuery] Incomplete job without jobReference");
            bq_op_free(op);
            return -1;
        }
        if (conn->query_timeout_sec > 0 &&
            time(NULL) - start > (time_t)conn->query_timeout_sec) {
            snprintf(conn->last_error, sizeof(conn->last_error),
                     "[Argus][BigQuery] Query timed out after %d s",
                     conn->query_timeout_sec);
            bq_op_free(op);
            return -1;
        }
        if (bq_get_query_results(conn, op, NULL, &complete) != 0) {
            bq_op_free(op);
            return -1;
        }
    }

    *out_op = op;
    return 0;
}

static int bq_get_operation_status(argus_backend_conn_t conn,
                                   argus_backend_op_t op, bool *finished)
{
    (void)conn; (void)op;
    if (finished) *finished = true;   /* execute() returns completed jobs */
    return 0;
}

static void bq_close_operation(argus_backend_conn_t conn,
                               argus_backend_op_t raw)
{
    (void)conn;
    bq_op_free((bq_op_t *)raw);
}

static int bq_cancel(argus_backend_conn_t raw, argus_backend_op_t rop)
{
    bq_conn_t *conn = (bq_conn_t *)raw;
    bq_op_t *op = (bq_op_t *)rop;
    if (!conn || !op || !op->job_id) return 0;

    char *e_job = g_uri_escape_string(op->job_id, NULL, FALSE);
    GString *url = g_string_new(NULL);
    g_string_printf(url, "%s/bigquery/v2/projects/%s/jobs/%s/cancel",
                    conn->base_url, conn->project, e_job);
    g_free(e_job);
    if (op->location && *op->location) {
        char *e_loc = g_uri_escape_string(op->location, NULL, FALSE);
        g_string_append_printf(url, "?location=%s", e_loc);
        g_free(e_loc);
    }
    bq_response_t resp = {0};
    int rc = bq_http(conn, url->str, "{}", &resp, NULL);
    g_string_free(url, TRUE);
    free(resp.data);
    return rc;
}

/* ── Fetch (page streaming) ──────────────────────────────────── */

static int bq_get_result_metadata(argus_backend_conn_t rconn,
                                  argus_backend_op_t raw,
                                  argus_column_desc_t *columns, int *num_cols)
{
    (void)rconn;
    bq_op_t *op = (bq_op_t *)raw;
    if (!op || !columns || !num_cols) return -1;
    if (op->columns && op->num_cols > 0)
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
    *num_cols = op->num_cols;
    return 0;
}

static int bq_fetch_results(argus_backend_conn_t rconn, argus_backend_op_t raw,
                            int max_rows, argus_row_cache_t *cache,
                            argus_column_desc_t *columns, int *num_cols)
{
    (void)max_rows;
    bq_conn_t *conn = (bq_conn_t *)rconn;
    bq_op_t *op = (bq_op_t *)raw;
    if (!op || !cache) return -1;

    if (columns && num_cols && op->columns && op->num_cols > 0) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    }

    /* Pull the next page when the current one was already delivered. */
    if (!op->page_ready && op->page_token && conn) {
        char *token = op->page_token;
        op->page_token = NULL;
        bool complete = true;
        int rc = bq_get_query_results(conn, op, token, &complete);
        free(token);
        if (rc != 0) return -1;
    }

    if (!op->page_ready) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    /* Transfer the page to the ODBC layer. */
    cache->rows = op->cache.rows;
    cache->num_rows = op->cache.num_rows;
    cache->capacity = op->cache.capacity;
    cache->num_cols = op->cache.num_cols;
    cache->current_row = 0;
    cache->exhausted = (op->page_token == NULL);
    op->cache.rows = NULL;
    op->cache.num_rows = 0;
    op->cache.capacity = 0;
    op->page_ready = false;
    return 0;
}

static bool bq_get_last_error(argus_backend_conn_t raw, char *buf,
                              size_t buflen)
{
    bq_conn_t *conn = (bq_conn_t *)raw;
    if (!conn || !conn->last_error[0] || buflen == 0) return false;
    strncpy(buf, conn->last_error, buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
}

/* ── Catalog: datasets / tables / columns ────────────────────── */

/* Collect dataset ids matching the pattern (handles pageToken). */
static GPtrArray *bq_list_datasets(bq_conn_t *conn, const char *pattern)
{
    GPtrArray *out = g_ptr_array_new_with_free_func(g_free);
    char *page = NULL;
    do {
        GString *url = g_string_new(NULL);
        g_string_printf(url, "%s/bigquery/v2/projects/%s/datasets"
                             "?maxResults=1000",
                        conn->base_url, conn->project);
        if (page) {
            char *e = g_uri_escape_string(page, NULL, FALSE);
            g_string_append_printf(url, "&pageToken=%s", e);
            g_free(e);
        }
        g_free(page);
        page = NULL;

        bq_response_t resp = {0};
        int rc = bq_http(conn, url->str, NULL, &resp, NULL);
        g_string_free(url, TRUE);
        if (rc != 0) { free(resp.data); break; }
        JsonParser *p = bq_parse(resp.data);
        free(resp.data);
        if (!p) break;
        JsonObject *o = json_node_get_object(json_parser_get_root(p));
        if (o && json_object_has_member(o, "nextPageToken"))
            page = g_strdup(json_object_get_string_member(o, "nextPageToken"));
        JsonArray *ds = (o && json_object_has_member(o, "datasets"))
            ? json_object_get_array_member(o, "datasets") : NULL;
        int n = ds ? (int)json_array_get_length(ds) : 0;
        for (int i = 0; i < n; i++) {
            JsonObject *d = json_array_get_object_element(ds, (guint)i);
            JsonObject *ref = (d && json_object_has_member(d, "datasetReference"))
                ? json_object_get_object_member(d, "datasetReference") : NULL;
            const char *id = (ref && json_object_has_member(ref, "datasetId"))
                ? json_object_get_string_member(ref, "datasetId") : NULL;
            if (id && bq_like(pattern, id))
                g_ptr_array_add(out, g_strdup(id));
        }
        g_object_unref(p);
    } while (page);
    return out;
}

static int bq_get_schemas(argus_backend_conn_t raw, const char *catalog,
                          const char *schema, argus_backend_op_t *out_op)
{
    (void)catalog;
    bq_conn_t *conn = (bq_conn_t *)raw;
    if (!conn || !out_op) return -1;

    bq_op_t *op = bq_op_new();
    if (!op) return -1;
    static const char *cn[2] = {"TABLE_SCHEM", "TABLE_CATALOG"};
    if (bq_op_set_meta_columns(op, cn, 2, -1) != 0) { bq_op_free(op); return -1; }

    GPtrArray *ds = bq_list_datasets(conn, schema);
    for (guint i = 0; i < ds->len; i++) {
        const char *vals[2] = { g_ptr_array_index(ds, i), conn->project };
        bq_cache_append(&op->cache, 2, vals);
    }
    g_ptr_array_unref(ds);
    op->page_ready = (op->cache.num_rows > 0);
    *out_op = op;
    return 0;
}

static int bq_get_catalogs(argus_backend_conn_t raw, argus_backend_op_t *out_op)
{
    bq_conn_t *conn = (bq_conn_t *)raw;
    if (!conn || !out_op) return -1;
    bq_op_t *op = bq_op_new();
    if (!op) return -1;
    static const char *cn[1] = {"TABLE_CAT"};
    if (bq_op_set_meta_columns(op, cn, 1, -1) != 0) { bq_op_free(op); return -1; }
    const char *vals[1] = { conn->project };
    bq_cache_append(&op->cache, 1, vals);
    op->page_ready = true;
    *out_op = op;
    return 0;
}

/* List tables of one dataset into a SQLTables-shaped cache. */
static void bq_tables_of_dataset(bq_conn_t *conn, bq_op_t *op,
                                 const char *dataset, const char *table_pat)
{
    char *page = NULL;
    char *e_ds = g_uri_escape_string(dataset, NULL, FALSE);
    do {
        GString *url = g_string_new(NULL);
        g_string_printf(url, "%s/bigquery/v2/projects/%s/datasets/%s/tables"
                             "?maxResults=1000",
                        conn->base_url, conn->project, e_ds);
        if (page) {
            char *e = g_uri_escape_string(page, NULL, FALSE);
            g_string_append_printf(url, "&pageToken=%s", e);
            g_free(e);
        }
        g_free(page);
        page = NULL;

        bq_response_t resp = {0};
        int rc = bq_http(conn, url->str, NULL, &resp, NULL);
        g_string_free(url, TRUE);
        if (rc != 0) { free(resp.data); break; }
        JsonParser *p = bq_parse(resp.data);
        free(resp.data);
        if (!p) break;
        JsonObject *o = json_node_get_object(json_parser_get_root(p));
        if (o && json_object_has_member(o, "nextPageToken"))
            page = g_strdup(json_object_get_string_member(o, "nextPageToken"));
        JsonArray *ts = (o && json_object_has_member(o, "tables"))
            ? json_object_get_array_member(o, "tables") : NULL;
        int n = ts ? (int)json_array_get_length(ts) : 0;
        for (int i = 0; i < n; i++) {
            JsonObject *t = json_array_get_object_element(ts, (guint)i);
            JsonObject *ref = (t && json_object_has_member(t, "tableReference"))
                ? json_object_get_object_member(t, "tableReference") : NULL;
            const char *id = (ref && json_object_has_member(ref, "tableId"))
                ? json_object_get_string_member(ref, "tableId") : NULL;
            const char *ty = (t && json_object_has_member(t, "type"))
                ? json_object_get_string_member(t, "type") : "TABLE";
            if (!id || !bq_like(table_pat, id)) continue;
            const char *odbc_type = (strcasecmp(ty, "VIEW") == 0 ||
                                     strcasecmp(ty, "MATERIALIZED_VIEW") == 0)
                ? "VIEW" : "TABLE";
            const char *vals[5] = { conn->project, dataset, id,
                                    odbc_type, NULL };
            bq_cache_append(&op->cache, 5, vals);
        }
        g_object_unref(p);
    } while (page);
    g_free(e_ds);
}

static int bq_get_tables(argus_backend_conn_t raw, const char *catalog,
                         const char *schema, const char *table_name,
                         const char *table_types, argus_backend_op_t *out_op)
{
    (void)catalog; (void)table_types;
    bq_conn_t *conn = (bq_conn_t *)raw;
    if (!conn || !out_op) return -1;

    bq_op_t *op = bq_op_new();
    if (!op) return -1;
    static const char *cn[5] = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                                "TABLE_TYPE", "REMARKS"};
    if (bq_op_set_meta_columns(op, cn, 5, -1) != 0) { bq_op_free(op); return -1; }

    if (bq_is_concrete(schema)) {
        bq_tables_of_dataset(conn, op, schema, table_name);
    } else {
        const char *ds_pat = schema;
        if ((!ds_pat || !*ds_pat) && conn->dataset)
            ds_pat = NULL;   /* no filter: list every dataset */
        GPtrArray *ds = bq_list_datasets(conn, ds_pat);
        for (guint i = 0; i < ds->len; i++)
            bq_tables_of_dataset(conn, op, g_ptr_array_index(ds, i),
                                 table_name);
        g_ptr_array_unref(ds);
    }
    op->page_ready = (op->cache.num_rows > 0);
    *out_op = op;
    return 0;
}

/* Emit SQLColumns rows for one table (tables.get -> schema.fields). */
static void bq_columns_of_table(bq_conn_t *conn, bq_op_t *op,
                                const char *dataset, const char *table,
                                const char *column_pat)
{
    char *e_ds = g_uri_escape_string(dataset, NULL, FALSE);
    char *e_t = g_uri_escape_string(table, NULL, FALSE);
    GString *url = g_string_new(NULL);
    g_string_printf(url, "%s/bigquery/v2/projects/%s/datasets/%s/tables/%s",
                    conn->base_url, conn->project, e_ds, e_t);
    g_free(e_ds);
    g_free(e_t);

    bq_response_t resp = {0};
    int rc = bq_http(conn, url->str, NULL, &resp, NULL);
    g_string_free(url, TRUE);
    if (rc != 0) { free(resp.data); return; }
    JsonParser *p = bq_parse(resp.data);
    free(resp.data);
    if (!p) return;

    JsonObject *o = json_node_get_object(json_parser_get_root(p));
    JsonObject *schema = (o && json_object_has_member(o, "schema"))
        ? json_object_get_object_member(o, "schema") : NULL;
    JsonArray *fields = (schema && json_object_has_member(schema, "fields"))
        ? json_object_get_array_member(schema, "fields") : NULL;
    int n = fields ? (int)json_array_get_length(fields) : 0;
    for (int i = 0; i < n; i++) {
        JsonObject *f = json_array_get_object_element(fields, (guint)i);
        const char *nm = (f && json_object_has_member(f, "name"))
            ? json_object_get_string_member(f, "name") : NULL;
        const char *ty = (f && json_object_has_member(f, "type"))
            ? json_object_get_string_member(f, "type") : "STRING";
        const char *mode = (f && json_object_has_member(f, "mode"))
            ? json_object_get_string_member(f, "mode") : "NULLABLE";
        if (!nm || !bq_like(column_pat, nm)) continue;

        char dt[8], pos[12];
        snprintf(dt, sizeof(dt), "%d", (int)bq_type_to_sql_type(ty));
        snprintf(pos, sizeof(pos), "%d", i + 1);
        const char *nullable = (strcasecmp(mode, "REQUIRED") == 0)
            ? "NO" : "YES";
        const char *vals[8] = { conn->project, dataset, table, nm,
                                dt, ty, pos, nullable };
        bq_cache_append(&op->cache, 8, vals);
    }
    g_object_unref(p);
}

static int bq_get_columns(argus_backend_conn_t raw, const char *catalog,
                          const char *schema, const char *table_name,
                          const char *column_name, argus_backend_op_t *out_op)
{
    (void)catalog;
    bq_conn_t *conn = (bq_conn_t *)raw;
    if (!conn || !out_op) return -1;

    bq_op_t *op = bq_op_new();
    if (!op) return -1;
    static const char *cn[8] = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                                "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                                "ORDINAL_POSITION", "IS_NULLABLE"};
    if (bq_op_set_meta_columns(op, cn, 8, 4) != 0) { bq_op_free(op); return -1; }
    op->columns[6].sql_type = SQL_INTEGER;   /* ORDINAL_POSITION */

    /* Resolve the dataset: explicit filter, else the connection default. */
    const char *ds_filter = (schema && *schema) ? schema : conn->dataset;

    if (bq_is_concrete(ds_filter) && bq_is_concrete(table_name)) {
        bq_columns_of_table(conn, op, ds_filter, table_name, column_name);
    } else {
        GPtrArray *ds = bq_list_datasets(conn, ds_filter);
        int visited = 0;
        for (guint i = 0; i < ds->len && visited < 200; i++) {
            const char *d = g_ptr_array_index(ds, i);
            bq_op_t *tmp = bq_op_new();
            if (!tmp) break;
            bq_tables_of_dataset(conn, tmp, d, table_name);
            for (size_t r = 0; r < tmp->cache.num_rows && visited < 200; r++) {
                const char *t = tmp->cache.rows[r].cells[2].data;
                if (t) {
                    bq_columns_of_table(conn, op, d, t, column_name);
                    visited++;
                }
            }
            bq_op_free(tmp);
        }
        g_ptr_array_unref(ds);
    }
    op->page_ready = (op->cache.num_rows > 0);
    *out_op = op;
    return 0;
}

/* ── Backend vtable ──────────────────────────────────────────── */

static const argus_backend_t bigquery_backend = {
    .name                  = "bigquery",
    .connect               = bq_connect,
    .disconnect            = bq_disconnect,
    .is_alive              = bq_is_alive,
    .execute               = bq_execute,
    .get_operation_status  = bq_get_operation_status,
    .close_operation       = bq_close_operation,
    .cancel                = bq_cancel,
    .fetch_results         = bq_fetch_results,
    .get_result_metadata   = bq_get_result_metadata,
    .get_tables            = bq_get_tables,
    .get_columns           = bq_get_columns,
    .get_type_info         = NULL,   /* ODBC-layer synthetic fallback */
    .get_schemas           = bq_get_schemas,
    .get_catalogs          = bq_get_catalogs,
    .get_primary_keys      = NULL,   /* BigQuery has no primary keys */
    .get_statistics        = NULL,
    .get_last_error        = bq_get_last_error,
};

const argus_backend_t *argus_bigquery_backend_get(void)
{
    return &bigquery_backend;
}
