#include "pinot_internal.h"
#include "argus/handle.h"
#include "argus/log.h"
#include "argus/compat.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── CURL helpers ────────────────────────────────────────────── */

static size_t write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t total = size * nmemb;
    pinot_response_t *resp = (pinot_response_t *)userp;
    char *p = realloc(resp->data, resp->size + total + 1);
    if (!p) return 0;
    resp->data = p;
    memcpy(resp->data + resp->size, contents, total);
    resp->size += total;
    resp->data[resp->size] = '\0';
    return total;
}

static void apply_curl(pinot_conn_t *conn, CURL *curl)
{
    if (conn->ssl_enabled) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, conn->ssl_verify ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, conn->ssl_verify ? 2L : 0L);
    }
    if (conn->connect_timeout_sec > 0)
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                         (long)conn->connect_timeout_sec);
    if (conn->user && *conn->user) {
        char up[512];
        snprintf(up, sizeof(up), "%s:%s", conn->user,
                 conn->password ? conn->password : "");
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        curl_easy_setopt(curl, CURLOPT_USERPWD, up);
    }
}

static int http(pinot_conn_t *conn, const char *url, const char *post_body,
                pinot_response_t *resp)
{
    CURL *curl = conn->curl;
    curl_easy_reset(curl);
    apply_curl(conn, curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    if (post_body) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->headers);
    } else {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    }
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
    resp->data = NULL;
    resp->size = 0;

    if (curl_easy_perform(curl) != CURLE_OK) return -1;
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    if (code >= 400) return -1;
    return 0;
}

/* ── Connection lifecycle ────────────────────────────────────── */

static int pinot_connect(argus_dbc_t *dbc,
                         const char *host, int port,
                         const char *username, const char *password,
                         const char *database, const char *auth_mechanism,
                         argus_backend_conn_t *out_conn)
{
    (void)database;
    (void)auth_mechanism;
    if (!out_conn || !host) return -1;

    pinot_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) return -1;

    conn->curl = curl_easy_init();
    if (!conn->curl) { free(conn); return -1; }

    if (dbc) {
        conn->ssl_enabled = dbc->ssl_enabled;
        conn->ssl_verify = dbc->ssl_verify;
        conn->connect_timeout_sec = dbc->connect_timeout_sec;
    }
    const char *scheme = conn->ssl_enabled ? "https" : "http";
    int broker_port = port > 0 ? port : 8000;

    char url[512];
    snprintf(url, sizeof(url), "%s://%s:%d", scheme, host, broker_port);
    conn->broker_url = strdup(url);
    /* The controller (table listing) is the broker host on its default port. */
    snprintf(url, sizeof(url), "%s://%s:9000", scheme, host);
    conn->controller_url = strdup(url);

    if (username && *username) conn->user = strdup(username);
    if (password && *password) conn->password = strdup(password);

    conn->headers = curl_slist_append(NULL, "Content-Type: application/json");
    conn->headers = curl_slist_append(conn->headers, "Accept: application/json");

    /* Validate connectivity with a trivial query. */
    pinot_response_t resp = {0};
    char q[256];
    snprintf(q, sizeof(q), "%s/query/sql", conn->broker_url);
    if (http(conn, q, "{\"sql\":\"SELECT 1\"}", &resp) != 0) {
        ARGUS_LOG_ERROR("Pinot: connectivity check failed");
        free(resp.data);
        curl_slist_free_all(conn->headers);
        curl_easy_cleanup(conn->curl);
        free(conn->broker_url); free(conn->controller_url);
        free(conn->user); free(conn->password); free(conn);
        if (dbc) argus_set_error(&dbc->diag, "08001",
                                 "[Argus][Pinot] Failed to reach broker", 0);
        return -1;
    }
    free(resp.data);

    *out_conn = conn;
    return 0;
}

static void pinot_disconnect(argus_backend_conn_t raw)
{
    pinot_conn_t *conn = (pinot_conn_t *)raw;
    if (!conn) return;
    if (conn->headers) curl_slist_free_all(conn->headers);
    if (conn->curl) curl_easy_cleanup(conn->curl);
    free(conn->broker_url);
    free(conn->controller_url);
    free(conn->user);
    free(conn->password);
    free(conn);
}

static bool pinot_is_alive(argus_backend_conn_t raw)
{
    return raw != NULL;
}

/* ── Value + result parsing ──────────────────────────────────── */

static char *json_value_to_str(JsonNode *node, size_t *len)
{
    GType vt = json_node_get_value_type(node);
    char *out = NULL;
    if (vt == G_TYPE_STRING) {
        const char *s = json_node_get_string(node);
        out = strdup(s ? s : "");
        *len = strlen(out);
    } else if (vt == G_TYPE_INT64) {
        out = malloc(24);
        if (out) *len = (size_t)snprintf(out, 24, "%lld",
                                         (long long)json_node_get_int(node));
    } else if (vt == G_TYPE_DOUBLE) {
        out = malloc(32);
        if (out) *len = (size_t)snprintf(out, 32, "%.15g",
                                         json_node_get_double(node));
    } else if (vt == G_TYPE_BOOLEAN) {
        gboolean v = json_node_get_boolean(node);
        out = strdup(v ? "true" : "false");
        *len = v ? 4 : 5;
    } else {
        /* arrays / objects → JSON text */
        JsonGenerator *gen = json_generator_new();
        json_generator_set_root(gen, node);
        out = json_generator_to_data(gen, len);
        g_object_unref(gen);
    }
    return out;
}

/* Parse a Pinot resultTable into the op's columns + row cache. */
static void parse_result_table(pinot_op_t *op, JsonObject *rt)
{
    JsonObject *schema = json_object_has_member(rt, "dataSchema")
        ? json_object_get_object_member(rt, "dataSchema") : NULL;
    JsonArray *names = (schema && json_object_has_member(schema, "columnNames"))
        ? json_object_get_array_member(schema, "columnNames") : NULL;
    JsonArray *types = (schema && json_object_has_member(schema, "columnDataTypes"))
        ? json_object_get_array_member(schema, "columnDataTypes") : NULL;

    int ncols = names ? (int)json_array_get_length(names) : 0;
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;
    op->num_cols = ncols;
    op->columns = calloc((size_t)(ncols > 0 ? ncols : 1),
                         sizeof(argus_column_desc_t));
    if (!op->columns) { op->num_cols = 0; return; }

    for (int c = 0; c < ncols; c++) {
        argus_column_desc_t *col = &op->columns[c];
        const char *nm = json_array_get_string_element(names, (guint)c);
        strncpy((char *)col->name, nm ? nm : "", ARGUS_MAX_COLUMN_NAME - 1);
        col->name_len = (SQLSMALLINT)strlen((char *)col->name);
        const char *ty = types ? json_array_get_string_element(types, (guint)c)
                               : "STRING";
        col->sql_type = pinot_type_to_sql_type(ty);
        col->column_size = pinot_type_column_size(col->sql_type);
        col->nullable = SQL_NULLABLE;
    }

    JsonArray *rows = json_object_has_member(rt, "rows")
        ? json_object_get_array_member(rt, "rows") : NULL;
    int nrows = rows ? (int)json_array_get_length(rows) : 0;
    if (nrows <= 0) return;

    op->cache.rows = calloc((size_t)nrows, sizeof(argus_row_t));
    if (!op->cache.rows) return;
    op->cache.capacity = (size_t)nrows;
    op->cache.num_cols = ncols;

    size_t r = 0;
    for (int i = 0; i < nrows; i++) {
        JsonArray *row = json_array_get_array_element(rows, (guint)i);
        if (!row) continue;
        op->cache.rows[r].cells = calloc((size_t)ncols, sizeof(argus_cell_t));
        if (!op->cache.rows[r].cells) break;
        int rl = (int)json_array_get_length(row);
        for (int c = 0; c < ncols && c < rl; c++) {
            argus_cell_t *cell = &op->cache.rows[r].cells[c];
            JsonNode *v = json_array_get_element(row, (guint)c);
            if (!v || json_node_is_null(v)) { cell->is_null = true; continue; }
            cell->data = json_value_to_str(v, &cell->data_len);
            cell->is_null = (cell->data == NULL);
        }
        r++;
    }
    op->cache.num_rows = r;
}

static pinot_op_t *op_new(void)
{
    pinot_op_t *op = calloc(1, sizeof(*op));
    if (op) argus_row_cache_init(&op->cache);
    return op;
}

/* ── Execute ─────────────────────────────────────────────────── */

int pinot_execute(argus_backend_conn_t raw, const char *query,
                  argus_backend_op_t *out_op)
{
    pinot_conn_t *conn = (pinot_conn_t *)raw;
    if (!conn || !query || !out_op) return -1;

    conn->last_error[0] = '\0';

    /* JSON-escape the query into the {"sql": "..."} body. */
    JsonBuilder *b = json_builder_new();
    json_builder_begin_object(b);
    json_builder_set_member_name(b, "sql");
    json_builder_add_string_value(b, query);
    json_builder_end_object(b);
    JsonGenerator *gen = json_generator_new();
    json_generator_set_root(gen, json_builder_get_root(b));
    char *body = json_generator_to_data(gen, NULL);
    g_object_unref(gen);
    g_object_unref(b);

    char url[512];
    snprintf(url, sizeof(url), "%s/query/sql", conn->broker_url);
    pinot_response_t resp = {0};
    int rc = http(conn, url, body, &resp);
    g_free(body);
    if (rc != 0 || !resp.data) { free(resp.data); return -1; }

    JsonParser *parser = json_parser_new();
    if (!json_parser_load_from_data(parser, resp.data, -1, NULL)) {
        g_object_unref(parser); free(resp.data); return -1;
    }
    JsonObject *root = json_node_get_object(json_parser_get_root(parser));

    /* Pinot reports query errors in an "exceptions" array. */
    if (root && json_object_has_member(root, "exceptions")) {
        JsonArray *ex = json_object_get_array_member(root, "exceptions");
        if (ex && json_array_get_length(ex) > 0) {
            JsonObject *e0 = json_array_get_object_element(ex, 0);
            const char *msg = (e0 && json_object_has_member(e0, "message"))
                ? json_object_get_string_member(e0, "message") : NULL;
            if (msg) {
                strncpy(conn->last_error, msg, sizeof(conn->last_error) - 1);
                conn->last_error[sizeof(conn->last_error) - 1] = '\0';
            }
            g_object_unref(parser); free(resp.data);
            return -1;
        }
    }

    pinot_op_t *op = op_new();
    if (!op) { g_object_unref(parser); free(resp.data); return -1; }
    if (root && json_object_has_member(root, "resultTable"))
        parse_result_table(op, json_object_get_object_member(root, "resultTable"));

    g_object_unref(parser);
    free(resp.data);
    *out_op = op;
    return 0;
}

static int pinot_get_operation_status(argus_backend_conn_t conn,
                                      argus_backend_op_t op, bool *finished)
{
    (void)conn; (void)op;
    if (finished) *finished = true;
    return 0;
}

static void pinot_close_operation(argus_backend_conn_t conn,
                                  argus_backend_op_t raw)
{
    (void)conn;
    pinot_op_t *op = (pinot_op_t *)raw;
    if (!op) return;
    argus_row_cache_free(&op->cache);
    free(op->columns);
    free(op);
}

static int pinot_cancel(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn; (void)op;
    return 0;   /* synchronous */
}

/* ── Metadata + fetch ────────────────────────────────────────── */

static int pinot_get_result_metadata(argus_backend_conn_t rconn,
                                     argus_backend_op_t raw,
                                     argus_column_desc_t *columns, int *num_cols)
{
    (void)rconn;
    pinot_op_t *op = (pinot_op_t *)raw;
    if (!op || !columns || !num_cols) return -1;
    if (op->columns && op->num_cols > 0)
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
    *num_cols = op->num_cols;
    return 0;
}

static int pinot_fetch_results(argus_backend_conn_t rconn, argus_backend_op_t raw,
                               int max_rows, argus_row_cache_t *cache,
                               argus_column_desc_t *columns, int *num_cols)
{
    (void)rconn; (void)max_rows;
    pinot_op_t *op = (pinot_op_t *)raw;
    if (!op || !cache) return -1;

    if (columns && num_cols && op->columns && op->num_cols > 0) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    }

    if (op->delivered) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    /* Hand the fully-materialized result to the ODBC layer (transfer rows). */
    cache->rows = op->cache.rows;
    cache->num_rows = op->cache.num_rows;
    cache->capacity = op->cache.capacity;
    cache->num_cols = op->cache.num_cols;
    cache->current_row = 0;
    cache->exhausted = true;
    op->cache.rows = NULL;
    op->cache.num_rows = 0;
    op->cache.capacity = 0;
    op->delivered = true;
    return 0;
}

static bool pinot_get_last_error(argus_backend_conn_t raw, char *buf,
                                 size_t buflen)
{
    pinot_conn_t *conn = (pinot_conn_t *)raw;
    if (!conn || !conn->last_error[0] || buflen == 0) return false;
    strncpy(buf, conn->last_error, buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
}

/* ── SQLTables via the controller /tables endpoint ───────────── */

int pinot_get_tables(argus_backend_conn_t raw, const char *catalog,
                     const char *schema, const char *table_name,
                     const char *table_types, argus_backend_op_t *out_op)
{
    (void)catalog; (void)schema; (void)table_types;
    pinot_conn_t *conn = (pinot_conn_t *)raw;
    if (!conn || !out_op) return -1;

    char url[512];
    snprintf(url, sizeof(url), "%s/tables", conn->controller_url);
    pinot_response_t resp = {0};
    pinot_op_t *op = op_new();
    if (!op) return -1;

    /* Result columns mandated by ODBC SQLTables. */
    static const char *cn[5] = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                                "TABLE_TYPE", "REMARKS"};
    op->num_cols = 5;
    op->columns = calloc(5, sizeof(argus_column_desc_t));
    if (op->columns)
        for (int i = 0; i < 5; i++) {
            strncpy((char *)op->columns[i].name, cn[i], ARGUS_MAX_COLUMN_NAME - 1);
            op->columns[i].name_len = (SQLSMALLINT)strlen(cn[i]);
            op->columns[i].sql_type = SQL_VARCHAR;
            op->columns[i].column_size = 128;
            op->columns[i].nullable = SQL_NULLABLE;
        }

    if (http(conn, url, NULL, &resp) == 0 && resp.data) {
        JsonParser *p = json_parser_new();
        if (json_parser_load_from_data(p, resp.data, -1, NULL)) {
            JsonObject *o = json_node_get_object(json_parser_get_root(p));
            JsonArray *tables = (o && json_object_has_member(o, "tables"))
                ? json_object_get_array_member(o, "tables") : NULL;
            int n = tables ? (int)json_array_get_length(tables) : 0;
            if (n > 0) {
                op->cache.rows = calloc((size_t)n, sizeof(argus_row_t));
                op->cache.capacity = (size_t)n;
                op->cache.num_cols = 5;
                size_t r = 0;
                for (int i = 0; i < n; i++) {
                    const char *t = json_array_get_string_element(tables, (guint)i);
                    if (!t) continue;
                    if (table_name && *table_name && strcmp(t, table_name) != 0)
                        continue;
                    argus_cell_t *cells = calloc(5, sizeof(argus_cell_t));
                    if (!cells) break;
                    cells[0].is_null = true;
                    cells[1].is_null = true;
                    cells[2].data = strdup(t); cells[2].data_len = strlen(t);
                    cells[3].data = strdup("TABLE"); cells[3].data_len = 5;
                    cells[4].is_null = true;
                    op->cache.rows[r].cells = cells;
                    r++;
                }
                op->cache.num_rows = r;
            }
        }
        g_object_unref(p);
    }
    free(resp.data);
    *out_op = op;
    return 0;
}

/* ── SQLGetTypeInfo (synthetic) ──────────────────────────────── */

int pinot_get_type_info(argus_backend_conn_t raw, SQLSMALLINT sql_type,
                        argus_backend_op_t *out_op)
{
    (void)sql_type;
    pinot_conn_t *conn = (pinot_conn_t *)raw;
    if (!conn || !out_op) return -1;
    /* Pinot has no metadata endpoint for this; return an empty, correctly
     * shaped result so SQLGetTypeInfo does not error. */
    pinot_op_t *op = op_new();
    if (!op) return -1;
    static const char *cn[3] = {"TYPE_NAME", "DATA_TYPE", "COLUMN_SIZE"};
    op->num_cols = 3;
    op->columns = calloc(3, sizeof(argus_column_desc_t));
    if (op->columns)
        for (int i = 0; i < 3; i++) {
            strncpy((char *)op->columns[i].name, cn[i], ARGUS_MAX_COLUMN_NAME - 1);
            op->columns[i].name_len = (SQLSMALLINT)strlen(cn[i]);
            op->columns[i].sql_type = (i == 0) ? SQL_VARCHAR : SQL_INTEGER;
        }
    *out_op = op;
    return 0;
}

/* ── Backend vtable ──────────────────────────────────────────── */

static const argus_backend_t pinot_backend = {
    .name                  = "pinot",
    .connect               = pinot_connect,
    .disconnect            = pinot_disconnect,
    .is_alive              = pinot_is_alive,
    .execute               = pinot_execute,
    .get_operation_status  = pinot_get_operation_status,
    .close_operation       = pinot_close_operation,
    .cancel                = pinot_cancel,
    .fetch_results         = pinot_fetch_results,
    .get_result_metadata   = pinot_get_result_metadata,
    .get_tables            = pinot_get_tables,
    .get_columns           = NULL,
    .get_type_info         = pinot_get_type_info,
    .get_schemas           = NULL,
    .get_catalogs          = NULL,
    .get_primary_keys      = NULL,
    .get_statistics        = NULL,
    .get_last_error        = pinot_get_last_error,
};

const argus_backend_t *argus_pinot_backend_get(void)
{
    return &pinot_backend;
}
