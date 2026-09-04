#include "druid_internal.h"
#include "argus/handle.h"
#include "argus/log.h"
#include "argus/compat.h"
#include "../curl_common.h"
#include "argus/numtext.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── CURL helpers ────────────────────────────────────────────── */

static size_t write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t total = size * nmemb;
    druid_response_t *resp = (druid_response_t *)userp;
    char *p = realloc(resp->data, resp->size + total + 1);
    if (!p) return 0;
    resp->data = p;
    memcpy(resp->data + resp->size, contents, total);
    resp->size += total;
    resp->data[resp->size] = '\0';
    return total;
}

/* POST a body to /druid/v2/sql; keeps the body even on HTTP >= 400 so the
 * caller can read the error document. Returns -1 only on transport failure. */
static int http_post(druid_conn_t *conn, const char *url, const char *body,
                     druid_response_t *resp)
{
    CURL *curl = conn->curl;
    curl_easy_reset(curl);
    argus_curl_apply_baseline(curl);
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
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
    resp->data = NULL; resp->size = 0; resp->http_code = 0;

    if (curl_easy_perform(curl) != CURLE_OK) return -1;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &resp->http_code);
    return 0;
}

/* Build a {"query": "...", "resultFormat":"array", "header":true,
 * "sqlTypesHeader":true} body with the SQL properly JSON-escaped. */
static char *build_query_body(const char *sql)
{
    JsonBuilder *b = json_builder_new();
    json_builder_begin_object(b);
    json_builder_set_member_name(b, "query");
    json_builder_add_string_value(b, sql);
    json_builder_set_member_name(b, "resultFormat");
    json_builder_add_string_value(b, "array");
    json_builder_set_member_name(b, "header");
    json_builder_add_boolean_value(b, TRUE);
    json_builder_set_member_name(b, "sqlTypesHeader");
    json_builder_add_boolean_value(b, TRUE);
    json_builder_end_object(b);
    JsonGenerator *g = json_generator_new();
    json_generator_set_root(g, json_builder_get_root(b));
    char *body = json_generator_to_data(g, NULL);
    g_object_unref(g);
    g_object_unref(b);
    return body;
}

/* ── Connection lifecycle ────────────────────────────────────── */

static int druid_connect(argus_dbc_t *dbc,
                         const char *host, int port,
                         const char *username, const char *password,
                         const char *database, const char *auth_mechanism,
                         argus_backend_conn_t *out_conn)
{
    (void)database;
    (void)auth_mechanism;
    if (!out_conn || !host) return -1;

    druid_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) return -1;
    conn->curl = curl_easy_init();
    if (!conn->curl) { free(conn); return -1; }

    if (dbc) {
        conn->ssl_enabled = dbc->ssl_enabled;
        conn->ssl_verify = dbc->ssl_verify;
        conn->connect_timeout_sec = dbc->connect_timeout_sec;
    }
    const char *scheme = conn->ssl_enabled ? "https" : "http";
    int p = port > 0 ? port : 8888;   /* Druid router default */
    char url[512];
    snprintf(url, sizeof(url), "%s://%s:%d", scheme, host, p);
    conn->base_url = strdup(url);
    if (username && *username) conn->user = strdup(username);
    if (password && *password) conn->password = strdup(password);
    conn->headers = curl_slist_append(NULL, "Content-Type: application/json");

    /* Connectivity probe. */
    char sqlurl[512];
    snprintf(sqlurl, sizeof(sqlurl), "%s/druid/v2/sql", conn->base_url);
    char *body = build_query_body("SELECT 1");
    druid_response_t resp = {0};
    int rc = http_post(conn, sqlurl, body, &resp);
    g_free(body);
    int ok = (rc == 0 && resp.http_code < 400);
    free(resp.data);
    if (!ok) {
        curl_slist_free_all(conn->headers);
        curl_easy_cleanup(conn->curl);
        free(conn->base_url); free(conn->user); free(conn->password); free(conn);
        if (dbc) argus_set_error(&dbc->diag, "08001",
                                 "[Argus][Druid] Failed to reach broker", 0);
        return -1;
    }
    *out_conn = conn;
    return 0;
}

static void druid_disconnect(argus_backend_conn_t raw)
{
    druid_conn_t *conn = (druid_conn_t *)raw;
    if (!conn) return;
    if (conn->headers) curl_slist_free_all(conn->headers);
    if (conn->curl) curl_easy_cleanup(conn->curl);
    free(conn->base_url);
    free(conn->user);
    free(conn->password);
    free(conn);
}

static size_t discard_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
    (void)contents; (void)userp;
    return size * nmemb;
}

/* Real liveness probe: the pool relies on this to avoid handing out
 * connections to a dead broker, so a pointer check is not enough. The
 * router's /status/health endpoint answers in microseconds. */
static bool druid_is_alive(argus_backend_conn_t raw)
{
    druid_conn_t *conn = (druid_conn_t *)raw;
    if (!conn || !conn->curl || !conn->base_url) return false;

    char url[560];
    snprintf(url, sizeof(url), "%s/status/health", conn->base_url);

    CURL *curl = conn->curl;
    curl_easy_reset(curl);
    argus_curl_apply_baseline(curl);
    if (conn->ssl_enabled) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, conn->ssl_verify ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, conn->ssl_verify ? 2L : 0L);
    }
    if (conn->user && *conn->user) {
        char up[512];
        snprintf(up, sizeof(up), "%s:%s", conn->user,
                 conn->password ? conn->password : "");
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        curl_easy_setopt(curl, CURLOPT_USERPWD, up);
    }
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_cb);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    if (curl_easy_perform(curl) != CURLE_OK) return false;
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    return code >= 200 && code < 400;
}

/* ── Value parsing ───────────────────────────────────────────── */

static char *json_value_to_str(JsonNode *node, size_t *len)
{
    GType vt = json_node_get_value_type(node);
    char *out = NULL;
    if (vt == G_TYPE_STRING) {
        const char *s = json_node_get_string(node);
        out = strdup(s ? s : ""); *len = strlen(out);
    } else if (vt == G_TYPE_INT64) {
        out = malloc(24);
        if (out) *len = (size_t)snprintf(out, 24, "%lld",
                                         (long long)json_node_get_int(node));
    } else if (vt == G_TYPE_DOUBLE) {
        out = malloc(32);
        if (out) *len = argus_dtoa(out, 32, 15, json_node_get_double(node));
    } else if (vt == G_TYPE_BOOLEAN) {
        gboolean v = json_node_get_boolean(node);
        out = strdup(v ? "true" : "false"); *len = v ? 4 : 5;
    } else {
        JsonGenerator *g = json_generator_new();
        json_generator_set_root(g, node);
        out = json_generator_to_data(g, len);
        g_object_unref(g);
    }
    return out;
}

/* rows[0]=column names, rows[1]=SQL types, rows[2..]=data */
static void parse_result(druid_op_t *op, JsonArray *rows)
{
    int total = (int)json_array_get_length(rows);
    if (total < 1) return;
    JsonArray *names = json_array_get_array_element(rows, 0);
    JsonArray *types = total >= 2 ? json_array_get_array_element(rows, 1) : NULL;
    int ncols = names ? (int)json_array_get_length(names) : 0;
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;
    op->num_cols = ncols;
    op->columns = calloc((size_t)(ncols > 0 ? ncols : 1),
                         sizeof(argus_column_desc_t));
    if (!op->columns) { op->num_cols = 0; return; }
    for (int c = 0; c < ncols; c++) {
        const char *nm = json_array_get_string_element(names, (guint)c);
        strncpy((char *)op->columns[c].name, nm ? nm : "",
                ARGUS_MAX_COLUMN_NAME - 1);
        op->columns[c].name_len = (SQLSMALLINT)strlen((char *)op->columns[c].name);
        const char *ty = types ? json_array_get_string_element(types, (guint)c)
                               : "VARCHAR";
        op->columns[c].sql_type = druid_type_to_sql_type(ty);
        op->columns[c].column_size = druid_type_column_size(op->columns[c].sql_type);
        op->columns[c].decimal_digits =
            druid_type_decimal_digits(op->columns[c].sql_type);
        op->columns[c].nullable = SQL_NULLABLE;
    }

    int ndata = total - 2;   /* data rows start after the 2 header rows */
    if (ndata <= 0) return;
    op->cache.rows = calloc((size_t)ndata, sizeof(argus_row_t));
    if (!op->cache.rows) return;
    op->cache.capacity = (size_t)ndata;
    op->cache.num_cols = ncols;
    size_t r = 0;
    for (int i = 2; i < total; i++) {
        JsonArray *row = json_array_get_array_element(rows, (guint)i);
        if (!row) continue;
        op->cache.rows[r].cells = calloc((size_t)ncols, sizeof(argus_cell_t));
        if (!op->cache.rows[r].cells) break;
        int rl = (int)json_array_get_length(row);
        for (int c = 0; c < ncols && c < rl; c++) {
            argus_cell_t *cell = &op->cache.rows[r].cells[c];
            JsonNode *v = json_array_get_element(row, (guint)c);
            if (!v || json_node_is_null(v)) { cell->is_null = true; continue; }
            /* Keep JSON numbers as native typed values (no text round-trip);
             * strings and everything else stay as text cells. */
            GType vt = json_node_get_value_type(v);
            if (vt == G_TYPE_INT64) {
                cell->native_kind = ARGUS_NATIVE_I64;
                cell->native.i64 = json_node_get_int(v);
            } else if (vt == G_TYPE_DOUBLE) {
                cell->native_kind = ARGUS_NATIVE_F64;
                cell->native.f64 = json_node_get_double(v);
            } else {
                cell->data = json_value_to_str(v, &cell->data_len);
                cell->is_null = (cell->data == NULL);
            }
        }
        r++;
    }
    op->cache.num_rows = r;
}

static druid_op_t *op_new(void)
{
    druid_op_t *op = calloc(1, sizeof(*op));
    if (op) argus_row_cache_init(&op->cache);
    return op;
}

/* ── Execute ─────────────────────────────────────────────────── */

int druid_execute(argus_backend_conn_t raw, const char *query,
                  argus_backend_op_t *out_op)
{
    druid_conn_t *conn = (druid_conn_t *)raw;
    if (!conn || !query || !out_op) return -1;
    conn->last_error[0] = '\0';

    char url[512];
    snprintf(url, sizeof(url), "%s/druid/v2/sql", conn->base_url);
    char *body = build_query_body(query);
    druid_response_t resp = {0};
    int rc = http_post(conn, url, body, &resp);
    g_free(body);
    if (rc != 0 || !resp.data) { free(resp.data); return -1; }

    JsonParser *parser = json_parser_new();
    if (!json_parser_load_from_data(parser, resp.data, -1, NULL)) {
        g_object_unref(parser); free(resp.data); return -1;
    }
    JsonNode *root = json_parser_get_root(parser);

    /* Error: HTTP >= 400 and/or an error object instead of the result array. */
    if (resp.http_code >= 400 || !JSON_NODE_HOLDS_ARRAY(root)) {
        if (root && JSON_NODE_HOLDS_OBJECT(root)) {
            JsonObject *o = json_node_get_object(root);
            const char *m = json_object_has_member(o, "errorMessage")
                ? json_object_get_string_member(o, "errorMessage")
                : (json_object_has_member(o, "error")
                   ? json_object_get_string_member(o, "error") : NULL);
            if (m) {
                strncpy(conn->last_error, m, sizeof(conn->last_error) - 1);
                conn->last_error[sizeof(conn->last_error) - 1] = '\0';
            }
        }
        g_object_unref(parser); free(resp.data);
        return -1;
    }

    druid_op_t *op = op_new();
    if (!op) { g_object_unref(parser); free(resp.data); return -1; }
    parse_result(op, json_node_get_array(root));
    g_object_unref(parser);
    free(resp.data);
    *out_op = op;
    return 0;
}

static int druid_get_operation_status(argus_backend_conn_t conn,
                                      argus_backend_op_t op, bool *finished)
{
    (void)conn; (void)op;
    if (finished) *finished = true;
    return 0;
}

static void druid_close_operation(argus_backend_conn_t conn,
                                  argus_backend_op_t raw)
{
    (void)conn;
    druid_op_t *op = (druid_op_t *)raw;
    if (!op) return;
    argus_row_cache_free(&op->cache);
    free(op->columns);
    free(op);
}

/* Execution is synchronous over HTTP: by the time SQLCancel can reach this,
 * the query has already completed, and cancelling a finished operation is a
 * no-op success per ODBC. Mid-flight cancellation (Druid's
 * DELETE /druid/v2/{queryId}) would require issuing the query asynchronously
 * with a client-set sqlQueryId; not implemented. */
static int druid_cancel(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn; (void)op;
    return 0;
}

static int druid_get_result_metadata(argus_backend_conn_t rconn,
                                     argus_backend_op_t raw,
                                     argus_column_desc_t *columns, int *num_cols)
{
    (void)rconn;
    druid_op_t *op = (druid_op_t *)raw;
    if (!op || !columns || !num_cols) return -1;
    if (op->columns && op->num_cols > 0)
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
    *num_cols = op->num_cols;
    return 0;
}

static int druid_fetch_results(argus_backend_conn_t rconn, argus_backend_op_t raw,
                               int max_rows, argus_row_cache_t *cache,
                               argus_column_desc_t *columns, int *num_cols)
{
    (void)rconn; (void)max_rows;
    druid_op_t *op = (druid_op_t *)raw;
    if (!op || !cache) return -1;
    if (columns && num_cols && op->columns && op->num_cols > 0) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    }
    if (op->delivered) { cache->num_rows = 0; cache->exhausted = true; return 0; }
    cache->rows = op->cache.rows;
    cache->num_rows = op->cache.num_rows;
    cache->capacity = op->cache.capacity;
    cache->num_cols = op->cache.num_cols;
    cache->current_row = 0;
    cache->exhausted = true;
    op->cache.rows = NULL; op->cache.num_rows = 0; op->cache.capacity = 0;
    op->delivered = true;
    return 0;
}

static bool druid_get_last_error(argus_backend_conn_t raw, char *buf,
                                 size_t buflen)
{
    druid_conn_t *conn = (druid_conn_t *)raw;
    if (!conn || !conn->last_error[0] || buflen == 0) return false;
    strncpy(buf, conn->last_error, buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
}

/* ── Catalog operations via INFORMATION_SCHEMA ───────────────── */

/* Every ODBC search pattern reaches the query text through
 * argus_sql_quote_literal: the patterns come from the application. Druid SQL
 * (Calcite) string literals are ANSI — no backslash escapes. */

static bool append_filter(GString *q, const char *column, const char *op,
                          const char *value)
{
    if (!value || !*value) return true;
    char *lit = argus_sql_quote_literal(value, false);
    if (!lit) return false;
    g_string_append_printf(q, " AND %s %s %s", column, op, lit);
    free(lit);
    return true;
}

static char *finish(GString *q, bool ok, const char *order_by)
{
    if (!ok) { g_string_free(q, TRUE); return NULL; }
    g_string_append(q, order_by);
    return g_string_free(q, FALSE);
}

char *druid_build_tables_query(const char *catalog, const char *schema,
                               const char *table_name, const char *table_types)
{
    GString *q = g_string_new(
        "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, "
        "TABLE_NAME, TABLE_TYPE, CAST(NULL AS VARCHAR) AS REMARKS "
        "FROM INFORMATION_SCHEMA.TABLES WHERE 1=1");
    bool ok = append_filter(q, "TABLE_CATALOG", "=", catalog) &&
              append_filter(q, "TABLE_SCHEMA", "LIKE", schema) &&
              append_filter(q, "TABLE_NAME", "LIKE", table_name);
    /* ODBC "TABLE" vs SQL-standard "BASE TABLE": accept both. The list is
     * only ever inspected, never copied into the query. */
    if (ok && table_types && *table_types && strstr(table_types, "TABLE"))
        g_string_append(q, " AND TABLE_TYPE IN ('TABLE','BASE TABLE')");
    return finish(q, ok, " ORDER BY TABLE_SCHEMA, TABLE_NAME");
}

char *druid_build_columns_query(const char *catalog, const char *schema,
                                const char *table_name, const char *column_name)
{
    GString *q = g_string_new(
        "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, "
        "TABLE_NAME, COLUMN_NAME, "
        "CASE "
        "WHEN DATA_TYPE = 'BIGINT' THEN -5 "
        "WHEN DATA_TYPE = 'INTEGER' THEN 4 "
        "WHEN DATA_TYPE = 'SMALLINT' THEN 5 "
        "WHEN DATA_TYPE = 'TINYINT' THEN -6 "
        "WHEN DATA_TYPE = 'DOUBLE' THEN 8 "
        "WHEN DATA_TYPE IN ('FLOAT','REAL') THEN 7 "
        "WHEN DATA_TYPE = 'DECIMAL' THEN 3 "
        "WHEN DATA_TYPE = 'BOOLEAN' THEN -7 "
        "WHEN DATA_TYPE = 'DATE' THEN 91 "
        "WHEN DATA_TYPE = 'TIMESTAMP' THEN 93 "
        "ELSE 12 END AS DATA_TYPE, "
        "DATA_TYPE AS TYPE_NAME, "
        "ORDINAL_POSITION, IS_NULLABLE "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE 1=1");
    bool ok = append_filter(q, "TABLE_CATALOG", "=", catalog) &&
              append_filter(q, "TABLE_SCHEMA", "LIKE", schema) &&
              append_filter(q, "TABLE_NAME", "LIKE", table_name) &&
              append_filter(q, "COLUMN_NAME", "LIKE", column_name);
    return finish(q, ok, " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");
}

char *druid_build_schemas_query(const char *catalog, const char *schema)
{
    GString *q = g_string_new(
        "SELECT SCHEMA_NAME AS TABLE_SCHEM, CATALOG_NAME AS TABLE_CATALOG "
        "FROM INFORMATION_SCHEMA.SCHEMATA WHERE 1=1");
    bool ok = append_filter(q, "CATALOG_NAME", "=", catalog) &&
              append_filter(q, "SCHEMA_NAME", "LIKE", schema);
    return finish(q, ok, " ORDER BY SCHEMA_NAME");
}

static int run_built_query(argus_backend_conn_t conn, char *query,
                           argus_backend_op_t *out_op)
{
    if (!query) return -1;
    int rc = druid_execute(conn, query, out_op);
    g_free(query);
    return rc;
}

static int druid_get_tables(argus_backend_conn_t conn, const char *catalog,
                            const char *schema, const char *table_name,
                            const char *table_types, argus_backend_op_t *out_op)
{
    return run_built_query(conn,
                           druid_build_tables_query(catalog, schema,
                                                    table_name, table_types),
                           out_op);
}

static int druid_get_columns(argus_backend_conn_t conn, const char *catalog,
                             const char *schema, const char *table_name,
                             const char *column_name, argus_backend_op_t *out_op)
{
    return run_built_query(conn,
                           druid_build_columns_query(catalog, schema,
                                                     table_name, column_name),
                           out_op);
}

static int druid_get_schemas(argus_backend_conn_t conn, const char *catalog,
                             const char *schema, argus_backend_op_t *out_op)
{
    return run_built_query(conn,
                           druid_build_schemas_query(catalog, schema),
                           out_op);
}

/* ── Backend vtable ──────────────────────────────────────────── */

static const argus_backend_t druid_backend = {
    .name                  = "druid",
    .connect               = druid_connect,
    .disconnect            = druid_disconnect,
    .is_alive              = druid_is_alive,
    .execute               = druid_execute,
    .get_operation_status  = druid_get_operation_status,
    .close_operation       = druid_close_operation,
    .cancel                = druid_cancel,
    .fetch_results         = druid_fetch_results,
    .get_result_metadata   = druid_get_result_metadata,
    .get_tables            = druid_get_tables,
    .get_columns           = druid_get_columns,
    .get_type_info         = NULL,
    .get_schemas           = druid_get_schemas,
    .get_catalogs          = NULL,
    .get_primary_keys      = NULL,
    .get_statistics        = NULL,
    .get_last_error        = druid_get_last_error,
};

const argus_backend_t *argus_druid_backend_get(void)
{
    return &druid_backend;
}
