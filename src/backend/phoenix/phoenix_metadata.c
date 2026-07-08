#include "phoenix_internal.h"
#include "argus/compat.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/*
 * Phoenix Query Server exposes Avatica catalog RPCs:
 * getTables, getColumns, getSchemas, getCatalogs, getTypeInfo.
 * These map directly to the ODBC catalog functions.
 */

/* Keep only the first n columns of a catalog result. Avatica's getTables
 * returns the full JDBC shape (TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
 * TABLE_TYPE, REMARKS, then extra JDBC columns) — 19 columns for PQS 5.0 —
 * but ODBC's SQLTables must return exactly 5, in that order. Passing the
 * wider result through crashes strict ODBC clients (.NET). The JDBC and
 * ODBC leading columns match one-to-one, so projecting to the first n is
 * correct. Frees the dropped cells to avoid a leak. */
static void phoenix_project_columns(phoenix_operation_t *op, int n)
{
    if (!op || op->num_cols <= n) return;

    argus_row_cache_t *fc = &op->first_frame;
    for (size_t r = 0; r < fc->num_rows; r++) {
        argus_cell_t *cells = fc->rows[r].cells;
        if (!cells) continue;
        for (int c = n; c < op->num_cols; c++) {
            free(cells[c].data);
            cells[c].data = NULL;
        }
    }
    fc->num_cols = n;
    op->num_cols = n;
}

/* ── Helper: build a catalog operation result ────────────────── */

static int phoenix_catalog_request(phoenix_conn_t *conn,
                                    const char *rpc_name,
                                    JsonBuilder *params,
                                    phoenix_operation_t **out_op)
{
    JsonParser *parser = NULL;
    int rc = phoenix_avatica_request(conn, rpc_name, params, &parser);
    if (rc != 0) {
        if (parser) g_object_unref(parser);
        return -1;
    }

    phoenix_operation_t *op = phoenix_operation_new();
    if (!op) {
        g_object_unref(parser);
        return -1;
    }

    op->statement_id = conn->next_statement_id++;
    op->connection_id = strdup(conn->connection_id);
    op->has_result_set = true;

    /* Parse the result set from the response */
    JsonNode *root = json_parser_get_root(parser);
    JsonObject *resp_obj = json_node_get_object(root);

    /* Avatica catalog responses have signature + firstFrame */
    if (json_object_has_member(resp_obj, "signature")) {
        JsonObject *sig = json_object_get_object_member(resp_obj, "signature");
        op->columns = calloc(ARGUS_MAX_COLUMNS, sizeof(argus_column_desc_t));
        if (op->columns) {
            phoenix_parse_columns(sig, op->columns, &op->num_cols);
            op->metadata_fetched = true;
        }
    }

    if (json_object_has_member(resp_obj, "firstFrame")) {
        JsonObject *frame = json_object_get_object_member(resp_obj,
                                                            "firstFrame");
        if (frame && json_object_has_member(frame, "done")) {
            op->finished = json_object_get_boolean_member(frame, "done");
        }
        if (frame && json_object_has_member(frame, "offset")) {
            op->offset = (int)json_object_get_int_member(frame, "offset");
        }
        /* Stash the inline rows so the ODBC fetch delivers them (catalog
         * responses return their rows in the first frame too). */
        if (frame && op->num_cols > 0) {
            phoenix_parse_frame(frame, &op->first_frame, op->num_cols);
            op->offset += (int)op->first_frame.num_rows;
            op->first_frame_ready = true;
        }
    } else {
        op->finished = true;
    }

    g_object_unref(parser);

    /* No columns means the RPC returned an empty/unsupported result (PQS 5.0
     * getTypeInfo) or an Avatica error body (getPrimaryKeys rejects our
     * field). Fail rather than hand the ODBC layer a malformed 0-column
     * result set — SQLGetTypeInfo then falls back to the built-in type list,
     * and other functions surface a clean error instead of crashing clients. */
    if (op->num_cols <= 0) {
        phoenix_operation_free(op);
        return -1;
    }

    *out_op = op;
    return 0;
}

/* ── GetTables via Avatica getTables ─────────────────────────── */

int phoenix_get_tables(argus_backend_conn_t raw_conn,
                       const char *catalog,
                       const char *schema,
                       const char *table_name,
                       const char *table_types,
                       argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn) return -1;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);

    if (catalog && *catalog) {
        json_builder_set_member_name(params, "catalog");
        json_builder_add_string_value(params, catalog);
    }
    if (schema && *schema) {
        json_builder_set_member_name(params, "schemaPattern");
        json_builder_add_string_value(params, schema);
    }
    if (table_name && *table_name) {
        json_builder_set_member_name(params, "tableNamePattern");
        json_builder_add_string_value(params, table_name);
    }
    if (table_types && *table_types) {
        json_builder_set_member_name(params, "typeList");
        json_builder_begin_array(params);
        json_builder_add_string_value(params, table_types);
        json_builder_end_array(params);
    }
    json_builder_end_object(params);

    phoenix_operation_t *op = NULL;
    int rc = phoenix_catalog_request(conn, "getTables", params, &op);
    g_object_unref(params);

    if (rc != 0) return -1;

    /* ODBC SQLTables must expose exactly 5 columns. */
    phoenix_project_columns(op, 5);

    *out_op = op;
    return 0;
}

/* ── GetColumns via Avatica getColumns ───────────────────────── */

int phoenix_get_columns(argus_backend_conn_t raw_conn,
                        const char *catalog,
                        const char *schema,
                        const char *table_name,
                        const char *column_name,
                        argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn) return -1;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);

    if (catalog && *catalog) {
        json_builder_set_member_name(params, "catalog");
        json_builder_add_string_value(params, catalog);
    }
    if (schema && *schema) {
        json_builder_set_member_name(params, "schemaPattern");
        json_builder_add_string_value(params, schema);
    }
    if (table_name && *table_name) {
        json_builder_set_member_name(params, "tableNamePattern");
        json_builder_add_string_value(params, table_name);
    }
    if (column_name && *column_name) {
        json_builder_set_member_name(params, "columnNamePattern");
        json_builder_add_string_value(params, column_name);
    }
    json_builder_end_object(params);

    phoenix_operation_t *op = NULL;
    int rc = phoenix_catalog_request(conn, "getColumns", params, &op);
    g_object_unref(params);

    if (rc != 0) return -1;

    /* ODBC SQLColumns must expose exactly 18 columns; Avatica's getColumns
     * returns the wider JDBC shape (29 for PQS 5.0) whose leading 18 match
     * ODBC one-to-one. The extra columns crash strict clients (.NET's
     * OdbcMetaDataFactory null-refs when enumerating table metadata). */
    phoenix_project_columns(op, 18);

    *out_op = op;
    return 0;
}

/* ── GetTypeInfo via Avatica getTypeInfo ──────────────────────── */

int phoenix_get_type_info(argus_backend_conn_t raw_conn,
                          SQLSMALLINT sql_type,
                          argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn) return -1;
    (void)sql_type;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);
    json_builder_end_object(params);

    phoenix_operation_t *op = NULL;
    int rc = phoenix_catalog_request(conn, "getTypeInfo", params, &op);
    g_object_unref(params);

    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetSchemas via Avatica getSchemas ───────────────────────── */

int phoenix_get_schemas(argus_backend_conn_t raw_conn,
                        const char *catalog,
                        const char *schema,
                        argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn) return -1;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);

    if (catalog && *catalog) {
        json_builder_set_member_name(params, "catalog");
        json_builder_add_string_value(params, catalog);
    }
    if (schema && *schema) {
        json_builder_set_member_name(params, "schemaPattern");
        json_builder_add_string_value(params, schema);
    }
    json_builder_end_object(params);

    phoenix_operation_t *op = NULL;
    int rc = phoenix_catalog_request(conn, "getSchemas", params, &op);
    g_object_unref(params);

    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetPrimaryKeys via Avatica getPrimaryKeys ───────────────── */

int phoenix_get_primary_keys(argus_backend_conn_t raw_conn,
                              const char *catalog,
                              const char *schema,
                              const char *table_name,
                              argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn || !table_name || !*table_name) return -1;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);

    if (catalog && *catalog) {
        json_builder_set_member_name(params, "catalog");
        json_builder_add_string_value(params, catalog);
    }
    if (schema && *schema) {
        json_builder_set_member_name(params, "schemaPattern");
        json_builder_add_string_value(params, schema);
    }
    json_builder_set_member_name(params, "table");
    json_builder_add_string_value(params, table_name);
    json_builder_end_object(params);

    phoenix_operation_t *op = NULL;
    int rc = phoenix_catalog_request(conn, "getPrimaryKeys", params, &op);
    g_object_unref(params);

    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetCatalogs via Avatica getCatalogs ──────────────────────── */

int phoenix_get_catalogs(argus_backend_conn_t raw_conn,
                         argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn) return -1;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);
    json_builder_end_object(params);

    phoenix_operation_t *op = NULL;
    int rc = phoenix_catalog_request(conn, "getCatalogs", params, &op);
    g_object_unref(params);

    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}
