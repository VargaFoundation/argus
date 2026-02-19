#include "phoenix_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Forward declaration */
int phoenix_get_result_metadata(argus_backend_conn_t raw_conn,
                                 argus_backend_op_t raw_op,
                                 argus_column_desc_t *columns,
                                 int *num_cols);

/* ── Parse column metadata from Avatica signature ────────────── */

int phoenix_parse_columns(JsonObject *signature,
                          argus_column_desc_t *columns,
                          int *num_cols)
{
    if (!signature || !columns || !num_cols) return -1;

    if (!json_object_has_member(signature, "columns")) return -1;

    JsonArray *cols_arr = json_object_get_array_member(signature, "columns");
    if (!cols_arr) return -1;

    int ncols = (int)json_array_get_length(cols_arr);
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;

    for (int i = 0; i < ncols; i++) {
        JsonObject *col_obj = json_array_get_object_element(cols_arr,
                                                             (guint)i);
        if (!col_obj) continue;

        argus_column_desc_t *col = &columns[i];
        memset(col, 0, sizeof(*col));

        /* Column label (display name) */
        if (json_object_has_member(col_obj, "columnName")) {
            const char *name = json_object_get_string_member(col_obj,
                                                              "columnName");
            if (name) {
                strncpy((char *)col->name, name, ARGUS_MAX_COLUMN_NAME - 1);
                col->name_len = (SQLSMALLINT)strlen(name);
            }
        } else if (json_object_has_member(col_obj, "label")) {
            const char *name = json_object_get_string_member(col_obj, "label");
            if (name) {
                strncpy((char *)col->name, name, ARGUS_MAX_COLUMN_NAME - 1);
                col->name_len = (SQLSMALLINT)strlen(name);
            }
        }

        /* Type name from Avatica ColumnMetaData */
        const char *type_name = "VARCHAR";
        if (json_object_has_member(col_obj, "type")) {
            JsonObject *type_obj = json_object_get_object_member(col_obj,
                                                                   "type");
            if (type_obj && json_object_has_member(type_obj, "name")) {
                type_name = json_object_get_string_member(type_obj, "name");
            }
        }

        col->sql_type       = phoenix_type_to_sql_type(type_name);
        col->column_size    = phoenix_type_column_size(col->sql_type);
        col->decimal_digits = phoenix_type_decimal_digits(col->sql_type);
        col->nullable       = SQL_NULLABLE_UNKNOWN;

        /* Override with explicit precision if available */
        if (json_object_has_member(col_obj, "precision")) {
            gint64 prec = json_object_get_int_member(col_obj, "precision");
            if (prec > 0) col->column_size = (SQLULEN)prec;
        }
        if (json_object_has_member(col_obj, "scale")) {
            gint64 scale = json_object_get_int_member(col_obj, "scale");
            if (scale >= 0) col->decimal_digits = (SQLSMALLINT)scale;
        }
        if (json_object_has_member(col_obj, "nullable")) {
            gint64 nullable = json_object_get_int_member(col_obj, "nullable");
            if (nullable == 0) col->nullable = SQL_NO_NULLS;
            else if (nullable == 1) col->nullable = SQL_NULLABLE;
        }
    }

    *num_cols = ncols;
    return 0;
}

/* ── Parse data rows from Avatica frame into row cache ───────── */

int phoenix_parse_frame(JsonObject *frame,
                        argus_row_cache_t *cache,
                        int num_cols)
{
    if (!frame || !cache) return -1;

    if (!json_object_has_member(frame, "rows")) {
        cache->num_rows = 0;
        return 0;
    }

    JsonArray *rows_arr = json_object_get_array_member(frame, "rows");
    if (!rows_arr) {
        cache->num_rows = 0;
        return 0;
    }

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
                const char *s = json_node_get_string(val_node);
                cell->data = strdup(s ? s : "");
                cell->data_len = strlen(cell->data);
            } else if (vtype == G_TYPE_INT64) {
                gint64 v = json_node_get_int(val_node);
                cell->data = malloc(24);
                if (cell->data)
                    cell->data_len = (size_t)snprintf(cell->data, 24,
                                                       "%lld", (long long)v);
            } else if (vtype == G_TYPE_DOUBLE) {
                gdouble v = json_node_get_double(val_node);
                cell->data = malloc(32);
                if (cell->data)
                    cell->data_len = (size_t)snprintf(cell->data, 32,
                                                       "%.15g", v);
            } else if (vtype == G_TYPE_BOOLEAN) {
                gboolean v = json_node_get_boolean(val_node);
                cell->data = strdup(v ? "true" : "false");
                cell->data_len = v ? 4 : 5;
            } else {
                /* Complex types: serialize to JSON string */
                JsonGenerator *gen = json_generator_new();
                json_generator_set_root(gen, val_node);
                cell->data = json_generator_to_data(gen, &cell->data_len);
                g_object_unref(gen);
            }
        }
    }

    return 0;
}

/* ── FetchResults via Avatica fetch RPC ──────────────────────── */

int phoenix_fetch_results(argus_backend_conn_t raw_conn,
                          argus_backend_op_t raw_op,
                          int max_rows,
                          argus_row_cache_t *cache,
                          argus_column_desc_t *columns,
                          int *num_cols)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    phoenix_operation_t *op = (phoenix_operation_t *)raw_op;
    if (!conn || !op) return -1;

    /* Return metadata if available */
    if (op->metadata_fetched && columns && num_cols) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    } else if (!op->metadata_fetched && columns && num_cols) {
        phoenix_get_result_metadata(raw_conn, raw_op, columns, num_cols);
    }

    /* No more data to fetch */
    if (op->finished) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    /* Build Avatica fetch request */
    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, op->connection_id);
    json_builder_set_member_name(params, "statementId");
    json_builder_add_int_value(params, op->statement_id);
    json_builder_set_member_name(params, "offset");
    json_builder_add_int_value(params, op->offset);
    json_builder_set_member_name(params, "fetchMaxRowCount");
    json_builder_add_int_value(params, max_rows > 0 ? max_rows : 1000);
    json_builder_end_object(params);

    JsonParser *parser = NULL;
    int rc = phoenix_avatica_request(conn, "fetch", params, &parser);
    g_object_unref(params);

    if (rc != 0) {
        if (parser) g_object_unref(parser);
        return -1;
    }

    /* Parse frame from response */
    JsonNode *root = json_parser_get_root(parser);
    JsonObject *resp_obj = json_node_get_object(root);

    if (json_object_has_member(resp_obj, "frame")) {
        JsonObject *frame = json_object_get_object_member(resp_obj, "frame");
        int ncols = op->num_cols > 0 ? op->num_cols : 1;
        phoenix_parse_frame(frame, cache, ncols);

        /* Update offset and done status */
        if (json_object_has_member(frame, "done")) {
            op->finished = json_object_get_boolean_member(frame, "done");
        }
        if (json_object_has_member(frame, "offset")) {
            op->offset = (int)json_object_get_int_member(frame, "offset");
        }
        op->offset += (int)cache->num_rows;

        if (op->finished)
            cache->exhausted = true;
    } else {
        cache->num_rows = 0;
        cache->exhausted = true;
        op->finished = true;
    }

    g_object_unref(parser);
    return 0;
}

/* ── Get result set metadata ──────────────────────────────────── */

int phoenix_get_result_metadata(argus_backend_conn_t raw_conn,
                                 argus_backend_op_t raw_op,
                                 argus_column_desc_t *columns,
                                 int *num_cols)
{
    (void)raw_conn;
    phoenix_operation_t *op = (phoenix_operation_t *)raw_op;
    if (!op) return -1;

    /* Return cached metadata if available */
    if (op->metadata_fetched && op->columns && op->num_cols > 0) {
        if (columns && num_cols) {
            memcpy(columns, op->columns,
                   (size_t)op->num_cols * sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        }
        return 0;
    }

    return -1;  /* Metadata not yet available */
}
