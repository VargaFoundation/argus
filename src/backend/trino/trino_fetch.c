#include "trino_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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
                const char *s = json_node_get_string(val_node);
                cell->data = strdup(s ? s : "");
                cell->data_len = strlen(cell->data);
            } else if (vtype == G_TYPE_INT64) {
                gint64 v = json_node_get_int(val_node);
                cell->data = malloc(24);
                if (cell->data)
                    cell->data_len = (size_t)snprintf(cell->data, 24, "%ld", (long)v);
            } else if (vtype == G_TYPE_DOUBLE) {
                gdouble v = json_node_get_double(val_node);
                cell->data = malloc(32);
                if (cell->data)
                    cell->data_len = (size_t)snprintf(cell->data, 32, "%.15g", v);
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
            trino_parse_data(data_node, cache,
                             op->num_cols > 0 ? op->num_cols : 1);

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

        /* Parse column metadata */
        if (json_object_has_member(obj, "columns")) {
            JsonNode *columns_node = json_object_get_member(obj, "columns");
            op->columns = calloc(ARGUS_MAX_COLUMNS, sizeof(argus_column_desc_t));
            if (op->columns) {
                trino_parse_columns(columns_node, op->columns, &op->num_cols);
                op->metadata_fetched = true;
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
