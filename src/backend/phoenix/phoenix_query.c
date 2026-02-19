#include "phoenix_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Create/free operation handles ────────────────────────────── */

phoenix_operation_t *phoenix_operation_new(void)
{
    phoenix_operation_t *op = calloc(1, sizeof(phoenix_operation_t));
    return op;
}

void phoenix_operation_free(phoenix_operation_t *op)
{
    if (!op) return;
    free(op->connection_id);
    free(op->columns);
    free(op);
}

/* ── Execute a statement via Avatica prepareAndExecute ────────── */

int phoenix_execute(argus_backend_conn_t raw_conn,
                    const char *query,
                    argus_backend_op_t *out_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn || !query) return -1;

    int stmt_id = conn->next_statement_id++;

    /* Build prepareAndExecute request */
    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, conn->connection_id);
    json_builder_set_member_name(params, "statementId");
    json_builder_add_int_value(params, stmt_id);
    json_builder_set_member_name(params, "sql");
    json_builder_add_string_value(params, query);
    json_builder_set_member_name(params, "maxRowCount");
    json_builder_add_int_value(params, -1);  /* unlimited */
    json_builder_end_object(params);

    JsonParser *parser = NULL;
    int rc = phoenix_avatica_request(conn, "prepareAndExecute",
                                     params, &parser);
    g_object_unref(params);

    if (rc != 0) {
        if (parser) g_object_unref(parser);
        return -1;
    }

    /* Parse response */
    phoenix_operation_t *op = phoenix_operation_new();
    if (!op) {
        g_object_unref(parser);
        return -1;
    }

    op->statement_id = stmt_id;
    op->connection_id = strdup(conn->connection_id);
    op->offset = 0;

    /* Extract result metadata from response */
    JsonNode *root = json_parser_get_root(parser);
    JsonObject *resp_obj = json_node_get_object(root);

    /* Avatica returns results array; take the first result */
    if (json_object_has_member(resp_obj, "results")) {
        JsonArray *results = json_object_get_array_member(resp_obj, "results");
        if (results && json_array_get_length(results) > 0) {
            JsonObject *result = json_array_get_object_element(results, 0);
            op->has_result_set = true;

            /* Parse column metadata from signature */
            if (json_object_has_member(result, "signature")) {
                JsonObject *sig = json_object_get_object_member(result,
                                                                 "signature");
                op->columns = calloc(ARGUS_MAX_COLUMNS,
                                     sizeof(argus_column_desc_t));
                if (op->columns) {
                    phoenix_parse_columns(sig, op->columns, &op->num_cols);
                    op->metadata_fetched = true;
                }
            }

            /* Parse initial frame data */
            if (json_object_has_member(result, "firstFrame")) {
                JsonObject *frame = json_object_get_object_member(result,
                                                                    "firstFrame");
                if (frame && json_object_has_member(frame, "done")) {
                    op->finished = json_object_get_boolean_member(frame,
                                                                    "done");
                }
                if (frame && json_object_has_member(frame, "offset")) {
                    op->offset = (int)json_object_get_int_member(frame,
                                                                   "offset");
                }
            }
        }
    }

    g_object_unref(parser);

    *out_op = op;
    return 0;
}

/* ── Get operation status ─────────────────────────────────────── */

int phoenix_get_operation_status(argus_backend_conn_t raw_conn,
                                  argus_backend_op_t raw_op,
                                  bool *finished)
{
    (void)raw_conn;
    phoenix_operation_t *op = (phoenix_operation_t *)raw_op;
    if (!op) return -1;

    *finished = op->finished;
    return 0;
}

/* ── Cancel a running operation (closeStatement) ─────────────── */

int phoenix_cancel(argus_backend_conn_t raw_conn,
                   argus_backend_op_t raw_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    phoenix_operation_t *op = (phoenix_operation_t *)raw_op;
    if (!conn || !op) return -1;

    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    json_builder_add_string_value(params, op->connection_id);
    json_builder_set_member_name(params, "statementId");
    json_builder_add_int_value(params, op->statement_id);
    json_builder_end_object(params);

    JsonParser *parser = NULL;
    int rc = phoenix_avatica_request(conn, "closeStatement", params, &parser);
    g_object_unref(params);
    if (parser) g_object_unref(parser);

    if (rc == 0) {
        op->finished = true;
    }
    return rc;
}

/* ── Close an operation ───────────────────────────────────────── */

void phoenix_close_operation(argus_backend_conn_t raw_conn,
                              argus_backend_op_t raw_op)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    phoenix_operation_t *op = (phoenix_operation_t *)raw_op;
    if (!conn || !op) return;

    if (!op->finished) {
        phoenix_cancel(raw_conn, raw_op);
    }

    phoenix_operation_free(op);
}
