#include "trino_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Create/free operation handles ────────────────────────────── */

trino_operation_t *trino_operation_new(void)
{
    trino_operation_t *op = calloc(1, sizeof(trino_operation_t));
    return op;
}

void trino_operation_free(trino_operation_t *op)
{
    if (!op) return;
    free(op->query_id);
    free(op->next_uri);
    free(op->columns);
    free(op);
}

/* ── Execute a statement via Trino REST API ──────────────────── */

int trino_execute(argus_backend_conn_t raw_conn,
                  const char *query,
                  argus_backend_op_t *out_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn || !query) return -1;

    /* POST /v1/statement with SQL as body */
    char stmt_url[1024];
    snprintf(stmt_url, sizeof(stmt_url), "%s/v1/statement", conn->base_url);

    trino_response_t resp = {0};
    if (trino_http_post(conn, stmt_url, query, &resp) != 0) {
        free(resp.data);
        return -1;
    }

    if (!resp.data) return -1;

    /* Parse response JSON */
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

    trino_operation_t *op = trino_operation_new();
    if (!op) {
        g_object_unref(parser);
        free(resp.data);
        return -1;
    }

    /* Extract query ID */
    if (json_object_has_member(obj, "id")) {
        op->query_id = strdup(json_object_get_string_member(obj, "id"));
    }

    /* Extract nextUri for polling */
    if (json_object_has_member(obj, "nextUri")) {
        op->next_uri = strdup(json_object_get_string_member(obj, "nextUri"));
    }

    /* Parse column metadata if present in first response */
    if (json_object_has_member(obj, "columns")) {
        JsonNode *columns_node = json_object_get_member(obj, "columns");
        op->columns = calloc(ARGUS_MAX_COLUMNS, sizeof(argus_column_desc_t));
        if (op->columns) {
            trino_parse_columns(columns_node, op->columns, &op->num_cols);
            op->metadata_fetched = true;
        }
    }

    op->has_result_set = true;
    op->finished = (op->next_uri == NULL);

    g_object_unref(parser);
    free(resp.data);

    *out_op = op;
    return 0;
}

/* ── Get operation status ─────────────────────────────────────── */

int trino_get_operation_status(argus_backend_conn_t raw_conn,
                                argus_backend_op_t raw_op,
                                bool *finished)
{
    (void)raw_conn;
    trino_operation_t *op = (trino_operation_t *)raw_op;
    if (!op) return -1;

    *finished = op->finished || (op->next_uri == NULL);
    return 0;
}

/* ── Close an operation ───────────────────────────────────────── */

void trino_close_operation(argus_backend_conn_t raw_conn,
                            argus_backend_op_t raw_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    trino_operation_t *op = (trino_operation_t *)raw_op;
    if (!conn || !op) return;

    /* Cancel the query if it's still running */
    if (op->query_id && !op->finished) {
        char cancel_url[1024];
        snprintf(cancel_url, sizeof(cancel_url),
                 "%s/v1/query/%s", conn->base_url, op->query_id);
        trino_http_delete(conn, cancel_url);
    }

    trino_operation_free(op);
}
