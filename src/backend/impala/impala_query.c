#include "impala_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Create/free operation handles ────────────────────────────── */

impala_operation_t *impala_operation_new(void)
{
    impala_operation_t *op = calloc(1, sizeof(impala_operation_t));
    return op;
}

void impala_operation_free(impala_operation_t *op)
{
    if (!op) return;
    if (op->op_handle) g_object_unref(op->op_handle);
    free(op->columns);
    free(op);
}

/* ── Execute a statement via TCLIService ─────────────────────── */

int impala_execute(argus_backend_conn_t raw_conn,
                   const char *query,
                   argus_backend_op_t *out_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    if (!conn || !conn->client || !query) return -1;

    GError *error = NULL;

    TExecuteStatementReq *req = g_object_new(
        TYPE_T_EXECUTE_STATEMENT_REQ, NULL);
    g_object_set(req,
                 "sessionHandle", conn->session_handle,
                 "statement", query,
                 "runAsync", FALSE,
                 NULL);

    TExecuteStatementResp *resp = g_object_new(
        TYPE_T_EXECUTE_STATEMENT_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_execute_statement(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

    /* Check status */
    TStatus *status = NULL;
    g_object_get(resp, "status", &status, NULL);
    if (status) {
        TStatusCode status_code;
        g_object_get(status, "statusCode", &status_code, NULL);
        if (status_code == T_STATUS_CODE_ERROR_STATUS) {
            g_object_unref(status);
            g_object_unref(req);
            g_object_unref(resp);
            return -1;
        }
        g_object_unref(status);
    }

    /* Extract operation handle */
    impala_operation_t *op = impala_operation_new();
    if (!op) {
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_get(resp, "operationHandle", &op->op_handle, NULL);
    op->has_result_set = (op->op_handle != NULL);

    g_object_unref(req);
    g_object_unref(resp);

    *out_op = op;
    return 0;
}

/* ── Get operation status ─────────────────────────────────────── */

int impala_get_operation_status(argus_backend_conn_t raw_conn,
                                 argus_backend_op_t raw_op,
                                 bool *finished)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    impala_operation_t *op = (impala_operation_t *)raw_op;
    if (!conn || !op || !op->op_handle) return -1;

    GError *error = NULL;

    TGetOperationStatusReq *req = g_object_new(
        TYPE_T_GET_OPERATION_STATUS_REQ, NULL);
    g_object_set(req, "operationHandle", op->op_handle, NULL);

    TGetOperationStatusResp *resp = g_object_new(
        TYPE_T_GET_OPERATION_STATUS_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_operation_status(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

    TOperationState op_state;
    g_object_get(resp, "operationState", &op_state, NULL);

    *finished = (op_state == T_OPERATION_STATE_FINISHED_STATE ||
                 op_state == T_OPERATION_STATE_ERROR_STATE ||
                 op_state == T_OPERATION_STATE_CANCELED_STATE ||
                 op_state == T_OPERATION_STATE_CLOSED_STATE);

    g_object_unref(req);
    g_object_unref(resp);
    return 0;
}

/* ── Cancel a running operation ──────────────────────────────── */

int impala_cancel(argus_backend_conn_t raw_conn,
                  argus_backend_op_t raw_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    impala_operation_t *op = (impala_operation_t *)raw_op;
    if (!conn || !op || !op->op_handle) return -1;

    GError *error = NULL;

    TCancelOperationReq *req = g_object_new(
        TYPE_T_CANCEL_OPERATION_REQ, NULL);
    g_object_set(req, "operationHandle", op->op_handle, NULL);

    TCancelOperationResp *resp = g_object_new(
        TYPE_T_CANCEL_OPERATION_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_cancel_operation(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

    g_object_unref(req);
    g_object_unref(resp);
    return 0;
}

/* ── Close an operation ───────────────────────────────────────── */

void impala_close_operation(argus_backend_conn_t raw_conn,
                             argus_backend_op_t raw_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    impala_operation_t *op = (impala_operation_t *)raw_op;
    if (!conn || !op) return;

    if (op->op_handle) {
        GError *error = NULL;

        TCloseOperationReq *req = g_object_new(
            TYPE_T_CLOSE_OPERATION_REQ, NULL);
        g_object_set(req, "operationHandle", op->op_handle, NULL);

        TCloseOperationResp *resp = g_object_new(
            TYPE_T_CLOSE_OPERATION_RESP, NULL);

        t_c_l_i_service_client_close_operation(
            conn->client, &resp, req, &error);

        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
    }

    impala_operation_free(op);
}
