#include "hive_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "../hs2_fetch.h"
#include "../hs2_types.h"

/* Forward declaration */
int hive_get_result_metadata(argus_backend_conn_t raw_conn,
                              argus_backend_op_t raw_op,
                              argus_column_desc_t *columns,
                              int *num_cols);


/* ── FetchResults via TCLIService ────────────────────────────── */

int hive_fetch_results(argus_backend_conn_t raw_conn,
                       argus_backend_op_t raw_op,
                       int max_rows,
                       argus_row_cache_t *cache,
                       argus_column_desc_t *columns,
                       int *num_cols)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
    hive_operation_t *op = (hive_operation_t *)raw_op;
    if (!conn || !op || !op->op_handle) return -1;

    /* Fetch metadata if not yet done */
    if (!op->metadata_fetched && columns && num_cols) {
        hive_get_result_metadata(raw_conn, raw_op, columns, num_cols);
    }

    TFetchResultsReq *req = g_object_new(TYPE_T_FETCH_RESULTS_REQ, NULL);
    g_object_set(req,
                 "operationHandle", op->op_handle,
                 "orientation", T_FETCH_ORIENTATION_FETCH_NEXT,
                 "maxRows", (gint64)max_rows,
                 NULL);

    int rc = -1;
    for (;;) {
        GError *error = NULL;
        TFetchResultsResp *resp = g_object_new(TYPE_T_FETCH_RESULTS_RESP, NULL);

        gboolean ok = t_c_l_i_service_client_fetch_results(
            conn->client, &resp, req, &error);
        if (!ok || !resp) {
            g_snprintf(conn->last_error, sizeof(conn->last_error),
                       "FetchResults failed: %s",
                       error && error->message ? error->message
                                               : "transport error");
            if (error) g_error_free(error);
            if (resp) g_object_unref(resp);
            break;
        }

        TStatus *status = NULL;
        g_object_get(resp, "status", &status, NULL);
        bool status_ok = argus_hs2_status_ok(status, conn->last_error,
                                             sizeof(conn->last_error));
        if (status) g_object_unref(status);
        if (!status_ok) {
            g_object_unref(resp);
            break;
        }

        /* The generated struct exposes its fields; going through
         * g_object_get for the boxed `columns` array would hand back an
         * extra reference that nothing releases. */
        TRowSet *row_set = NULL;
        g_object_get(resp, "results", &row_set, NULL);
        rc = argus_hs2_rowset_to_cache(row_set ? row_set->columns : NULL,
                                       cache, num_cols);
        if (row_set) g_object_unref(row_set);

        gboolean more = resp->__isset_hasMoreRows && resp->hasMoreRows;
        g_object_unref(resp);

        /* An empty batch while the server still promises rows is not the
         * end of the result (see hs2_fetch.h): ask again. Every other
         * outcome, rows or a plain empty batch, goes back to the caller. */
        if (rc == 0 && cache->num_rows == 0 && more) continue;
        break;
    }

    g_object_unref(req);
    return rc;
}

/* ── Get result set metadata ──────────────────────────────────── */

int hive_get_result_metadata(argus_backend_conn_t raw_conn,
                              argus_backend_op_t raw_op,
                              argus_column_desc_t *columns,
                              int *num_cols)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
    hive_operation_t *op = (hive_operation_t *)raw_op;
    if (!conn || !op || !op->op_handle) return -1;

    /* Return cached metadata if available */
    if (op->metadata_fetched && op->columns && op->num_cols > 0) {
        if (columns && num_cols) {
            memcpy(columns, op->columns,
                   (size_t)op->num_cols * sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        }
        return 0;
    }

    GError *error = NULL;

    TGetResultSetMetadataReq *req = g_object_new(
        TYPE_T_GET_RESULT_SET_METADATA_REQ, NULL);
    g_object_set(req, "operationHandle", op->op_handle, NULL);

    TGetResultSetMetadataResp *resp = g_object_new(
        TYPE_T_GET_RESULT_SET_METADATA_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_result_set_metadata(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

    /* Extract schema */
    TTableSchema *schema = NULL;
    g_object_get(resp, "schema", &schema, NULL);

    if (!schema) {
        g_object_unref(req);
        g_object_unref(resp);
        if (num_cols) *num_cols = 0;
        return 0;
    }

    GPtrArray *col_descs = NULL;
    col_descs = schema->columns;

    if (!col_descs) {
        g_object_unref(schema);
        g_object_unref(req);
        g_object_unref(resp);
        if (num_cols) *num_cols = 0;
        return 0;
    }

    int ncols = (int)col_descs->len;
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;

    for (int i = 0; i < ncols; i++) {
        TColumnDesc *cd = (TColumnDesc *)g_ptr_array_index(col_descs, i);
        if (columns) argus_hs2_describe_column(cd, &columns[i]);
    }

    if (num_cols) *num_cols = ncols;

    /* Cache metadata in the operation */
    op->metadata_fetched = true;
    op->num_cols = ncols;
    if (columns) {
        op->columns = malloc((size_t)ncols * sizeof(argus_column_desc_t));
        if (op->columns)
            memcpy(op->columns, columns,
                   (size_t)ncols * sizeof(argus_column_desc_t));
    }

    g_object_unref(schema);
    g_object_unref(req);
    g_object_unref(resp);

    return 0;
}
