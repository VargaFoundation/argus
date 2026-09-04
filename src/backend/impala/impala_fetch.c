/* SPDX-License-Identifier: Apache-2.0 */
#include "impala_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "../hs2_fetch.h"

/* Forward declaration */
int impala_get_result_metadata(argus_backend_conn_t raw_conn,
                                argus_backend_op_t raw_op,
                                argus_column_desc_t *columns,
                                int *num_cols);


/* ── FetchResults via TCLIService ────────────────────────────── */

int impala_fetch_results(argus_backend_conn_t raw_conn,
                       argus_backend_op_t raw_op,
                       int max_rows,
                       argus_row_cache_t *cache,
                       argus_column_desc_t *columns,
                       int *num_cols)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    impala_operation_t *op = (impala_operation_t *)raw_op;
    if (!conn || !op || !op->op_handle) return -1;

    /* Fetch metadata if not yet done */
    if (!op->metadata_fetched && columns && num_cols) {
        impala_get_result_metadata(raw_conn, raw_op, columns, num_cols);
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
