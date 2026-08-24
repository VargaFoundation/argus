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


/* ── Determine number of rows from first column ──────────────── */

static int get_column_row_count(GObject *column_obj)
{
    TColumn *tcol = T_COLUMN(column_obj);

    if (tcol->__isset_stringVal && tcol->stringVal) {
        GPtrArray *v = NULL;
        g_object_get(tcol->stringVal, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_i32Val && tcol->i32Val) {
        GArray *v = NULL;
        g_object_get(tcol->i32Val, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_i64Val && tcol->i64Val) {
        GArray *v = NULL;
        g_object_get(tcol->i64Val, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_doubleVal && tcol->doubleVal) {
        GArray *v = NULL;
        g_object_get(tcol->doubleVal, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_boolVal && tcol->boolVal) {
        GArray *v = NULL;
        g_object_get(tcol->boolVal, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_byteVal && tcol->byteVal) {
        GArray *v = NULL;
        g_object_get(tcol->byteVal, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_i16Val && tcol->i16Val) {
        GArray *v = NULL;
        g_object_get(tcol->i16Val, "values", &v, NULL);
        return v ? (int)v->len : 0;
    }
    return 0;
}

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

    GError *error = NULL;

    /* Fetch metadata if not yet done */
    if (!op->metadata_fetched && columns && num_cols) {
        impala_get_result_metadata(raw_conn, raw_op, columns, num_cols);
    }

    /* Fetch results */
    TFetchResultsReq *req = g_object_new(TYPE_T_FETCH_RESULTS_REQ, NULL);
    g_object_set(req,
                 "operationHandle", op->op_handle,
                 "orientation", T_FETCH_ORIENTATION_FETCH_NEXT,
                 "maxRows", (gint64)max_rows,
                 NULL);

    TFetchResultsResp *resp = g_object_new(TYPE_T_FETCH_RESULTS_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_fetch_results(
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
        g_object_unref(status);
        if (status_code == T_STATUS_CODE_ERROR_STATUS) {
            g_object_unref(req);
            g_object_unref(resp);
            return -1;
        }
    }

    /* Get the TRowSet */
    TRowSet *row_set = NULL;
    g_object_get(resp, "results", &row_set, NULL);

    if (!row_set) {
        g_object_unref(req);
        g_object_unref(resp);
        cache->num_rows = 0;
        return 0;
    }

    /* Get columns from TRowSet */
    GPtrArray *tcolumns = NULL;
    g_object_get(row_set, "columns", &tcolumns, NULL);

    if (!tcolumns || tcolumns->len == 0) {
        if (row_set) g_object_unref(row_set);
        g_object_unref(req);
        g_object_unref(resp);
        cache->num_rows = 0;
        return 0;
    }

    int ncols = (int)tcolumns->len;
    if (num_cols) *num_cols = ncols;
    cache->num_cols = ncols;

    /* Determine the row count as the max populated length across all columns
     * (the first column may be entirely NULL, e.g. a NULL TABLE_CAT). */
    int nrows = 0;
    for (int c = 0; c < ncols; c++) {
        int rc = get_column_row_count((GObject *)g_ptr_array_index(tcolumns, c));
        if (rc > nrows) nrows = rc;
    }

    if (nrows == 0) {
        g_object_unref(row_set);
        g_object_unref(req);
        g_object_unref(resp);
        cache->num_rows = 0;
        return 0;
    }

    /* Allocate rows */
    cache->rows = calloc((size_t)nrows, sizeof(argus_row_t));
    if (!cache->rows) {
        g_object_unref(row_set);
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }
    cache->num_rows = (size_t)nrows;
    cache->capacity = (size_t)nrows;

    /* Shared columnar parser: ONE allocation per row (cells + payloads). */
    if (argus_hs2_parse_rowset(tcolumns, ncols, nrows, cache) != 0) {
        g_object_unref(row_set);
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_unref(row_set);
    g_object_unref(req);
    g_object_unref(resp);

    return 0;
}
