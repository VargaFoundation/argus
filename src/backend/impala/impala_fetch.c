#include "impala_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Forward declaration */
int impala_get_result_metadata(argus_backend_conn_t raw_conn,
                                argus_backend_op_t raw_op,
                                argus_column_desc_t *columns,
                                int *num_cols);

/* ── Parse columnar TRowSet into argus_row_cache ─────────────── */

static int parse_column_values(GObject *column_obj,
                                int col_idx,
                                argus_row_cache_t *cache,
                                int num_rows)
{
    /*
     * TColumn is a union — exactly one field is set.
     * Use the __isset flags to determine which field contains data,
     * since g_object_get always returns a pre-initialized child object.
     */
    TColumn *tcol = T_COLUMN(column_obj);

    /* Try stringVal (TStringColumn) */
    if (tcol->__isset_stringVal) {
        TStringColumn *str_col = tcol->stringVal;
        GPtrArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(str_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gchar *val = (gchar *)g_ptr_array_index(values, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null && val) {
                    cell->data = strdup(val);
                    cell->data_len = strlen(val);
                } else {
                    cell->data = NULL;
                    cell->data_len = 0;
                }
            }
        }
        return 0;
    }

    /* Try i32Val (TI32Column) */
    if (tcol->__isset_i32Val) {
        TI32Column *i32_col = tcol->i32Val;
        GArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(i32_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gint32 val = g_array_index(values, gint32, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null) {
                    cell->data = malloc(16);
                    if (cell->data)
                        cell->data_len = (size_t)snprintf(cell->data, 16, "%d", val);
                }
            }
        }
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(i32_col);
        return 0;
    }

    /* Try i64Val (TI64Column) */
    if (tcol->__isset_i64Val) {
        TI64Column *i64_col = tcol->i64Val;
        GArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(i64_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gint64 val = g_array_index(values, gint64, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null) {
                    cell->data = malloc(24);
                    if (cell->data)
                        cell->data_len = (size_t)snprintf(cell->data, 24, "%ld", (long)val);
                }
            }
        }
        return 0;
    }

    /* Try doubleVal (TDoubleColumn) */
    if (tcol->__isset_doubleVal) {
        TDoubleColumn *dbl_col = tcol->doubleVal;
        GArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(dbl_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gdouble val = g_array_index(values, gdouble, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null) {
                    cell->data = malloc(32);
                    if (cell->data)
                        cell->data_len = (size_t)snprintf(cell->data, 32, "%.15g", val);
                }
            }
        }
        return 0;
    }

    /* Try boolVal (TBoolColumn) */
    if (tcol->__isset_boolVal) {
        TBoolColumn *bool_col = tcol->boolVal;
        GArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(bool_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gboolean val = g_array_index(values, gboolean, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null) {
                    cell->data = strdup(val ? "true" : "false");
                    cell->data_len = val ? 4 : 5;
                }
            }
        }
        return 0;
    }

    /* Try byteVal (TByteColumn) for TINYINT */
    if (tcol->__isset_byteVal) {
        TByteColumn *byte_col = tcol->byteVal;
        GArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(byte_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gint8 val = g_array_index(values, gint8, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null) {
                    cell->data = malloc(8);
                    if (cell->data)
                        cell->data_len = (size_t)snprintf(cell->data, 8, "%d", val);
                }
            }
        }
        return 0;
    }

    /* Try i16Val (TI16Column) */
    if (tcol->__isset_i16Val) {
        TI16Column *i16_col = tcol->i16Val;
        GArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(i16_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                gint16 val = g_array_index(values, gint16, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null) {
                    cell->data = malloc(8);
                    if (cell->data)
                        cell->data_len = (size_t)snprintf(cell->data, 8, "%d", val);
                }
            }
        }
        return 0;
    }

    /* Try binaryVal (TBinaryColumn) */
    if (tcol->__isset_binaryVal) {
        TBinaryColumn *bin_col = tcol->binaryVal;
        GPtrArray *values = NULL;
        GByteArray *nulls = NULL;
        g_object_get(bin_col, "values", &values, "nulls", &nulls, NULL);

        if (values) {
            for (int r = 0; r < num_rows && r < (int)values->len; r++) {
                GByteArray *val = (GByteArray *)g_ptr_array_index(values, r);
                argus_cell_t *cell = &cache->rows[r].cells[col_idx];

                bool is_null = false;
                if (nulls && nulls->len > 0) {
                    int byte_idx = r / 8;
                    int bit_idx = r % 8;
                    if (byte_idx < (int)nulls->len)
                        is_null = (nulls->data[byte_idx] >> bit_idx) & 1;
                }

                cell->is_null = is_null;
                if (!is_null && val) {
                    cell->data = malloc(val->len * 2 + 1);
                    if (cell->data) {
                        for (guint i = 0; i < val->len; i++)
                            snprintf(cell->data + i * 2, 3, "%02x", val->data[i]);
                        cell->data_len = val->len * 2;
                    }
                }
            }
        }
        return 0;
    }

    return -1;
}

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

    for (int r = 0; r < nrows; r++) {
        cache->rows[r].cells = calloc((size_t)ncols, sizeof(argus_cell_t));
        if (!cache->rows[r].cells) {
            g_object_unref(row_set);
            g_object_unref(req);
            g_object_unref(resp);
            return -1;
        }
    }

    /* Parse each column */
    for (int c = 0; c < ncols; c++) {
        GObject *col_obj = (GObject *)g_ptr_array_index(tcolumns, c);
        parse_column_values(col_obj, c, cache, nrows);
    }

    g_object_unref(row_set);
    g_object_unref(req);
    g_object_unref(resp);

    return 0;
}
