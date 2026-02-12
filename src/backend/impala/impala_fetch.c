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
    /* TColumn is a union - try each value type.
     * Impala uses the same columnar format as Hive. */

    /* Try stringVal (TStringColumn) */
    TStringColumn *str_col = NULL;
    g_object_get(column_obj, "stringVal", &str_col, NULL);

    if (str_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(str_col);
        return 0;
    }

    /* Try i32Val (TI32Column) */
    TI32Column *i32_col = NULL;
    g_object_get(column_obj, "i32Val", &i32_col, NULL);
    if (i32_col) {
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
    TI64Column *i64_col = NULL;
    g_object_get(column_obj, "i64Val", &i64_col, NULL);
    if (i64_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(i64_col);
        return 0;
    }

    /* Try doubleVal (TDoubleColumn) */
    TDoubleColumn *dbl_col = NULL;
    g_object_get(column_obj, "doubleVal", &dbl_col, NULL);
    if (dbl_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(dbl_col);
        return 0;
    }

    /* Try boolVal (TBoolColumn) */
    TBoolColumn *bool_col = NULL;
    g_object_get(column_obj, "boolVal", &bool_col, NULL);
    if (bool_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(bool_col);
        return 0;
    }

    /* Try byteVal (TByteColumn) for TINYINT */
    TByteColumn *byte_col = NULL;
    g_object_get(column_obj, "byteVal", &byte_col, NULL);
    if (byte_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(byte_col);
        return 0;
    }

    /* Try i16Val (TI16Column) */
    TI16Column *i16_col = NULL;
    g_object_get(column_obj, "i16Val", &i16_col, NULL);
    if (i16_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(i16_col);
        return 0;
    }

    /* Try binaryVal (TBinaryColumn) */
    TBinaryColumn *bin_col = NULL;
    g_object_get(column_obj, "binaryVal", &bin_col, NULL);
    if (bin_col) {
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
        if (nulls) g_byte_array_unref(nulls);
        g_object_unref(bin_col);
        return 0;
    }

    return -1;
}

/* ── Determine number of rows from first column ──────────────── */

static int get_column_row_count(GObject *column_obj)
{
    TStringColumn *str_col = NULL;
    g_object_get(column_obj, "stringVal", &str_col, NULL);
    if (str_col) {
        GPtrArray *values = NULL;
        g_object_get(str_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(str_col);
        return count;
    }

    TI32Column *i32_col = NULL;
    g_object_get(column_obj, "i32Val", &i32_col, NULL);
    if (i32_col) {
        GArray *values = NULL;
        g_object_get(i32_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(i32_col);
        return count;
    }

    TI64Column *i64_col = NULL;
    g_object_get(column_obj, "i64Val", &i64_col, NULL);
    if (i64_col) {
        GArray *values = NULL;
        g_object_get(i64_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(i64_col);
        return count;
    }

    TDoubleColumn *dbl_col = NULL;
    g_object_get(column_obj, "doubleVal", &dbl_col, NULL);
    if (dbl_col) {
        GArray *values = NULL;
        g_object_get(dbl_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(dbl_col);
        return count;
    }

    TBoolColumn *bool_col = NULL;
    g_object_get(column_obj, "boolVal", &bool_col, NULL);
    if (bool_col) {
        GArray *values = NULL;
        g_object_get(bool_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(bool_col);
        return count;
    }

    TByteColumn *byte_col = NULL;
    g_object_get(column_obj, "byteVal", &byte_col, NULL);
    if (byte_col) {
        GArray *values = NULL;
        g_object_get(byte_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(byte_col);
        return count;
    }

    TI16Column *i16_col = NULL;
    g_object_get(column_obj, "i16Val", &i16_col, NULL);
    if (i16_col) {
        GArray *values = NULL;
        g_object_get(i16_col, "values", &values, NULL);
        int count = values ? (int)values->len : 0;
        g_object_unref(i16_col);
        return count;
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

    /* Determine number of rows from first column */
    GObject *first_col = (GObject *)g_ptr_array_index(tcolumns, 0);
    int nrows = get_column_row_count(first_col);

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
