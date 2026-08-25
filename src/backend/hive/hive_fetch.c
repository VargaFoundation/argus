#include "hive_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "../hs2_fetch.h"

/* Forward declaration */
int hive_get_result_metadata(argus_backend_conn_t raw_conn,
                              argus_backend_op_t raw_op,
                              argus_column_desc_t *columns,
                              int *num_cols);


/* ── Determine number of rows from first column ──────────────── */

static int get_column_row_count(GObject *column_obj)
{
    TColumn *tcol = T_COLUMN(column_obj);

    if (tcol->__isset_stringVal && tcol->stringVal) {
        GPtrArray *v = NULL;
        v = tcol->stringVal->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_i32Val && tcol->i32Val) {
        GArray *v = NULL;
        v = tcol->i32Val->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_i64Val && tcol->i64Val) {
        GArray *v = NULL;
        v = tcol->i64Val->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_doubleVal && tcol->doubleVal) {
        GArray *v = NULL;
        v = tcol->doubleVal->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_boolVal && tcol->boolVal) {
        GArray *v = NULL;
        v = tcol->boolVal->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_byteVal && tcol->byteVal) {
        GArray *v = NULL;
        v = tcol->byteVal->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_i16Val && tcol->i16Val) {
        GArray *v = NULL;
        v = tcol->i16Val->values;
        return v ? (int)v->len : 0;
    }
    return 0;
}

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

    GError *error = NULL;

    /* Fetch metadata if not yet done */
    if (!op->metadata_fetched && columns && num_cols) {
        hive_get_result_metadata(raw_conn, raw_op, columns, num_cols);
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
    tcolumns = row_set->columns;

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

    /* Determine the row count as the max populated length across all columns.
     * Using only the first column is wrong when it is entirely NULL (empty
     * values array), e.g. GetTables returns a NULL TABLE_CAT first column. */
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

        gchar *col_name = NULL;
        g_object_get(cd, "columnName", &col_name, NULL);

        TTypeDesc *type_desc = NULL;
        g_object_get(cd, "typeDesc", &type_desc, NULL);

        /* Get type name from type descriptor */
        const char *type_name = "STRING";
        if (type_desc) {
            GPtrArray *types = NULL;
            types = type_desc->types;
            if (types && types->len > 0) {
                TTypeEntry *te = (TTypeEntry *)g_ptr_array_index(types, 0);
                TPrimitiveTypeEntry *pte = NULL;
                g_object_get(te, "primitiveEntry", &pte, NULL);
                if (pte) {
                    TTypeId type_id;
                    g_object_get(pte, "type", &type_id, NULL);

                    /* Map TTypeId to type name string */
                    switch (type_id) {
                    case T_TYPE_ID_BOOLEAN_TYPE:   type_name = "BOOLEAN"; break;
                    case T_TYPE_ID_TINYINT_TYPE:   type_name = "TINYINT"; break;
                    case T_TYPE_ID_SMALLINT_TYPE:  type_name = "SMALLINT"; break;
                    case T_TYPE_ID_INT_TYPE:        type_name = "INT"; break;
                    case T_TYPE_ID_BIGINT_TYPE:     type_name = "BIGINT"; break;
                    case T_TYPE_ID_FLOAT_TYPE:      type_name = "FLOAT"; break;
                    case T_TYPE_ID_DOUBLE_TYPE:     type_name = "DOUBLE"; break;
                    case T_TYPE_ID_STRING_TYPE:     type_name = "STRING"; break;
                    case T_TYPE_ID_TIMESTAMP_TYPE:  type_name = "TIMESTAMP"; break;
                    case T_TYPE_ID_BINARY_TYPE:     type_name = "BINARY"; break;
                    case T_TYPE_ID_DECIMAL_TYPE:    type_name = "DECIMAL"; break;
                    case T_TYPE_ID_DATE_TYPE:       type_name = "DATE"; break;
                    case T_TYPE_ID_VARCHAR_TYPE:    type_name = "VARCHAR"; break;
                    case T_TYPE_ID_CHAR_TYPE:       type_name = "CHAR"; break;
                    default:                        type_name = "STRING"; break;
                    }
                    g_object_unref(pte);
                }
            }
            g_object_unref(type_desc);
        }

        if (columns) {
            argus_column_desc_t *col = &columns[i];
            memset(col, 0, sizeof(*col));

            if (col_name) {
                strncpy((char *)col->name, col_name,
                        ARGUS_MAX_COLUMN_NAME - 1);
                col->name_len = (SQLSMALLINT)strlen(col_name);
            }

            col->sql_type       = hive_type_to_sql_type(type_name);
            col->column_size    = hive_type_column_size(col->sql_type);
            col->decimal_digits = hive_type_decimal_digits(col->sql_type);
            col->nullable       = SQL_NULLABLE_UNKNOWN;
        }

        g_free(col_name);
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
