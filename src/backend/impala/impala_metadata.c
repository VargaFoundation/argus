#include "impala_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>

/* ── Get result set metadata ──────────────────────────────────── */

int impala_get_result_metadata(argus_backend_conn_t raw_conn,
                                argus_backend_op_t raw_op,
                                argus_column_desc_t *columns,
                                int *num_cols)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    impala_operation_t *op = (impala_operation_t *)raw_op;
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
    g_object_get(schema, "columns", &col_descs, NULL);

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
            g_object_get(type_desc, "types", &types, NULL);
            if (types && types->len > 0) {
                TTypeEntry *te = (TTypeEntry *)g_ptr_array_index(types, 0);
                TPrimitiveTypeEntry *pte = NULL;
                g_object_get(te, "primitiveEntry", &pte, NULL);
                if (pte) {
                    TTypeId type_id;
                    g_object_get(pte, "type", &type_id, NULL);

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

            col->sql_type       = impala_type_to_sql_type(type_name);
            col->column_size    = impala_type_column_size(col->sql_type);
            col->decimal_digits = impala_type_decimal_digits(col->sql_type);
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

/* ── GetTables via TCLIService ───────────────────────────────── */

int impala_get_tables(argus_backend_conn_t raw_conn,
                      const char *catalog,
                      const char *schema,
                      const char *table_name,
                      const char *table_types,
                      argus_backend_op_t *out_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    if (!conn || !conn->client) return -1;

    GError *error = NULL;

    TGetTablesReq *req = g_object_new(TYPE_T_GET_TABLES_REQ, NULL);
    g_object_set(req, "sessionHandle", conn->session_handle, NULL);

    if (catalog && *catalog)
        g_object_set(req, "catalogName", catalog, NULL);
    if (schema && *schema)
        g_object_set(req, "schemaName", schema, NULL);
    if (table_name && *table_name)
        g_object_set(req, "tableName", table_name, NULL);

    if (table_types && *table_types) {
        GPtrArray *types_list = g_ptr_array_new_with_free_func(g_free);

        char *copy = strdup(table_types);
        if (copy) {
            char *saveptr = NULL;
            char *tok = strtok_r(copy, ",", &saveptr);
            while (tok) {
                while (*tok == ' ' || *tok == '\'') tok++;
                size_t len = strlen(tok);
                while (len > 0 && (tok[len - 1] == ' ' || tok[len - 1] == '\''))
                    len--;
                if (len > 0) {
                    char *type = strndup(tok, len);
                    g_ptr_array_add(types_list, type);
                }
                tok = strtok_r(NULL, ",", &saveptr);
            }
            free(copy);
        }

        g_object_set(req, "tableTypes", types_list, NULL);
    }

    TGetTablesResp *resp = g_object_new(TYPE_T_GET_TABLES_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_tables(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

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

    impala_operation_t *op = impala_operation_new();
    if (!op) {
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_get(resp, "operationHandle", &op->op_handle, NULL);
    op->has_result_set = true;

    g_object_unref(req);
    g_object_unref(resp);

    *out_op = op;
    return 0;
}

/* ── GetColumns via TCLIService ──────────────────────────────── */

int impala_get_columns(argus_backend_conn_t raw_conn,
                       const char *catalog,
                       const char *schema,
                       const char *table_name,
                       const char *column_name,
                       argus_backend_op_t *out_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    if (!conn || !conn->client) return -1;

    GError *error = NULL;

    TGetColumnsReq *req = g_object_new(TYPE_T_GET_COLUMNS_REQ, NULL);
    g_object_set(req, "sessionHandle", conn->session_handle, NULL);

    if (catalog && *catalog)
        g_object_set(req, "catalogName", catalog, NULL);
    if (schema && *schema)
        g_object_set(req, "schemaName", schema, NULL);
    if (table_name && *table_name)
        g_object_set(req, "tableName", table_name, NULL);
    if (column_name && *column_name)
        g_object_set(req, "columnName", column_name, NULL);

    TGetColumnsResp *resp = g_object_new(TYPE_T_GET_COLUMNS_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_columns(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

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

    impala_operation_t *op = impala_operation_new();
    if (!op) {
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_get(resp, "operationHandle", &op->op_handle, NULL);
    op->has_result_set = true;

    g_object_unref(req);
    g_object_unref(resp);

    *out_op = op;
    return 0;
}

/* ── GetTypeInfo via TCLIService ─────────────────────────────── */

int impala_get_type_info(argus_backend_conn_t raw_conn,
                         SQLSMALLINT sql_type,
                         argus_backend_op_t *out_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    if (!conn || !conn->client) return -1;

    GError *error = NULL;

    TGetTypeInfoReq *req = g_object_new(TYPE_T_GET_TYPE_INFO_REQ, NULL);
    g_object_set(req, "sessionHandle", conn->session_handle, NULL);

    (void)sql_type; /* Impala returns all types regardless */

    TGetTypeInfoResp *resp = g_object_new(TYPE_T_GET_TYPE_INFO_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_type_info(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

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

    impala_operation_t *op = impala_operation_new();
    if (!op) {
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_get(resp, "operationHandle", &op->op_handle, NULL);
    op->has_result_set = true;

    g_object_unref(req);
    g_object_unref(resp);

    *out_op = op;
    return 0;
}

/* ── GetSchemas via TCLIService ──────────────────────────────── */

int impala_get_schemas(argus_backend_conn_t raw_conn,
                       const char *catalog,
                       const char *schema,
                       argus_backend_op_t *out_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    if (!conn || !conn->client) return -1;

    GError *error = NULL;

    TGetSchemasReq *req = g_object_new(TYPE_T_GET_SCHEMAS_REQ, NULL);
    g_object_set(req, "sessionHandle", conn->session_handle, NULL);

    if (catalog && *catalog)
        g_object_set(req, "catalogName", catalog, NULL);
    if (schema && *schema)
        g_object_set(req, "schemaName", schema, NULL);

    TGetSchemasResp *resp = g_object_new(TYPE_T_GET_SCHEMAS_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_schemas(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

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

    impala_operation_t *op = impala_operation_new();
    if (!op) {
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_get(resp, "operationHandle", &op->op_handle, NULL);
    op->has_result_set = true;

    g_object_unref(req);
    g_object_unref(resp);

    *out_op = op;
    return 0;
}

/* ── GetCatalogs via TCLIService ─────────────────────────────── */

int impala_get_catalogs(argus_backend_conn_t raw_conn,
                        argus_backend_op_t *out_op)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
    if (!conn || !conn->client) return -1;

    GError *error = NULL;

    TGetCatalogsReq *req = g_object_new(TYPE_T_GET_CATALOGS_REQ, NULL);
    g_object_set(req, "sessionHandle", conn->session_handle, NULL);

    TGetCatalogsResp *resp = g_object_new(TYPE_T_GET_CATALOGS_RESP, NULL);

    gboolean ok = t_c_l_i_service_client_get_catalogs(
        conn->client, &resp, req, &error);

    if (!ok || !resp) {
        if (error) g_error_free(error);
        g_object_unref(req);
        if (resp) g_object_unref(resp);
        return -1;
    }

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

    impala_operation_t *op = impala_operation_new();
    if (!op) {
        g_object_unref(req);
        g_object_unref(resp);
        return -1;
    }

    g_object_get(resp, "operationHandle", &op->op_handle, NULL);
    op->has_result_set = true;

    g_object_unref(req);
    g_object_unref(resp);

    *out_op = op;
    return 0;
}
