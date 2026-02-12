#include "hive_internal.h"
#include <stdlib.h>
#include <string.h>

/* ── GetTables via TCLIService ───────────────────────────────── */

int hive_get_tables(argus_backend_conn_t raw_conn,
                    const char *catalog,
                    const char *schema,
                    const char *table_name,
                    const char *table_types,
                    argus_backend_op_t *out_op)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
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

    /* Parse table types into a list */
    if (table_types && *table_types) {
        GPtrArray *types_list = g_ptr_array_new_with_free_func(g_free);

        /* Split by comma */
        char *copy = strdup(table_types);
        if (copy) {
            char *saveptr = NULL;
            char *tok = strtok_r(copy, ",", &saveptr);
            while (tok) {
                /* Trim whitespace and quotes */
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
        /* Ownership transferred to req */
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

    hive_operation_t *op = hive_operation_new();
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

int hive_get_columns(argus_backend_conn_t raw_conn,
                     const char *catalog,
                     const char *schema,
                     const char *table_name,
                     const char *column_name,
                     argus_backend_op_t *out_op)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
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

    hive_operation_t *op = hive_operation_new();
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

int hive_get_type_info(argus_backend_conn_t raw_conn,
                       SQLSMALLINT sql_type,
                       argus_backend_op_t *out_op)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
    if (!conn || !conn->client) return -1;

    GError *error = NULL;

    TGetTypeInfoReq *req = g_object_new(TYPE_T_GET_TYPE_INFO_REQ, NULL);
    g_object_set(req, "sessionHandle", conn->session_handle, NULL);

    (void)sql_type; /* Hive returns all types regardless */

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

    hive_operation_t *op = hive_operation_new();
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

int hive_get_schemas(argus_backend_conn_t raw_conn,
                     const char *catalog,
                     const char *schema,
                     argus_backend_op_t *out_op)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
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

    hive_operation_t *op = hive_operation_new();
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

int hive_get_catalogs(argus_backend_conn_t raw_conn,
                      argus_backend_op_t *out_op)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
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

    hive_operation_t *op = hive_operation_new();
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
