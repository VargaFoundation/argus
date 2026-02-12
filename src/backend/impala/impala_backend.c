#include "argus/backend.h"
#include "argus/handle.h"
#include <stdlib.h>

/* Forward declarations for Impala backend functions */
int impala_connect(argus_dbc_t *dbc,
                   const char *host, int port,
                   const char *username, const char *password,
                   const char *database, const char *auth_mechanism,
                   argus_backend_conn_t *out_conn);

void impala_disconnect(argus_backend_conn_t conn);

int impala_execute(argus_backend_conn_t conn,
                   const char *query,
                   argus_backend_op_t *out_op);

int impala_get_operation_status(argus_backend_conn_t conn,
                                 argus_backend_op_t op,
                                 bool *finished);

void impala_close_operation(argus_backend_conn_t conn,
                             argus_backend_op_t op);

int impala_fetch_results(argus_backend_conn_t conn,
                         argus_backend_op_t op,
                         int max_rows,
                         argus_row_cache_t *cache,
                         argus_column_desc_t *columns,
                         int *num_cols);

int impala_get_result_metadata(argus_backend_conn_t conn,
                                argus_backend_op_t op,
                                argus_column_desc_t *columns,
                                int *num_cols);

int impala_get_tables(argus_backend_conn_t conn,
                      const char *catalog, const char *schema,
                      const char *table_name, const char *table_types,
                      argus_backend_op_t *out_op);

int impala_get_columns(argus_backend_conn_t conn,
                       const char *catalog, const char *schema,
                       const char *table_name, const char *column_name,
                       argus_backend_op_t *out_op);

int impala_get_type_info(argus_backend_conn_t conn,
                         SQLSMALLINT sql_type,
                         argus_backend_op_t *out_op);

int impala_get_schemas(argus_backend_conn_t conn,
                       const char *catalog, const char *schema,
                       argus_backend_op_t *out_op);

int impala_get_catalogs(argus_backend_conn_t conn,
                        argus_backend_op_t *out_op);

/* Impala backend vtable */
static const argus_backend_t impala_backend = {
    .name                  = "impala",
    .connect               = impala_connect,
    .disconnect            = impala_disconnect,
    .execute               = impala_execute,
    .get_operation_status  = impala_get_operation_status,
    .close_operation       = impala_close_operation,
    .fetch_results         = impala_fetch_results,
    .get_result_metadata   = impala_get_result_metadata,
    .get_tables            = impala_get_tables,
    .get_columns           = impala_get_columns,
    .get_type_info         = impala_get_type_info,
    .get_schemas           = impala_get_schemas,
    .get_catalogs          = impala_get_catalogs,
};

const argus_backend_t *argus_impala_backend_get(void)
{
    return &impala_backend;
}
