#include "argus/backend.h"
#include "argus/handle.h"
#include <stdlib.h>

/* Forward declarations for Hive backend functions */
int hive_connect(argus_dbc_t *dbc,
                 const char *host, int port,
                 const char *username, const char *password,
                 const char *database, const char *auth_mechanism,
                 argus_backend_conn_t *out_conn);

void hive_disconnect(argus_backend_conn_t conn);

int hive_execute(argus_backend_conn_t conn,
                 const char *query,
                 argus_backend_op_t *out_op);

int hive_get_operation_status(argus_backend_conn_t conn,
                               argus_backend_op_t op,
                               bool *finished);

void hive_close_operation(argus_backend_conn_t conn,
                           argus_backend_op_t op);

int hive_cancel(argus_backend_conn_t conn,
                argus_backend_op_t op);

int hive_fetch_results(argus_backend_conn_t conn,
                       argus_backend_op_t op,
                       int max_rows,
                       argus_row_cache_t *cache,
                       argus_column_desc_t *columns,
                       int *num_cols);

int hive_get_result_metadata(argus_backend_conn_t conn,
                              argus_backend_op_t op,
                              argus_column_desc_t *columns,
                              int *num_cols);

int hive_get_tables(argus_backend_conn_t conn,
                    const char *catalog, const char *schema,
                    const char *table_name, const char *table_types,
                    argus_backend_op_t *out_op);

int hive_get_columns(argus_backend_conn_t conn,
                     const char *catalog, const char *schema,
                     const char *table_name, const char *column_name,
                     argus_backend_op_t *out_op);

int hive_get_type_info(argus_backend_conn_t conn,
                       SQLSMALLINT sql_type,
                       argus_backend_op_t *out_op);

int hive_get_schemas(argus_backend_conn_t conn,
                     const char *catalog, const char *schema,
                     argus_backend_op_t *out_op);

int hive_get_catalogs(argus_backend_conn_t conn,
                      argus_backend_op_t *out_op);

/* Hive backend vtable */
static const argus_backend_t hive_backend = {
    .name                  = "hive",
    .connect               = hive_connect,
    .disconnect            = hive_disconnect,
    .execute               = hive_execute,
    .get_operation_status  = hive_get_operation_status,
    .close_operation       = hive_close_operation,
    .cancel                = hive_cancel,
    .fetch_results         = hive_fetch_results,
    .get_result_metadata   = hive_get_result_metadata,
    .get_tables            = hive_get_tables,
    .get_columns           = hive_get_columns,
    .get_type_info         = hive_get_type_info,
    .get_schemas           = hive_get_schemas,
    .get_catalogs          = hive_get_catalogs,
};

const argus_backend_t *argus_hive_backend_get(void)
{
    return &hive_backend;
}
