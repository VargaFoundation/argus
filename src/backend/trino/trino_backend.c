#include "argus/backend.h"
#include "argus/handle.h"
#include <stdlib.h>

/* Forward declarations for Trino backend functions */
int trino_connect(argus_dbc_t *dbc,
                  const char *host, int port,
                  const char *username, const char *password,
                  const char *database, const char *auth_mechanism,
                  argus_backend_conn_t *out_conn);

void trino_disconnect(argus_backend_conn_t conn);

int trino_execute(argus_backend_conn_t conn,
                  const char *query,
                  argus_backend_op_t *out_op);

int trino_get_operation_status(argus_backend_conn_t conn,
                                argus_backend_op_t op,
                                bool *finished);

void trino_close_operation(argus_backend_conn_t conn,
                            argus_backend_op_t op);

int trino_fetch_results(argus_backend_conn_t conn,
                        argus_backend_op_t op,
                        int max_rows,
                        argus_row_cache_t *cache,
                        argus_column_desc_t *columns,
                        int *num_cols);

int trino_get_result_metadata(argus_backend_conn_t conn,
                               argus_backend_op_t op,
                               argus_column_desc_t *columns,
                               int *num_cols);

int trino_get_tables(argus_backend_conn_t conn,
                     const char *catalog, const char *schema,
                     const char *table_name, const char *table_types,
                     argus_backend_op_t *out_op);

int trino_get_columns(argus_backend_conn_t conn,
                      const char *catalog, const char *schema,
                      const char *table_name, const char *column_name,
                      argus_backend_op_t *out_op);

int trino_get_type_info(argus_backend_conn_t conn,
                        SQLSMALLINT sql_type,
                        argus_backend_op_t *out_op);

int trino_get_schemas(argus_backend_conn_t conn,
                      const char *catalog, const char *schema,
                      argus_backend_op_t *out_op);

int trino_get_catalogs(argus_backend_conn_t conn,
                       argus_backend_op_t *out_op);

/* Trino backend vtable */
static const argus_backend_t trino_backend = {
    .name                  = "trino",
    .connect               = trino_connect,
    .disconnect            = trino_disconnect,
    .execute               = trino_execute,
    .get_operation_status  = trino_get_operation_status,
    .close_operation       = trino_close_operation,
    .fetch_results         = trino_fetch_results,
    .get_result_metadata   = trino_get_result_metadata,
    .get_tables            = trino_get_tables,
    .get_columns           = trino_get_columns,
    .get_type_info         = trino_get_type_info,
    .get_schemas           = trino_get_schemas,
    .get_catalogs          = trino_get_catalogs,
};

const argus_backend_t *argus_trino_backend_get(void)
{
    return &trino_backend;
}
