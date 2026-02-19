#include "argus/backend.h"
#include "argus/handle.h"
#include <stdlib.h>

/* Forward declarations for Phoenix backend functions */
int phoenix_connect(argus_dbc_t *dbc,
                    const char *host, int port,
                    const char *username, const char *password,
                    const char *database, const char *auth_mechanism,
                    argus_backend_conn_t *out_conn);

void phoenix_disconnect(argus_backend_conn_t conn);

int phoenix_execute(argus_backend_conn_t conn,
                    const char *query,
                    argus_backend_op_t *out_op);

int phoenix_get_operation_status(argus_backend_conn_t conn,
                                  argus_backend_op_t op,
                                  bool *finished);

void phoenix_close_operation(argus_backend_conn_t conn,
                              argus_backend_op_t op);

int phoenix_cancel(argus_backend_conn_t conn,
                   argus_backend_op_t op);

int phoenix_fetch_results(argus_backend_conn_t conn,
                          argus_backend_op_t op,
                          int max_rows,
                          argus_row_cache_t *cache,
                          argus_column_desc_t *columns,
                          int *num_cols);

int phoenix_get_result_metadata(argus_backend_conn_t conn,
                                 argus_backend_op_t op,
                                 argus_column_desc_t *columns,
                                 int *num_cols);

int phoenix_get_tables(argus_backend_conn_t conn,
                       const char *catalog, const char *schema,
                       const char *table_name, const char *table_types,
                       argus_backend_op_t *out_op);

int phoenix_get_columns(argus_backend_conn_t conn,
                        const char *catalog, const char *schema,
                        const char *table_name, const char *column_name,
                        argus_backend_op_t *out_op);

int phoenix_get_type_info(argus_backend_conn_t conn,
                          SQLSMALLINT sql_type,
                          argus_backend_op_t *out_op);

int phoenix_get_schemas(argus_backend_conn_t conn,
                        const char *catalog, const char *schema,
                        argus_backend_op_t *out_op);

int phoenix_get_catalogs(argus_backend_conn_t conn,
                         argus_backend_op_t *out_op);

/* Phoenix backend vtable */
static const argus_backend_t phoenix_backend = {
    .name                  = "phoenix",
    .connect               = phoenix_connect,
    .disconnect            = phoenix_disconnect,
    .execute               = phoenix_execute,
    .get_operation_status  = phoenix_get_operation_status,
    .close_operation       = phoenix_close_operation,
    .cancel                = phoenix_cancel,
    .fetch_results         = phoenix_fetch_results,
    .get_result_metadata   = phoenix_get_result_metadata,
    .get_tables            = phoenix_get_tables,
    .get_columns           = phoenix_get_columns,
    .get_type_info         = phoenix_get_type_info,
    .get_schemas           = phoenix_get_schemas,
    .get_catalogs          = phoenix_get_catalogs,
};

const argus_backend_t *argus_phoenix_backend_get(void)
{
    return &phoenix_backend;
}
