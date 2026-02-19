#include "argus/backend.h"
#include "argus/handle.h"
#include <stdlib.h>

/* Forward declarations for Kudu backend functions */
int kudu_connect(argus_dbc_t *dbc,
                 const char *host, int port,
                 const char *username, const char *password,
                 const char *database, const char *auth_mechanism,
                 argus_backend_conn_t *out_conn);

void kudu_disconnect(argus_backend_conn_t conn);

int kudu_execute(argus_backend_conn_t conn,
                 const char *query,
                 argus_backend_op_t *out_op);

int kudu_get_operation_status(argus_backend_conn_t conn,
                               argus_backend_op_t op,
                               bool *finished);

void kudu_close_operation(argus_backend_conn_t conn,
                           argus_backend_op_t op);

int kudu_cancel(argus_backend_conn_t conn,
                argus_backend_op_t op);

int kudu_fetch_results(argus_backend_conn_t conn,
                       argus_backend_op_t op,
                       int max_rows,
                       argus_row_cache_t *cache,
                       argus_column_desc_t *columns,
                       int *num_cols);

int kudu_get_result_metadata(argus_backend_conn_t conn,
                              argus_backend_op_t op,
                              argus_column_desc_t *columns,
                              int *num_cols);

int kudu_get_tables(argus_backend_conn_t conn,
                    const char *catalog, const char *schema,
                    const char *table_name, const char *table_types,
                    argus_backend_op_t *out_op);

int kudu_get_columns(argus_backend_conn_t conn,
                     const char *catalog, const char *schema,
                     const char *table_name, const char *column_name,
                     argus_backend_op_t *out_op);

int kudu_get_type_info(argus_backend_conn_t conn,
                       SQLSMALLINT sql_type,
                       argus_backend_op_t *out_op);

int kudu_get_schemas(argus_backend_conn_t conn,
                     const char *catalog, const char *schema,
                     argus_backend_op_t *out_op);

int kudu_get_catalogs(argus_backend_conn_t conn,
                      argus_backend_op_t *out_op);

/* Kudu backend vtable */
static const argus_backend_t kudu_backend = {
    .name                  = "kudu",
    .connect               = kudu_connect,
    .disconnect            = kudu_disconnect,
    .execute               = kudu_execute,
    .get_operation_status  = kudu_get_operation_status,
    .close_operation       = kudu_close_operation,
    .cancel                = kudu_cancel,
    .fetch_results         = kudu_fetch_results,
    .get_result_metadata   = kudu_get_result_metadata,
    .get_tables            = kudu_get_tables,
    .get_columns           = kudu_get_columns,
    .get_type_info         = kudu_get_type_info,
    .get_schemas           = kudu_get_schemas,
    .get_catalogs          = kudu_get_catalogs,
};

const argus_backend_t *argus_kudu_backend_get(void)
{
    return &kudu_backend;
}
