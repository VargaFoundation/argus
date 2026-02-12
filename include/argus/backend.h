#ifndef ARGUS_BACKEND_H
#define ARGUS_BACKEND_H

#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>
#include "argus/types.h"

/* Forward declarations */
typedef struct argus_dbc argus_dbc_t;
typedef struct argus_stmt argus_stmt_t;

/* Opaque backend connection/session handle */
typedef void *argus_backend_conn_t;

/* Opaque backend operation handle (for async operations) */
typedef void *argus_backend_op_t;

/*
 * Backend vtable - each backend (Hive, Impala, Trino, etc.)
 * implements this interface.
 */
typedef struct argus_backend {
    const char *name;  /* e.g. "hive", "impala", "trino" */

    /* Connection lifecycle */
    int (*connect)(argus_dbc_t *dbc,
                   const char *host, int port,
                   const char *username, const char *password,
                   const char *database,
                   const char *auth_mechanism,
                   argus_backend_conn_t *out_conn);

    void (*disconnect)(argus_backend_conn_t conn);

    /* Query execution */
    int (*execute)(argus_backend_conn_t conn,
                   const char *query,
                   argus_backend_op_t *out_op);

    int (*get_operation_status)(argus_backend_conn_t conn,
                                argus_backend_op_t op,
                                bool *finished);

    void (*close_operation)(argus_backend_conn_t conn,
                            argus_backend_op_t op);

    /* Result fetching */
    int (*fetch_results)(argus_backend_conn_t conn,
                         argus_backend_op_t op,
                         int max_rows,
                         argus_row_cache_t *cache,
                         argus_column_desc_t *columns,
                         int *num_cols);

    int (*get_result_metadata)(argus_backend_conn_t conn,
                               argus_backend_op_t op,
                               argus_column_desc_t *columns,
                               int *num_cols);

    /* Catalog operations */
    int (*get_tables)(argus_backend_conn_t conn,
                      const char *catalog,
                      const char *schema,
                      const char *table_name,
                      const char *table_types,
                      argus_backend_op_t *out_op);

    int (*get_columns)(argus_backend_conn_t conn,
                       const char *catalog,
                       const char *schema,
                       const char *table_name,
                       const char *column_name,
                       argus_backend_op_t *out_op);

    int (*get_type_info)(argus_backend_conn_t conn,
                         SQLSMALLINT sql_type,
                         argus_backend_op_t *out_op);

    int (*get_schemas)(argus_backend_conn_t conn,
                       const char *catalog,
                       const char *schema,
                       argus_backend_op_t *out_op);

    int (*get_catalogs)(argus_backend_conn_t conn,
                        argus_backend_op_t *out_op);
} argus_backend_t;

/* Backend registry */
#define ARGUS_MAX_BACKENDS 16

void argus_backend_register(const argus_backend_t *backend);
const argus_backend_t *argus_backend_find(const char *name);
void argus_backends_init(void);

#endif /* ARGUS_BACKEND_H */
