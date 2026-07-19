#ifndef ARGUS_BACKEND_H
#define ARGUS_BACKEND_H

#ifdef _WIN32
#include <windows.h>
#endif
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

    /* Connection liveness check (optional, may be NULL) */
    bool (*is_alive)(argus_backend_conn_t conn);

    /* Query execution */
    int (*execute)(argus_backend_conn_t conn,
                   const char *query,
                   argus_backend_op_t *out_op);

    int (*get_operation_status)(argus_backend_conn_t conn,
                                argus_backend_op_t op,
                                bool *finished);

    void (*close_operation)(argus_backend_conn_t conn,
                            argus_backend_op_t op);

    /* Cancel a running operation */
    int (*cancel)(argus_backend_conn_t conn,
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

    /* Extended catalog operations (optional, may be NULL) */
    int (*get_primary_keys)(argus_backend_conn_t conn,
                            const char *catalog,
                            const char *schema,
                            const char *table_name,
                            argus_backend_op_t *out_op);

    int (*get_statistics)(argus_backend_conn_t conn,
                          const char *catalog,
                          const char *schema,
                          const char *table_name,
                          unsigned short unique,
                          unsigned short reserved,
                          argus_backend_op_t *out_op);

    /* Most recent backend/server error message (optional, may be NULL). Writes
     * up to buflen-1 chars + NUL into buf; returns true if a message was
     * available. Lets the ODBC layer surface the real server error (e.g.
     * "table not found") instead of a generic one. */
    bool (*get_last_error)(argus_backend_conn_t conn, char *buf, size_t buflen);

    /* The server's own version string, e.g. "467" or "3.1.3" (optional, may be
     * NULL). Writes up to buflen-1 chars + NUL into buf; returns true if a
     * version was available. Backs SQLGetInfo(SQL_DBMS_VER), which BI tools use
     * to gate features — a driver that invents a version makes them gate on a
     * fiction, so a backend that cannot answer should leave this NULL and let
     * the ODBC layer report "unknown". Cache it at connect time rather than
     * paying a round trip per call. */
    bool (*get_server_version)(argus_backend_conn_t conn, char *buf, size_t buflen);
} argus_backend_t;

/* Backend registry */
#define ARGUS_MAX_BACKENDS 16

void argus_backend_register(const argus_backend_t *backend);
const argus_backend_t *argus_backend_find(const char *name);
void argus_backends_init(void);

#endif /* ARGUS_BACKEND_H */
