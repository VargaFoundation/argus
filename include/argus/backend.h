#ifndef ARGUS_BACKEND_H
#define ARGUS_BACKEND_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>
#include <stddef.h>
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

    /*
     * ── Everything below is optional and appended, never inserted ──
     *
     * Existing backends use designated initialisers, so a NULL hook here means
     * "behave exactly as before": the ODBC layer keeps its current answer for
     * every one of these. That is the whole contract — an engine that has
     * foreign keys or transactions opts in, and the ten that do not are
     * untouched.
     */

    /* Extended catalog. NULL → src/odbc/catalog.c returns the correctly-shaped
     * empty result set it returns today, which is the right answer for an
     * engine that genuinely has no such objects. */
    int (*get_foreign_keys)(argus_backend_conn_t conn,
                            const char *pk_catalog, const char *pk_schema,
                            const char *pk_table,
                            const char *fk_catalog, const char *fk_schema,
                            const char *fk_table,
                            argus_backend_op_t *out_op);

    int (*get_special_columns)(argus_backend_conn_t conn,
                               SQLUSMALLINT identifier_type,
                               const char *catalog, const char *schema,
                               const char *table,
                               SQLUSMALLINT scope, SQLUSMALLINT nullable,
                               argus_backend_op_t *out_op);

    int (*get_procedures)(argus_backend_conn_t conn,
                          const char *catalog, const char *schema,
                          const char *proc, argus_backend_op_t *out_op);

    int (*get_procedure_columns)(argus_backend_conn_t conn,
                                 const char *catalog, const char *schema,
                                 const char *proc, const char *column,
                                 argus_backend_op_t *out_op);

    int (*get_table_privileges)(argus_backend_conn_t conn,
                                const char *catalog, const char *schema,
                                const char *table, argus_backend_op_t *out_op);

    int (*get_column_privileges)(argus_backend_conn_t conn,
                                 const char *catalog, const char *schema,
                                 const char *table, const char *column,
                                 argus_backend_op_t *out_op);

    /* Transactions. Treat as one group: a backend either implements all three
     * or none. NULL → SQL_ATTR_AUTOCOMMIT is stored and SQLEndTran succeeds
     * without doing anything, which is correct for an engine with no
     * transactions and is what happens today. */
    int (*set_autocommit)(argus_backend_conn_t conn, bool on);
    int (*end_transaction)(argus_backend_conn_t conn, bool commit);
    int (*set_isolation)(argus_backend_conn_t conn, SQLUINTEGER odbc_isolation);

    /* Return a connection to a clean state before the Driver Manager's pool
     * parks it (SQL_ATTR_RESET_CONNECTION). Returns false when it cannot be
     * cleaned and must be discarded instead. NULL → nothing to clear; the
     * connection is parked as-is. */
    bool (*reset_session)(argus_backend_conn_t conn);

    /* Real parameter metadata for SQLDescribeParam. Pure metadata: it does not
     * change how parameters are executed (they are still rendered as literals
     * by src/odbc/execute.c). NULL → the generic SQL_VARCHAR answer. */
    int (*describe_params)(argus_backend_conn_t conn, const char *query,
                           argus_column_desc_t *params, int *num_params);

    /* Rows affected by the last DML statement, for SQLRowCount. NULL → the
     * statement's row count stays -1 ("not available"), which is what every
     * backend reported before this existed and is the truthful answer for an
     * engine whose protocol does not carry one. */
    bool (*get_affected_rows)(argus_backend_conn_t conn, argus_backend_op_t op,
                              SQLLEN *out_rows);

    /* The last error together with the server's own SQLSTATE. NULL → the ODBC
     * layer uses get_last_error and reports HY000, as today. PostgreSQL hands
     * back a real five-character SQLSTATE, and collapsing 42P01 to HY000
     * throws away the one thing an application can branch on. */
    bool (*get_last_error_ex)(argus_backend_conn_t conn,
                              char sqlstate[6], char *buf, size_t buflen);

    /* Static capability descriptor for SQLGetInfo (argus/caps.h).
     * NULL → the pre-capabilities answers. */
    const struct argus_backend_caps *caps;

    /* True when cancel() may be called from another thread while execute()
     * or fetch_results() is blocked on the same connection, and with a NULL
     * op (libpq's PQcancel opens a connection of its own to reach the
     * server). SQLCancel then sends the cancel at once, so the blocked call
     * returns early; a backend whose cancel shares the transport of the
     * running call leaves this false and is cancelled when that call
     * returns on its own. */
    bool cancel_from_any_thread;
} argus_backend_t;

/* Backend registry. The array is a handful of pointers, so the bound exists
 * only to keep registration total — leave real headroom rather than trimming
 * it to the current backend count. */
#define ARGUS_MAX_BACKENDS 24

void argus_backend_register(const argus_backend_t *backend);
const argus_backend_t *argus_backend_find(const char *name);
void argus_backends_init(void);

/* Iteration over the registry, for diagnostics and for the tests that assert
 * no backend's SQLGetInfo answers moved. */
size_t argus_backend_count(void);
const argus_backend_t *argus_backend_at(size_t index);

/* One line naming the version and every backend and auth feature compiled
 * into this binary: "argus-build 0.6.1 hive impala trino ... gssapi telemetry".
 * A release job greps it out of the artefact (scripts/check-build-manifest.sh)
 * so a runner missing a -dev package can no longer ship a driver that quietly
 * lacks a backend. */
const char *argus_build_manifest(void);

#endif /* ARGUS_BACKEND_H */
