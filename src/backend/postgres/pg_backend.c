#include "pg_common.h"
#include "argus/caps.h"
#include <string.h>

/*
 * BACKEND=postgres — vanilla PostgreSQL.
 *
 * The vtable is thin on purpose: everything here either forwards to the shared
 * libpq core or supplies the one thing that is specific to plain PostgreSQL —
 * hiding inheritance and partition children from SQLTables.
 */

/*
 * Declarative partition children and inheritance children are hidden.
 *
 * A BI tool asking for the table list wants the ten tables an analyst can
 * reason about, not the 240 monthly children behind them; every child has the
 * same columns as its parent and is queried through the parent. psqlODBC
 * filters on relkind alone and returns all of them, which is why a partitioned
 * schema makes its connection dialog crawl.
 *
 * The predicate itself is shared with the MPP backends (pg_mpp.c) because the
 * PostgreSQL half of it is identical; which tests it uses comes from the
 * connect-time probe, not from a version comparison.
 */
static const pg_profile_t postgres_profile = {
    .engine        = PG_ENGINE_POSTGRES,
    .backend_name  = "postgres",
    .tables_filter = pg_plain_tables_filter,
    .remarks_expr  = NULL,     /* plain obj_description */
};

static int postgres_connect(argus_dbc_t *dbc,
                            const char *host, int port,
                            const char *username, const char *password,
                            const char *database, const char *auth_mechanism,
                            argus_backend_conn_t *out_conn)
{
    return pg_connect(&postgres_profile, dbc, host, port, username, password,
                      database, auth_mechanism, out_conn);
}

/*
 * Capabilities. This is where the PostgreSQL family stops under-declaring:
 * real transactions, a schema level that is a schema, PostgreSQL's own
 * 63-byte identifier limit, and a SQLDescribeParam that answers from a
 * server-side Describe instead of guessing.
 *
 * SQL_TXN_READ_UNCOMMITTED is deliberately absent. PostgreSQL accepts the
 * syntax and silently gives READ COMMITTED, so advertising it would be exactly
 * the over-claiming the dialect layer's own rules argue against: an
 * application that asks for it would believe it got it.
 */
static const argus_backend_caps_t postgres_caps = {
    .dbms_name             = "PostgreSQL",
    .catalog_term          = "database",
    .schema_term           = "schema",
    .procedure_term        = "function",
    .max_identifier_len    = 63,
    .txn_capable           = SQL_TC_ALL,
    .txn_isolation_options = SQL_TXN_READ_COMMITTED |
                             SQL_TXN_REPEATABLE_READ |
                             SQL_TXN_SERIALIZABLE,
    .default_txn_isolation = SQL_TXN_READ_COMMITTED,
    .odbc_sql_conformance  = SQL_OSC_CORE,
    .describe_parameter    = true,
    /* .procedures stays false: SQL_PROCEDURES also promises the driver
     * accepts ODBC's {call ...} invocation syntax, and escape.c still
     * rejects it. SQLProcedures itself is implemented and useful; claiming
     * "Y" before {call} works would be a second kind of over-claim. */
};

static const argus_backend_t postgres_backend = {
    .name                  = "postgres",
    .connect               = postgres_connect,
    .disconnect            = pg_disconnect,
    .is_alive              = pg_is_alive,
    .execute               = pg_execute,
    .get_operation_status  = pg_get_operation_status,
    .close_operation       = pg_close_operation,
    .cancel                = pg_cancel,
    .fetch_results         = pg_fetch_results,
    .get_result_metadata   = pg_get_result_metadata,
    .get_tables            = pg_get_tables,
    .get_columns           = pg_get_columns,
    .get_type_info         = pg_get_type_info,
    .get_schemas           = pg_get_schemas,
    .get_catalogs          = pg_get_catalogs,
    .get_primary_keys      = pg_get_primary_keys,
    .get_statistics        = pg_get_statistics,
    .get_last_error        = pg_get_last_error,
    .get_last_error_ex     = pg_get_last_error_ex,
    .get_server_version    = pg_get_server_version,
    .get_foreign_keys      = pg_get_foreign_keys,
    .get_special_columns   = pg_get_special_columns,
    .get_procedures        = pg_get_procedures,
    .get_procedure_columns = pg_get_procedure_columns,
    .get_table_privileges  = pg_get_table_privileges,
    .get_column_privileges = pg_get_column_privileges,
    .set_autocommit        = pg_set_autocommit,
    .end_transaction       = pg_end_transaction,
    .set_isolation         = pg_set_isolation,
    .reset_session         = pg_reset_session,
    .describe_params       = pg_describe_params,
    .caps                  = &postgres_caps,
};

const argus_backend_t *argus_postgres_backend_get(void)
{
    return &postgres_backend;
}
