#include "pg_common.h"
#include "argus/caps.h"
#include <string.h>

/*
 * BACKEND=greenplum.
 *
 * Greenplum speaks the PostgreSQL wire protocol, so everything below the
 * catalog is the shared libpq core. What this backend adds is the part a
 * generic PostgreSQL ODBC driver cannot know about:
 *
 *   * partition children are hidden from SQLTables and SQLColumns, whether
 *     they are recorded the Greenplum 6 way (inheritance plus
 *     pg_partition_rule) or the Greenplum 7 way (declarative partitioning);
 *   * the distribution policy, append-optimized storage and external-table
 *     location reach the application through REMARKS.
 *
 * The first of those is the one that decides whether a Greenplum warehouse is
 * usable from a BI tool at all: a fact table partitioned monthly over ten
 * years, times a couple of hundred tables, is tens of thousands of child
 * relations that a driver filtering only on relkind puts in front of the user.
 *
 * Both are driven by what pg_probe_identity() found rather than by the version
 * number, so pointing this backend at something that is not Greenplum degrades
 * to plain PostgreSQL behaviour instead of failing.
 */

static const char *greenplum_tables_filter(const struct pg_conn *conn)
{
    if (!conn) return NULL;

    /* BACKEND=greenplum against something that is not an MPP engine: the
     * connection already warned, and the plain filter is the right answer. */
    if (!conn->has_gp_policy && !conn->has_partition_rule)
        return pg_plain_tables_filter(conn);

    return pg_mpp_tables_filter(conn);
}

static const char *greenplum_remarks_expr(const struct pg_conn *conn)
{
    return pg_mpp_remarks_expr(conn);
}

static const pg_profile_t greenplum_profile = {
    .engine        = PG_ENGINE_GREENPLUM,
    .backend_name  = "greenplum",
    .tables_filter = greenplum_tables_filter,
    .remarks_expr  = greenplum_remarks_expr,
};

static int greenplum_connect(argus_dbc_t *dbc,
                             const char *host, int port,
                             const char *username, const char *password,
                             const char *database, const char *auth_mechanism,
                             argus_backend_conn_t *out_conn)
{
    return pg_connect(&greenplum_profile, dbc, host, port, username, password,
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
static const argus_backend_caps_t greenplum_caps = {
    .dbms_name             = "Greenplum Database",
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
    /* SQL_PROCEDURES is not a field here: it is derived from the dialect's
     * call template, so it can only say "Y" while {call ...} really
     * translates. See include/argus/caps.h. */
};

static const argus_backend_t greenplum_backend = {
    .name                  = "greenplum",
    .connect               = greenplum_connect,
    .disconnect            = pg_disconnect,
    .is_alive              = pg_is_alive,
    .execute               = pg_execute,
    .get_operation_status  = pg_get_operation_status,
    .close_operation       = pg_close_operation,
    .cancel                = pg_cancel,
    .cancel_from_any_thread = true,
    .fetch_results         = pg_fetch_results,
    .get_result_metadata   = pg_get_result_metadata,
    .get_tables            = pg_get_tables,
    .get_columns           = pg_get_columns,
    .get_type_info         = pg_get_type_info,
    .get_schemas           = pg_get_schemas,
    .get_catalogs          = pg_get_catalogs,
    .get_primary_keys      = pg_get_primary_keys,
    .get_statistics        = pg_get_statistics,
    .get_affected_rows     = pg_get_affected_rows,
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
    .caps                  = &greenplum_caps,
};

const argus_backend_t *argus_greenplum_backend_get(void)
{
    return &greenplum_backend;
}
