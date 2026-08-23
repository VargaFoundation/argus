#include "pg_common.h"
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

static const argus_backend_t greenplum_backend = {
    .name                  = "greenplum",
    .connect               = greenplum_connect,
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
    .get_server_version    = pg_get_server_version,
};

const argus_backend_t *argus_greenplum_backend_get(void)
{
    return &greenplum_backend;
}
