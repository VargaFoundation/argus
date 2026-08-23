#include "pg_common.h"
#include <string.h>

/*
 * BACKEND=cloudberry — Apache Cloudberry.
 *
 * Cloudberry is a fork of Greenplum 7, which is a fork of PostgreSQL 12, so
 * its catalog is the Greenplum 7 catalog: declarative partitioning inherited
 * from PostgreSQL, gp_distribution_policy with distkey, and append-optimized
 * storage exposed as a table access method. The MPP fragments are therefore
 * the same ones Greenplum uses, and they are selected by what the probe found
 * rather than by which of the two answered.
 *
 * It is a separate backend all the same, and not an alias, because everything
 * a BI tool keys on is the backend name: SQL_DBMS_NAME, the dialect entry, the
 * Tableau connector, the Power BI backend list. A Cloudberry user picking
 * "Greenplum" in a connection dialog and being told the server is Greenplum is
 * a worse answer than one more eighty-line file. Keeping them apart also means
 * the day Cloudberry's catalog diverges — it is a young fork and it will —
 * there is somewhere for the difference to go.
 */

static const char *cloudberry_tables_filter(const struct pg_conn *conn)
{
    if (!conn) return NULL;

    /* Not actually an MPP engine: degrade rather than emit SQL against
     * catalogs that are not there. */
    if (!conn->has_gp_policy && !conn->has_partition_rule)
        return pg_plain_tables_filter(conn);

    return pg_mpp_tables_filter(conn);
}

static const char *cloudberry_remarks_expr(const struct pg_conn *conn)
{
    return pg_mpp_remarks_expr(conn);
}

static const pg_profile_t cloudberry_profile = {
    .engine        = PG_ENGINE_CLOUDBERRY,
    .backend_name  = "cloudberry",
    .tables_filter = cloudberry_tables_filter,
    .remarks_expr  = cloudberry_remarks_expr,
};

static int cloudberry_connect(argus_dbc_t *dbc,
                              const char *host, int port,
                              const char *username, const char *password,
                              const char *database, const char *auth_mechanism,
                              argus_backend_conn_t *out_conn)
{
    return pg_connect(&cloudberry_profile, dbc, host, port, username, password,
                      database, auth_mechanism, out_conn);
}

static const argus_backend_t cloudberry_backend = {
    .name                  = "cloudberry",
    .connect               = cloudberry_connect,
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

const argus_backend_t *argus_cloudberry_backend_get(void)
{
    return &cloudberry_backend;
}
