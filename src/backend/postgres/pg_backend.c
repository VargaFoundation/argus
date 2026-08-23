#include "pg_common.h"
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
    .get_server_version    = pg_get_server_version,
};

const argus_backend_t *argus_postgres_backend_get(void)
{
    return &postgres_backend;
}
