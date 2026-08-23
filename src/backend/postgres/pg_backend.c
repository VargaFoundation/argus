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
 * relispartition is PostgreSQL 10 and later. The pg_inherits test covers
 * classic inheritance and works on every version, so both are applied and 9.x
 * still gets the right answer.
 */
static const char *postgres_tables_filter(const struct pg_conn *conn)
{
    if (conn && conn->show_partitions) return NULL;

    if (conn && conn->pg_major >= 10)
        return " AND NOT c.relispartition"
               " AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i"
               " WHERE i.inhrelid = c.oid)";

    return " AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i"
           " WHERE i.inhrelid = c.oid)";
}

static const pg_profile_t postgres_profile = {
    .engine        = PG_ENGINE_POSTGRES,
    .backend_name  = "postgres",
    .tables_filter = postgres_tables_filter,
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
