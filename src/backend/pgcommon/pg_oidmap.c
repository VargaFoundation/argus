#include "pg_common.h"
#include "argus/log.h"
#include <string.h>

/*
 * Domains and enums.
 *
 * A column declared over a domain reports the *domain's* OID in the wire
 * protocol's RowDescription, not the type it is built on. So
 * `CREATE DOMAIN postcode AS varchar(10)` followed by `SELECT pc FROM …` hands
 * the driver an OID no static table can know, and without resolving it the
 * column comes back as an unbounded long string instead of varchar(10) — the
 * application sizes its buffers for the worst case and loses the length
 * constraint it was entitled to. Enums have the same problem and a simpler
 * answer.
 *
 * Why the map is built once at connect rather than filled on demand: in
 * streaming mode the connection is busy from PQsendQuery until the last row is
 * read, so the describe path — which is exactly where an unknown OID first
 * appears — cannot issue a query. Resolving every user-defined domain and enum
 * up front costs one round trip and makes describe purely local. A domain
 * created *after* the connection opened is not in the map and falls back to
 * the unresolved answer, which is the behaviour without this file at all.
 */

/* pg_type.typtype values that need resolving. */
#define PG_TYPTYPE_DOMAIN 'd'
#define PG_TYPTYPE_ENUM   'e'

/*
 * A hard ceiling on the map. A schema with more than this many domains and
 * enums is pathological, and priming would stop being the cheap thing it is
 * meant to be; the unresolved fallback is correct, just less precise.
 */
#define PG_OIDMAP_MAX 20000

typedef struct pg_oid_entry {
    Oid base;      /* the type to report instead */
    int typmod;    /* the domain's own modifier, or -1 */
} pg_oid_entry_t;

void pg_oidmap_free(pg_conn_t *conn)
{
    if (!conn || !conn->oid_map) return;
    g_hash_table_destroy((GHashTable *)conn->oid_map);
    conn->oid_map = NULL;
}

void pg_oidmap_prime(pg_conn_t *conn)
{
    if (!conn || !conn->pg) return;
    pg_oidmap_free(conn);

    /*
     * One recursive walk down every domain chain (a domain over a domain is
     * legal), keeping the innermost non-domain type and the outermost type
     * modifier that was actually declared. DISTINCT ON with depth DESC picks
     * the fully-resolved row for each starting OID.
     *
     * The depth guard is not paranoia about cycles — PostgreSQL forbids them —
     * but a bound on a recursive CTE running against a catalog.
     */
    static const char *sql =
        "WITH RECURSIVE chain(start_oid, oid, typtype, typbasetype, tmod, depth) AS ("
        "  SELECT t.oid, t.oid, t.typtype, t.typbasetype, t.typtypmod, 0"
        "    FROM pg_catalog.pg_type t"
        "   WHERE t.typtype IN ('d','e')"
        "  UNION ALL"
        "  SELECT c.start_oid, b.oid, b.typtype, b.typbasetype,"
        "         CASE WHEN c.tmod <> -1 THEN c.tmod ELSE b.typtypmod END,"
        "         c.depth + 1"
        "    FROM chain c"
        "    JOIN pg_catalog.pg_type b ON b.oid = c.typbasetype"
        "   WHERE c.typtype = 'd' AND c.depth < 8"
        ")"
        "SELECT DISTINCT ON (start_oid) start_oid, oid, typtype, tmod"
        "  FROM chain ORDER BY start_oid, depth DESC";

    PGresult *res = PQexec(conn->pg, sql);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) PQclear(res);
        /* Not fatal: without the map, domain columns report their unresolved
         * type, which is what happened before this existed. */
        ARGUS_LOG_DEBUG("PostgreSQL: could not build the domain/enum map");
        conn->last_error[0] = '\0';
        conn->last_sqlstate[0] = '\0';
        return;
    }

    int n = PQntuples(res);
    if (n > PG_OIDMAP_MAX) {
        ARGUS_LOG_WARN("PostgreSQL: %d domains/enums exceeds the %d the driver "
                       "maps; the rest report their unresolved type",
                       n, PG_OIDMAP_MAX);
        n = PG_OIDMAP_MAX;
    }
    if (n == 0) {
        PQclear(res);
        return;
    }

    GHashTable *map = g_hash_table_new_full(g_direct_hash, g_direct_equal,
                                            NULL, g_free);
    for (int i = 0; i < n; i++) {
        Oid  start   = (Oid)strtoul(PQgetvalue(res, i, 0), NULL, 10);
        Oid  base    = (Oid)strtoul(PQgetvalue(res, i, 1), NULL, 10);
        char typtype = PQgetvalue(res, i, 2)[0];
        int  tmod    = atoi(PQgetvalue(res, i, 3));

        pg_oid_entry_t *e = g_new0(pg_oid_entry_t, 1);
        if (typtype == PG_TYPTYPE_ENUM) {
            /*
             * An enum value is a label, and PostgreSQL caps labels at
             * NAMEDATALEN-1 = 63 bytes — which is exactly what `name` is. So
             * `name` is not an approximation here, it is the right type: the
             * column reports SQL_VARCHAR with a true maximum length instead of
             * an unbounded long string.
             */
            e->base   = PG_OID_NAME;
            e->typmod = -1;
        } else {
            e->base   = base;
            e->typmod = tmod;
        }
        g_hash_table_insert(map, GUINT_TO_POINTER(start), e);
    }
    PQclear(res);

    conn->oid_map = map;
    ARGUS_LOG_DEBUG("PostgreSQL: mapped %d domain/enum type(s)", n);
}

void pg_resolve_oid(const pg_conn_t *conn, Oid *oid, int *typmod)
{
    if (!conn || !conn->oid_map || !oid) return;

    pg_oid_entry_t *e = g_hash_table_lookup((GHashTable *)conn->oid_map,
                                            GUINT_TO_POINTER(*oid));
    if (!e) return;

    *oid = e->base;
    /* The column's own modifier wins when it has one — a domain can be given a
     * further constraint at the column level — and the domain's declared
     * modifier is what fills the usual case, where the column has none. */
    if (typmod && *typmod == -1) *typmod = e->typmod;
}
