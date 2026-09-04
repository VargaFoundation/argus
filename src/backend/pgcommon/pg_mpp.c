/* SPDX-License-Identifier: Apache-2.0 */
#include "pg_common.h"
#include <string.h>

/*
 * The catalog fragments Greenplum and Cloudberry share.
 *
 * Cloudberry is a fork of Greenplum 7, which is itself a fork of PostgreSQL,
 * so the three questions an MPP catalog has to answer are the same on both:
 * which relations are partition children, how is a table distributed across
 * segments, and how is it stored. Where the answers live is *not* the same —
 * Greenplum 6 records partitioning in pg_partition_rule, Greenplum 7 and
 * Cloudberry use PostgreSQL's declarative relispartition; append-optimized
 * storage is pg_appendonly on GP6 and a table access method on GP7/CBDB.
 *
 * Every fragment below is therefore gated on what pg_probe_identity() actually
 * found on this server, never on the version number alone. That matters more
 * than usual here: neither engine has a maintained public container image, so
 * this SQL could not be run against a real Greenplum or Cloudberry before it
 * shipped. Gating on the probe means the worst case is a missing annotation,
 * not a catalog call that fails with "relation gp_distribution_policy does
 * not exist" — which is exactly what SQL written from documentation does when
 * the documentation is a version out of date.
 *
 * The shapes themselves are exercised in tests/integration/test_pg_mpp_sim.c,
 * which builds the Greenplum catalog tables on a plain PostgreSQL and runs the
 * generated SQL against them. That proves the syntax and the logic; it does
 * not prove the real catalogs hold what we think, and the header of
 * src/odbc/dialect.c records that distinction.
 */

/* ── Partition children ──────────────────────────────────────── */

const char *pg_plain_tables_filter(const struct pg_conn *conn)
{
    if (!conn || conn->show_partitions) return NULL;

    /*
     * Both tests are wanted, not one or the other: relispartition covers
     * declarative partitioning, pg_inherits covers classic inheritance (and,
     * on Greenplum 6, partitioning is implemented *as* inheritance). On a
     * server without relispartition the pg_inherits test alone is correct.
     */
    if (conn->has_relispartition)
        return " AND NOT c.relispartition"
               " AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i"
               " WHERE i.inhrelid = c.oid)";

    return " AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i"
           " WHERE i.inhrelid = c.oid)";
}

const char *pg_mpp_tables_filter(const struct pg_conn *conn)
{
    if (!conn || conn->show_partitions) return NULL;

    /* Cached: SQLTables and SQLColumns both ask, on every call. */
    if (conn->mpp_tables_filter) return conn->mpp_tables_filter;

    GString *f = g_string_new(pg_plain_tables_filter(conn));

    /*
     * Greenplum 6 belt and braces. Its partition children are inheritance
     * children, so the clause above already excludes them — but a child whose
     * inheritance link was somehow dropped would still be listed, and
     * pg_partition_rule is the authoritative record. Cheap, and this is a
     * path that could not be tested live.
     */
    if (conn->has_partition_rule)
        g_string_append(f,
            " AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_partition_rule pr"
            " WHERE pr.parchildrelid = c.oid)");

    ((struct pg_conn *)conn)->mpp_tables_filter = g_string_free(f, FALSE);
    return conn->mpp_tables_filter;
}

/* ── REMARKS: storage, distribution, external location ───────── */

/*
 * REMARKS is the only per-table free-text field ODBC offers, and Tableau and
 * Power BI both surface it as the table description. On an MPP engine the
 * three facts worth putting there are the ones that explain why a query is
 * slow: how the table is distributed, whether it is append-optimized and
 * column-oriented, and whether it is actually external. An analyst who can see
 * `[DISTRIBUTED BY (customer_id)]` knows why joining on order_id shuffles.
 *
 * Kept terse and factual, and always after the user's own comment.
 */
const char *pg_mpp_remarks_expr(const struct pg_conn *conn)
{
    static const char *plain = "obj_description(c.oid, 'pg_class')";
    if (!conn) return plain;
    if (conn->mpp_remarks_expr) return conn->mpp_remarks_expr;

    /* concat_ws skips NULL arguments, so an absent annotation contributes
     * nothing rather than a stray separator. */
    GString *e = g_string_new("NULLIF(concat_ws(' ', "
                              "obj_description(c.oid, 'pg_class')");

    /* Storage. pg_appendonly exists on both GP6 and GP7 and is preferred;
     * the access-method form is the Cloudberry/GP7-only fallback. */
    if (conn->has_pg_appendonly) {
        g_string_append(e,
            ", (SELECT CASE WHEN ao.columnstore THEN '[AO column]' "
            "ELSE '[AO row]' END FROM pg_catalog.pg_appendonly ao "
            "WHERE ao.relid = c.oid)");
    } else if (conn->has_relam) {
        g_string_append(e,
            ", (SELECT CASE am.amname WHEN 'ao_row' THEN '[AO row]' "
            "WHEN 'ao_column' THEN '[AO column]' END "
            "FROM pg_catalog.pg_am am WHERE am.oid = c.relam)");
    }

    /*
     * Distribution. distkey is an int2vector; it is rendered through its text
     * form rather than cast to int2[] because the direct cast is not available
     * on every version in the family, while `'1 2'::text` is. An empty distkey
     * with a partitioned policy means DISTRIBUTED RANDOMLY, so NULLIF keeps
     * string_to_array from producing a one-element array of the empty string.
     */
    if (conn->has_gp_policy && conn->has_gp_policy_distkey) {
        g_string_append(e,
            ", (SELECT CASE WHEN d.policytype = 'r' "
                   "THEN '[DISTRIBUTED REPLICATED]' "
                   "ELSE COALESCE('[DISTRIBUTED BY (' || ("
                       "SELECT string_agg(a.attname, ', ' ORDER BY x.ord) "
                       "FROM unnest(string_to_array("
                           "NULLIF(btrim(d.distkey::text), ''), ' ')::int2[]) "
                           "WITH ORDINALITY AS x(attnum, ord) "
                       "JOIN pg_catalog.pg_attribute a "
                            "ON a.attrelid = c.oid AND a.attnum = x.attnum"
                   ") || ')]', '[DISTRIBUTED RANDOMLY]') END "
             "FROM pg_catalog.gp_distribution_policy d "
             "WHERE d.localoid = c.oid)");
    }

    /* External tables. Greenplum 6 keeps gpfdist/PXF locations in
     * pg_exttable; GP7 and Cloudberry moved them to the FDW machinery, where
     * the foreign server name is the useful label. */
    if (conn->has_pg_exttable) {
        g_string_append(e,
            ", (SELECT '[external: ' || "
                   "COALESCE(split_part(x.urilocation[1], ':', 1), 'unknown') "
                   "|| ']' "
             "FROM pg_catalog.pg_exttable x WHERE x.reloid = c.oid)");
    }
    g_string_append(e,
        ", (SELECT '[external: ' || s.srvname || ']' "
         "FROM pg_catalog.pg_foreign_table ft "
         "JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver "
         "WHERE ft.ftrelid = c.oid)");

    g_string_append(e, "), '')");

    ((struct pg_conn *)conn)->mpp_remarks_expr = g_string_free(e, FALSE);
    return conn->mpp_remarks_expr;
}
