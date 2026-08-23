#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "pg_common.h"

/*
 * The Greenplum / Cloudberry catalog SQL, run against a simulated catalog.
 *
 * Neither engine has a maintained public container image, so the MPP fragments
 * in pg_mpp.c could not be executed against a real Greenplum before shipping.
 * Shipping SQL that has never been parsed is how a catalog call ends up
 * failing with a syntax error on a customer's cluster, so this test does the
 * next best thing: it builds the Greenplum catalog tables — same names, same
 * column names, same types — in a `gp_sim` schema on an ordinary PostgreSQL,
 * rewrites `pg_catalog.` to `gp_sim.` in the generated fragment, and runs it.
 *
 * What that proves: the SQL parses, the int2vector distkey decoding works, the
 * CASE arms produce the strings they are meant to, and an absent annotation
 * contributes nothing rather than a stray separator.
 *
 * What it does not prove: that a real Greenplum's gp_distribution_policy holds
 * what we believe. That distinction is recorded in the header of
 * src/odbc/dialect.c, and the fragments are gated on a connect-time probe so
 * the worst case on a real cluster is a missing annotation, never a failed
 * query.
 *
 * Uses libpq directly rather than the ODBC surface: the point is the SQL.
 */

static const char *env_or(const char *key, const char *dflt)
{
    const char *v = getenv(key);
    return (v && *v) ? v : dflt;
}

static PGconn *g_pg = NULL;

static void run(const char *sql)
{
    PGresult *r = PQexec(g_pg, sql);
    ExecStatusType st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "%s", PQerrorMessage(g_pg));
        PQclear(r);
        fail_msg("setup failed: %s\n  SQL: %s", msg, sql);
    }
    PQclear(r);
}

static int setup(void **state)
{
    (void)state;
    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%s user=%s password=%s dbname=%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
             env_or("PG_DB", "argusdb"));

    g_pg = PQconnectdb(conninfo);
    if (PQstatus(g_pg) != CONNECTION_OK) {
        PQfinish(g_pg);
        g_pg = NULL;
        return -1;
    }

    /*
     * The Greenplum catalog, as Greenplum 6 and 7 declare it. Column names and
     * types matter — distkey being an int2vector is the whole reason the
     * fragment goes through its text form rather than casting to int2[].
     */
    run("DROP SCHEMA IF EXISTS gp_sim CASCADE");
    run("CREATE SCHEMA gp_sim");

    run("CREATE TABLE gp_sim.gp_distribution_policy ("
        "  localoid    oid PRIMARY KEY,"
        "  policytype  \"char\","
        "  numsegments integer,"
        "  distkey     int2vector,"
        "  distclass   oidvector)");

    run("CREATE TABLE gp_sim.pg_appendonly ("
        "  relid       oid PRIMARY KEY,"
        "  columnstore boolean)");

    run("CREATE TABLE gp_sim.pg_exttable ("
        "  reloid      oid PRIMARY KEY,"
        "  urilocation text[])");

    run("CREATE TABLE gp_sim.pg_partition_rule ("
        "  parchildrelid oid PRIMARY KEY)");

    /* Four tables standing in for the four shapes worth annotating. */
    run("CREATE TABLE gp_sim.sales (id integer, customer_id integer, amt numeric)");
    run("COMMENT ON TABLE gp_sim.sales IS 'fact table'");
    run("CREATE TABLE gp_sim.rnd (id integer)");
    run("CREATE TABLE gp_sim.repl (id integer)");
    run("CREATE TABLE gp_sim.ext (id integer)");

    /* sales: DISTRIBUTED BY (customer_id, id), column-oriented AO.
     * attnum 2 is customer_id and 1 is id, so the declaration order is the
     * reverse of attnum order — which is exactly the case a naive
     * implementation gets wrong. */
    run("INSERT INTO gp_sim.gp_distribution_policy VALUES "
        "('gp_sim.sales'::regclass, 'p', 3, '2 1'::int2vector, NULL)");
    run("INSERT INTO gp_sim.pg_appendonly VALUES "
        "('gp_sim.sales'::regclass, true)");

    /* rnd: DISTRIBUTED RANDOMLY — a partitioned policy with an empty key. */
    run("INSERT INTO gp_sim.gp_distribution_policy VALUES "
        "('gp_sim.rnd'::regclass, 'p', 3, ''::int2vector, NULL)");

    /* repl: DISTRIBUTED REPLICATED, and append-optimized row storage. */
    run("INSERT INTO gp_sim.gp_distribution_policy VALUES "
        "('gp_sim.repl'::regclass, 'r', 3, ''::int2vector, NULL)");
    run("INSERT INTO gp_sim.pg_appendonly VALUES "
        "('gp_sim.repl'::regclass, false)");

    /* ext: a gpfdist external table. */
    run("INSERT INTO gp_sim.pg_exttable VALUES "
        "('gp_sim.ext'::regclass, ARRAY['gpfdist://etl-host:8081/sales_*.csv'])");

    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (g_pg) {
        run("DROP SCHEMA IF EXISTS gp_sim CASCADE");
        PQfinish(g_pg);
        g_pg = NULL;
    }
    return 0;
}

/* A connection with the probe flags a Greenplum 6 would have produced. */
static pg_conn_t make_gp6_conn(void)
{
    pg_conn_t c;
    memset(&c, 0, sizeof(c));
    c.detected              = PG_ENGINE_GREENPLUM;
    c.declared              = PG_ENGINE_GREENPLUM;
    c.engine_major          = 6;
    c.pg_major              = 9;
    c.has_gp_policy         = true;
    c.has_gp_policy_distkey = true;
    c.has_pg_appendonly     = true;
    c.has_pg_exttable       = true;
    c.has_partition_rule    = true;
    c.has_relispartition    = false;   /* PG 9.4 has no declarative partitions */
    c.has_relam             = false;
    return c;
}

/* Greenplum 7 / Cloudberry: declarative partitioning, access methods. */
static pg_conn_t make_gp7_conn(void)
{
    pg_conn_t c;
    memset(&c, 0, sizeof(c));
    c.detected              = PG_ENGINE_CLOUDBERRY;
    c.declared              = PG_ENGINE_CLOUDBERRY;
    c.engine_major          = 2;
    c.pg_major              = 14;
    c.has_gp_policy         = true;
    c.has_gp_policy_distkey = true;
    c.has_pg_appendonly     = false;   /* storage read through pg_am instead */
    c.has_pg_exttable       = false;   /* external tables moved to FDW */
    c.has_partition_rule    = false;
    c.has_relispartition    = true;
    c.has_relam             = true;
    return c;
}

static void free_conn(pg_conn_t *c)
{
    g_free(c->mpp_remarks_expr);
    g_free(c->mpp_tables_filter);
}

/* Point the generated fragment at the simulated catalog. */
static char *retarget(const char *sql)
{
    char **parts = g_strsplit(sql, "pg_catalog.gp_", -1);
    char *s1 = g_strjoinv("gp_sim.gp_", parts);
    g_strfreev(parts);

    parts = g_strsplit(s1, "pg_catalog.pg_appendonly", -1);
    char *s2 = g_strjoinv("gp_sim.pg_appendonly", parts);
    g_strfreev(parts);
    g_free(s1);

    parts = g_strsplit(s2, "pg_catalog.pg_exttable", -1);
    char *s3 = g_strjoinv("gp_sim.pg_exttable", parts);
    g_strfreev(parts);
    g_free(s2);

    parts = g_strsplit(s3, "pg_catalog.pg_partition_rule", -1);
    char *s4 = g_strjoinv("gp_sim.pg_partition_rule", parts);
    g_strfreev(parts);
    g_free(s3);

    return s4;
}

/* Evaluate the REMARKS expression for one table. */
static char *remarks_for(const char *expr, const char *relname)
{
    char *retargeted = retarget(expr);

    char sql[4096];
    snprintf(sql, sizeof(sql),
             "SELECT COALESCE(%s, '<null>') "
             "FROM pg_catalog.pg_class c "
             "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
             "WHERE c.oid = '%s'::regclass",
             retargeted, relname);
    g_free(retargeted);

    PGresult *r = PQexec(g_pg, sql);
    if (PQresultStatus(r) != PGRES_TUPLES_OK || PQntuples(r) != 1) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "%s", PQerrorMessage(g_pg));
        PQclear(r);
        fail_msg("REMARKS query failed for %s: %s\n  SQL: %s", relname, msg, sql);
    }
    char *out = g_strdup(PQgetvalue(r, 0, 0));
    PQclear(r);
    return out;
}

/* ── Greenplum 6 shapes ──────────────────────────────────────── */

static void test_gp6_remarks(void **state)
{
    (void)state;
    pg_conn_t c = make_gp6_conn();
    const char *expr = pg_mpp_remarks_expr(&c);

    /* The comment first, then the facts, in a fixed order. The distribution
     * key must follow the declaration order (customer_id, id), not attnum
     * order — '2 1' in the int2vector. */
    char *r = remarks_for(expr, "gp_sim.sales");
    assert_string_equal(r,
        "fact table [AO column] [DISTRIBUTED BY (customer_id, id)]");
    g_free(r);

    /* No comment: the annotations still line up with no leading separator. */
    r = remarks_for(expr, "gp_sim.rnd");
    assert_string_equal(r, "[DISTRIBUTED RANDOMLY]");
    g_free(r);

    r = remarks_for(expr, "gp_sim.repl");
    assert_string_equal(r, "[AO row] [DISTRIBUTED REPLICATED]");
    g_free(r);

    /* An external table with no policy row at all. */
    r = remarks_for(expr, "gp_sim.ext");
    assert_string_equal(r, "[external: gpfdist]");
    g_free(r);

    /* A table this catalog says nothing about must produce NULL, not an empty
     * string — ODBC's REMARKS is nullable and an empty string is a value. */
    r = remarks_for(expr, "argus_test.customers");
    assert_string_equal(r, "customer dimension");
    g_free(r);

    r = remarks_for(expr, "argus_test.orders");
    assert_string_equal(r, "<null>");
    g_free(r);

    free_conn(&c);
}

/* ── Greenplum 7 / Cloudberry shapes ─────────────────────────── */

static void test_gp7_remarks(void **state)
{
    (void)state;
    pg_conn_t c = make_gp7_conn();
    const char *expr = pg_mpp_remarks_expr(&c);

    /* pg_appendonly is absent here, so storage comes from pg_am; on plain
     * PostgreSQL every table is 'heap', which matches neither arm, so no
     * storage marker appears. The distribution marker still must. */
    char *r = remarks_for(expr, "gp_sim.sales");
    assert_string_equal(r, "fact table [DISTRIBUTED BY (customer_id, id)]");
    g_free(r);

    r = remarks_for(expr, "gp_sim.repl");
    assert_string_equal(r, "[DISTRIBUTED REPLICATED]");
    g_free(r);

    /* pg_exttable is absent: the FDW arm is the only external source, and
     * gp_sim.ext is an ordinary table, so nothing is claimed. */
    r = remarks_for(expr, "gp_sim.ext");
    assert_string_equal(r, "<null>");
    g_free(r);

    free_conn(&c);
}

/* ── The SQLTables filter parses and selects the right relations ── */

/*
 * Count the seeded tables the filter leaves visible.
 *
 * Restricted to the names the seed creates rather than to the whole schema:
 * anything else that ends up in argus_test — a benchmark table, a scratch
 * table from another test — would otherwise change the expected count and
 * make this fail for a reason that has nothing to do with the filter.
 */
static long count_with_filter(const char *filter)
{
    char *retargeted = retarget(filter ? filter : "");

    char sql[4096];
    snprintf(sql, sizeof(sql),
             "SELECT count(*) FROM pg_catalog.pg_class c "
             "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
             "WHERE n.nspname = 'argus_test' AND c.relkind IN ('r','p') "
             "AND (c.relname IN ('all_types','customers','orders','events',"
                                "'nation','region') "
                  "OR c.relname LIKE 'events\\_2%%') %s",
             retargeted);
    g_free(retargeted);

    PGresult *r = PQexec(g_pg, sql);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "%s", PQerrorMessage(g_pg));
        PQclear(r);
        fail_msg("filter query failed: %s\n  SQL: %s", msg, sql);
    }
    long n = atol(PQgetvalue(r, 0, 0));
    PQclear(r);
    return n;
}

static void test_tables_filter_parses_and_excludes(void **state)
{
    (void)state;

    /* The seed schema has all_types, customers, orders, events (+24 monthly
     * children), nation and region: 6 parents and 24 partition children. */
    long unfiltered = count_with_filter(NULL);
    assert_int_equal(unfiltered, 30);

    pg_conn_t gp7 = make_gp7_conn();
    long filtered7 = count_with_filter(pg_mpp_tables_filter(&gp7));
    assert_int_equal(filtered7, 6);
    free_conn(&gp7);

    /* Greenplum 6 has no relispartition, so it relies on pg_inherits — which
     * catches PostgreSQL's declarative children too, since those are also
     * inheritance children. The extra pg_partition_rule test is additive and
     * must not exclude anything on a server whose sim table is empty. */
    pg_conn_t gp6 = make_gp6_conn();
    long filtered6 = count_with_filter(pg_mpp_tables_filter(&gp6));
    assert_int_equal(filtered6, 6);
    free_conn(&gp6);

    /* And pg_partition_rule really is consulted: naming a parent in it must
     * remove that parent from the list. */
    run("INSERT INTO gp_sim.pg_partition_rule VALUES "
        "('argus_test.customers'::regclass)");
    pg_conn_t gp6b = make_gp6_conn();
    assert_int_equal(count_with_filter(pg_mpp_tables_filter(&gp6b)), 5);
    free_conn(&gp6b);
    run("DELETE FROM gp_sim.pg_partition_rule");
}

/*
 * SHOWPARTITIONS turns the filter off entirely. A user debugging a partition
 * needs to be able to see it, and a filter with no escape hatch is a bug
 * waiting to be reported as "the driver hides my tables".
 */
static void test_show_partitions_disables_filter(void **state)
{
    (void)state;
    pg_conn_t c = make_gp7_conn();
    c.show_partitions = true;
    assert_null(pg_mpp_tables_filter(&c));
    assert_null(pg_plain_tables_filter(&c));
    free_conn(&c);
}

/*
 * Pointed at something that is not an MPP engine, the fragments must not
 * mention a gp_ catalog at all: that is what keeps BACKEND=greenplum against
 * plain PostgreSQL a warning rather than a broken catalog.
 */
static void test_non_mpp_emits_no_gp_catalogs(void **state)
{
    (void)state;
    pg_conn_t c;
    memset(&c, 0, sizeof(c));
    c.detected = PG_ENGINE_GREENPLUM;
    c.declared = PG_ENGINE_GREENPLUM;
    c.has_relispartition = true;      /* a modern plain PostgreSQL */

    const char *filter = pg_mpp_tables_filter(&c);
    assert_non_null(filter);
    assert_null(strstr(filter, "gp_"));

    const char *expr = pg_mpp_remarks_expr(&c);
    assert_null(strstr(expr, "gp_distribution_policy"));
    assert_null(strstr(expr, "pg_appendonly"));
    assert_null(strstr(expr, "pg_exttable"));

    /* It still has to be valid SQL against a plain server. */
    char *r = remarks_for(expr, "argus_test.customers");
    assert_string_equal(r, "customer dimension");
    g_free(r);

    free_conn(&c);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_gp6_remarks),
        cmocka_unit_test(test_gp7_remarks),
        cmocka_unit_test(test_tables_filter_parses_and_excludes),
        cmocka_unit_test(test_show_partitions_disables_filter),
        cmocka_unit_test(test_non_mpp_emits_no_gp_catalogs),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
