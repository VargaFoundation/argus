/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

/*
 * Per-connection options, and SQLRowCount after DML.
 *
 * The options here started as environment variables, which is the wrong shape
 * for a driver: one process routinely holds connections to several servers, and
 * a machine-wide switch cannot say "show partition children on the staging DSN
 * but not on production". Each test opens two connections that differ only in
 * the option, which is the property an environment variable cannot have.
 */

static const char *env_or(const char *key, const char *dflt)
{
    const char *v = getenv(key);
    return (v && *v) ? v : dflt;
}

static void open_with(SQLHENV *env, SQLHDBC *dbc, const char *extra)
{
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, *env, dbc), SQL_SUCCESS);

    char cs[640];
    snprintf(cs, sizeof(cs),
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=postgres;Database=%s%s%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
             env_or("PG_DB", "argusdb"),
             extra ? ";" : "", extra ? extra : "");

    SQLRETURN rc = SQLDriverConnect(*dbc, NULL, (SQLCHAR *)cs, SQL_NTS,
                                    NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER n = 0; SQLSMALLINT l = 0;
        SQLGetDiagRec(SQL_HANDLE_DBC, *dbc, 1, st, &n, msg, sizeof(msg), &l);
        fail_msg("connect failed with '%s': %s %s", extra ? extra : "", st, msg);
    }
}

static void close_conn(SQLHENV env, SQLHDBC dbc)
{
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static long count_tables(SQLHDBC dbc, const char *schema, const char *pattern)
{
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    assert_int_equal(SQLTables(s, NULL, 0,
                               (SQLCHAR *)schema, SQL_NTS,
                               (SQLCHAR *)pattern, SQL_NTS, NULL, 0),
                     SQL_SUCCESS);
    long n = 0;
    while (SQLFetch(s) == SQL_SUCCESS) n++;
    SQLFreeHandle(SQL_HANDLE_STMT, s);
    return n;
}

static void scalar_str(SQLHDBC dbc, const char *sql, char *out, size_t n)
{
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    SQLRETURN rc = SQLExecDirect(s, (SQLCHAR *)sql, SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER nn = 0; SQLSMALLINT l = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, st, &nn, msg, sizeof(msg), &l);
        SQLFreeHandle(SQL_HANDLE_STMT, s);
        fail_msg("%s: %s %s", sql, st, msg);
    }
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    SQLLEN ind = 0;
    out[0] = '\0';
    SQLGetData(s, 1, SQL_C_CHAR, out, (SQLLEN)n, &ind);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SHOWPARTITIONS ──────────────────────────────────────────── */

static void test_show_partitions_is_per_connection(void **state)
{
    (void)state;
    SQLHENV e1, e2; SQLHDBC hidden, shown;

    open_with(&e1, &hidden, NULL);
    open_with(&e2, &shown, "SHOWPARTITIONS=1");

    /* The seed's `events` has 24 monthly children. Two connections to the same
     * server, differing only in the option, must disagree — which is exactly
     * what a machine-wide environment variable could not express. */
    assert_int_equal(count_tables(hidden, "argus_test", "events%"), 1);
    assert_int_equal(count_tables(shown,  "argus_test", "events%"), 25);

    close_conn(e1, hidden);
    close_conn(e2, shown);
}

/* ── SHOWALLDATABASES ────────────────────────────────────────── */

static long count_catalogs(SQLHDBC dbc)
{
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    /* SQLTables with the catalog wildcard is ODBC's "list catalogs". */
    assert_int_equal(SQLTables(s, (SQLCHAR *)SQL_ALL_CATALOGS, SQL_NTS,
                               (SQLCHAR *)"", 0, (SQLCHAR *)"", 0, NULL, 0),
                     SQL_SUCCESS);
    long n = 0;
    while (SQLFetch(s) == SQL_SUCCESS) n++;
    SQLFreeHandle(SQL_HANDLE_STMT, s);
    return n;
}

static void test_show_all_databases(void **state)
{
    (void)state;
    SQLHENV e1, e2; SQLHDBC one, all;

    open_with(&e1, &one, NULL);
    open_with(&e2, &all, "SHOWALLDATABASES=1");

    /* By default only the database the session can actually query: a PostgreSQL
     * connection cannot cross databases, so offering the others in a BI
     * navigator produces entries that fail when expanded. */
    assert_int_equal(count_catalogs(one), 1);
    assert_true(count_catalogs(all) >= 1);

    close_conn(e1, one);
    close_conn(e2, all);
}

/* ── SQLTables' enumeration forms ────────────────────────────── */

/*
 * SQLTables(catalog="%", schema="", table="") is ODBC's "list the catalogs",
 * and schema="%" is "list the schemas". Both routed to get_catalogs and
 * get_schemas, which every backend has implemented since the vtable was
 * written and which nothing used to call — the special cases fell through to
 * get_tables, so asking for the schema list returned the table list.
 */
static void test_schema_enumeration(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_with(&env, &dbc, NULL);

    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    assert_int_equal(SQLTables(s, (SQLCHAR *)"", 0,
                               (SQLCHAR *)SQL_ALL_SCHEMAS, SQL_NTS,
                               (SQLCHAR *)"", 0, NULL, 0), SQL_SUCCESS);

    bool saw_argus_test = false, saw_a_table_name = false;
    char buf[128];
    long n = 0;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLLEN ind = 0;
        buf[0] = '\0';
        /* The schema list puts the schema in column 1 (TABLE_SCHEM). */
        SQLGetData(s, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
        if (strcmp(buf, "argus_test") == 0) saw_argus_test = true;
        /* A table name here would mean the call fell through to get_tables. */
        if (strcmp(buf, "customers") == 0 || strcmp(buf, "orders") == 0)
            saw_a_table_name = true;
        n++;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    assert_true(n > 0);
    assert_true(saw_argus_test);
    assert_false(saw_a_table_name);

    close_conn(env, dbc);
}

/* ── SEARCHPATH ──────────────────────────────────────────────── */

static void test_search_path(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_with(&env, &dbc, "SEARCHPATH=argus_test,public");

    char buf[128];
    scalar_str(dbc, "SHOW search_path", buf, sizeof(buf));
    assert_non_null(strstr(buf, "argus_test"));

    /* And it means what it says: an unqualified name resolves in it. */
    scalar_str(dbc, "SELECT count(*)::text FROM customers", buf, sizeof(buf));
    assert_string_equal(buf, "2");

    close_conn(env, dbc);
}

/* ── SSLMODE ─────────────────────────────────────────────────── */

static void test_sslmode_passthrough(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;

    /*
     * SSLMODE is handed to libpq verbatim, which is the only way to express
     * `prefer` — SSL/SSLVerify can say "off", "encrypted" or "verified" and
     * nothing in between. `prefer` must connect against a server with TLS
     * disabled, where `require` would fail.
     */
    open_with(&env, &dbc, "SSLMODE=prefer");
    char buf[64];
    scalar_str(dbc, "SELECT 1::text", buf, sizeof(buf));
    assert_string_equal(buf, "1");
    close_conn(env, dbc);

    open_with(&env, &dbc, "SSLMODE=disable");
    scalar_str(dbc, "SELECT 1::text", buf, sizeof(buf));
    assert_string_equal(buf, "1");
    close_conn(env, dbc);
}

/* ── ROWVERSIONING ───────────────────────────────────────────── */

static long count_rowver(SQLHDBC dbc)
{
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    assert_int_equal(SQLSpecialColumns(s, SQL_ROWVER, NULL, 0,
                                       (SQLCHAR *)"argus_test", SQL_NTS,
                                       (SQLCHAR *)"customers", SQL_NTS,
                                       SQL_SCOPE_TRANSACTION, SQL_NO_NULLS),
                     SQL_SUCCESS);
    long n = 0;
    while (SQLFetch(s) == SQL_SUCCESS) n++;
    SQLFreeHandle(SQL_HANDLE_STMT, s);
    return n;
}

static void test_row_versioning(void **state)
{
    (void)state;
    SQLHENV e1, e2; SQLHDBC off, on;

    open_with(&e1, &off, NULL);
    open_with(&e2, &on, "ROWVERSIONING=1");

    /* xmin is a real row version but a wrapping 32-bit counter, so it is only
     * offered when asked for. */
    assert_int_equal(count_rowver(off), 0);
    assert_int_equal(count_rowver(on), 1);

    close_conn(e1, off);
    close_conn(e2, on);
}

/* ── SQLRowCount after DML ───────────────────────────────────── */

static void test_row_count_after_dml(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_with(&env, &dbc, NULL);

    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    SQLExecDirect(s, (SQLCHAR *)"DROP TABLE IF EXISTS argus_test.rc_probe",
                  SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(s,
        (SQLCHAR *)"CREATE TABLE argus_test.rc_probe (id integer)", SQL_NTS),
        SQL_SUCCESS);
    /* DDL has no row count: ODBC says -1, not 0. The two mean different
     * things and a tool that reads 0 as "nothing changed" would be misled. */
    SQLLEN rc = 0;
    assert_int_equal(SQLRowCount(s, &rc), SQL_SUCCESS);
    assert_int_equal((int)rc, -1);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    struct { const char *sql; int expect; } cases[] = {
        { "INSERT INTO argus_test.rc_probe "
          "SELECT g FROM generate_series(1, 17) g",                17 },
        { "UPDATE argus_test.rc_probe SET id = id + 100 "
          "WHERE id <= 5",                                          5 },
        { "DELETE FROM argus_test.rc_probe WHERE id > 100",         5 },
        /* A DML that matches nothing is 0, and that is not the same as -1. */
        { "DELETE FROM argus_test.rc_probe WHERE id = 99999",       0 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
        SQLRETURN r = SQLExecDirect(s, (SQLCHAR *)cases[i].sql, SQL_NTS);
        if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
            SQLCHAR st[6] = {0}, msg[512] = {0};
            SQLINTEGER n = 0; SQLSMALLINT l = 0;
            SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, st, &n, msg, sizeof(msg), &l);
            fail_msg("%s: %s %s", cases[i].sql, st, msg);
        }
        rc = -999;
        assert_int_equal(SQLRowCount(s, &rc), SQL_SUCCESS);
        if ((int)rc != cases[i].expect)
            fail_msg("%s: expected SQLRowCount %d, got %d",
                     cases[i].sql, cases[i].expect, (int)rc);
        SQLFreeHandle(SQL_HANDLE_STMT, s);
    }

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &s), SQL_SUCCESS);
    SQLExecDirect(s, (SQLCHAR *)"DROP TABLE argus_test.rc_probe", SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    close_conn(env, dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_show_partitions_is_per_connection),
        cmocka_unit_test(test_show_all_databases),
        cmocka_unit_test(test_schema_enumeration),
        cmocka_unit_test(test_search_path),
        cmocka_unit_test(test_sslmode_passthrough),
        cmocka_unit_test(test_row_versioning),
        cmocka_unit_test(test_row_count_after_dml),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
