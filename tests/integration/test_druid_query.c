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

/*
 * Integration tests: connect to a real Apache Druid through the Druid
 * backend and read from it. The Druid backend had no integration test at
 * all, so nothing exercised its query submission, its result parsing or its
 * catalog SQL against a server that answers.
 *
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d \
 *               druid-zk druid-coordinator druid-historical druid-broker
 *
 * Override with DRUID_HOST / DRUID_PORT (the broker, default localhost:8082).
 */

static const char *druid_host(void)
{
    const char *h = getenv("DRUID_HOST");
    return h ? h : "localhost";
}

static int druid_port(void)
{
    const char *p = getenv("DRUID_PORT");
    return p ? atoi(p) : 8082;
}

static void make_conn_str(char *buf, size_t n)
{
    snprintf(buf, n, "HOST=%s;PORT=%d;Backend=druid",
             druid_host(), druid_port());
}

static SQLHDBC connect_druid(SQLHENV *env)
{
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, *env, &dbc), SQL_SUCCESS);

    char conn_str[512];
    make_conn_str(conn_str, sizeof(conn_str));
    assert_int_equal(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
                     SQL_SUCCESS);
    return dbc;
}

static void disconnect(SQLHENV env, SQLHDBC dbc)
{
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: connect / disconnect ──────────────────────────────── */

static void test_driver_connect(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = connect_druid(&env);
    disconnect(env, dbc);
}

/* A broker that is not there fails at connect rather than at the first
 * query, which is where an application can still do something about it. */
static void test_connect_refused(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str), "HOST=%s;PORT=1;Backend=druid",
             druid_host());
    assert_int_not_equal(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str,
                                          SQL_NTS, NULL, 0, NULL,
                                          SQL_DRIVER_NOPROMPT),
                         SQL_SUCCESS);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: a value comes back, typed and with its length ─────── */

static void test_scalar_query(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = connect_druid(&env);
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    assert_true(SQL_SUCCEEDED(SQLExecDirect(
        stmt, (SQLCHAR *)"SELECT 1 + 1 AS two, 'abc' AS s", SQL_NTS)));

    SQLSMALLINT ncols = 0;
    assert_int_equal(SQLNumResultCols(stmt, &ncols), SQL_SUCCESS);
    assert_int_equal(ncols, 2);

    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    SQLLEN ind = 0;
    SQLINTEGER two = 0;
    assert_true(SQL_SUCCEEDED(SQLGetData(stmt, 1, SQL_C_SLONG, &two,
                                         sizeof(two), &ind)));
    assert_int_equal(two, 2);

    char s[32] = {0};
    assert_true(SQL_SUCCEEDED(SQLGetData(stmt, 2, SQL_C_CHAR, s, sizeof(s),
                                         &ind)));
    assert_string_equal(s, "abc");
    assert_int_equal(ind, 3);

    assert_int_equal(SQLFetch(stmt), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    disconnect(env, dbc);
}

/* A NULL must arrive as NULL, not as a zero or an empty string -- the
 * conversion path this exercises is where that used to be lost. */
static void test_null_is_null(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = connect_druid(&env);
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    assert_true(SQL_SUCCEEDED(SQLExecDirect(
        stmt, (SQLCHAR *)"SELECT CAST(NULL AS VARCHAR) AS n", SQL_NTS)));
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    char buf[32];
    memset(buf, 'x', sizeof(buf));
    SQLLEN ind = 0;
    assert_true(SQL_SUCCEEDED(SQLGetData(stmt, 1, SQL_C_CHAR, buf,
                                         sizeof(buf), &ind)));
    assert_int_equal(ind, SQL_NULL_DATA);
    assert_int_equal(buf[0], 'x');          /* not written */

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    disconnect(env, dbc);
}

/* ── Test: the catalog SQL this backend builds runs on Druid ─── */

static void test_catalog_functions(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = connect_druid(&env);
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    /* SQLTables over INFORMATION_SCHEMA: the query the driver writes has to
     * be one Druid's planner accepts, ESCAPE clause included. */
    assert_true(SQL_SUCCEEDED(SQLTables(stmt, NULL, 0, NULL, 0,
                                        (SQLCHAR *)"%", SQL_NTS, NULL, 0)));
    SQLSMALLINT ncols = 0;
    assert_int_equal(SQLNumResultCols(stmt, &ncols), SQL_SUCCESS);
    assert_int_equal(ncols, 5);
    /* INFORMATION_SCHEMA always has tables of its own, so this is not empty. */
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLFreeStmt(stmt, SQL_CLOSE);

    /* An underscore in an identifier is a wildcard in a pattern; with
     * SQL_ATTR_METADATA_ID it must not be. Both spellings have to run. */
    assert_true(SQL_SUCCEEDED(SQLColumns(stmt, NULL, 0, NULL, 0,
                                         (SQLCHAR *)"TABLES", SQL_NTS,
                                         (SQLCHAR *)"%", SQL_NTS)));
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLFreeStmt(stmt, SQL_CLOSE);

    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_METADATA_ID,
                                    (SQLPOINTER)SQL_TRUE, 0), SQL_SUCCESS);
    assert_true(SQL_SUCCEEDED(SQLTables(stmt, (SQLCHAR *)"druid", SQL_NTS,
                                        (SQLCHAR *)"INFORMATION_SCHEMA",
                                        SQL_NTS,
                                        (SQLCHAR *)"TABLES", SQL_NTS,
                                        NULL, 0)));
    SQLFreeStmt(stmt, SQL_CLOSE);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    disconnect(env, dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_connect),
        cmocka_unit_test(test_connect_refused),
        cmocka_unit_test(test_scalar_query),
        cmocka_unit_test(test_null_is_null),
        cmocka_unit_test(test_catalog_functions),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
