#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/*
 * Integration tests: HiveServer2 over HTTP transport with SPNEGO auth.
 *
 * Set HIVE_HOST and HIVE_PORT environment variables.
 * Requires a valid Kerberos TGT (kinit).
 */

static const char *get_hive_host(void) {
    const char *h = getenv("HIVE_HOST");
    return h ? h : "localhost";
}

static int get_hive_port(void) {
    const char *p = getenv("HIVE_PORT");
    return p ? atoi(p) : 10001;
}

static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;

static int setup(void **state)
{
    (void)state;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;HTTPPATH=cliservice;AuthMech=KERBEROS;"
             "Database=default;BACKEND=hive;SSL=1;SSLVERIFY=0",
             get_hive_host(), get_hive_port());

    SQLRETURN ret = SQLDriverConnect(g_dbc, NULL,
                                      (SQLCHAR *)conn_str, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    return (ret == SQL_SUCCESS) ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

/* ── Test: Simple SELECT ─────────────────────────────────────── */

static void test_select_literal(void **state)
{
    (void)state;
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT 42 AS answer, 'hello' AS greeting", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ncols, 2);

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLCHAR buf[64];
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "42");

    ret = SQLGetData(stmt, 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "hello");

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLPrepare + SQLExecute ───────────────────────────── */

static void test_prepare_execute(void **state)
{
    (void)state;
    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLPrepare(stmt,
        (SQLCHAR *)"SELECT 1 + 1 AS result", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecute(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLCHAR buf[32];
    SQLLEN ind;
    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "2");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLTables ─────────────────────────────────────────── */

static void test_tables(void **state)
{
    (void)state;
    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLTables(stmt, NULL, 0,
                               (SQLCHAR *)"%", SQL_NTS,
                               (SQLCHAR *)"%", SQL_NTS,
                               (SQLCHAR *)"TABLE", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_true(ncols >= 5);

    ret = SQLFetch(stmt);
    assert_true(ret == SQL_SUCCESS || ret == SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: Multiple data types ───────────────────────────────── */

static void test_data_types(void **state)
{
    (void)state;
    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT "
                   "CAST(1 AS TINYINT) AS t, "
                   "CAST(2 AS SMALLINT) AS s, "
                   "CAST(3 AS INT) AS i, "
                   "CAST(4 AS BIGINT) AS b, "
                   "CAST(2.5 AS DOUBLE) AS d, "
                   "true AS bo, "
                   "'test_string' AS str",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLCHAR buf[64];
    SQLLEN ind;

    SQLGetData(stmt, 3, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "3");

    SQLGetData(stmt, 7, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "test_string");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_literal),
        cmocka_unit_test(test_prepare_execute),
        cmocka_unit_test(test_tables),
        cmocka_unit_test(test_data_types),
    };
    return cmocka_run_group_tests_name("hive_http", tests, setup, teardown);
}
