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
 * Integration tests: Execute queries against a real Spark Thrift Server (STS),
 * reached through the Hive backend (HiveServer2-compatible protocol).
 *
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d spark-thrift
 */

static const char *get_spark_host(void) {
    const char *h = getenv("SPARK_HOST");
    return h ? h : "localhost";
}

static int get_spark_port(void) {
    const char *p = getenv("SPARK_PORT");
    return p ? atoi(p) : 10010;
}

static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;

static int setup(void **state)
{
    (void)state;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "BACKEND=hive;HOST=%s;PORT=%d;UID=spark;AuthMech=NOSASL;Database=default",
             get_spark_host(), get_spark_port());

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

/* ── Test: Simple SELECT literal ─────────────────────────────── */

static void test_select_literal(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1 AS num, 'hello' AS msg",
                        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    ret = SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ncols, 2);

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLCHAR buf[64];
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "1");

    ret = SQLGetData(stmt, 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "hello");

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_literal),
    };
    return cmocka_run_group_tests_name("spark_query", tests, setup, teardown);
}
