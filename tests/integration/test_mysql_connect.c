#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>

/*
 * Integration tests: connect to a real MariaDB via the MySQL-wire backend.
 * The same backend serves StarRocks, Doris and ClickHouse.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 *
 * Override with MYSQL_HOST / MYSQL_PORT.
 */

static const char *mysql_host(void)
{
    const char *h = getenv("MYSQL_HOST");
    return h ? h : "localhost";
}

static int mysql_port(void)
{
    const char *p = getenv("MYSQL_PORT");
    return p ? atoi(p) : 3306;
}

static void make_conn_str(char *buf, size_t n)
{
    snprintf(buf, n,
             "HOST=%s;PORT=%d;UID=root;PWD=root123;Backend=mysql;Database=testdb",
             mysql_host(), mysql_port());
}

/* ── Test: connect / disconnect ──────────────────────────────── */

static void test_driver_connect(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    char conn_str[512];
    make_conn_str(conn_str, sizeof(conn_str));

    SQLCHAR out[1024];
    SQLSMALLINT out_len;
    assert_int_equal(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                      out, sizeof(out), &out_len,
                                      SQL_DRIVER_NOPROMPT),
                     SQL_SUCCESS);

    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: disconnect then reconnect on the same handle ──────── */

static void test_double_connect(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    make_conn_str(conn_str, sizeof(conn_str));

    assert_int_equal(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
                     SQL_SUCCESS);
    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    assert_int_equal(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
                     SQL_SUCCESS);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_connect),
        cmocka_unit_test(test_double_connect),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
