/*
 * Integration test: HOST=h1,h2 failover against a live Trino.
 *
 * The first host is a closed local port (connection refused, fast); the
 * driver must fail over to the real Trino within the same attempt and the
 * connection must be fully usable. Also asserts the single-host form still
 * behaves exactly as before.
 *
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 * TRINO_HOST / TRINO_PORT override the defaults.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static const char *trino_host(void)
{
    const char *h = getenv("TRINO_HOST");
    return h ? h : "localhost";
}

static int trino_port(void)
{
    const char *p = getenv("TRINO_PORT");
    return p ? atoi(p) : 8080;
}

static SQLHDBC connect_with(SQLHENV *env_out, const char *conn_str)
{
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    SQLCHAR out[1024];
    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                     out, sizeof(out), &out_len,
                                     SQL_DRIVER_NOPROMPT);
    assert_true(SQL_SUCCEEDED(ret));
    *env_out = env;
    return dbc;
}

static void run_select_1(SQLHDBC dbc)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_true(SQL_SUCCEEDED(
        SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS)));
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

static void test_failover_to_second_host(void **state)
{
    (void)state;
    char conn_str[512];
    /* 127.0.0.1:59999 refuses instantly; the second entry is the real one. */
    snprintf(conn_str, sizeof(conn_str),
             "HOST=127.0.0.1:59999,%s:%d;UID=test;Backend=trino;Database=memory",
             trino_host(), trino_port());

    SQLHENV env;
    SQLHDBC dbc = connect_with(&env, conn_str);
    run_select_1(dbc);
    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_single_host_unchanged(void **state)
{
    (void)state;
    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=trino;Database=memory",
             trino_host(), trino_port());

    SQLHENV env;
    SQLHDBC dbc = connect_with(&env, conn_str);
    run_select_1(dbc);
    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_all_hosts_down(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    SQLRETURN ret = SQLDriverConnect(
        dbc, NULL,
        (SQLCHAR *)"HOST=127.0.0.1:59998,127.0.0.1:59999;UID=t;Backend=trino",
        SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_ERROR);

    SQLCHAR sqlstate[6] = {0}, msg[256] = {0};
    SQLINTEGER native = 0;
    SQLSMALLINT mlen = 0;
    assert_true(SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlstate,
                                            &native, msg, sizeof(msg), &mlen)));
    assert_string_equal((char *)sqlstate, "08001");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_failover_to_second_host),
        cmocka_unit_test(test_single_host_unchanged),
        cmocka_unit_test(test_all_hosts_down),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
