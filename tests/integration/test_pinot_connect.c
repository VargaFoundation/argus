#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>

/*
 * Integration tests: connect to a real Apache Pinot via the Pinot backend.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d pinot
 *
 * Override with PINOT_HOST / PINOT_PORT (broker, default localhost:8000).
 */

static const char *pinot_host(void)
{
    const char *h = getenv("PINOT_HOST");
    return h ? h : "localhost";
}

static int pinot_port(void)
{
    const char *p = getenv("PINOT_PORT");
    return p ? atoi(p) : 8000;
}

static void make_conn_str(char *buf, size_t n)
{
    snprintf(buf, n, "HOST=%s;PORT=%d;Backend=pinot", pinot_host(), pinot_port());
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

/* ── Test: a bad broker port is rejected at connect ──────────── */

static void test_connect_refused(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str), "HOST=%s;PORT=1;Backend=pinot",
             pinot_host());

    SQLRETURN rc = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                    NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_int_not_equal(rc, SQL_SUCCESS);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_connect),
        cmocka_unit_test(test_connect_refused),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
