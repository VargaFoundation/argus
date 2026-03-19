#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>

/*
 * Integration tests: TLS connection to Trino.
 * Requires:
 *   1. cd tests/integration/certs && ./generate.sh
 *   2. docker compose -f tests/integration/docker-compose.yml up -d
 *
 * Set TRINO_SSL_HOST / TRINO_SSL_PORT to override defaults.
 */

static const char *get_host(void)
{
    const char *h = getenv("TRINO_SSL_HOST");
    return h ? h : "localhost";
}

static int get_port(void)
{
    const char *p = getenv("TRINO_SSL_PORT");
    return p ? atoi(p) : 8443;
}

static const char *get_ca_file(void)
{
    const char *f = getenv("TRINO_SSL_CA_FILE");
    return f ? f : "tests/integration/certs/ca-cert.pem";
}

/* ── Test: TLS connect with CA verification ──────────────────── */

static void test_ssl_connect(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                        (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    char conn_str[1024];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=trino;"
             "Database=memory;SSL=1;SSL_CA=%s;SSL_VERIFY=1",
             get_host(), get_port(), get_ca_file());

    SQLCHAR out_conn[1024];
    SQLSMALLINT out_len;

    ret = SQLDriverConnect(dbc, NULL,
                           (SQLCHAR *)conn_str, SQL_NTS,
                           out_conn, sizeof(out_conn), &out_len,
                           SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Verify connection works by executing a simple query */
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLINTEGER val = 0;
    ret = SQLGetData(stmt, 1, SQL_C_SLONG, &val, sizeof(val), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(val, 1);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: TLS connect with SSL_VERIFY=0 (skip verification) ── */

static void test_ssl_no_verify(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[1024];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=trino;"
             "Database=memory;SSL=1;SSL_VERIFY=0",
             get_host(), get_port());

    SQLRETURN ret = SQLDriverConnect(dbc, NULL,
                                     (SQLCHAR *)conn_str, SQL_NTS,
                                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: TLS connect fails with wrong CA ───────────────────── */

static void test_ssl_bad_ca_fails(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[1024];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=trino;"
             "Database=memory;SSL=1;SSL_CA=/nonexistent/ca.pem;SSL_VERIFY=1",
             get_host(), get_port());

    SQLRETURN ret = SQLDriverConnect(dbc, NULL,
                                     (SQLCHAR *)conn_str, SQL_NTS,
                                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    /* Should fail — bad CA path */
    assert_true(ret == SQL_ERROR || ret == SQL_SUCCESS_WITH_INFO);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_ssl_connect),
        cmocka_unit_test(test_ssl_no_verify),
        cmocka_unit_test(test_ssl_bad_ca_fails),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
