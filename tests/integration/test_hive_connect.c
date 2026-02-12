#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>

/*
 * Integration tests: Connect to a real HiveServer2 instance.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 *
 * Set HIVE_HOST and HIVE_PORT environment variables to override defaults.
 */

static const char *get_hive_host(void) {
    const char *h = getenv("HIVE_HOST");
    return h ? h : "localhost";
}

static int get_hive_port(void) {
    const char *p = getenv("HIVE_PORT");
    return p ? atoi(p) : 10000;
}

/* ── Test: Connect and disconnect via SQLDriverConnect ───────── */

static void test_driver_connect(void **state)
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

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=hive;AuthMech=NOSASL;Database=default",
             get_hive_host(), get_hive_port());

    SQLCHAR out_conn[1024];
    SQLSMALLINT out_len;

    ret = SQLDriverConnect(dbc, NULL,
                            (SQLCHAR *)conn_str, SQL_NTS,
                            out_conn, sizeof(out_conn), &out_len,
                            SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLDisconnect(dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    assert_int_equal(ret, SQL_SUCCESS);
}

/* ── Test: Double connect fails ──────────────────────────────── */

static void test_double_connect(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;AuthMech=NOSASL",
             get_hive_host(), get_hive_port());

    SQLRETURN ret = SQLDriverConnect(dbc, NULL,
                                      (SQLCHAR *)conn_str, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Second connect should fail */
    ret = SQLDriverConnect(dbc, NULL,
                            (SQLCHAR *)conn_str, SQL_NTS,
                            NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_ERROR);

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: Alloc stmt on connected DBC ───────────────────────── */

static void test_alloc_stmt(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLHSTMT stmt = SQL_NULL_HSTMT;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;AuthMech=NOSASL",
             get_hive_host(), get_hive_port());

    SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_non_null(stmt);

    ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_connect),
        cmocka_unit_test(test_double_connect),
        cmocka_unit_test(test_alloc_stmt),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
