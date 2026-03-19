#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>

/*
 * Integration tests: Query execution on a real Phoenix Query Server.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 */

static const char *get_phoenix_host(void)
{
    const char *h = getenv("PHOENIX_HOST");
    return h ? h : "localhost";
}

static int get_phoenix_port(void)
{
    const char *p = getenv("PHOENIX_PORT");
    return p ? atoi(p) : 8765;
}

/* Helper: setup a connected env/dbc/stmt */
static SQLHENV g_env;
static SQLHDBC g_dbc;

static int test_setup(void **state)
{
    (void)state;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=phoenix",
             get_phoenix_host(), get_phoenix_port());

    SQLRETURN ret = SQLDriverConnect(g_dbc, NULL,
                                     (SQLCHAR *)conn_str, SQL_NTS,
                                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS) return -1;
    return 0;
}

static int test_teardown(void **state)
{
    (void)state;
    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

/* ── Test: Execute a simple SELECT ──────────────────────────── */

static void test_select_one(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1 AS val", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols;
    ret = SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(ncols >= 1);

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLINTEGER val;
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_SLONG, &val, sizeof(val), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(val, 1);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLTables ────────────────────────────────────────── */

static void test_tables(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols;
    SQLNumResultCols(stmt, &ncols);
    assert_true(ncols >= 5);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_one),
        cmocka_unit_test(test_tables),
    };
    return cmocka_run_group_tests(tests, test_setup, test_teardown);
}
