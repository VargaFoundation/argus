#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>

/*
 * Integration tests: Query execution on a real Apache Kudu cluster.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 */

static const char *get_kudu_host(void)
{
    const char *h = getenv("KUDU_HOST");
    return h ? h : "localhost";
}

static int get_kudu_port(void)
{
    const char *p = getenv("KUDU_PORT");
    return p ? atoi(p) : 7051;
}

/* Helper: setup a connected env/dbc */
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
             "HOST=%s;PORT=%d;Backend=kudu;Database=default",
             get_kudu_host(), get_kudu_port());

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

/* ── Test: SQLTables lists Kudu tables ──────────────────────── */

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

/* ── Test: SQLColumns ───────────────────────────────────────── */

static void test_columns(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    /* List columns for all tables */
    SQLRETURN ret = SQLColumns(stmt, NULL, 0, NULL, 0,
                                (SQLCHAR *)"%", SQL_NTS, NULL, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols;
    SQLNumResultCols(stmt, &ncols);
    assert_true(ncols >= 18);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_tables),
        cmocka_unit_test(test_columns),
    };
    return cmocka_run_group_tests(tests, test_setup, test_teardown);
}
