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
 * Async execution status + SQLCancelHandle.
 *
 * The driver reports SQL_ASYNC_MODE = SQL_AM_NONE. This is deliberate and this
 * test is the guard: the statement-async scaffolding exists, but the backends'
 * get_operation_status is passive (it never advances the Trino query, which
 * only progresses when its nextUri is polled during fetch), so an async execute
 * returns SQL_STILL_EXECUTING forever. Advertising SQL_AM_STATEMENT would make
 * BI tools hang. This test asserts the honest NONE and demonstrates the hang,
 * so nobody re-advertises async without first making the operation advance.
 *
 * Defaults target Trino. Override with BI_HOST/BI_PORT/BI_BACKEND/BI_DATABASE.
 */

static const char *env_or(const char *n, const char *d)
{
    const char *v = getenv(n);
    return (v && *v) ? v : d;
}

static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;

static int setup(void **state)
{
    (void)state;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char conn[512];
    snprintf(conn, sizeof(conn), "HOST=%s;PORT=%s;UID=test;Backend=%s;Database=%s",
             env_or("BI_HOST", "localhost"), env_or("BI_PORT", "8080"),
             env_or("BI_BACKEND", "trino"), env_or("BI_DATABASE", "tpch"));

    if (SQLDriverConnect(g_dbc, NULL, (SQLCHAR *)conn, SQL_NTS,
                         NULL, 0, NULL, SQL_DRIVER_NOPROMPT) != SQL_SUCCESS)
        return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

/* Async is honestly reported NOT available. */
static void test_async_mode_is_none(void **state)
{
    (void)state;
    SQLUINTEGER mode = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_ASYNC_MODE, &mode, sizeof(mode), NULL),
                     SQL_SUCCESS);
    assert_int_equal(mode, SQL_AM_NONE);
}

/*
 * Demonstrate WHY async is not advertised: with SQL_ATTR_ASYNC_ENABLE on, an
 * execute never completes — get_operation_status is passive and the query is
 * never advanced. A bounded number of polls all return SQL_STILL_EXECUTING.
 * This is the evidence behind reporting SQL_AM_NONE; if a future change makes
 * async advance, flip this to assert completion and advertise SQL_AM_STATEMENT.
 */
static void test_async_does_not_complete_yet(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0), SQL_SUCCESS);

    SQLRETURN r = SQL_STILL_EXECUTING;
    for (int i = 0; i < 200 && r == SQL_STILL_EXECUTING; i++)
        r = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS);

    /* Passive status → never finishes. Documents the known limitation. */
    assert_int_equal(r, SQL_STILL_EXECUTING);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* SQLCancelHandle on a statement is accepted (maps to SQLCancel). */
static void test_cancel_handle_on_statement(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    /* Nothing running: cancel is a no-op success (the backend cancel is a
     * best-effort, and there is no active operation to reject). */
    SQLRETURN r = SQLCancelHandle(SQL_HANDLE_STMT, stmt);
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_async_mode_is_none),
        cmocka_unit_test(test_async_does_not_complete_yet),
        cmocka_unit_test(test_cancel_handle_on_statement),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
