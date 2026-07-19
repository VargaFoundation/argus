#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

/*
 * Statement-level asynchronous execution.
 *
 * The driver runs the backend execute on a worker thread, so SQLExecDirect /
 * SQLExecute return SQL_STILL_EXECUTING immediately and the application polls
 * (or blocks with SQLCompleteAsync). This is backend-agnostic, so the driver
 * advertises SQL_ASYNC_MODE = SQL_AM_STATEMENT. These tests prove it actually
 * completes and produces the right result — the opposite of the old guard that
 * documented the (now fixed) never-completes behaviour.
 *
 * Defaults target Trino. Override with BI_HOST/BI_PORT/BI_BACKEND/BI_DATABASE.
 */

/* ODBC 3.8; absent from some unixODBC headers. Identical redeclaration is
 * harmless if the header already provides it. */
SQLRETURN SQL_API SQLCompleteAsync(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   RETCODE *AsyncRetCodePtr);

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

/* Async is now honestly advertised at statement level. */
static void test_async_mode_is_statement(void **state)
{
    (void)state;
    SQLUINTEGER mode = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_ASYNC_MODE, &mode, sizeof(mode), NULL),
                     SQL_SUCCESS);
    assert_int_equal(mode, SQL_AM_STATEMENT);
}

/*
 * With SQL_ATTR_ASYNC_ENABLE on, the first SQLExecDirect returns immediately
 * with SQL_STILL_EXECUTING (the execute is on a worker thread); re-calling it
 * polls, and it completes with SUCCESS. The result is then readable.
 */
static void test_async_execute_completes(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0), SQL_SUCCESS);

    /* Poll the way a real ODBC app does: re-call, waiting a little between
     * tries, until it completes (bounded so a hung backend fails the test). */
    SQLRETURN r = SQL_STILL_EXECUTING;
    int polls = 0;
    while (r == SQL_STILL_EXECUTING && polls < 30000) {   /* up to ~30s */
        r = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
        polls++;
        if (r == SQL_STILL_EXECUTING) usleep(1000);       /* 1 ms */
    }

    /* It really ran asynchronously (returned STILL_EXECUTING at least once) and
     * then completed. */
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    assert_true(polls > 1);

    /* The result is correct. Read it synchronously. */
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_OFF, 0), SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    assert_int_equal(SQLNumResultCols(stmt, &ncols), SQL_SUCCESS);
    assert_int_equal(ncols, 1);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLINTEGER v = 0;
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData(stmt, 1, SQL_C_SLONG, &v, sizeof(v), &ind), SQL_SUCCESS);
    assert_int_equal(v, 1);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/*
 * SQLCompleteAsync blocks for an outstanding async operation and hands back its
 * return code.
 */
static void test_complete_async_blocks_for_result(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0), SQL_SUCCESS);

    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
    assert_int_equal(r, SQL_STILL_EXECUTING);

    RETCODE async_rc = SQL_ERROR;
    assert_int_equal(SQLCompleteAsync(SQL_HANDLE_STMT, stmt, &async_rc), SQL_SUCCESS);
    assert_true(async_rc == SQL_SUCCESS || async_rc == SQL_SUCCESS_WITH_INFO);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* SQLCancelHandle on a statement is accepted (maps to SQLCancel). */
static void test_cancel_handle_on_statement(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    SQLRETURN r = SQLCancelHandle(SQL_HANDLE_STMT, stmt);
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_async_mode_is_statement),
        cmocka_unit_test(test_complete_async_blocks_for_result),
        cmocka_unit_test(test_async_execute_completes),
        cmocka_unit_test(test_cancel_handle_on_statement),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
