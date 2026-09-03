/*
 * SQLFetch must never report a backend failure as SQL_NO_DATA.
 *
 * A dropped connection or a server-side error while fetching the first row
 * of a rowset used to come back as SQL_NO_DATA: BI tools then treated a
 * truncated result as a complete one. These tests drive SQLFetch through a
 * fake backend whose fetch_results() fails, and check that the error and its
 * diagnostic record reach the application.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdlib.h>
#include <string.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"

static int g_fetch_calls;

/* fetch_results() that fails on every call, without setting a diagnostic:
 * the driver must supply one itself. */
static int failing_fetch(argus_backend_conn_t conn, argus_backend_op_t op,
                         int max_rows, argus_row_cache_t *cache,
                         argus_column_desc_t *columns, int *num_cols)
{
    (void)conn; (void)op; (void)max_rows; (void)cache; (void)columns;
    (void)num_cols;
    g_fetch_calls++;
    return -1;
}

/* fetch_results() that returns a single one-column row on the first call
 * and fails on the second, like a connection dropping mid-result. */
static int fetch_one_row_then_fail(argus_backend_conn_t conn,
                                   argus_backend_op_t op, int max_rows,
                                   argus_row_cache_t *cache,
                                   argus_column_desc_t *columns,
                                   int *num_cols)
{
    (void)conn; (void)op; (void)max_rows; (void)columns;
    if (g_fetch_calls++ > 0)
        return -1;

    cache->rows = calloc(1, sizeof(argus_row_t));
    cache->rows[0].cells = calloc(1, sizeof(argus_cell_t));
    cache->rows[0].cells[0].data = strdup("42");
    cache->rows[0].cells[0].data_len = 2;
    cache->num_rows = 1;
    cache->num_cols = 1;
    *num_cols = 1;
    return 0;
}

/* fetch_results() reporting a genuinely empty result set. */
static int empty_fetch(argus_backend_conn_t conn, argus_backend_op_t op,
                       int max_rows, argus_row_cache_t *cache,
                       argus_column_desc_t *columns, int *num_cols)
{
    (void)conn; (void)op; (void)max_rows; (void)cache; (void)columns;
    g_fetch_calls++;
    *num_cols = 1;
    return 0;
}

static argus_stmt_t *setup_executed_stmt(const argus_backend_t *backend,
                                         argus_dbc_t **out_dbc)
{
    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);
    dbc->backend = backend;
    dbc->backend_conn = (argus_backend_conn_t)(uintptr_t)0xBEEF;
    dbc->connected = true;

    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);
    stmt->executed = true;
    stmt->num_cols = 1;
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->columns[0].column_size = 32;

    g_fetch_calls = 0;
    *out_dbc = dbc;
    return stmt;
}

static void teardown_stmt(argus_stmt_t *stmt, argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    argus_free_stmt(stmt);
    dbc->connected = false;
    dbc->backend = NULL;
    dbc->backend_conn = NULL;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

static void assert_has_sqlstate(argus_stmt_t *stmt, const char *sqlstate)
{
    SQLCHAR state[6] = {0};
    SQLINTEGER native = 0;
    SQLCHAR msg[256] = {0};
    SQLSMALLINT msg_len = 0;
    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_STMT, (SQLHSTMT)stmt, 1, state,
                                  &native, msg, sizeof(msg), &msg_len);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((const char *)state, sqlstate);
}

/* ── Test: backend failure on the first row is SQL_ERROR ─────── */

static void test_fetch_first_row_failure_is_error(void **state)
{
    (void)state;
    static const argus_backend_t backend = {
        .name = "fake",
        .fetch_results = failing_fetch,
    };

    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_executed_stmt(&backend, &dbc);

    SQLRETURN ret = SQLFetch((SQLHSTMT)stmt);
    assert_int_equal(ret, SQL_ERROR);
    assert_int_equal(g_fetch_calls, 1);
    assert_has_sqlstate(stmt, "HY000");

    teardown_stmt(stmt, dbc);
}

/* ── Test: block cursor, failure on the first row of the rowset ─ */

static void test_fetch_block_cursor_failure_is_error(void **state)
{
    (void)state;
    static const argus_backend_t backend = {
        .name = "fake",
        .fetch_results = failing_fetch,
    };

    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_executed_stmt(&backend, &dbc);

    SQLUSMALLINT row_status[4] = {0};
    SQLULEN rows_fetched = 99;
    stmt->row_array_size = 4;
    stmt->row_status_ptr = row_status;
    stmt->rows_fetched_ptr = &rows_fetched;

    SQLRETURN ret = SQLFetch((SQLHSTMT)stmt);
    assert_int_equal(ret, SQL_ERROR);
    assert_int_equal(rows_fetched, 0);
    assert_int_equal(row_status[0], SQL_ROW_ERROR);
    assert_int_equal(row_status[1], SQL_ROW_NOROW);

    teardown_stmt(stmt, dbc);
}

/* ── Test: failure after a delivered row is still reported ────── */

static void test_fetch_failure_after_first_batch(void **state)
{
    (void)state;
    static const argus_backend_t backend = {
        .name = "fake",
        .fetch_results = fetch_one_row_then_fail,
    };

    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_executed_stmt(&backend, &dbc);
    dbc->fetch_buffer_size = 1;

    char buf[32];
    SQLLEN ind = 0;
    SQLRETURN ret = SQLBindCol((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf,
                               sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFetch((SQLHSTMT)stmt);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "42");

    /* The second batch fails: the application must see the error, not
     * a clean end of data. */
    ret = SQLFetch((SQLHSTMT)stmt);
    assert_int_equal(ret, SQL_ERROR);
    assert_int_equal(g_fetch_calls, 2);
    assert_has_sqlstate(stmt, "HY000");

    teardown_stmt(stmt, dbc);
}

/* ── Test: an empty result set is still SQL_NO_DATA ──────────── */

static void test_fetch_empty_result_is_no_data(void **state)
{
    (void)state;
    static const argus_backend_t backend = {
        .name = "fake",
        .fetch_results = empty_fetch,
    };

    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_executed_stmt(&backend, &dbc);

    SQLRETURN ret = SQLFetch((SQLHSTMT)stmt);
    assert_int_equal(ret, SQL_NO_DATA);
    assert_int_equal(stmt->diag.count, 0);

    /* Subsequent calls stay at SQL_NO_DATA without hitting the backend. */
    ret = SQLFetch((SQLHSTMT)stmt);
    assert_int_equal(ret, SQL_NO_DATA);
    assert_int_equal(g_fetch_calls, 1);

    teardown_stmt(stmt, dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_fetch_first_row_failure_is_error),
        cmocka_unit_test(test_fetch_block_cursor_failure_is_error),
        cmocka_unit_test(test_fetch_failure_after_first_batch),
        cmocka_unit_test(test_fetch_empty_result_is_no_data),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
