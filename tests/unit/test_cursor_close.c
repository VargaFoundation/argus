/*
 * Closing a cursor keeps the statement: SQLFreeStmt(SQL_CLOSE) and
 * SQLCloseCursor drop the result set and nothing else, so the prepared
 * SQL, the parameter and column bindings and the statement attributes are
 * still there for the next SQLExecute. Driven through a fake backend that
 * records the SQL it executes and answers every execution with two rows
 * naming that execution ("e1r1", "e1r2", then "e2r1", ...).
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"

static char g_executed[256];
static int  g_execute_calls;
static int  g_open_ops;

typedef struct {
    int exec_no;
    int fetches;
} fake_op_t;

static int recording_execute(argus_backend_conn_t conn, const char *query,
                             argus_backend_op_t *out_op)
{
    (void)conn;
    snprintf(g_executed, sizeof(g_executed), "%s", query);
    fake_op_t *op = calloc(1, sizeof(*op));
    op->exec_no = ++g_execute_calls;
    g_open_ops++;
    *out_op = (argus_backend_op_t)op;
    return 0;
}

static void close_op(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn;
    g_open_ops--;
    free(op);
}

/* Two rows on the first fetch of an operation, none afterwards. */
static int two_rows(argus_backend_conn_t conn, argus_backend_op_t op_,
                    int max_rows, argus_row_cache_t *cache,
                    argus_column_desc_t *columns, int *num_cols)
{
    (void)conn; (void)max_rows;
    fake_op_t *op = op_;
    snprintf((char *)columns[0].name, sizeof(columns[0].name), "v");
    columns[0].sql_type = SQL_VARCHAR;
    columns[0].column_size = 16;
    *num_cols = 1;
    cache->num_cols = 1;
    if (op->fetches++ > 0) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }
    cache->rows = calloc(2, sizeof(argus_row_t));
    for (int r = 0; r < 2; r++) {
        char buf[16];
        snprintf(buf, sizeof(buf), "e%dr%d", op->exec_no, r + 1);
        cache->rows[r].cells = calloc(1, sizeof(argus_cell_t));
        cache->rows[r].cells[0].data = strdup(buf);
        cache->rows[r].cells[0].data_len = strlen(buf);
    }
    cache->num_rows = 2;
    return 0;
}

static int metadata(argus_backend_conn_t conn, argus_backend_op_t op,
                    argus_column_desc_t *columns, int *num_cols)
{
    (void)conn; (void)op;
    snprintf((char *)columns[0].name, sizeof(columns[0].name), "v");
    columns[0].sql_type = SQL_VARCHAR;
    columns[0].column_size = 16;
    *num_cols = 1;
    return 0;
}

static const argus_backend_t g_backend = {
    .name = "fake",
    .execute = recording_execute,
    .close_operation = close_op,
    .fetch_results = two_rows,
    .get_result_metadata = metadata,
};

typedef struct {
    argus_env_t  *env;
    argus_dbc_t  *dbc;
    argus_stmt_t *stmt;
} fixture_t;

static int setup(void **state)
{
    fixture_t *f = calloc(1, sizeof(*f));
    argus_alloc_env(&f->env);
    f->env->odbc_version = SQL_OV_ODBC3;
    argus_alloc_dbc(f->env, &f->dbc);
    f->dbc->backend = &g_backend;
    f->dbc->backend_conn = (argus_backend_conn_t)(uintptr_t)0xBEEF;
    f->dbc->connected = true;
    argus_alloc_stmt(f->dbc, &f->stmt);
    g_executed[0] = '\0';
    g_execute_calls = 0;
    g_open_ops = 0;
    *state = f;
    return 0;
}

static int teardown(void **state)
{
    fixture_t *f = *state;
    argus_free_stmt(f->stmt);
    assert_int_equal(g_open_ops, 0);
    f->dbc->connected = false;
    f->dbc->backend = NULL;
    f->dbc->backend_conn = NULL;
    argus_free_dbc(f->dbc);
    argus_free_env(f->env);
    free(f);
    return 0;
}

static void expect_state(argus_stmt_t *stmt, const char *sqlstate)
{
    SQLCHAR st[6] = {0}, msg[256];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, (SQLHSTMT)stmt, 1, st,
                                   &native, msg, sizeof(msg), &len),
                     SQL_SUCCESS);
    assert_string_equal((const char *)st, sqlstate);
}

/* ── Tests ─────────────────────────────────────────────────────── */

/* prepare, execute, close, execute: the second execution reuses the SQL,
 * sees the parameter's new value through the same binding, and delivers
 * its rows into the column bound before the first execution. */
static void test_close_keeps_sql_and_bindings(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;
    SQLINTEGER value = 7;
    SQLLEN ind = 0;
    char col[16] = {0};
    SQLLEN col_ind = 0;

    assert_int_equal(SQLBindParameter(h, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
                                      SQL_INTEGER, 0, 0, &value, 0, &ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLBindCol(h, 1, SQL_C_CHAR, col, sizeof(col), &col_ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLPrepare(h, (SQLCHAR *)"SELECT ?", SQL_NTS),
                     SQL_SUCCESS);

    assert_int_equal(SQLExecute(h), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 7");
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_string_equal(col, "e1r1");

    assert_int_equal(SQLCloseCursor(h), SQL_SUCCESS);
    assert_int_equal(g_open_ops, 0);

    value = 8;
    assert_int_equal(SQLExecute(h), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 8");
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_string_equal(col, "e2r1");
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_string_equal(col, "e2r2");
    assert_int_equal(SQLFetch(h), SQL_NO_DATA);

    assert_int_equal(SQLFreeStmt(h, SQL_CLOSE), SQL_SUCCESS);
    value = 9;
    assert_int_equal(SQLExecute(h), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 9");
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_string_equal(col, "e3r1");
    assert_int_equal(g_execute_calls, 3);
}

/* SQLCloseCursor needs an open cursor (24000); SQLFreeStmt(SQL_CLOSE) does
 * not care. Closing twice is the same as closing a fresh statement. */
static void test_close_without_cursor(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;

    assert_int_equal(SQLCloseCursor(h), SQL_ERROR);
    expect_state(f->stmt, "24000");
    assert_int_equal(SQLFreeStmt(h, SQL_CLOSE), SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLCloseCursor(h), SQL_SUCCESS);
    assert_int_equal(SQLCloseCursor(h), SQL_ERROR);
    expect_state(f->stmt, "24000");
    assert_int_equal(SQLFreeStmt(h, SQL_CLOSE), SQL_SUCCESS);
    assert_int_equal(g_open_ops, 0);
}

/* Statement attributes and bindings are not part of the cursor: SQL_CLOSE
 * leaves them; SQL_RESET_PARAMS and SQL_UNBIND are what remove them. */
static void test_close_keeps_attributes_and_bindings(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;
    SQLINTEGER value = 1;
    char col[16];
    SQLLEN col_ind;

    assert_int_equal(SQLSetStmtAttr(h, SQL_ATTR_PARAMSET_SIZE,
                                    (SQLPOINTER)(uintptr_t)3, 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLBindParameter(h, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
                                      SQL_INTEGER, 0, 0, &value, 0, NULL),
                     SQL_SUCCESS);
    assert_int_equal(SQLBindCol(h, 1, SQL_C_CHAR, col, sizeof(col), &col_ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFreeStmt(h, SQL_CLOSE), SQL_SUCCESS);

    SQLULEN paramset = 0;
    assert_int_equal(SQLGetStmtAttr(h, SQL_ATTR_PARAMSET_SIZE, &paramset, 0,
                                    NULL), SQL_SUCCESS);
    assert_int_equal(paramset, 3);
    assert_int_equal(f->stmt->num_param_bindings, 1);
    assert_true(f->stmt->param_bindings[0].bound);
    assert_true(f->stmt->bindings[0].bound);

    assert_int_equal(SQLFreeStmt(h, SQL_RESET_PARAMS), SQL_SUCCESS);
    assert_int_equal(f->stmt->num_param_bindings, 0);
    assert_true(f->stmt->bindings[0].bound);
    assert_int_equal(SQLFreeStmt(h, SQL_UNBIND), SQL_SUCCESS);
    assert_false(f->stmt->bindings[0].bound);
}

/* A static cursor materialises the result on the first SQLFetchScroll.
 * Re-executing without closing the cursor must not keep serving it. */
static void test_reexecution_drops_scroll_cache(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;
    char col[16] = {0};
    SQLLEN col_ind = 0;

    assert_int_equal(SQLSetStmtAttr(h, SQL_ATTR_CURSOR_TYPE,
                                    (SQLPOINTER)SQL_CURSOR_STATIC, 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLBindCol(h, 1, SQL_C_CHAR, col, sizeof(col), &col_ind),
                     SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetchScroll(h, SQL_FETCH_LAST, 0), SQL_SUCCESS);
    assert_string_equal(col, "e1r2");
    assert_true(f->stmt->scroll_cached);

    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_false(f->stmt->scroll_cached);
    assert_int_equal(SQLFetchScroll(h, SQL_FETCH_FIRST, 0), SQL_SUCCESS);
    assert_string_equal(col, "e2r1");
    assert_int_equal(SQLFetchScroll(h, SQL_FETCH_NEXT, 0), SQL_SUCCESS);
    assert_string_equal(col, "e2r2");

    /* And closing the cursor drops it too, keeping the cursor type. */
    assert_int_equal(SQLFreeStmt(h, SQL_CLOSE), SQL_SUCCESS);
    assert_false(f->stmt->scroll_cached);
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetchScroll(h, SQL_FETCH_ABSOLUTE, 2), SQL_SUCCESS);
    assert_string_equal(col, "e3r2");
}

/* SQLGetData progress belongs to the row: a partial read must not carry
 * over to the first row of the next execution. */
static void test_reexecution_resets_getdata_progress(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;
    char buf[3] = {0};
    SQLLEN ind = 0;

    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_int_equal(SQLGetData(h, 1, SQL_C_CHAR, buf, sizeof(buf), &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_string_equal(buf, "e1");

    assert_int_equal(SQLCloseCursor(h), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_int_equal(SQLGetData(h, 1, SQL_C_CHAR, buf, sizeof(buf), &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_string_equal(buf, "e2");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_close_keeps_sql_and_bindings,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_close_without_cursor,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_close_keeps_attributes_and_bindings,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_reexecution_drops_scroll_cache,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_reexecution_resets_getdata_progress,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
