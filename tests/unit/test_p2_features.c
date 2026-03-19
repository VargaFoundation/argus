/*
 * Unit tests for P2 features:
 *   - Configurable pool limits
 *   - SQLBrowseConnect
 *   - Async execution state machine
 *   - SQLParamData / SQLPutData (data-at-execution)
 *   - Scrollable cursors
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "argus/handle.h"

/* ── Helpers ─────────────────────────────────────────────────── */

static argus_stmt_t *create_test_stmt(void)
{
    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    if (!stmt) return NULL;
    stmt->signature = ARGUS_STMT_SIGNATURE;
    g_mutex_init(&stmt->mutex);
    argus_row_cache_init(&stmt->row_cache);
    stmt->row_array_size = 1;
    stmt->paramset_size = 1;
    return stmt;
}

static void free_test_stmt(argus_stmt_t *stmt)
{
    if (!stmt) return;
    free(stmt->query);
    free(stmt->async_query);
    if (stmt->dae_buffer)
        g_byte_array_free(stmt->dae_buffer, TRUE);
    if (stmt->scroll_rows) {
        int nc = stmt->num_cols > 0 ? stmt->num_cols
                                     : stmt->row_cache.num_cols;
        for (size_t i = 0; i < stmt->scroll_row_count; i++) {
            argus_row_t *row = &stmt->scroll_rows[i];
            if (row->cells) {
                for (int c = 0; c < nc; c++)
                    free(row->cells[c].data);
                free(row->cells);
            }
        }
        free(stmt->scroll_rows);
    }
    argus_row_cache_free(&stmt->row_cache);
    free(stmt->columns);
    free(stmt->bindings);
    g_mutex_clear(&stmt->mutex);
    stmt->signature = 0;
    free(stmt);
}

/* ──────────────────────────────────────────────────────────────
 * P2-1: Pool limits
 * ────────────────────────────────────────────────────────────── */

static void test_pool_default_config(void **state)
{
    (void)state;

    int mpk = 0, mt = 0, idle = 0, ttl = 0;
    argus_pool_get_config(&mpk, &mt, &idle, &ttl);

    assert_true(mpk > 0);
    assert_true(mt > 0);
    assert_true(idle > 0);
    assert_true(ttl > 0);
}

static void test_pool_configure(void **state)
{
    (void)state;

    /* Save original values */
    int orig_mpk, orig_mt, orig_idle, orig_ttl;
    argus_pool_get_config(&orig_mpk, &orig_mt, &orig_idle, &orig_ttl);

    /* Configure new values */
    argus_pool_configure(4, 32, 120, 1800);

    int mpk = 0, mt = 0, idle = 0, ttl = 0;
    argus_pool_get_config(&mpk, &mt, &idle, &ttl);

    assert_int_equal(mpk, 4);
    assert_int_equal(mt, 32);
    assert_int_equal(idle, 120);
    assert_int_equal(ttl, 1800);

    /* Restore original values */
    argus_pool_configure(orig_mpk, orig_mt, orig_idle, orig_ttl);
}

/* ──────────────────────────────────────────────────────────────
 * P2-2: SQLBrowseConnect
 * ────────────────────────────────────────────────────────────── */

static void test_browse_connect_need_data(void **state)
{
    (void)state;

    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    SQLSetEnvAttr((SQLHENV)env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);

    /* First call: only HOST specified, PORT and BACKEND missing */
    SQLCHAR out[512];
    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLBrowseConnect(
        (SQLHDBC)dbc,
        (SQLCHAR *)"HOST=myserver", SQL_NTS,
        out, sizeof(out), &out_len);

    assert_int_equal(ret, SQL_NEED_DATA);
    assert_true(out_len > 0);
    /* Output should mention PORT and BACKEND */
    assert_non_null(strstr((char *)out, "PORT"));
    assert_non_null(strstr((char *)out, "BACKEND"));

    /* Clean up */
    free(dbc->browse_buf);
    dbc->browse_buf = NULL;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

static void test_browse_connect_iterative(void **state)
{
    (void)state;

    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    SQLSetEnvAttr((SQLHENV)env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);

    SQLCHAR out[512];
    SQLSMALLINT out_len = 0;

    /* First call: only HOST */
    SQLRETURN ret = SQLBrowseConnect(
        (SQLHDBC)dbc,
        (SQLCHAR *)"HOST=myserver", SQL_NTS,
        out, sizeof(out), &out_len);
    assert_int_equal(ret, SQL_NEED_DATA);

    /* Second call: add PORT but still missing BACKEND */
    ret = SQLBrowseConnect(
        (SQLHDBC)dbc,
        (SQLCHAR *)"PORT=8080", SQL_NTS,
        out, sizeof(out), &out_len);
    assert_int_equal(ret, SQL_NEED_DATA);
    assert_non_null(strstr((char *)out, "BACKEND"));
    /* PORT should not be listed as missing anymore */
    assert_null(strstr((char *)out, "PORT"));

    /* Clean up browse state */
    free(dbc->browse_buf);
    dbc->browse_buf = NULL;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ──────────────────────────────────────────────────────────────
 * P2-3: Async execution
 * ────────────────────────────────────────────────────────────── */

static void test_async_attr_store(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    /* Default: async disabled */
    SQLULEN val = 999;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_ASYNC_ENABLE,
                   &val, 0, NULL);
    assert_int_equal(val, SQL_ASYNC_ENABLE_OFF);

    /* Enable async */
    SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_ASYNC_ENABLE,
                   (SQLPOINTER)(uintptr_t)SQL_ASYNC_ENABLE_ON, 0);

    val = 0;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_ASYNC_ENABLE,
                   &val, 0, NULL);
    assert_int_equal(val, SQL_ASYNC_ENABLE_ON);
    assert_true(stmt->async_enabled);

    /* Disable async */
    SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_ASYNC_ENABLE,
                   (SQLPOINTER)(uintptr_t)SQL_ASYNC_ENABLE_OFF, 0);
    assert_false(stmt->async_enabled);

    free_test_stmt(stmt);
}

static void test_async_state_machine(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    /* Verify initial state */
    assert_int_equal(stmt->async_state, ARGUS_ASYNC_IDLE);

    /* Simulate state transitions */
    stmt->async_state = ARGUS_ASYNC_SUBMITTED;
    assert_int_equal(stmt->async_state, ARGUS_ASYNC_SUBMITTED);

    stmt->async_state = ARGUS_ASYNC_RUNNING;
    assert_int_equal(stmt->async_state, ARGUS_ASYNC_RUNNING);

    stmt->async_state = ARGUS_ASYNC_DONE;
    assert_int_equal(stmt->async_state, ARGUS_ASYNC_DONE);

    /* Test error state */
    stmt->async_state = ARGUS_ASYNC_ERROR;
    assert_int_equal(stmt->async_state, ARGUS_ASYNC_ERROR);

    /* Reset via cancel */
    stmt->async_state = ARGUS_ASYNC_SUBMITTED;
    SQLCancel((SQLHSTMT)stmt);
    assert_int_equal(stmt->async_state, ARGUS_ASYNC_IDLE);

    free_test_stmt(stmt);
}

/* ──────────────────────────────────────────────────────────────
 * P2-4: SQLParamData / SQLPutData
 * ────────────────────────────────────────────────────────────── */

static void test_putdata_accumulation(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    /* Set up a data-at-execution parameter */
    SQLLEN dae_ind = SQL_DATA_AT_EXEC;
    char user_ptr = 'X'; /* sentinel for identification */

    stmt->param_bindings[0].bound = true;
    stmt->param_bindings[0].value_type = SQL_C_CHAR;
    stmt->param_bindings[0].param_type = SQL_VARCHAR;
    stmt->param_bindings[0].value = &user_ptr;
    stmt->param_bindings[0].str_len_or_ind = &dae_ind;
    stmt->num_param_bindings = 1;

    /* Simulate the DAE flow: SQLExecute would return SQL_NEED_DATA,
     * but we can't call it without a backend, so test PutData directly */
    stmt->dae_state = ARGUS_DAE_PUTTING;
    stmt->dae_current_param = 0;

    /* Send 3 chunks of data via SQLPutData */
    SQLRETURN ret;
    ret = SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"Hello", 5);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)", ", 2);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"World", 5);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Verify accumulated data */
    assert_non_null(stmt->dae_buffer);
    assert_int_equal(stmt->dae_buffer->len, 12);
    assert_memory_equal(stmt->dae_buffer->data, "Hello, World", 12);

    free_test_stmt(stmt);
}

static void test_putdata_null(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    stmt->dae_state = ARGUS_DAE_PUTTING;
    stmt->dae_current_param = 0;
    stmt->param_bindings[0].bound = true;
    stmt->num_param_bindings = 1;

    /* Send NULL data */
    SQLRETURN ret = SQLPutData((SQLHSTMT)stmt, NULL, SQL_NULL_DATA);
    assert_int_equal(ret, SQL_SUCCESS);

    /* str_len_or_ind should be set to SQL_NULL_DATA */
    assert_non_null(stmt->param_bindings[0].str_len_or_ind);
    assert_int_equal(*stmt->param_bindings[0].str_len_or_ind, SQL_NULL_DATA);

    free_test_stmt(stmt);
}

static void test_putdata_sequence_error(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    /* PutData without being in DAE state should fail */
    stmt->dae_state = ARGUS_DAE_IDLE;
    SQLRETURN ret = SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"data", 4);
    assert_int_equal(ret, SQL_ERROR);

    free_test_stmt(stmt);
}

static void test_paramdata_sequence_error(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    /* ParamData without being in DAE state should fail */
    stmt->dae_state = ARGUS_DAE_IDLE;
    SQLPOINTER val = NULL;
    SQLRETURN ret = SQLParamData((SQLHSTMT)stmt, &val);
    assert_int_equal(ret, SQL_ERROR);

    free_test_stmt(stmt);
}

/* ──────────────────────────────────────────────────────────────
 * P2-5: Scrollable cursors
 * ────────────────────────────────────────────────────────────── */

static void test_cursor_type_attr(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    /* Default: forward only */
    SQLULEN ct = 999;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_CURSOR_TYPE,
                   &ct, 0, NULL);
    assert_int_equal(ct, SQL_CURSOR_FORWARD_ONLY);

    /* Set to static */
    SQLRETURN ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_CURSOR_TYPE,
                   (SQLPOINTER)(uintptr_t)SQL_CURSOR_STATIC, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    ct = 0;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_CURSOR_TYPE,
                   &ct, 0, NULL);
    assert_int_equal(ct, SQL_CURSOR_STATIC);

    /* Set to keyset-driven (should downgrade to static with info) */
    ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_CURSOR_TYPE,
                   (SQLPOINTER)(uintptr_t)SQL_CURSOR_KEYSET_DRIVEN, 0);
    assert_int_equal(ret, SQL_SUCCESS_WITH_INFO);

    ct = 0;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_CURSOR_TYPE,
                   &ct, 0, NULL);
    assert_int_equal(ct, SQL_CURSOR_STATIC);

    free_test_stmt(stmt);
}

static void test_scroll_cache_navigation(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    stmt->cursor_type = SQL_CURSOR_STATIC;
    stmt->executed = true;

    /* Build a mock scroll cache with 5 rows, 1 column */
    stmt->num_cols = 1;
    stmt->scroll_row_count = 5;
    stmt->scroll_rows = calloc(5, sizeof(argus_row_t));
    assert_non_null(stmt->scroll_rows);

    for (size_t i = 0; i < 5; i++) {
        stmt->scroll_rows[i].cells = calloc(1, sizeof(argus_cell_t));
        char buf[16];
        snprintf(buf, sizeof(buf), "row%zu", i);
        stmt->scroll_rows[i].cells[0].data = strdup(buf);
        stmt->scroll_rows[i].cells[0].data_len = strlen(buf);
        stmt->scroll_rows[i].cells[0].is_null = false;
    }
    stmt->scroll_cached = true;
    stmt->scroll_position = 0;

    /* Set up column binding */
    argus_stmt_ensure_bindings(stmt, 1);
    char buf[64];
    SQLLEN ind;
    stmt->bindings[0].target_type = SQL_C_CHAR;
    stmt->bindings[0].target_value = buf;
    stmt->bindings[0].buffer_length = sizeof(buf);
    stmt->bindings[0].str_len_or_ind = &ind;
    stmt->bindings[0].bound = true;

    SQLRETURN ret;

    /* FETCH_FIRST */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_FIRST, 0);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row0");

    /* FETCH_NEXT */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_NEXT, 0);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row1");

    /* FETCH_LAST */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_LAST, 0);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row4");

    /* FETCH_PRIOR from last */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_PRIOR, 0);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row3");

    /* FETCH_ABSOLUTE(3) -> row2 (1-based) */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_ABSOLUTE, 3);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row2");

    /* FETCH_RELATIVE(-1) -> row1 */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_RELATIVE, -1);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row1");

    /* FETCH_ABSOLUTE(-1) -> last row (row4) */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_ABSOLUTE, -1);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "row4");

    /* FETCH_PRIOR from first -> NO_DATA */
    stmt->scroll_position = 1; /* at row0+1 */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_PRIOR, 0);
    assert_int_equal(ret, SQL_NO_DATA);

    /* FETCH_BOOKMARK -> unsupported */
    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_BOOKMARK, 0);
    assert_int_equal(ret, SQL_ERROR);

    free_test_stmt(stmt);
}

static void test_forward_only_rejects_scroll(void **state)
{
    (void)state;

    argus_stmt_t *stmt = create_test_stmt();
    assert_non_null(stmt);

    stmt->cursor_type = SQL_CURSOR_FORWARD_ONLY;
    stmt->executed = true;

    /* Non-NEXT orientations should fail on forward-only cursor */
    SQLRETURN ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_FIRST, 0);
    assert_int_equal(ret, SQL_ERROR);

    ret = SQLFetchScroll((SQLHSTMT)stmt, SQL_FETCH_PRIOR, 0);
    assert_int_equal(ret, SQL_ERROR);

    free_test_stmt(stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        /* P2-1: Pool limits */
        cmocka_unit_test(test_pool_default_config),
        cmocka_unit_test(test_pool_configure),
        /* P2-2: SQLBrowseConnect */
        cmocka_unit_test(test_browse_connect_need_data),
        cmocka_unit_test(test_browse_connect_iterative),
        /* P2-3: Async execution */
        cmocka_unit_test(test_async_attr_store),
        cmocka_unit_test(test_async_state_machine),
        /* P2-4: SQLParamData/SQLPutData */
        cmocka_unit_test(test_putdata_accumulation),
        cmocka_unit_test(test_putdata_null),
        cmocka_unit_test(test_putdata_sequence_error),
        cmocka_unit_test(test_paramdata_sequence_error),
        /* P2-5: Scrollable cursors */
        cmocka_unit_test(test_cursor_type_attr),
        cmocka_unit_test(test_scroll_cache_navigation),
        cmocka_unit_test(test_forward_only_rejects_scroll),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
