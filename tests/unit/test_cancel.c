/*
 * SQLCancel from another thread. The call being cancelled (a synchronous
 * SQLExecDirect or SQLFetch, or the asynchronous worker) holds the statement
 * lock, so SQLCancel must return without it and the running call answers
 * HY008 at its next checkpoint, cancelling and dropping the operation on its
 * own thread. Also covers the diagnostics of an asynchronous execute, which
 * the polling call must not clear. Driven through a fake backend that can
 * hold an execute or a fetch until the test releases it.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cmocka.h>
#include <glib.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"

typedef struct { int fetches; } fake_op_t;

static int g_open_ops;
static int g_execute_calls;
static int g_cancel_calls;
static GThread *g_cancel_thread;   /* the thread the backend cancel ran on */
static gboolean g_fail_execute;

/* Hold the next execute / fetch until the test releases it. */
static GMutex   g_lock;
static GCond    g_cond;
static gboolean g_block_execute;
static gboolean g_block_fetch;
static gboolean g_in_call;
static gboolean g_release;

static void block_here(void)
{
    g_mutex_lock(&g_lock);
    g_in_call = TRUE;
    g_cond_broadcast(&g_cond);
    while (!g_release)
        g_cond_wait(&g_cond, &g_lock);
    g_mutex_unlock(&g_lock);
}

static void wait_in_call(void)
{
    g_mutex_lock(&g_lock);
    while (!g_in_call)
        g_cond_wait(&g_cond, &g_lock);
    g_mutex_unlock(&g_lock);
}

static void release(void)
{
    g_mutex_lock(&g_lock);
    g_release = TRUE;
    g_cond_broadcast(&g_cond);
    g_mutex_unlock(&g_lock);
}

static int fake_execute(argus_backend_conn_t conn, const char *query,
                        argus_backend_op_t *out_op)
{
    (void)conn; (void)query;
    g_execute_calls++;
    if (g_block_execute) block_here();
    if (g_fail_execute) return -1;
    g_open_ops++;
    *out_op = (argus_backend_op_t)calloc(1, sizeof(fake_op_t));
    return 0;
}

static void close_op(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn;
    g_open_ops--;
    free(op);
}

static int fake_cancel(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn; (void)op;
    g_cancel_calls++;
    g_cancel_thread = g_thread_self();
    return 0;
}

/* The PostgreSQL model: a cancel sent from another thread reaches the
 * server, and the call blocked on the connection comes back with an error.
 * Here it makes the held execute / fetch fail and releases it. */
static gboolean g_fail_fetch;

static int interrupting_cancel(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn;
    assert_null(op);   /* sent while the call runs: there is no op yet */
    g_cancel_calls++;
    g_cancel_thread = g_thread_self();
    g_fail_execute = TRUE;
    g_fail_fetch = TRUE;
    release();
    return 0;
}

/* One row per fetch, three in all; the first fetch can be held. */
static int one_row(argus_backend_conn_t conn, argus_backend_op_t op_,
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
    if (op->fetches == 0 && g_block_fetch) block_here();
    if (g_fail_fetch) return -1;
    if (op->fetches++ >= 3) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }
    char buf[16];
    snprintf(buf, sizeof(buf), "r%d", op->fetches);
    cache->rows = calloc(1, sizeof(argus_row_t));
    cache->rows[0].cells = calloc(1, sizeof(argus_cell_t));
    cache->rows[0].cells[0].data = strdup(buf);
    cache->rows[0].cells[0].data_len = strlen(buf);
    cache->num_rows = 1;
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

/* Consulted after every execute as well (asynchronous backends surface
 * their errors there), so it reports one only when the execute failed. */
static bool last_error(argus_backend_conn_t conn, char *buf, size_t buflen)
{
    (void)conn;
    if (!g_fail_execute) return false;
    snprintf(buf, buflen, "boom");
    return true;
}

static const argus_backend_t g_backend = {
    .name = "fake",
    .execute = fake_execute,
    .cancel = fake_cancel,
    .close_operation = close_op,
    .fetch_results = one_row,
    .get_result_metadata = metadata,
    .get_last_error = last_error,
};

static const argus_backend_t g_interruptible_backend = {
    .name = "fake-pg",
    .execute = fake_execute,
    .cancel = interrupting_cancel,
    .cancel_from_any_thread = true,
    .close_operation = close_op,
    .fetch_results = one_row,
    .get_result_metadata = metadata,
    .get_last_error = last_error,
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
    g_open_ops = 0;
    g_execute_calls = 0;
    g_cancel_calls = 0;
    g_cancel_thread = NULL;
    g_fail_execute = FALSE;
    g_fail_fetch = FALSE;
    g_block_execute = FALSE;
    g_block_fetch = FALSE;
    g_in_call = FALSE;
    g_release = FALSE;
    *state = f;
    return 0;
}

static int teardown(void **state)
{
    fixture_t *f = *state;
    argus_free_stmt(f->stmt);
    f->dbc->connected = false;
    argus_free_dbc(f->dbc);
    argus_free_env(f->env);
    free(f);
    assert_int_equal(g_open_ops, 0);
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

/* A call made on a second thread, with its result. */
typedef struct {
    argus_stmt_t *stmt;
    SQLRETURN     ret;
    SQLRETURN     ret2;
    GThread      *thread;
} call_t;

static gpointer run_exec_direct(gpointer data)
{
    call_t *c = data;
    c->thread = g_thread_self();
    c->ret = SQLExecDirect((SQLHSTMT)c->stmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
    return NULL;
}

static gpointer run_two_fetches(gpointer data)
{
    call_t *c = data;
    c->thread = g_thread_self();
    c->ret = SQLFetch((SQLHSTMT)c->stmt);
    c->ret2 = SQLFetch((SQLHSTMT)c->stmt);
    return NULL;
}

/* SQLCancel while a synchronous execute is in the backend: it returns at
 * once, and the execute comes back SQL_ERROR / HY008 with the operation
 * cancelled (on the executing thread) and closed. The statement is then
 * usable again, with no cancel left over. */
static void test_cancel_interrupts_sync_execute(void **state)
{
    fixture_t *f = *state;
    call_t c = { .stmt = f->stmt };

    g_block_execute = TRUE;
    GThread *t = g_thread_new("exec", run_exec_direct, &c);
    wait_in_call();

    assert_int_equal(SQLCancel((SQLHSTMT)f->stmt), SQL_SUCCESS);
    /* It returned while the execute is still held: nothing was released. */
    assert_int_equal(g_cancel_calls, 0);

    release();
    g_thread_join(t);

    assert_int_equal(c.ret, SQL_ERROR);
    expect_state(f->stmt, "HY008");
    assert_int_equal(g_cancel_calls, 1);
    assert_ptr_equal(g_cancel_thread, c.thread);
    assert_int_equal(g_open_ops, 0);
    assert_false(f->stmt->executed);

    g_block_execute = FALSE;
    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT 2",
                                   SQL_NTS), SQL_SUCCESS);
    assert_int_equal(SQLFetch((SQLHSTMT)f->stmt), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 1);
}

/* SQLCancel while a fetch is on the wire: the fetch in progress completes
 * with its rows, the next one answers HY008 and the cursor is gone. */
static void test_cancel_interrupts_fetch(void **state)
{
    fixture_t *f = *state;
    call_t c = { .stmt = f->stmt };

    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT 1",
                                   SQL_NTS), SQL_SUCCESS);
    g_block_fetch = TRUE;
    GThread *t = g_thread_new("fetch", run_two_fetches, &c);
    wait_in_call();

    assert_int_equal(SQLCancel((SQLHSTMT)f->stmt), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 0);

    release();
    g_thread_join(t);

    assert_int_equal(c.ret, SQL_SUCCESS);
    assert_int_equal(c.ret2, SQL_ERROR);
    expect_state(f->stmt, "HY008");
    assert_int_equal(g_cancel_calls, 1);
    assert_ptr_equal(g_cancel_thread, c.thread);
    assert_int_equal(g_open_ops, 0);
    assert_int_equal(SQLFetch((SQLHSTMT)f->stmt), SQL_ERROR);
    expect_state(f->stmt, "HY010");
}

/* SQLCancel during an asynchronous execute: the polls keep answering
 * SQL_STILL_EXECUTING without waiting on the worker, the cancel returns at
 * once, and the completing poll reports SQL_ERROR / HY008. */
static void test_cancel_async_execute(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;

    assert_int_equal(SQLSetStmtAttr(h, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0),
                     SQL_SUCCESS);
    g_block_execute = TRUE;
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_STILL_EXECUTING);
    wait_in_call();

    /* The worker holds the statement: polling and cancelling do not wait. */
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_STILL_EXECUTING);
    assert_int_equal(SQLCancel(h), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_STILL_EXECUTING);

    release();
    SQLRETURN ret;
    do {
        ret = SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS);
        if (ret == SQL_STILL_EXECUTING) g_usleep(1000);
    } while (ret == SQL_STILL_EXECUTING);

    assert_int_equal(ret, SQL_ERROR);
    expect_state(f->stmt, "HY008");
    assert_int_equal(f->stmt->async_state, ARGUS_ASYNC_ERROR);
    assert_int_equal(g_cancel_calls, 1);
    assert_int_equal(g_open_ops, 0);

    /* Back to synchronous use, no cancel left over. */
    g_block_execute = FALSE;
    assert_int_equal(SQLFreeStmt(h, SQL_CLOSE), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(h, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_OFF, 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 2", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
}

/* An asynchronous execute that fails keeps its diagnostic through the
 * polls that complete it. */
static void test_async_error_keeps_diagnostic(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;

    assert_int_equal(SQLSetStmtAttr(h, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0),
                     SQL_SUCCESS);
    g_fail_execute = TRUE;
    SQLRETURN ret = SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS);
    while (ret == SQL_STILL_EXECUTING) {
        g_usleep(1000);
        ret = SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS);
    }
    assert_int_equal(ret, SQL_ERROR);

    SQLCHAR st[6] = {0}, msg[256] = {0};
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, h, 1, st, &native, msg,
                                   sizeof(msg), &len), SQL_SUCCESS);
    assert_string_equal((const char *)st, "HY000");
    assert_non_null(strstr((const char *)msg, "boom"));
}

/* SQLCompleteAsync waits for the worker (giving up the statement lock
 * while it runs) and hands back its return code. */
static void test_complete_async_waits_for_worker(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;

    assert_int_equal(SQLSetStmtAttr(h, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0),
                     SQL_SUCCESS);
    g_block_execute = TRUE;
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_STILL_EXECUTING);
    wait_in_call();
    release();

    RETCODE rc = SQL_ERROR;
    assert_int_equal(SQLCompleteAsync(SQL_HANDLE_STMT, h, &rc), SQL_SUCCESS);
    assert_int_equal(rc, SQL_SUCCESS);
    assert_int_equal(f->stmt->async_state, ARGUS_ASYNC_DONE);
    assert_int_equal(g_open_ops, 1);
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
}

/* Nothing running: SQLCancel on a fresh statement is a no-op, and on an
 * executed one it cancels the operation in place (the cursor stays). */
static void test_cancel_idle_statement(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;

    assert_int_equal(SQLCancel(h), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 0);

    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLCancel(h), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 1);
    assert_ptr_equal(g_cancel_thread, g_thread_self());
    assert_int_equal(g_open_ops, 1);
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
}

/* A backend whose cancel is safe from any thread (PostgreSQL): SQLCancel
 * sends it at once, the blocked execute comes back early, and the failure
 * it comes back with is reported as HY008 rather than as a backend error. */
static void test_cancel_interrupts_backend_execute(void **state)
{
    fixture_t *f = *state;
    f->dbc->backend = &g_interruptible_backend;
    call_t c = { .stmt = f->stmt };

    g_block_execute = TRUE;
    GThread *t = g_thread_new("exec", run_exec_direct, &c);
    wait_in_call();

    assert_int_equal(SQLCancel((SQLHSTMT)f->stmt), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 1);
    assert_ptr_equal(g_cancel_thread, g_thread_self());
    /* No release() here: the cancel is what lets the execute return. */
    g_thread_join(t);

    assert_int_equal(c.ret, SQL_ERROR);
    expect_state(f->stmt, "HY008");
    assert_int_equal(g_open_ops, 0);
}

/* Same, with the fetch blocked on the connection: that fetch itself comes
 * back HY008, and the operation is closed. */
static void test_cancel_interrupts_backend_fetch(void **state)
{
    fixture_t *f = *state;
    f->dbc->backend = &g_interruptible_backend;
    call_t c = { .stmt = f->stmt };

    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT 1",
                                   SQL_NTS), SQL_SUCCESS);
    g_block_fetch = TRUE;
    GThread *t = g_thread_new("fetch", run_two_fetches, &c);
    wait_in_call();

    assert_int_equal(SQLCancel((SQLHSTMT)f->stmt), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 1);
    g_thread_join(t);

    assert_int_equal(c.ret, SQL_ERROR);
    assert_int_equal(c.ret2, SQL_ERROR);   /* HY010: no result any more */
    assert_int_equal(g_open_ops, 0);
    assert_false(f->stmt->executed);
}

/* An SQLCancel that lands while a brief call holds the statement is not
 * applied to the execute that follows it. */
static void test_stale_cancel_does_not_hit_next_execute(void **state)
{
    fixture_t *f = *state;
    SQLHSTMT h = (SQLHSTMT)f->stmt;

    argus_stmt_request_cancel(f->stmt);   /* what a racing SQLCancel leaves */
    assert_int_equal(SQLExecDirect(h, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(h), SQL_SUCCESS);
    assert_int_equal(g_cancel_calls, 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_cancel_interrupts_sync_execute,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_interrupts_fetch,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_async_execute,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_async_error_keeps_diagnostic,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_complete_async_waits_for_worker,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_idle_statement,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_interrupts_backend_execute,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_interrupts_backend_fetch,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_stale_cancel_does_not_hit_next_execute,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
