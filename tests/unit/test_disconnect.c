/* SPDX-License-Identifier: Apache-2.0 */
/*
 * SQLDisconnect and the handles under a connection. ODBC has the driver free
 * the statements and the explicitly allocated descriptors of a connection
 * when it is disconnected, and refuse (HY010) while an asynchronous execute
 * is still running on one of them. Explicit descriptors also outlive the
 * statement they were associated with. Driven through a fake backend that
 * counts its open operations and can hold an execute until released.
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

static int g_open_ops;
static int g_disconnect_calls;
static int g_open_ops_at_disconnect;

/* Hold every execute until the test releases it (async test). */
static GMutex   g_lock;
static GCond    g_cond;
static gboolean g_block_execute;
static gboolean g_in_execute;
static gboolean g_release;

static int counting_execute(argus_backend_conn_t conn, const char *query,
                            argus_backend_op_t *out_op)
{
    (void)conn; (void)query;
    g_mutex_lock(&g_lock);
    if (g_block_execute) {
        g_in_execute = TRUE;
        g_cond_broadcast(&g_cond);
        while (!g_release)
            g_cond_wait(&g_cond, &g_lock);
    }
    g_open_ops++;
    g_mutex_unlock(&g_lock);
    *out_op = (argus_backend_op_t)calloc(1, 16);
    return 0;
}

static void close_op(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn;
    g_mutex_lock(&g_lock);
    g_open_ops--;
    g_mutex_unlock(&g_lock);
    free(op);
}

static void counting_disconnect(argus_backend_conn_t conn)
{
    (void)conn;
    g_disconnect_calls++;
    g_open_ops_at_disconnect = g_open_ops;
}

static const argus_backend_t g_backend = {
    .name = "fake",
    .execute = counting_execute,
    .close_operation = close_op,
    .disconnect = counting_disconnect,
};

typedef struct {
    argus_env_t *env;
    argus_dbc_t *dbc;
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
    g_open_ops = 0;
    g_disconnect_calls = 0;
    g_open_ops_at_disconnect = -1;
    g_block_execute = FALSE;
    g_in_execute = FALSE;
    g_release = FALSE;
    *state = f;
    return 0;
}

static int teardown(void **state)
{
    fixture_t *f = *state;
    if (f->dbc->connected)
        assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_SUCCESS);
    assert_int_equal(argus_free_dbc(f->dbc), SQL_SUCCESS);
    argus_free_env(f->env);
    free(f);
    assert_int_equal(g_open_ops, 0);
    return 0;
}

static void expect_dbc_state(argus_dbc_t *dbc, const char *sqlstate)
{
    SQLCHAR st[6] = {0}, msg[256];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_DBC, (SQLHDBC)dbc, 1, st,
                                   &native, msg, sizeof(msg), &len),
                     SQL_SUCCESS);
    assert_string_equal((const char *)st, sqlstate);
}

static argus_stmt_t *alloc_stmt(fixture_t *f)
{
    argus_stmt_t *stmt = NULL;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, (SQLHANDLE)f->dbc,
                                    (SQLHANDLE *)&stmt), SQL_SUCCESS);
    return stmt;
}

/* Statements and explicit descriptors go with the connection; the backend
 * operations are closed before the backend is disconnected. */
static void test_disconnect_frees_statements_and_descriptors(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *a = alloc_stmt(f);
    argus_stmt_t *b = alloc_stmt(f);

    assert_int_equal(SQLExecDirect((SQLHSTMT)a, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)b, (SQLCHAR *)"SELECT 2", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(g_open_ops, 2);

    SQLHDESC associated = NULL, loose = NULL;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)f->dbc,
                                    &associated), SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)f->dbc,
                                    &loose), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)a, SQL_ATTR_APP_ROW_DESC,
                                    associated, 0), SQL_SUCCESS);

    assert_non_null(f->dbc->stmts);
    assert_non_null(f->dbc->descs);

    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_SUCCESS);

    assert_false(f->dbc->connected);
    assert_int_equal(g_disconnect_calls, 1);
    assert_int_equal(g_open_ops, 0);
    assert_int_equal(g_open_ops_at_disconnect, 0);
    assert_null(f->dbc->stmts);
    assert_null(f->dbc->descs);
}

static void test_disconnect_not_connected(void **state)
{
    fixture_t *f = *state;
    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_SUCCESS);
    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_ERROR);
    expect_dbc_state(f->dbc, "08003");
}

/* An asynchronous execute in flight makes SQLDisconnect a function sequence
 * error; nothing is freed and the connection stays open. Once the worker
 * has finished, the disconnect goes through (and joins the worker). */
static void test_disconnect_while_async_running(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = alloc_stmt(f);
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0),
                     SQL_SUCCESS);

    g_block_execute = TRUE;
    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt, (SQLCHAR *)"SELECT 1",
                                   SQL_NTS), SQL_STILL_EXECUTING);
    g_mutex_lock(&g_lock);
    while (!g_in_execute)
        g_cond_wait(&g_cond, &g_lock);
    g_mutex_unlock(&g_lock);

    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_ERROR);
    expect_dbc_state(f->dbc, "HY010");
    assert_true(f->dbc->connected);
    assert_int_equal(g_disconnect_calls, 0);
    assert_ptr_equal(f->dbc->stmts, stmt);

    g_mutex_lock(&g_lock);
    g_release = TRUE;
    g_cond_broadcast(&g_cond);
    g_mutex_unlock(&g_lock);

    /* The application keeps polling until the execute completes. */
    SQLRETURN ret;
    while ((ret = SQLExecDirect((SQLHSTMT)stmt, (SQLCHAR *)"SELECT 1",
                                SQL_NTS)) == SQL_STILL_EXECUTING)
        g_usleep(1000);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(g_open_ops, 1);

    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_SUCCESS);
    assert_int_equal(g_open_ops, 0);
    assert_null(f->dbc->stmts);
}

/* A worker that has completed but was never polled is not "in progress":
 * the disconnect joins it and frees the statement. */
static void test_disconnect_joins_finished_worker(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = alloc_stmt(f);
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_ASYNC_ENABLE,
                                    (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt, (SQLCHAR *)"SELECT 1",
                                   SQL_NTS), SQL_STILL_EXECUTING);
    while (!g_atomic_int_get(&stmt->async_done))
        g_usleep(1000);

    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_SUCCESS);
    assert_int_equal(g_open_ops, 0);
    assert_null(f->dbc->stmts);
}

/* Freeing a statement leaves its explicit descriptors usable and freeable:
 * they used to keep a pointer to the freed statement. */
static void test_explicit_descriptor_outlives_statement(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = alloc_stmt(f);

    SQLHDESC ard = NULL, apd = NULL;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)f->dbc, &ard),
                     SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)f->dbc, &apd),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_APP_ROW_DESC,
                                    ard, 0), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_APP_PARAM_DESC,
                                    apd, 0), SQL_SUCCESS);
    assert_ptr_equal(((argus_desc_t *)ard)->stmt, stmt);
    assert_ptr_equal(((argus_desc_t *)apd)->stmt, stmt);

    assert_int_equal(SQLFreeHandle(SQL_HANDLE_STMT, (SQLHANDLE)stmt),
                     SQL_SUCCESS);
    assert_null(((argus_desc_t *)ard)->stmt);
    assert_null(((argus_desc_t *)apd)->stmt);

    /* Still valid handles: diagnostics and fields work, then they free. */
    SQLCHAR st[6] = {0}, msg[64];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_DESC, ard, 1, st, &native,
                                   msg, sizeof(msg), &len), SQL_NO_DATA);
    int marker = 0;
    assert_int_equal(SQLSetDescField(ard, 1, SQL_DESC_DATA_PTR, &marker, 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLFreeHandle(SQL_HANDLE_DESC, ard), SQL_SUCCESS);
    assert_int_equal(SQLFreeHandle(SQL_HANDLE_DESC, apd), SQL_SUCCESS);
    assert_null(f->dbc->descs);
}

/* An explicit descriptor can be allocated on a disconnected connection; the
 * connection frees whatever is still allocated when it goes. */
static void test_free_dbc_frees_explicit_descriptors(void **state)
{
    fixture_t *f = *state;
    assert_int_equal(SQLDisconnect((SQLHDBC)f->dbc), SQL_SUCCESS);

    SQLHDESC desc = NULL;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)f->dbc, &desc),
                     SQL_SUCCESS);
    assert_ptr_equal(f->dbc->descs, desc);
    /* Freed by argus_free_dbc in the teardown; the leak checker sees a
     * descriptor that is not. */
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(
            test_disconnect_frees_statements_and_descriptors, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_disconnect_not_connected, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_disconnect_while_async_running, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_disconnect_joins_finished_worker, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_explicit_descriptor_outlives_statement, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_free_dbc_frees_explicit_descriptors, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
