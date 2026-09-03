/*
 * Parameter rendering: what SQLBindParameter values become in the SQL the
 * backend receives, driven through SQLExecDirect against a backend that
 * records the statement it is handed.
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

static char g_executed[1024];
static int  g_execute_calls;

static int recording_execute(argus_backend_conn_t conn, const char *query,
                             argus_backend_op_t *out_op)
{
    (void)conn;
    snprintf(g_executed, sizeof(g_executed), "%s", query);
    g_execute_calls++;
    *out_op = (argus_backend_op_t)(uintptr_t)0x1;
    return 0;
}

static void noop_close(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn; (void)op;
}

static const argus_backend_t g_backend = {
    .name = "fake",
    .execute = recording_execute,
    .close_operation = noop_close,
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
    *state = f;
    return 0;
}

static int teardown(void **state)
{
    fixture_t *f = *state;
    argus_free_stmt(f->stmt);
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

/* ── SQL_C_BINARY ─────────────────────────────────────────────── */

/* The indicator gives the byte count; without one, BufferLength does. */
static void test_binary_renders_hex(void **state)
{
    fixture_t *f = *state;
    static const unsigned char bytes[] = { 0xDE, 0xAD, 0x00, 0x01 };
    SQLLEN ind = 3;

    assert_int_equal(SQLBindParameter((SQLHSTMT)f->stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_BINARY, SQL_VARBINARY, 0, 0,
                                      (SQLPOINTER)bytes, sizeof(bytes), &ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT X'DEAD00'");

    assert_int_equal(SQLBindParameter((SQLHSTMT)f->stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_BINARY, SQL_VARBINARY, 0, 0,
                                      (SQLPOINTER)bytes, 2, NULL),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT X'DEAD'");
}

/* A negative BufferLength with no indicator used to wrap the hex buffer's
 * size to a few bytes and write the whole "value" past it. */
static void test_binary_rejects_negative_length(void **state)
{
    fixture_t *f = *state;
    static const unsigned char bytes[] = { 0xDE, 0xAD };

    assert_int_equal(SQLBindParameter((SQLHSTMT)f->stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_BINARY, SQL_VARBINARY, 0, 0,
                                      (SQLPOINTER)bytes, -1, NULL),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_ERROR);
    expect_state(f->stmt, "HYC00");
    assert_int_equal(g_execute_calls, 0);

    SQLLEN ind = -5; /* neither a count nor a special value */
    assert_int_equal(SQLBindParameter((SQLHSTMT)f->stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_BINARY, SQL_VARBINARY, 0, 0,
                                      (SQLPOINTER)bytes, -1, &ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_ERROR);
    expect_state(f->stmt, "HYC00");
    assert_int_equal(g_execute_calls, 0);
}

/* SQL_NULL_DATA needs no value pointer. */
static void test_null_data_without_value_pointer(void **state)
{
    fixture_t *f = *state;
    SQLLEN ind = SQL_NULL_DATA;

    assert_int_equal(SQLBindParameter((SQLHSTMT)f->stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_CHAR, SQL_VARCHAR, 0, 0,
                                      NULL, 0, &ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecDirect((SQLHSTMT)f->stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT NULL");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_binary_renders_hex,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_binary_rejects_negative_length,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_null_data_without_value_pointer,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
