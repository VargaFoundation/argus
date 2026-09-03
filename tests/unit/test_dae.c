/*
 * Data-at-execution: SQLExecute/SQLExecDirect answer SQL_NEED_DATA, the
 * application feeds each parameter through SQLParamData/SQLPutData, and the
 * statement runs with the bytes it collected.
 *
 * The cycle used to keep one shared buffer and point the application's
 * binding at it: with two data-at-execution parameters the first one's
 * bytes were overwritten by the second's, the length lived in a static
 * shared by every statement in the process, and after the execution the
 * binding still pointed at the freed buffer, so re-executing read freed
 * memory instead of asking for the data again. These tests drive the whole
 * cycle through the public API against a backend that records the SQL it
 * is handed, and check the bindings afterwards.
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

/* ── Fake backend that records the SQL it is asked to execute ─── */

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

static argus_stmt_t *new_stmt(fixture_t *f)
{
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(f->dbc, &stmt);
    return stmt;
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

static void bind_char(argus_stmt_t *stmt, SQLUSMALLINT n, SQLPOINTER value,
                      SQLLEN *ind)
{
    assert_int_equal(SQLBindParameter((SQLHSTMT)stmt, n, SQL_PARAM_INPUT,
                                      SQL_C_CHAR, SQL_VARCHAR, 0, 0, value,
                                      0, ind),
                     SQL_SUCCESS);
}

/* ── Tests ───────────────────────────────────────────────────── */

/* Two data-at-execution parameters around a regular one: each keeps its
 * own bytes, and the application's bindings come out untouched. */
static void test_two_dae_params_keep_their_own_data(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    char   regular[] = "abc";
    SQLLEN regular_ind = SQL_NTS;
    int    token1 = 1, token2 = 2;
    SQLLEN dae1 = SQL_DATA_AT_EXEC;
    SQLLEN dae2 = SQL_LEN_DATA_AT_EXEC(3);

    bind_char(stmt, 1, &token1, &dae1);
    bind_char(stmt, 2, regular, &regular_ind);
    bind_char(stmt, 3, &token2, &dae2);

    assert_int_equal(SQLPrepare((SQLHSTMT)stmt,
                                (SQLCHAR *)"INSERT INTO t VALUES (?, ?, ?)",
                                SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLExecute((SQLHSTMT)stmt), SQL_NEED_DATA);
    assert_int_equal(g_execute_calls, 0);

    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_ptr_equal(which, &token1);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"Hello", 5),
                     SQL_SUCCESS);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)", ", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"World", 5),
                     SQL_SUCCESS);

    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_ptr_equal(which, &token2);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"x'y", 3),
                     SQL_SUCCESS);

    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_SUCCESS);
    assert_int_equal(g_execute_calls, 1);
    assert_string_equal(g_executed,
                        "INSERT INTO t VALUES ('Hello, World', 'abc', 'x''y')");

    /* The bindings still describe what the application bound */
    assert_ptr_equal(stmt->param_bindings[0].value, &token1);
    assert_ptr_equal(stmt->param_bindings[0].str_len_or_ind, &dae1);
    assert_int_equal(dae1, SQL_DATA_AT_EXEC);
    assert_ptr_equal(stmt->param_bindings[2].value, &token2);
    assert_ptr_equal(stmt->param_bindings[2].str_len_or_ind, &dae2);
    assert_int_equal(dae2, SQL_LEN_DATA_AT_EXEC(3));
    assert_int_equal(stmt->dae_state, ARGUS_DAE_IDLE);
    assert_null(stmt->dae_values);
}

/* Re-executing a prepared statement asks for the data again and runs with
 * what the second cycle supplies, not with the freed first one. */
static void test_reexecution_asks_again(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    int    token = 7;
    SQLLEN dae = SQL_DATA_AT_EXEC;
    bind_char(stmt, 1, &token, &dae);
    assert_int_equal(SQLPrepare((SQLHSTMT)stmt,
                                (SQLCHAR *)"SELECT ?", SQL_NTS),
                     SQL_SUCCESS);

    for (int round = 0; round < 3; round++) {
        char chunk[16];
        snprintf(chunk, sizeof(chunk), "round%d", round);

        assert_int_equal(SQLExecute((SQLHSTMT)stmt), SQL_NEED_DATA);
        SQLPOINTER which = NULL;
        assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
        assert_ptr_equal(which, &token);
        assert_int_equal(SQLPutData((SQLHSTMT)stmt, chunk, SQL_NTS),
                         SQL_SUCCESS);
        assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_SUCCESS);

        char expected[64];
        snprintf(expected, sizeof(expected), "SELECT '%s'", chunk);
        assert_string_equal(g_executed, expected);
        assert_int_equal(g_execute_calls, round + 1);

        /* Closing the cursor keeps the prepared SQL and the binding. */
        assert_int_equal(SQLFreeStmt((SQLHSTMT)stmt, SQL_CLOSE), SQL_SUCCESS);
    }
}

/* SQLExecDirect takes the same path as SQLExecute. */
static void test_execdirect_needs_data(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    int    token = 1;
    SQLLEN dae = SQL_DATA_AT_EXEC;
    bind_char(stmt, 1, &token, &dae);

    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt,
                                   (SQLCHAR *)"SELECT ?", SQL_NTS),
                     SQL_NEED_DATA);
    assert_int_equal(g_execute_calls, 0);

    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"direct", 6),
                     SQL_SUCCESS);
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 'direct'");
}

/* SQLPutData(SQL_NULL_DATA) renders NULL without touching the
 * application's indicator; no SQLPutData at all is an empty value. */
static void test_null_and_empty_values(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    int    token1 = 1, token2 = 2;
    SQLLEN dae1 = SQL_DATA_AT_EXEC, dae2 = SQL_DATA_AT_EXEC;
    bind_char(stmt, 1, &token1, &dae1);
    bind_char(stmt, 2, &token2, &dae2);

    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt,
                                   (SQLCHAR *)"SELECT ?, ?", SQL_NTS),
                     SQL_NEED_DATA);
    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, NULL, SQL_NULL_DATA),
                     SQL_SUCCESS);
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_ptr_equal(which, &token2);
    /* no SQLPutData for the second parameter */
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_SUCCESS);

    assert_string_equal(g_executed, "SELECT NULL, ''");
    assert_int_equal(dae1, SQL_DATA_AT_EXEC);
    assert_int_equal(dae2, SQL_DATA_AT_EXEC);
}

/* Binary and wide-character parameters use the byte count they were put
 * with, not a string length. */
static void test_binary_and_wchar_lengths(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    int    token1 = 1, token2 = 2;
    SQLLEN dae1 = SQL_DATA_AT_EXEC, dae2 = SQL_DATA_AT_EXEC;
    assert_int_equal(SQLBindParameter((SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_BINARY, SQL_VARBINARY, 0, 0,
                                      &token1, 0, &dae1),
                     SQL_SUCCESS);
    assert_int_equal(SQLBindParameter((SQLHSTMT)stmt, 2, SQL_PARAM_INPUT,
                                      SQL_C_WCHAR, SQL_WVARCHAR, 0, 0,
                                      &token2, 0, &dae2),
                     SQL_SUCCESS);

    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt,
                                   (SQLCHAR *)"SELECT ?, ?", SQL_NTS),
                     SQL_NEED_DATA);
    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    static const unsigned char bytes[] = { 0x00, 0xAB, 0x00, 0xFF };
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)bytes, 2),
                     SQL_SUCCESS);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)(bytes + 2), 2),
                     SQL_SUCCESS);

    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    static const SQLWCHAR wide[] = { 'c', 'a', 'f', 0xE9 };
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)wide,
                                (SQLLEN)sizeof(wide)),
                     SQL_SUCCESS);
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_SUCCESS);

    assert_string_equal(g_executed, "SELECT X'00AB00FF', 'caf\xC3\xA9'");
}

/* Two statements in a cycle at the same time do not see each other's
 * data or lengths. */
static void test_statements_do_not_share_state(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *a = f->stmt;
    argus_stmt_t *b = new_stmt(f);

    int    ta = 1, tb = 2;
    SQLLEN da = SQL_DATA_AT_EXEC, db = SQL_DATA_AT_EXEC;
    bind_char(a, 1, &ta, &da);
    bind_char(b, 1, &tb, &db);

    assert_int_equal(SQLExecDirect((SQLHSTMT)a, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_NEED_DATA);
    assert_int_equal(SQLExecDirect((SQLHSTMT)b, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_NEED_DATA);
    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)a, &which), SQL_NEED_DATA);
    assert_int_equal(SQLParamData((SQLHSTMT)b, &which), SQL_NEED_DATA);
    assert_int_equal(SQLPutData((SQLHSTMT)a, (SQLPOINTER)"from a", 6),
                     SQL_SUCCESS);
    assert_int_equal(SQLPutData((SQLHSTMT)b, (SQLPOINTER)"from b, longer",
                                14), SQL_SUCCESS);

    assert_int_equal(SQLParamData((SQLHSTMT)b, &which), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 'from b, longer'");
    assert_int_equal(SQLParamData((SQLHSTMT)a, &which), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 'from a'");

    argus_free_stmt(b);
}

/* SQLCancel abandons the cycle: SQLPutData is a sequence error again and
 * the next SQLExecute opens a fresh cycle. */
static void test_cancel_abandons_cycle(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    int    token = 1;
    SQLLEN dae = SQL_DATA_AT_EXEC;
    bind_char(stmt, 1, &token, &dae);
    assert_int_equal(SQLPrepare((SQLHSTMT)stmt, (SQLCHAR *)"SELECT ?",
                                SQL_NTS), SQL_SUCCESS);

    assert_int_equal(SQLExecute((SQLHSTMT)stmt), SQL_NEED_DATA);
    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"partial", 7),
                     SQL_SUCCESS);
    assert_int_equal(SQLCancel((SQLHSTMT)stmt), SQL_SUCCESS);
    assert_int_equal(stmt->dae_state, ARGUS_DAE_IDLE);
    assert_null(stmt->dae_values);

    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"more", 4),
                     SQL_ERROR);
    expect_state(stmt, "HY010");
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_ERROR);
    expect_state(stmt, "HY010");
    assert_int_equal(g_execute_calls, 0);

    assert_int_equal(SQLExecute((SQLHSTMT)stmt), SQL_NEED_DATA);
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"whole", 5),
                     SQL_SUCCESS);
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 'whole'");
}

/* While a cycle is open, executing again is a sequence error, not an
 * execution with the token pointers as values. */
static void test_execute_during_cycle_is_sequence_error(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = f->stmt;

    int    token = 1;
    SQLLEN dae = SQL_DATA_AT_EXEC;
    bind_char(stmt, 1, &token, &dae);
    assert_int_equal(SQLPrepare((SQLHSTMT)stmt, (SQLCHAR *)"SELECT ?",
                                SQL_NTS), SQL_SUCCESS);
    assert_int_equal(SQLExecute((SQLHSTMT)stmt), SQL_NEED_DATA);

    assert_int_equal(SQLExecute((SQLHSTMT)stmt), SQL_ERROR);
    expect_state(stmt, "HY010");
    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_ERROR);
    expect_state(stmt, "HY010");
    assert_int_equal(g_execute_calls, 0);

    /* SQLFreeStmt(SQL_RESET_PARAMS) drops the cycle with the bindings */
    assert_int_equal(SQLFreeStmt((SQLHSTMT)stmt, SQL_RESET_PARAMS),
                     SQL_SUCCESS);
    assert_int_equal(stmt->dae_state, ARGUS_DAE_IDLE);
    assert_null(stmt->dae_values);
}

/* A statement freed in the middle of a cycle leaks nothing (ASan). */
static void test_free_during_cycle(void **state)
{
    fixture_t *f = *state;
    argus_stmt_t *stmt = new_stmt(f);

    int    token = 1;
    SQLLEN dae = SQL_DATA_AT_EXEC;
    bind_char(stmt, 1, &token, &dae);
    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt, (SQLCHAR *)"SELECT ?",
                                   SQL_NTS), SQL_NEED_DATA);
    SQLPOINTER which = NULL;
    assert_int_equal(SQLParamData((SQLHSTMT)stmt, &which), SQL_NEED_DATA);
    assert_int_equal(SQLPutData((SQLHSTMT)stmt, (SQLPOINTER)"abandoned", 9),
                     SQL_SUCCESS);
    argus_free_stmt(stmt);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_two_dae_params_keep_their_own_data,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_reexecution_asks_again,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_execdirect_needs_data,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_null_and_empty_values,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_binary_and_wchar_lengths,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_statements_do_not_share_state,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_abandons_cycle,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_execute_during_cycle_is_sequence_error,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_free_during_cycle,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
