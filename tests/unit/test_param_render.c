/* SPDX-License-Identifier: Apache-2.0 */
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

/* The dialect is looked up by backend name, so a fake backend named after
 * a real one is scanned and rendered with that engine's rules. */
static const argus_backend_t g_mysql_backend = {
    .name = "mysql",
    .execute = recording_execute,
    .close_operation = noop_close,
};

static const argus_backend_t g_postgres_backend = {
    .name = "postgres",
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

/* ── Parameter markers ────────────────────────────────────────── */

static void bind_int(argus_stmt_t *stmt, SQLUSMALLINT n, SQLINTEGER *value)
{
    assert_int_equal(SQLBindParameter((SQLHSTMT)stmt, n, SQL_PARAM_INPUT,
                                      SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                      value, 0, NULL),
                     SQL_SUCCESS);
}

static void exec_expect(argus_stmt_t *stmt, const char *sql,
                        const char *expect)
{
    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt, (SQLCHAR *)sql, SQL_NTS),
                     SQL_SUCCESS);
    assert_string_equal(g_executed, expect);
}

/* A backslash-escaped quote does not end a literal on the engines that
 * escape with backslashes, so the '?' after it is still inside the literal.
 * On an ANSI engine the same text is a closed literal, a marker and an
 * unterminated quote, and the marker is substituted. */
static void test_marker_in_backslash_escaped_literal(void **state)
{
    fixture_t *f = *state;
    SQLINTEGER v = 42;
    bind_int(f->stmt, 1, &v);

    f->dbc->backend = &g_mysql_backend;
    exec_expect(f->stmt, "SELECT 'a\\'b?c', ?",
                "SELECT 'a\\'b?c', 42");
    /* An escaped backslash before the quote leaves the quote closing. */
    exec_expect(f->stmt, "SELECT 'a\\\\', ?",
                "SELECT 'a\\\\', 42");
    /* Double quotes delimit a string literal there, backticks an identifier. */
    exec_expect(f->stmt, "SELECT \"a\\\"b?c\", `x?y`, ?",
                "SELECT \"a\\\"b?c\", `x?y`, 42");

    f->dbc->backend = &g_backend;   /* ANSI: '\\' is an ordinary character */
    exec_expect(f->stmt, "SELECT 'a\\'b?c'", "SELECT 'a\\'b42c'");
    /* An unclosed literal hides what follows: no marker, no 07002. */
    exec_expect(f->stmt, "SELECT 'a\\'b, '?", "SELECT 'a\\'b, '?");
}

/* PostgreSQL's string forms: an E'...' escape string escapes with
 * backslashes although the dialect does not, and a dollar-quoted string
 * holds anything, question marks included. */
static void test_marker_in_postgres_strings(void **state)
{
    fixture_t *f = *state;
    SQLINTEGER v = 7;
    bind_int(f->stmt, 1, &v);
    f->dbc->backend = &g_postgres_backend;

    exec_expect(f->stmt, "SELECT E'a\\'b?c', e'\\'?', ?",
                "SELECT E'a\\'b?c', e'\\'?', 7");
    /* A plain literal keeps the ANSI rules: '\\' is an ordinary character. */
    exec_expect(f->stmt, "SELECT 'a\\', ?", "SELECT 'a\\', 7");
    exec_expect(f->stmt,
                "CREATE FUNCTION f() RETURNS text AS $$ SELECT '?' $$ "
                "LANGUAGE sql; SELECT $q$?$q$, $_x1$ ? $_x1$, ?",
                "CREATE FUNCTION f() RETURNS text AS $$ SELECT '?' $$ "
                "LANGUAGE sql; SELECT $q$?$q$, $_x1$ ? $_x1$, 7");
    /* Not dollar quotes: a $1 reference, a '$' inside an identifier and a
     * tag that is not an identifier. */
    exec_expect(f->stmt, "SELECT $1, a$b$c, $1$, ?",
                "SELECT $1, a$b$c, $1$, 7");
    /* An opening delimiter never closed hides the rest. */
    exec_expect(f->stmt, "SELECT $$ ? ", "SELECT $$ ? ");

    /* The MySQL dialect has none of this: E is an identifier tail and '$'
     * an ordinary character, so the markers count. */
    f->dbc->backend = &g_mysql_backend;
    SQLINTEGER w = 8;
    bind_int(f->stmt, 2, &w);
    exec_expect(f->stmt, "SELECT $$?$$, ?", "SELECT $$7$$, 8");
}

/* A '?' in a comment is not a marker, and comments are copied verbatim. */
static void test_marker_in_comments(void **state)
{
    fixture_t *f = *state;
    SQLINTEGER v = 1, w = 2;
    bind_int(f->stmt, 1, &v);
    bind_int(f->stmt, 2, &w);

    exec_expect(f->stmt, "SELECT ? -- ?\n, /* ? */ ?, '--?', '/*?'",
                "SELECT 1 -- ?\n, /* ? */ 2, '--?', '/*?'");
    exec_expect(f->stmt, "SELECT ? /* unclosed ?", "SELECT 1 /* unclosed ?");
    exec_expect(f->stmt, "SELECT ?, 2 / 1 - 1 -- ?",
                "SELECT 1, 2 / 1 - 1 -- ?");
}

/* SQLNumParams counts with the same scanner, so it too knows the dialect. */
static void test_num_params_follows_dialect(void **state)
{
    fixture_t *f = *state;
    SQLSMALLINT n = -1;
    const char *sql = "SELECT 'a\\'b?c', ?";

    assert_int_equal(SQLPrepare((SQLHSTMT)f->stmt, (SQLCHAR *)sql, SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLNumParams((SQLHSTMT)f->stmt, &n), SQL_SUCCESS);
    assert_int_equal(n, 1);   /* ANSI: the literal closes at \', so one */

    f->dbc->backend = &g_mysql_backend;
    assert_int_equal(SQLNumParams((SQLHSTMT)f->stmt, &n), SQL_SUCCESS);
    assert_int_equal(n, 1);   /* MySQL: 'a\'b?c' is one literal, so one */

    assert_int_equal(SQLPrepare((SQLHSTMT)f->stmt,
                                (SQLCHAR *)"SELECT 'a\\'b?c'", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLNumParams((SQLHSTMT)f->stmt, &n), SQL_SUCCESS);
    assert_int_equal(n, 0);
    f->dbc->backend = &g_backend;
    assert_int_equal(SQLNumParams((SQLHSTMT)f->stmt, &n), SQL_SUCCESS);
    assert_int_equal(n, 1);
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
        cmocka_unit_test_setup_teardown(test_marker_in_backslash_escaped_literal,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_in_postgres_strings,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_in_comments,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_num_params_follows_dialect,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
