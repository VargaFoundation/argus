/*
 * Unit tests for SQLBindParameter — client-side parameter binding
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/* ── Helper: count parameter markers in a SQL string ──────────── */

/* Defined in execute.c — we test via the ODBC API */

/* ── Helper: create a fake connected stmt for testing ────────── */

static argus_stmt_t *create_test_stmt(void)
{
    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    stmt->signature = ARGUS_STMT_SIGNATURE;
    stmt->row_count = -1;
    stmt->row_array_size = 1;
    argus_diag_clear(&stmt->diag);
    argus_row_cache_init(&stmt->row_cache);
    return stmt;
}

static void destroy_test_stmt(argus_stmt_t *stmt)
{
    free(stmt->query);
    argus_row_cache_free(&stmt->row_cache);
    stmt->signature = 0;
    free(stmt);
}

/* ── Test: SQLBindParameter accepts SQL_PARAM_INPUT ──────────── */

static void test_bind_parameter_basic(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLINTEGER value = 42;
    SQLLEN ind = sizeof(SQLINTEGER);

    SQLRETURN ret = SQLBindParameter(
        (SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
        SQL_C_SLONG, SQL_INTEGER, 0, 0,
        &value, sizeof(value), &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(stmt->param_bindings[0].bound);
    assert_int_equal(stmt->param_bindings[0].value_type, SQL_C_SLONG);
    assert_int_equal(stmt->num_param_bindings, 1);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLBindParameter rejects output params ─────────────── */

static void test_bind_parameter_rejects_output(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLINTEGER value = 1;
    SQLLEN ind = sizeof(SQLINTEGER);

    SQLRETURN ret = SQLBindParameter(
        (SQLHSTMT)stmt, 1, SQL_PARAM_OUTPUT,
        SQL_C_SLONG, SQL_INTEGER, 0, 0,
        &value, sizeof(value), &ind);

    assert_int_equal(ret, SQL_ERROR);
    assert_false(stmt->param_bindings[0].bound);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLBindParameter validates parameter number ────────── */

static void test_bind_parameter_invalid_number(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLINTEGER value = 1;
    SQLLEN ind = sizeof(SQLINTEGER);

    /* ParameterNumber = 0 is invalid */
    SQLRETURN ret = SQLBindParameter(
        (SQLHSTMT)stmt, 0, SQL_PARAM_INPUT,
        SQL_C_SLONG, SQL_INTEGER, 0, 0,
        &value, sizeof(value), &ind);

    assert_int_equal(ret, SQL_ERROR);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLBindParameter string with SQL escaping ──────────── */

static void test_bind_parameter_string(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    char str_value[] = "O'Brien";
    SQLLEN ind = SQL_NTS;

    SQLRETURN ret = SQLBindParameter(
        (SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
        SQL_C_CHAR, SQL_VARCHAR, 50, 0,
        str_value, sizeof(str_value), &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(stmt->param_bindings[0].bound);
    assert_int_equal(stmt->param_bindings[0].value_type, SQL_C_CHAR);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLBindParameter NULL value ───────────────────────── */

static void test_bind_parameter_null(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLLEN ind = SQL_NULL_DATA;

    SQLRETURN ret = SQLBindParameter(
        (SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
        SQL_C_CHAR, SQL_VARCHAR, 50, 0,
        NULL, 0, &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(stmt->param_bindings[0].bound);

    destroy_test_stmt(stmt);
}

/* ── Test: Multiple parameter bindings ───────────────────────── */

static void test_bind_multiple_params(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLINTEGER int_val = 100;
    SQLLEN int_ind = sizeof(SQLINTEGER);

    char str_val[] = "test";
    SQLLEN str_ind = SQL_NTS;

    SQLDOUBLE dbl_val = 3.14;
    SQLLEN dbl_ind = sizeof(SQLDOUBLE);

    SQLBindParameter((SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
                     SQL_C_SLONG, SQL_INTEGER, 0, 0,
                     &int_val, sizeof(int_val), &int_ind);

    SQLBindParameter((SQLHSTMT)stmt, 2, SQL_PARAM_INPUT,
                     SQL_C_CHAR, SQL_VARCHAR, 50, 0,
                     str_val, sizeof(str_val), &str_ind);

    SQLBindParameter((SQLHSTMT)stmt, 3, SQL_PARAM_INPUT,
                     SQL_C_DOUBLE, SQL_DOUBLE, 0, 0,
                     &dbl_val, sizeof(dbl_val), &dbl_ind);

    assert_int_equal(stmt->num_param_bindings, 3);
    assert_true(stmt->param_bindings[0].bound);
    assert_true(stmt->param_bindings[1].bound);
    assert_true(stmt->param_bindings[2].bound);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLNumParams counts ? markers ─────────────────────── */

static void test_num_params(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    stmt->query = strdup("SELECT * FROM t WHERE a = ? AND b = ?");

    SQLSMALLINT count = 0;
    SQLRETURN ret = SQLNumParams((SQLHSTMT)stmt, &count);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(count, 2);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLNumParams ignores ? inside quotes ──────────────── */

static void test_num_params_ignores_quoted(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    stmt->query = strdup("SELECT * FROM t WHERE a = '?' AND b = ?");

    SQLSMALLINT count = 0;
    SQLNumParams((SQLHSTMT)stmt, &count);

    assert_int_equal(count, 1);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLFreeStmt SQL_RESET_PARAMS clears bindings ──────── */

static void test_reset_params(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLINTEGER value = 42;
    SQLLEN ind = sizeof(SQLINTEGER);

    SQLBindParameter((SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
                     SQL_C_SLONG, SQL_INTEGER, 0, 0,
                     &value, sizeof(value), &ind);

    assert_int_equal(stmt->num_param_bindings, 1);

    SQLRETURN ret = SQLFreeStmt((SQLHSTMT)stmt, SQL_RESET_PARAMS);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_param_bindings, 0);
    assert_false(stmt->param_bindings[0].bound);

    destroy_test_stmt(stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_bind_parameter_basic),
        cmocka_unit_test(test_bind_parameter_rejects_output),
        cmocka_unit_test(test_bind_parameter_invalid_number),
        cmocka_unit_test(test_bind_parameter_string),
        cmocka_unit_test(test_bind_parameter_null),
        cmocka_unit_test(test_bind_multiple_params),
        cmocka_unit_test(test_num_params),
        cmocka_unit_test(test_num_params_ignores_quoted),
        cmocka_unit_test(test_reset_params),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
