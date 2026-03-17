/*
 * Unit tests for fetch-related features: max_rows, row_status_ptr,
 * stubs (SQLSetPos, SQLBulkOperations, SQLSetScrollOptions, SQLDescribeParam).
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/odbc_api.h"

/* ── Helper: create an env + dbc (not connected) ─────────────── */

static argus_dbc_t *create_dbc(void)
{
    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);
    return dbc;
}

/* ── Test: SQLSetPos returns HYC00 for non-POSITION operations ── */

static void test_setpos_stub(void **state)
{
    (void)state;

    /* Create a fake stmt via alloc */
    argus_dbc_t *dbc = create_dbc();
    /* Force connected for stmt alloc */
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    /* SQL_POSITION should succeed */
    SQLRETURN ret = SQLSetPos((SQLHSTMT)stmt, 1, SQL_POSITION, SQL_LOCK_NO_CHANGE);
    assert_int_equal(ret, SQL_SUCCESS);

    /* SQL_UPDATE should fail with HYC00 */
    ret = SQLSetPos((SQLHSTMT)stmt, 1, SQL_UPDATE, SQL_LOCK_NO_CHANGE);
    assert_int_equal(ret, SQL_ERROR);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLBulkOperations returns HYC00 ──────────────────── */

static void test_bulkops_stub(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    SQLRETURN ret = SQLBulkOperations((SQLHSTMT)stmt, SQL_ADD);
    assert_int_equal(ret, SQL_ERROR);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLSetScrollOptions returns HYC00 ─────────────────── */

static void test_scroll_options_stub(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    SQLRETURN ret = SQLSetScrollOptions((SQLHSTMT)stmt,
                                         SQL_CONCUR_READ_ONLY, 0, 1);
    assert_int_equal(ret, SQL_ERROR);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLDescribeParam returns generic description ──────── */

static void test_describe_param(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    SQLSMALLINT type = 0;
    SQLULEN size = 0;
    SQLSMALLINT digits = -1;
    SQLSMALLINT nullable = -1;

    SQLRETURN ret = SQLDescribeParam((SQLHSTMT)stmt, 1,
                                      &type, &size, &digits, &nullable);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(type, SQL_VARCHAR);
    assert_int_equal(size, 255);
    assert_int_equal(digits, 0);
    assert_int_equal(nullable, SQL_NULLABLE);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLSetStmtAttr SQL_ATTR_MAX_ROWS ──────────────────── */

static void test_max_rows_attr(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    SQLRETURN ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                                    (SQLPOINTER)42, 0);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->max_rows, 42);

    SQLULEN val = 0;
    ret = SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                          &val, sizeof(val), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(val, 42);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLSetStmtAttr invalid attribute returns HY092 ────── */

static void test_stmt_attr_invalid(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    SQLRETURN ret = SQLSetStmtAttr((SQLHSTMT)stmt, 99999,
                                    (SQLPOINTER)0, 0);
    assert_int_equal(ret, SQL_ERROR);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLSetConnectAttr invalid attribute returns HY092 ─── */

static void test_conn_attr_invalid(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();

    SQLRETURN ret = SQLSetConnectAttr((SQLHDBC)dbc, 99999,
                                      (SQLPOINTER)0, 0);
    assert_int_equal(ret, SQL_ERROR);

    argus_env_t *env = dbc->env;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: SQLSetEnvAttr invalid attribute returns HY092 ─────── */

static void test_env_attr_invalid(void **state)
{
    (void)state;

    argus_env_t *env = NULL;
    argus_alloc_env(&env);

    SQLRETURN ret = SQLSetEnvAttr((SQLHENV)env, 99999,
                                   (SQLPOINTER)0, 0);
    assert_int_equal(ret, SQL_ERROR);

    argus_free_env(env);
}

/* ── Test: SQLSetDescRec basic binding ───────────────────────── */

static void test_set_desc_rec(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_dbc();
    dbc->connected = true;
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    char buf[64];
    SQLLEN ind = 0;
    SQLRETURN ret = SQLSetDescRec((SQLHDESC)stmt, 1,
                                   SQL_C_CHAR, 0, sizeof(buf), 0, 0,
                                   buf, &ind, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(stmt->bindings[0].bound);
    assert_int_equal(stmt->bindings[0].target_type, SQL_C_CHAR);
    assert_ptr_equal(stmt->bindings[0].target_value, buf);

    argus_free_stmt(stmt);
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_setpos_stub),
        cmocka_unit_test(test_bulkops_stub),
        cmocka_unit_test(test_scroll_options_stub),
        cmocka_unit_test(test_describe_param),
        cmocka_unit_test(test_max_rows_attr),
        cmocka_unit_test(test_stmt_attr_invalid),
        cmocka_unit_test(test_conn_attr_invalid),
        cmocka_unit_test(test_env_attr_invalid),
        cmocka_unit_test(test_set_desc_rec),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
