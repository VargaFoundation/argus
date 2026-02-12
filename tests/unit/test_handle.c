#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"

/* ── Test: Allocate and free environment handle ──────────────── */

static void test_alloc_free_env(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_non_null(env);
    assert_true(argus_valid_env(env));

    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    assert_int_equal(ret, SQL_SUCCESS);
}

/* ── Test: Allocate DBC requires valid env ───────────────────── */

static void test_alloc_dbc_requires_env(void **state)
{
    (void)state;

    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, SQL_NULL_HANDLE, &dbc);
    assert_int_equal(ret, SQL_INVALID_HANDLE);
}

/* ── Test: Allocate env -> dbc -> stmt chain ─────────────────── */

static void test_alloc_chain(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                        (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_non_null(dbc);
    assert_true(argus_valid_dbc(dbc));

    /* Cannot alloc stmt without connection */
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    assert_int_equal(ret, SQL_ERROR);

    /* Free in reverse order */
    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    assert_int_equal(ret, SQL_SUCCESS);
}

/* ── Test: Environment signature check ───────────────────────── */

static void test_env_signature(void **state)
{
    (void)state;

    argus_env_t *env = NULL;
    SQLRETURN ret = argus_alloc_env(&env);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(env->signature, ARGUS_ENV_SIGNATURE);
    assert_true(argus_valid_env(env));

    argus_free_env(env);
}

/* ── Test: DBC signature check ───────────────────────────────── */

static void test_dbc_signature(void **state)
{
    (void)state;

    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;

    argus_dbc_t *dbc = NULL;
    SQLRETURN ret = argus_alloc_dbc(env, &dbc);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(dbc->signature, ARGUS_DBC_SIGNATURE);
    assert_true(argus_valid_dbc(dbc));
    assert_false(dbc->connected);

    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: Invalid handle type ───────────────────────────────── */

static void test_invalid_handle_type(void **state)
{
    (void)state;

    SQLHANDLE h = SQL_NULL_HANDLE;
    SQLRETURN ret = SQLAllocHandle(99, SQL_NULL_HANDLE, &h);
    assert_int_equal(ret, SQL_ERROR);
}

/* ── Test: SQLFreeStmt options ───────────────────────────────── */

static void test_free_stmt_invalid_option(void **state)
{
    (void)state;

    /* With a fake stmt, SQLFreeStmt should return invalid handle */
    SQLRETURN ret = SQLFreeStmt(SQL_NULL_HSTMT, SQL_CLOSE);
    assert_int_equal(ret, SQL_INVALID_HANDLE);
}

/* ── Test: Free NULL handle returns invalid handle ────────────── */

static void test_free_null_handle(void **state)
{
    (void)state;

    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE);
    assert_int_equal(ret, SQL_INVALID_HANDLE);

    ret = SQLFreeHandle(SQL_HANDLE_DBC, SQL_NULL_HANDLE);
    assert_int_equal(ret, SQL_INVALID_HANDLE);

    ret = SQLFreeHandle(SQL_HANDLE_STMT, SQL_NULL_HANDLE);
    assert_int_equal(ret, SQL_INVALID_HANDLE);
}

/* ── Test: NULL output handle ────────────────────────────────── */

static void test_null_output_handle(void **state)
{
    (void)state;

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, NULL);
    assert_int_equal(ret, SQL_ERROR);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_alloc_free_env),
        cmocka_unit_test(test_alloc_dbc_requires_env),
        cmocka_unit_test(test_alloc_chain),
        cmocka_unit_test(test_env_signature),
        cmocka_unit_test(test_dbc_signature),
        cmocka_unit_test(test_invalid_handle_type),
        cmocka_unit_test(test_free_stmt_invalid_option),
        cmocka_unit_test(test_free_null_handle),
        cmocka_unit_test(test_null_output_handle),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
