/*
 * Unit tests for ODBC 2.x compatibility functions.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/odbc_api.h"

/* ── Test: SQLAllocEnv / SQLFreeEnv ──────────────────────────── */

static void test_alloc_free_env_v2(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLRETURN ret = SQLAllocEnv(&env);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_non_null(env);
    assert_true(argus_valid_env(env));

    ret = SQLFreeEnv(env);
    assert_int_equal(ret, SQL_SUCCESS);
}

/* ── Test: SQLAllocConnect / SQLFreeConnect ───────────────────── */

static void test_alloc_free_connect_v2(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLRETURN ret = SQLAllocEnv(&env);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLHDBC dbc = SQL_NULL_HDBC;
    ret = SQLAllocConnect(env, &dbc);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_non_null(dbc);
    assert_true(argus_valid_dbc(dbc));

    ret = SQLFreeConnect(dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFreeEnv(env);
    assert_int_equal(ret, SQL_SUCCESS);
}

/* ── Test: SQLAllocStmt (ODBC 2.x) requires connection ───────── */

static void test_alloc_stmt_v2_no_connection(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLAllocEnv(&env);

    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocConnect(env, &dbc);

    /* Should fail — not connected */
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN ret = SQLAllocStmt(dbc, &stmt);
    assert_int_equal(ret, SQL_ERROR);

    SQLFreeConnect(dbc);
    SQLFreeEnv(env);
}

/* ── Test: SQLTransact with DBC handle ───────────────────────── */

static void test_transact_dbc(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLAllocEnv(&env);

    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocConnect(env, &dbc);

    /* SQLTransact should succeed (transactions are no-op) */
    SQLRETURN ret = SQLTransact(SQL_NULL_HENV, dbc, SQL_COMMIT);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLTransact(SQL_NULL_HENV, dbc, SQL_ROLLBACK);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLFreeConnect(dbc);
    SQLFreeEnv(env);
}

/* ── Test: SQLTransact with ENV handle ───────────────────────── */

static void test_transact_env(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLAllocEnv(&env);

    SQLRETURN ret = SQLTransact(env, SQL_NULL_HDBC, SQL_COMMIT);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLFreeEnv(env);
}

/* ── Test: SQLTransact with NULL handles ─────────────────────── */

static void test_transact_null(void **state)
{
    (void)state;

    SQLRETURN ret = SQLTransact(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_COMMIT);
    assert_int_equal(ret, SQL_INVALID_HANDLE);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_alloc_free_env_v2),
        cmocka_unit_test(test_alloc_free_connect_v2),
        cmocka_unit_test(test_alloc_stmt_v2_no_connection),
        cmocka_unit_test(test_transact_dbc),
        cmocka_unit_test(test_transact_env),
        cmocka_unit_test(test_transact_null),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
