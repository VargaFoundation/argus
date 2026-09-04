/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include "argus/handle.h"
#include "argus/obs_hooks.h"

/*
 * A guardrail tap sets a ceiling on a statement; an application setting
 * SQL_ATTR_MAX_ROWS or SQL_ATTR_QUERY_TIMEOUT may lower it and must not be
 * able to raise it.
 *
 * It used to be applied by writing the policy straight into those two
 * attributes at SQLAllocHandle, guarded by "only if the application has not
 * set one". A freshly allocated statement always has them at 0, so that test
 * always passed -- and the application's next SQLSetStmtAttr simply
 * overwrote the policy. A ceiling any caller can raise is not a ceiling, and
 * the product documentation said the opposite.
 *
 * The taps are weak symbols in the open driver, so this file provides a
 * strong definition and becomes the policy for the tests below.
 */

static unsigned long g_guard_rows;
static unsigned long g_guard_timeout_ms;

int argus_obs_hook_guards(const void *dbc, unsigned long *max_rows,
                          unsigned long *timeout_ms)
{
    (void)dbc;
    if (!g_guard_rows && !g_guard_timeout_ms) return 0;
    if (max_rows)   *max_rows   = g_guard_rows;
    if (timeout_ms) *timeout_ms = g_guard_timeout_ms;
    return 1;
}

static argus_dbc_t *create_dbc(void)
{
    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;
    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);
    dbc->connected = true;
    return dbc;
}

static void free_dbc(argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* The policy lands on the ceiling, and leaves the attributes alone. */
static void test_guard_does_not_become_the_attribute(void **state)
{
    (void)state;
    g_guard_rows = 100;
    g_guard_timeout_ms = 5000;

    argus_dbc_t *dbc = create_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    assert_int_equal((int)stmt->guard_max_rows, 100);
    assert_int_equal((int)stmt->guard_timeout_sec, 5);
    /* The application has set nothing, so the attributes read back as unset. */
    assert_int_equal((int)stmt->max_rows, 0);
    assert_int_equal((int)stmt->query_timeout, 0);
    /* With nothing set by the application, the ceiling is what applies. */
    assert_int_equal((int)argus_stmt_effective_max_rows(stmt), 100);
    assert_int_equal((int)argus_stmt_effective_timeout(stmt), 5);

    argus_free_stmt(stmt);
    free_dbc(dbc);
    g_guard_rows = g_guard_timeout_ms = 0;
}

/* The application may lower the limit, and may not raise it. */
static void test_application_may_lower_but_not_raise(void **state)
{
    (void)state;
    g_guard_rows = 100;
    g_guard_timeout_ms = 5000;

    argus_dbc_t *dbc = create_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    /* Raising it: refused in effect, though the attribute keeps the value
     * the application set, as ODBC requires of an attribute. */
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                                    (SQLPOINTER)(uintptr_t)1000000, 0),
                     SQL_SUCCESS);
    assert_int_equal((int)argus_stmt_effective_max_rows(stmt), 100);
    SQLULEN back = 0;
    assert_int_equal(SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                                    &back, 0, NULL), SQL_SUCCESS);
    assert_int_equal((int)back, 1000000);

    /* Lowering it: honoured. */
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                                    (SQLPOINTER)(uintptr_t)10, 0),
                     SQL_SUCCESS);
    assert_int_equal((int)argus_stmt_effective_max_rows(stmt), 10);

    /* Back to unlimited: the ceiling is still there. */
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                                    (SQLPOINTER)(uintptr_t)0, 0),
                     SQL_SUCCESS);
    assert_int_equal((int)argus_stmt_effective_max_rows(stmt), 100);

    /* The same for the timeout. */
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_QUERY_TIMEOUT,
                                    (SQLPOINTER)(uintptr_t)3600, 0),
                     SQL_SUCCESS);
    assert_int_equal((int)argus_stmt_effective_timeout(stmt), 5);
    assert_int_equal(SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_QUERY_TIMEOUT,
                                    (SQLPOINTER)(uintptr_t)2, 0),
                     SQL_SUCCESS);
    assert_int_equal((int)argus_stmt_effective_timeout(stmt), 2);

    argus_free_stmt(stmt);
    free_dbc(dbc);
    g_guard_rows = g_guard_timeout_ms = 0;
}

/* With no policy, the application's own limits are the only ones. */
static void test_no_policy_leaves_the_application_alone(void **state)
{
    (void)state;
    g_guard_rows = g_guard_timeout_ms = 0;

    argus_dbc_t *dbc = create_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    assert_int_equal((int)stmt->guard_max_rows, 0);
    assert_int_equal((int)argus_stmt_effective_max_rows(stmt), 0);

    SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                   (SQLPOINTER)(uintptr_t)42, 0);
    assert_int_equal((int)argus_stmt_effective_max_rows(stmt), 42);

    argus_free_stmt(stmt);
    free_dbc(dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_guard_does_not_become_the_attribute),
        cmocka_unit_test(test_application_may_lower_but_not_raise),
        cmocka_unit_test(test_no_policy_leaves_the_application_alone),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
