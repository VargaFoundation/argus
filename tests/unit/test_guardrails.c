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
 * This file is a tap provider, the way obs_hooks.h describes one: it defines
 * the taps and its definitions are the ones the driver calls.
 *
 * It defines ALL TWELVE of them, including ten it has no use for, and that
 * is deliberate. The driver's no-ops live together in one obs_hooks.c, so
 * they reach this link as one archive member; a member is pulled in whole or
 * not at all. Defining only argus_obs_hook_guards leaves the other eleven
 * undefined, the member is pulled in to supply them, and it brings a second
 * argus_obs_hook_guards with it. On ELF the no-op is weak and loses; on
 * PE/COFF a weak definition cannot be linked at all, so obs_hooks.c makes
 * them strong there and the link fails outright:
 *
 *   ld: libargus_odbc_static.a(obs_hooks.c.obj): multiple definition of
 *       `argus_obs_hook_guards'; test_guardrails.c.obj: first defined here
 *
 * Defining every tap leaves the member with nothing to contribute, so it is
 * never extracted and there is no second definition to rank against the
 * first. That holds on both platforms, which is why there is no #ifdef here.
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

/* The other eleven, as the no-ops obs_hooks.c would have supplied. */
void argus_obs_hook_connect(const void *dbc, const char *connstr,
                            const char *backend, const char *host,
                            const char *user, int ok, double connect_ms)
{
    (void)dbc; (void)connstr; (void)backend; (void)host;
    (void)user; (void)ok; (void)connect_ms;
}

void argus_obs_hook_statement(const void *dbc, const char *backend,
                              const char *sql, double exec_ms,
                              unsigned long rows, unsigned long bytes,
                              const char *sqlstate)
{
    (void)dbc; (void)backend; (void)sql; (void)exec_ms;
    (void)rows; (void)bytes; (void)sqlstate;
}

void argus_obs_hook_disconnect(const void *dbc) { (void)dbc; }

char *argus_obs_hook_resolve_secret(const char *value)
{
    (void)value;
    return NULL;
}

char *argus_obs_hook_token_get(const char *issuer, const char *client_id,
                               const char *scope, const char *subject)
{
    (void)issuer; (void)client_id; (void)scope; (void)subject;
    return NULL;
}

void argus_obs_hook_token_put(const char *issuer, const char *client_id,
                              const char *scope, const char *subject,
                              const char *token, long long expiry_epoch_ms)
{
    (void)issuer; (void)client_id; (void)scope; (void)subject;
    (void)token; (void)expiry_epoch_ms;
}

long argus_obs_hook_fetch_preset(const char *app_name)
{
    (void)app_name;
    return 0;
}

int argus_obs_hook_connect_gate(const void *dbc, const char *backend,
                                const char *connstr, char **reason)
{
    (void)dbc; (void)backend; (void)connstr; (void)reason;
    return 1;                                    /* admit */
}

int argus_obs_hook_pick_host(const void *dbc, const char *hosts_csv,
                             int nhosts)
{
    (void)dbc; (void)hosts_csv; (void)nhosts;
    return -1;                                   /* the driver's own order */
}

void argus_obs_hook_host_result(const void *dbc, const char *hosts_csv,
                                int idx, int ok)
{
    (void)dbc; (void)hosts_csv; (void)idx; (void)ok;
}

int argus_obs_hook_unload(int may_wait)
{
    (void)may_wait;
    return 1;                                    /* nothing of ours runs */
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
