#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <string.h>
#include <stdio.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/obs_hooks.h"

/*
 * License-gate SEAM tests (open, Apache-2.0 build).
 *
 * The community driver links only the WEAK no-op definition of
 * argus_obs_hook_check_license(); the enterprise addon (argus_ee) supplies the
 * strong, enforcing definition and its own crypto / deny / revocation tests.
 * A weak and a strong definition of the same symbol cannot coexist in one
 * linked binary, so these tests pin the OPEN-build invariant only: without the
 * addon the gate always allows, and the driver still captures the License=
 * token (so the addon has it) and masks it in the observability copy.
 */

/* ── The weak stub must ALLOW every connection and never set a reason ── */

static void test_weak_stub_allows(void **state)
{
    (void)state;
    char *reason = (char *)0x1;   /* poison: the stub must overwrite to NULL */

    assert_int_equal(
        argus_obs_hook_check_license(NULL, "trino", NULL, &reason), 1);
    assert_null(reason);

    reason = (char *)0x1;
    assert_int_equal(
        argus_obs_hook_check_license(NULL, "hive", "any-token", &reason), 1);
    assert_null(reason);

    /* A NULL reason out-param must be tolerated. */
    assert_int_equal(
        argus_obs_hook_check_license(NULL, "bigquery", "tok", NULL), 1);
}

/* ── License= is parsed into the DBC and masked in obs_connstr ──
 * An unknown backend makes do_connect() fail fast (no network) AFTER the
 * connection string — including License= — has already been parsed and the
 * redacted obs_connstr captured, so we can assert both without a live server. */

static void drive_and_check(const char *key)
{
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &hdbc), SQL_SUCCESS);

    char in[256];
    snprintf(in, sizeof(in),
             "DRIVER=Argus;BACKEND=__argus_seam_nobackend__;"
             "%s=SEAMTEST-TOKEN;HOST=localhost;PORT=1", key);

    SQLCHAR out[256];
    SQLSMALLINT outlen = 0;
    SQLRETURN r = SQLDriverConnect(hdbc, NULL, (SQLCHAR *)in, SQL_NTS,
                                   out, sizeof(out), &outlen,
                                   SQL_DRIVER_NOPROMPT);
    assert_int_equal(r, SQL_ERROR);   /* unknown backend, before any network */

    argus_dbc_t *d = (argus_dbc_t *)hdbc;
    assert_non_null(d->license);
    assert_string_equal(d->license, "SEAMTEST-TOKEN");

    /* The token must never survive into the observability copy of the connstr. */
    assert_non_null(d->obs_connstr);
    assert_null(strstr(d->obs_connstr, "SEAMTEST-TOKEN"));

    assert_int_equal(SQLFreeHandle(SQL_HANDLE_DBC, hdbc), SQL_SUCCESS);
    assert_int_equal(SQLFreeHandle(SQL_HANDLE_ENV, env), SQL_SUCCESS);
}

static void test_connstr_license_captured(void **state)
{
    (void)state;
    drive_and_check("LICENSE");
}

static void test_connstr_licensekey_alias(void **state)
{
    (void)state;
    drive_and_check("LICENSEKEY");
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_weak_stub_allows),
        cmocka_unit_test(test_connstr_license_captured),
        cmocka_unit_test(test_connstr_licensekey_alias),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
