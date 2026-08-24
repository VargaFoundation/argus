/*
 * test_telemetry.c - Opt-in gating for anonymous telemetry.
 *
 * Verifies argus_telemetry_active() honours the layered controls:
 *   - disabled by default (no env, no per-connection opt-in),
 *   - per-connection opt-in (TELEMETRY=1 -> dbc->telemetry_enabled),
 *   - machine-wide opt-in (ARGUS_TELEMETRY=1),
 *   - hard kill switch (ARGUS_TELEMETRY=0) overriding every opt-in.
 *
 * These cases never enqueue an event, so no network I/O occurs.
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#include "argus/handle.h"
#include "argus/telemetry.h"

static void make_dbc(argus_dbc_t *dbc, bool opt_in)
{
    memset(dbc, 0, sizeof(*dbc));
    dbc->signature        = ARGUS_DBC_SIGNATURE;
    dbc->backend_name     = (char *)"trino";
    dbc->telemetry_enabled = opt_in;
}

/*
 * Re-read env by re-initializing the telemetry subsystem for each case.
 *
 * g_setenv rather than setenv: the UCRT has no POSIX setenv/unsetenv, so the
 * MinGW build failed to compile this file. glib's version also writes through
 * to the CRT environment, so telemetry.c's plain getenv still sees it — and
 * test_dsn.c already reaches for g_setenv for the same reason.
 */
static void reinit(const char *mode_env)
{
    if (mode_env)
        g_setenv("ARGUS_TELEMETRY", mode_env, TRUE);
    else
        g_unsetenv("ARGUS_TELEMETRY");
    argus_telemetry_init();
}

static void test_default_off(void **state)
{
    (void)state;
    reinit(NULL);
    argus_dbc_t dbc;
    make_dbc(&dbc, false);
    assert_false(argus_telemetry_active(&dbc));
    argus_telemetry_shutdown();
}

static void test_per_connection_optin(void **state)
{
    (void)state;
    reinit(NULL);
    argus_dbc_t dbc;
    make_dbc(&dbc, true);
    assert_true(argus_telemetry_active(&dbc));
    argus_telemetry_shutdown();
}

static void test_env_force_on(void **state)
{
    (void)state;
    reinit("1");
    argus_dbc_t dbc;
    make_dbc(&dbc, false);   /* no per-connection opt-in */
    assert_true(argus_telemetry_active(&dbc));
    argus_telemetry_shutdown();
}

static void test_env_kill_switch(void **state)
{
    (void)state;
    reinit("0");
    argus_dbc_t dbc;
    make_dbc(&dbc, true);    /* opt-in must be overridden */
    assert_false(argus_telemetry_active(&dbc));
    argus_telemetry_shutdown();
}

static void test_null_dbc_off(void **state)
{
    (void)state;
    reinit(NULL);
    assert_false(argus_telemetry_active(NULL));
    argus_telemetry_shutdown();
}

int main(void)
{
    /* Keep the install-id file out of the real home directory. Same reason as
     * reinit() for g_setenv; g_get_tmp_dir keeps the path off a hard-coded
     * /tmp, which does not exist on the Windows runner. */
    char *cfg = g_build_filename(g_get_tmp_dir(), "argus-test-cfg", NULL);
    g_setenv("XDG_CONFIG_HOME", cfg, TRUE);
    g_free(cfg);

    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_default_off),
        cmocka_unit_test(test_per_connection_optin),
        cmocka_unit_test(test_env_force_on),
        cmocka_unit_test(test_env_kill_switch),
        cmocka_unit_test(test_null_dbc_off),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
