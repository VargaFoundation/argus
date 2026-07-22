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

#include "argus/handle.h"
#include "argus/telemetry.h"

static void make_dbc(argus_dbc_t *dbc, bool opt_in)
{
    memset(dbc, 0, sizeof(*dbc));
    dbc->signature        = ARGUS_DBC_SIGNATURE;
    dbc->backend_name     = (char *)"trino";
    dbc->telemetry_enabled = opt_in;
}

/* Re-read env by re-initializing the telemetry subsystem for each case. */
static void reinit(const char *mode_env)
{
    if (mode_env)
        setenv("ARGUS_TELEMETRY", mode_env, 1);
    else
        unsetenv("ARGUS_TELEMETRY");
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
    /* Keep the install-id file out of the real home directory. */
    setenv("XDG_CONFIG_HOME", "/tmp/argus-test-cfg", 1);

    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_default_off),
        cmocka_unit_test(test_per_connection_optin),
        cmocka_unit_test(test_env_force_on),
        cmocka_unit_test(test_env_kill_switch),
        cmocka_unit_test(test_null_dbc_off),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
