/*
 * test_telemetry.c - Opt-in gating and sender lifecycle for telemetry.
 *
 * Verifies argus_telemetry_active() honours the layered controls:
 *   - disabled by default (no env, no per-connection opt-in),
 *   - per-connection opt-in (TELEMETRY=1 -> dbc->telemetry_enabled),
 *   - machine-wide opt-in (ARGUS_TELEMETRY=1),
 *   - hard kill switch (ARGUS_TELEMETRY=0) overriding every opt-in.
 * These cases never enqueue an event, so no network I/O occurs.
 *
 * The lifecycle cases point the sender at a loopback listener that never
 * answers, so a POST hangs until the driver's HTTP timeout: stopping the
 * sender must not wait for that (the driver is about to be unmapped), and
 * the sender must come back for the next event after a stop.
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>

#include "argus/handle.h"
#include "argus/telemetry.h"

#ifndef _WIN32
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>
#endif

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

#ifndef _WIN32
/* Listen on an ephemeral loopback port. Connections complete in the kernel
 * backlog and never get a byte back, so a POST waits for its response
 * until libcurl's timeout fires. */
static int hanging_listener(char *url, size_t urlsz)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    socklen_t len = sizeof(sa);
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0 ||
        listen(fd, 8) < 0 ||
        getsockname(fd, (struct sockaddr *)&sa, &len) < 0) {
        close(fd);
        return -1;
    }
    snprintf(url, urlsz, "http://127.0.0.1:%d/v1/events",
             (int)ntohs(sa.sin_port));
    return fd;
}

/* Accept the next connection within timeout_ms, or return -1. Accepting
 * changes nothing for the client: still no response. */
static int wait_connection(int lfd, int timeout_ms)
{
    struct pollfd p = { lfd, POLLIN, 0 };
    if (poll(&p, 1, timeout_ms) <= 0)
        return -1;
    return accept(lfd, NULL, NULL);
}
#endif

static void test_stop_idle_is_clean(void **state)
{
    (void)state;
    reinit(NULL);
    /* Nothing was ever enqueued: no sender, nothing to wait for, and it is
     * idempotent -- SQLFreeHandle(SQL_HANDLE_ENV) calls this every time the
     * last environment goes away. */
    assert_true(argus_telemetry_stop(true));
    assert_true(argus_telemetry_stop(true));
    assert_true(argus_telemetry_stop(false));
    argus_telemetry_shutdown();
}

static void test_stop_is_bounded_and_sender_restarts(void **state)
{
    (void)state;
#ifdef _WIN32
    skip();
#else
    char url[64];
    int lfd = hanging_listener(url, sizeof(url));
    assert_true(lfd >= 0);
    g_setenv("ARGUS_TELEMETRY_ENDPOINT", url, TRUE);
    reinit("1");
    argus_dbc_t dbc;
    make_dbc(&dbc, true);

    /* First event: the sender starts and its POST hangs on our listener. */
    argus_telemetry_statement(&dbc, 1.0, 10, 0);
    int c1 = wait_connection(lfd, 5000);
    assert_true(c1 >= 0);

    /* Stopping must abort that POST, not sit through the 10 s HTTP timeout. */
    gint64 t0 = g_get_monotonic_time();
    assert_true(argus_telemetry_stop(true));
    gint64 elapsed_ms = (g_get_monotonic_time() - t0) / G_TIME_SPAN_MILLISECOND;
    assert_true(elapsed_ms < 5000);

    /* Not a one-shot: the next event brings the sender back. */
    argus_telemetry_statement(&dbc, 1.0, 10, 0);
    int c2 = wait_connection(lfd, 5000);
    assert_true(c2 >= 0);
    assert_true(argus_telemetry_stop(true));

    close(c1);
    close(c2);
    close(lfd);
    argus_telemetry_shutdown();
    g_unsetenv("ARGUS_TELEMETRY_ENDPOINT");
#endif
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
        cmocka_unit_test(test_stop_idle_is_clean),
        cmocka_unit_test(test_stop_is_bounded_and_sender_restarts),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
