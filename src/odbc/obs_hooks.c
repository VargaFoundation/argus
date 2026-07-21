/*
 * obs_hooks.c — weak, no-op observability tap points (open driver).
 *
 * These weak definitions make the open driver link and run with zero behaviour
 * change. When the enterprise addon is linked in, its strong definitions of
 * the same symbols win, and the taps emit. `__attribute__((weak))` is
 * supported by GCC and Clang — the only compilers the driver targets (Linux
 * GCC, macOS Clang, Windows MinGW-GCC).
 */
#include "argus/obs_hooks.h"

__attribute__((weak))
void argus_obs_hook_connect(const void *dbc, const char *connstr,
                            const char *backend, const char *host,
                            int ok, double connect_ms)
{
    (void)dbc; (void)connstr; (void)backend; (void)host;
    (void)ok; (void)connect_ms;
}

__attribute__((weak))
void argus_obs_hook_statement(const void *dbc, const char *backend,
                              const char *sql, double exec_ms,
                              unsigned long rows, unsigned long bytes,
                              const char *sqlstate)
{
    (void)dbc; (void)backend; (void)sql; (void)exec_ms;
    (void)rows; (void)bytes; (void)sqlstate;
}

__attribute__((weak))
void argus_obs_hook_disconnect(const void *dbc)
{
    (void)dbc;
}
