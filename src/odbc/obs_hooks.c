/*
 * obs_hooks.c — weak, no-op capability tap points (open driver).
 *
 * These weak definitions make the open driver link and run with zero behaviour
 * change. When the enterprise addon is linked in, its strong definitions of
 * the same symbols win, and the taps light up. `__attribute__((weak))` is
 * supported by GCC and Clang — the only compilers the driver targets (Linux
 * GCC, macOS Clang, Windows MinGW-GCC).
 */
#include "argus/obs_hooks.h"
#include <stddef.h>

__attribute__((weak))
void argus_obs_hook_connect(const void *dbc, const char *connstr,
                            const char *backend, const char *host,
                            const char *user, int ok, double connect_ms)
{
    (void)dbc; (void)connstr; (void)backend; (void)host;
    (void)user; (void)ok; (void)connect_ms;
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

__attribute__((weak))
char *argus_obs_hook_resolve_secret(const char *value)
{
    (void)value;
    return NULL;
}

__attribute__((weak))
char *argus_obs_hook_token_get(const char *issuer, const char *client_id,
                               const char *scope, const char *subject)
{
    (void)issuer; (void)client_id; (void)scope; (void)subject;
    return NULL;
}

__attribute__((weak))
void argus_obs_hook_token_put(const char *issuer, const char *client_id,
                              const char *scope, const char *subject,
                              const char *token, long long expiry_epoch_ms)
{
    (void)issuer; (void)client_id; (void)scope; (void)subject;
    (void)token; (void)expiry_epoch_ms;
}

__attribute__((weak))
long argus_obs_hook_fetch_preset(const char *app_name)
{
    (void)app_name;
    return 0;
}

__attribute__((weak))
int argus_obs_hook_guards(const void *dbc, unsigned long *max_rows,
                          unsigned long *timeout_ms)
{
    (void)dbc; (void)max_rows; (void)timeout_ms;
    return 0;
}

__attribute__((weak))
int argus_obs_hook_pick_host(const void *dbc, const char *hosts_csv,
                             int nhosts)
{
    (void)dbc; (void)hosts_csv; (void)nhosts;
    return -1;
}

__attribute__((weak))
void argus_obs_hook_host_result(const void *dbc, const char *hosts_csv,
                                int idx, int ok)
{
    (void)dbc; (void)hosts_csv; (void)idx; (void)ok;
}
