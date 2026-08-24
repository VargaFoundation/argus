/*
 * obs_hooks.c — weak, no-op capability tap points (open driver).
 *
 * These definitions make the open driver link and run with zero behaviour
 * change. When an object providing strong definitions of the same symbols is
 * linked in, those win and the taps light up — that override is what
 * `__attribute__((weak))` buys, and GCC and Clang both honour it on ELF and
 * Mach-O.
 *
 * PE/COFF is the exception. A weak *definition* there becomes a weak external
 * that the linker only resolves through an explicit default alias, so ld
 * reports every use of these symbols as undefined and libargus_odbc.dll fails
 * to link. On Windows the definitions are therefore plain and strong: the
 * driver links and behaves identically, and a build that wants to override a
 * tap replaces this translation unit rather than out-ranking it.
 */
#include "argus/obs_hooks.h"
#include <stddef.h>

#if defined(_WIN32)
#define ARGUS_OBS_WEAK
#else
#define ARGUS_OBS_WEAK __attribute__((weak))
#endif

ARGUS_OBS_WEAK
void argus_obs_hook_connect(const void *dbc, const char *connstr,
                            const char *backend, const char *host,
                            const char *user, int ok, double connect_ms)
{
    (void)dbc; (void)connstr; (void)backend; (void)host;
    (void)user; (void)ok; (void)connect_ms;
}

ARGUS_OBS_WEAK
void argus_obs_hook_statement(const void *dbc, const char *backend,
                              const char *sql, double exec_ms,
                              unsigned long rows, unsigned long bytes,
                              const char *sqlstate)
{
    (void)dbc; (void)backend; (void)sql; (void)exec_ms;
    (void)rows; (void)bytes; (void)sqlstate;
}

ARGUS_OBS_WEAK
void argus_obs_hook_disconnect(const void *dbc)
{
    (void)dbc;
}

ARGUS_OBS_WEAK
char *argus_obs_hook_resolve_secret(const char *value)
{
    (void)value;
    return NULL;
}

ARGUS_OBS_WEAK
char *argus_obs_hook_token_get(const char *issuer, const char *client_id,
                               const char *scope, const char *subject)
{
    (void)issuer; (void)client_id; (void)scope; (void)subject;
    return NULL;
}

ARGUS_OBS_WEAK
void argus_obs_hook_token_put(const char *issuer, const char *client_id,
                              const char *scope, const char *subject,
                              const char *token, long long expiry_epoch_ms)
{
    (void)issuer; (void)client_id; (void)scope; (void)subject;
    (void)token; (void)expiry_epoch_ms;
}

ARGUS_OBS_WEAK
long argus_obs_hook_fetch_preset(const char *app_name)
{
    (void)app_name;
    return 0;
}

ARGUS_OBS_WEAK
int argus_obs_hook_guards(const void *dbc, unsigned long *max_rows,
                          unsigned long *timeout_ms)
{
    (void)dbc; (void)max_rows; (void)timeout_ms;
    return 0;
}

ARGUS_OBS_WEAK
int argus_obs_hook_pick_host(const void *dbc, const char *hosts_csv,
                             int nhosts)
{
    (void)dbc; (void)hosts_csv; (void)nhosts;
    return -1;
}

ARGUS_OBS_WEAK
void argus_obs_hook_host_result(const void *dbc, const char *hosts_csv,
                                int idx, int ok)
{
    (void)dbc; (void)hosts_csv; (void)idx; (void)ok;
}

ARGUS_OBS_WEAK
int argus_obs_hook_connect_gate(const void *dbc, const char *backend,
                                const char *connstr, char **reason)
{
    (void)dbc; (void)backend; (void)connstr;
    if (reason) *reason = NULL;
    return 1;   /* Apache-2.0 community build admits every connection */
}
