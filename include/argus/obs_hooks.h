/*
 * argus/obs_hooks.h — generic observability tap points (open driver).
 *
 * These are the only additions the open, Apache-2.0 driver needs to become
 * enterprise-observable. They are declared here and defined as WEAK no-ops in
 * obs_hooks.c, so the open driver alone does nothing. An enterprise build that
 * links libargus_enterprise provides STRONG definitions (see
 * argus_ee/integration.h) and the taps light up. Signatures are primitives
 * only, so the open driver never depends on any enterprise type.
 *
 * `dbc` is the connection handle as an OPAQUE identity token: implementations
 * may key correlation state on it but must never dereference it. `connstr` is
 * a REDACTED copy of the connection string (secret-bearing values already
 * masked by the driver) — never the raw string.
 *
 * Call them PER STATEMENT / PER CONNECTION, never per row (hot-path
 * invariant). The statement tap fires once per statement handle, at release,
 * with cumulative counters; `bytes` is 0 when the driver does not track it.
 */
#ifndef ARGUS_OBS_HOOKS_H
#define ARGUS_OBS_HOOKS_H

#ifdef __cplusplus
extern "C" {
#endif

void argus_obs_hook_connect(const void *dbc, const char *connstr,
                            const char *backend, const char *host,
                            int ok, double connect_ms);

void argus_obs_hook_statement(const void *dbc, const char *backend,
                              const char *sql, double exec_ms,
                              unsigned long rows, unsigned long bytes,
                              const char *sqlstate);

void argus_obs_hook_disconnect(const void *dbc);

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_OBS_HOOKS_H */
