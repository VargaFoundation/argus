/*
 * argus/obs_hooks.h — generic capability tap points (open driver).
 *
 * These are the only additions the open, Apache-2.0 driver needs to become
 * enterprise-extensible. They are declared here and defined as WEAK no-ops in
 * obs_hooks.c, so the open driver alone changes nothing. A build that links
 * the enterprise addon provides STRONG definitions (see argus_ee/integration.h)
 * and the taps light up. Signatures are primitives only, so the open driver
 * never depends on any external type.
 *
 * Conventions:
 *  - `dbc` is the connection handle as an OPAQUE identity token: consumers may
 *    key correlation state on it but must never dereference it.
 *  - `connstr` is a REDACTED copy of the connection string (secret-bearing
 *    values already masked by the driver) — never the raw string.
 *  - Functions returning `char *` return buffers released with free(); tap
 *    providers are compiled into the same module, so allocators match.
 *  - Observability taps fire PER STATEMENT / PER CONNECTION, never per row
 *    (hot-path invariant). The statement tap fires once per statement handle,
 *    at release, with cumulative counters; `bytes` is 0 when untracked.
 */
#ifndef ARGUS_OBS_HOOKS_H
#define ARGUS_OBS_HOOKS_H

#ifdef __cplusplus
extern "C" {
#endif

/* ── Observability ─────────────────────────────────────────────── */

void argus_obs_hook_connect(const void *dbc, const char *connstr,
                            const char *backend, const char *host,
                            const char *user, int ok, double connect_ms);

void argus_obs_hook_statement(const void *dbc, const char *backend,
                              const char *sql, double exec_ms,
                              unsigned long rows, unsigned long bytes,
                              const char *sqlstate);

void argus_obs_hook_disconnect(const void *dbc);

/* ── Secret resolution ─────────────────────────────────────────────
 * `value` may embed ${scheme:ref} references (e.g. PWD=${vault:kv/db#pw}).
 * Returns the resolved value (free()), or NULL when nothing resolves. The
 * driver only calls this for values containing "${". */
char *argus_obs_hook_resolve_secret(const char *value);

/* ── OAuth2 token cache ────────────────────────────────────────────
 * get: a still-fresh cached access token for (issuer, client, scope, subject),
 * or NULL. Caller wipes and free()s. put: publish a token with its ABSOLUTE
 * expiry (epoch milliseconds); the provider copies it. */
char *argus_obs_hook_token_get(const char *issuer, const char *client_id,
                               const char *scope, const char *subject);
void  argus_obs_hook_token_put(const char *issuer, const char *client_id,
                               const char *scope, const char *subject,
                               const char *token, long long expiry_epoch_ms);

/* ── Per-BI-tool fetch preset ──────────────────────────────────────
 * Rows per backend round-trip suggested for `app_name`, or 0 for none. Only
 * consulted when neither the DSN nor the app set FETCHBUFFERSIZE. */
long argus_obs_hook_fetch_preset(const char *app_name);

/* ── Statement guardrails ──────────────────────────────────────────
 * Returns 1 and fills the limits (0 = unlimited) when a policy applies to
 * this connection; the driver applies them only where the application did not
 * set stricter values itself. */
int argus_obs_hook_guards(const void *dbc, unsigned long *max_rows,
                          unsigned long *timeout_ms);

/* ── Multi-host selection (HOST=h1,h2,h3 failover) ─────────────────
 * pick: index of the host to try next given the original comma-separated
 * list (a stable key), or -1 to let the driver use its own order. result:
 * outcome of the attempt on hosts[idx], feeding circuit-breaker state. */
int  argus_obs_hook_pick_host(const void *dbc, const char *hosts_csv,
                              int nhosts);
void argus_obs_hook_host_result(const void *dbc, const char *hosts_csv,
                                int idx, int ok);

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_OBS_HOOKS_H */
