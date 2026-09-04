/* SPDX-License-Identifier: Apache-2.0 */
/*
 * argus/obs_hooks.h — generic capability tap points (open driver).
 *
 * A small, optional extension seam: each tap is declared here and defined as a
 * WEAK no-op in obs_hooks.c, so the open, Apache-2.0 driver links and runs with
 * zero behaviour change on its own. A build that links an object providing
 * STRONG definitions of the same symbols overrides the no-ops and the taps light
 * up.
 *
 * A provider must define EVERY tap below, not only the ones it cares about.
 * The no-ops all live in one obs_hooks.c, so they reach a link as one object;
 * an archive member is extracted whole or not at all, and a reference to any
 * tap a provider left undefined pulls in the no-op of every other tap with
 * it -- including the ones the provider did define. On ELF the no-op is weak
 * and loses quietly; on PE/COFF it is strong (a weak definition cannot be
 * linked there at all) and the link fails on a duplicate definition.
 * Defining all of them leaves the member nothing to contribute, so it is
 * never extracted. A build that compiles the driver from source can instead
 * define ARGUS_OBS_HOOKS_EXTERNAL, which empties obs_hooks.c entirely.
 *
 * Signatures are primitives only, so the open driver never depends on any
 * external type.
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
 *  - A provider that starts threads stops them in argus_obs_hook_unload();
 *    nothing of a provider may still run once the driver is unmapped.
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

/* ── Connection admission gate ─────────────────────────────────────
 * Fires once per connection in do_connect(), AFTER the backend is resolved and
 * BEFORE any host is tried, so a tap provider may veto any connection exactly
 * once. `connstr` is the redacted connection string (or NULL). On deny it MAY
 * set *reason to a malloc'd string (the driver free()s it and surfaces it in
 * the diagnostic). Returns 1 to ALLOW, 0 to DENY. The weak open-build
 * definition always returns 1, so the open, Apache-2.0 driver admits every
 * connection. */
int argus_obs_hook_connect_gate(const void *dbc, const char *backend,
                                const char *connstr, char **reason);

/* ── Multi-host selection (HOST=h1,h2,h3 failover) ─────────────────
 * pick: index of the host to try next given the original comma-separated
 * list (a stable key), or -1 to let the driver use its own order. result:
 * outcome of the attempt on hosts[idx], feeding circuit-breaker state. */
int  argus_obs_hook_pick_host(const void *dbc, const char *hosts_csv,
                              int nhosts);
void argus_obs_hook_host_result(const void *dbc, const char *hosts_csv,
                                int idx, int ok);

/* ── Driver unload ─────────────────────────────────────────────────
 * Fires when the driver is about to become unloadable, so a provider that
 * started threads can stop them before the code they run is unmapped. The
 * driver calls it from SQLFreeHandle(SQL_HANDLE_ENV) when the last
 * environment handle goes away -- a Driver Manager always does that before
 * it dlclose()s / FreeLibrary()s a driver -- and once more, as a last
 * resort, from the library destructor or DllMain(DLL_PROCESS_DETACH).
 * `may_wait` is 1 in the first case: an ordinary ODBC call is in progress,
 * so the provider should stop AND join its threads, within a bounded time
 * (a couple of seconds at most). It is 0 in the second: the Windows loader
 * lock is held, so the provider may only signal its threads and must never
 * block on one. Returns 1 when no thread of the provider is left running,
 * 0 otherwise; the driver then skips the teardown (curl_global_cleanup and
 * the like) that a still-running thread could trip over. The tap is
 * idempotent, and taps may fire again after it (a new environment handle),
 * so a provider must be able to start over lazily. */
int argus_obs_hook_unload(int may_wait);

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_OBS_HOOKS_H */
