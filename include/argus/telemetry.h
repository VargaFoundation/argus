#ifndef ARGUS_TELEMETRY_H
#define ARGUS_TELEMETRY_H

/*
 * telemetry.h - Anonymous, opt-in usage telemetry for the Argus ODBC driver.
 *
 * Telemetry is DISABLED by default. It is only ever transmitted when a
 * connection explicitly opts in (TELEMETRY=1 in the DSN / connection string)
 * or when the machine-wide ARGUS_TELEMETRY=1 environment variable is set.
 * ARGUS_TELEMETRY=0 is a hard kill switch that overrides every opt-in.
 *
 * Only a strict whitelist of non-identifying fields ever leaves the machine
 * (see docs/TELEMETRY.md and PRIVACY.md): event type, a random resettable
 * install id, driver/build version, OS/arch, backend name, latencies, coarse
 * row-count buckets, error counts, and SQLSTATE codes. Never: hostnames, user
 * names, database names, query text, or backend error messages.
 *
 * When the driver is built without ARGUS_HAS_TELEMETRY the emitters below
 * compile to nothing, so call sites need no #ifdef guards.
 */

#include <stdbool.h>

struct argus_dbc;

#ifdef ARGUS_HAS_TELEMETRY

/* Process-wide lifecycle (see lifecycle.h). init runs from the library
 * constructor. stop halts the background sender: a POST in flight gets a
 * short grace period, is then told to abort, and the caller waits about two
 * seconds at most; with may_wait false (DllMain, loader lock held) it only
 * signals the sender. Returns true when no sender thread is left; the sender
 * restarts lazily on the next event. shutdown releases everything else and
 * does nothing while a sender is still running. */
void argus_telemetry_init(void);
bool argus_telemetry_stop(bool may_wait);
void argus_telemetry_shutdown(void);

/* True only if a connection's events would actually be transmitted, i.e. the
 * global kill switch is not set and either the machine-wide opt-in or the
 * connection's own opt-in flag is active. */
bool argus_telemetry_active(const struct argus_dbc *dbc);

/* Event emitters. Each returns immediately (no allocation, no I/O) unless
 * telemetry is active for dbc. Emission is asynchronous and best-effort: the
 * event is queued for a background sender and can never block, slow, or fail
 * the calling ODBC operation. */
void argus_telemetry_connect(const struct argus_dbc *dbc, bool success,
                             int attempts);
void argus_telemetry_statement(const struct argus_dbc *dbc, double execute_ms,
                               unsigned long rows_fetched,
                               unsigned long errors);
void argus_telemetry_error(const struct argus_dbc *dbc, const char *sqlstate,
                           long native_error);
void argus_telemetry_session_end(const struct argus_dbc *dbc);

#else /* telemetry compiled out — no-op stubs */

static inline void argus_telemetry_init(void) {}
static inline bool argus_telemetry_stop(bool may_wait) { (void)may_wait; return true; }
static inline void argus_telemetry_shutdown(void) {}
static inline bool argus_telemetry_active(const struct argus_dbc *dbc)
{ (void)dbc; return false; }
static inline void argus_telemetry_connect(const struct argus_dbc *dbc,
                                           bool success, int attempts)
{ (void)dbc; (void)success; (void)attempts; }
static inline void argus_telemetry_statement(const struct argus_dbc *dbc,
                                             double execute_ms,
                                             unsigned long rows_fetched,
                                             unsigned long errors)
{ (void)dbc; (void)execute_ms; (void)rows_fetched; (void)errors; }
static inline void argus_telemetry_error(const struct argus_dbc *dbc,
                                         const char *sqlstate, long native_error)
{ (void)dbc; (void)sqlstate; (void)native_error; }
static inline void argus_telemetry_session_end(const struct argus_dbc *dbc)
{ (void)dbc; }

#endif /* ARGUS_HAS_TELEMETRY */

#endif /* ARGUS_TELEMETRY_H */
