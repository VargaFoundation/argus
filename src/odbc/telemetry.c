/*
 * telemetry.c - Anonymous, opt-in usage telemetry (see telemetry.h).
 *
 * Design invariants:
 *  - Disabled unless explicitly opted in; ARGUS_TELEMETRY=0 is a hard override.
 *  - Strict field whitelist built here; no caller string other than the
 *    backend name (from our own registry) and SQLSTATE (5 fixed chars) is ever
 *    serialized. Hostnames, users, databases, query text and backend error
 *    messages never reach this file.
 *  - Fully asynchronous and best-effort: emitters only enqueue; a single
 *    background thread batches and POSTs. Nothing here can block or fail an
 *    ODBC call. A bounded queue drops events under backpressure.
 *  - Stops in bounded time: the sender is started lazily and stopped by
 *    argus_telemetry_stop() before the driver can be unmapped (lifecycle.h);
 *    an in-flight POST is aborted rather than waited for.
 */

#include "argus/telemetry.h"
#include "argus/handle.h"
#include "argus/log.h"
#include "../backend/http_client.h"

#include <glib.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#if defined(__has_include)
#  if __has_include("argus_build_id.h")
#    include "argus_build_id.h"
#  endif
#endif
#ifndef ARGUS_BUILD_ID
#define ARGUS_BUILD_ID "unknown"
#endif

#ifndef ARGUS_TELEMETRY_ENDPOINT
#define ARGUS_TELEMETRY_ENDPOINT ""
#endif

#define ARGUS_TELEMETRY_SCHEMA_VERSION 1
#define ARGUS_TELEMETRY_QUEUE_MAX      1000   /* drop new events past this */
#define ARGUS_TELEMETRY_BATCH_MAX      64
#define ARGUS_TELEMETRY_POLL_US        (2 * G_USEC_PER_SEC)
#define ARGUS_TELEMETRY_HTTP_TIMEOUT   10L
/* argus_telemetry_stop(): how long an in-flight POST may still finish on its
 * own before it is told to abort, and how long the caller waits for the
 * sender in total. libcurl polls the abort callback at least about once a
 * second, so the second bound leaves room for one such round after the
 * first. */
#define ARGUS_TELEMETRY_STOP_GRACE_MS  500
#define ARGUS_TELEMETRY_STOP_WAIT_MS   2000

/* ── Static state (initialized once at library load, before threads) ─── */

static int          g_mode = 0;        /* -1 forced off, 0 default, 1 forced on */
static char        *g_endpoint = NULL;
static char        *g_install_id = NULL;
static char         g_os[32] = "unknown";
static char         g_arch[32] = "unknown";
static char         g_os_version[64] = "";
static char         g_driver_version[24] = "";

static GAsyncQueue *g_queue = NULL;

/* Sender thread state. g_sender_lock guards g_sender, g_sender_done and
 * g_sender_abandoned; the two flags below are read from the sender without
 * it. g_shutting_down tells the sender (and enqueue) that a stop is under
 * way; g_abort_http tells the POST in flight to give up. */
static GMutex       g_sender_lock;
static GMutex       g_stop_lock;        /* one argus_telemetry_stop() at a time */
static GCond        g_sender_cond;      /* signalled when the sender is done */
static GThread     *g_sender = NULL;
static gboolean     g_sender_done = FALSE;
static gboolean     g_sender_abandoned = FALSE; /* stopped without a join */
static volatile gint g_shutting_down = 0;
static volatile gint g_abort_http = 0;

/* Sentinel pushed on shutdown to wake the sender from its blocking pop. */
static char g_stop_sentinel;

/* ── Platform detection ──────────────────────────────────────────────── */

#ifdef _WIN32
#include <windows.h>

static void detect_platform(void)
{
    g_strlcpy(g_os, "windows", sizeof(g_os));

    SYSTEM_INFO si;
    GetNativeSystemInfo(&si);
    switch (si.wProcessorArchitecture) {
    case PROCESSOR_ARCHITECTURE_AMD64: g_strlcpy(g_arch, "x86_64", sizeof(g_arch)); break;
    case PROCESSOR_ARCHITECTURE_ARM64: g_strlcpy(g_arch, "aarch64", sizeof(g_arch)); break;
    case PROCESSOR_ARCHITECTURE_INTEL: g_strlcpy(g_arch, "x86", sizeof(g_arch)); break;
    default: g_strlcpy(g_arch, "unknown", sizeof(g_arch)); break;
    }

    /* GetVersionEx is deprecated and lies without a manifest; RtlGetVersion
     * (ntdll) returns the real build. Resolved dynamically to avoid a link
     * dependency and the winternl types. */
    HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
    if (ntdll) {
        typedef LONG (WINAPI *rtl_get_version_fn)(void *);
        rtl_get_version_fn p =
            (rtl_get_version_fn)(void *)GetProcAddress(ntdll, "RtlGetVersion");
        if (p) {
            OSVERSIONINFOEXW info;
            memset(&info, 0, sizeof(info));
            info.dwOSVersionInfoSize = sizeof(info);
            if (p(&info) == 0) {
                snprintf(g_os_version, sizeof(g_os_version), "%lu.%lu.%lu",
                         (unsigned long)info.dwMajorVersion,
                         (unsigned long)info.dwMinorVersion,
                         (unsigned long)info.dwBuildNumber);
            }
        }
    }
}

#else
#include <sys/utsname.h>

static void detect_platform(void)
{
    struct utsname u;
    if (uname(&u) == 0) {
        /* Lowercase the OS name: "Linux" -> "linux", "Darwin" -> "darwin". */
        size_t i;
        for (i = 0; u.sysname[i] && i < sizeof(g_os) - 1; i++)
            g_os[i] = (char)g_ascii_tolower(u.sysname[i]);
        g_os[i] = '\0';
        g_strlcpy(g_arch, u.machine, sizeof(g_arch));
        g_strlcpy(g_os_version, u.release, sizeof(g_os_version));
    }
}
#endif

/* ── Install id + first-run notice (persisted, non-identifying) ───────── */

static char *config_path(const char *name)
{
    /* g_get_user_config_dir(): %APPDATA% on Windows, $XDG_CONFIG_HOME or
     * ~/.config on POSIX. */
    return g_build_filename(g_get_user_config_dir(), "argus", name, NULL);
}

static void load_or_create_install_id(void)
{
    char *path = config_path("install_id");
    char *contents = NULL;
    gsize len = 0;

    if (g_file_get_contents(path, &contents, &len, NULL) && contents) {
        g_strstrip(contents);
        if (*contents)
            g_install_id = g_strdup(contents);
    }
    g_free(contents);

    if (!g_install_id) {
        /* Random, not derived from any hardware/user identifier; deleting the
         * file resets it. */
        g_install_id = g_uuid_string_random();
        char *dir = g_path_get_dirname(path);
        g_mkdir_with_parents(dir, 0700);
        g_free(dir);
        g_file_set_contents(path, g_install_id, -1, NULL);
    }
    g_free(path);
}

/* Idempotent, thread-safe lazy creation: only reached from code paths where
 * telemetry is active for a connection, i.e. after an explicit opt-in. */
static void ensure_install_id(void)
{
    static gsize once = 0;
    if (g_once_init_enter(&once)) {
        load_or_create_install_id();
        g_once_init_leave(&once, 1);
    }
}

static void maybe_emit_notice(void)
{
    ensure_install_id();
    char *path = config_path("telemetry_notice_shown");
    if (g_file_test(path, G_FILE_TEST_EXISTS)) {
        g_free(path);
        return;
    }
    ARGUS_LOG_INFO(
        "Argus telemetry is ON for this connection. Anonymous usage data "
        "(backend, latencies, OS, error codes; never hostnames, credentials, "
        "or query text) is sent to %s. Disable with TELEMETRY=0 or "
        "ARGUS_TELEMETRY=0. See docs/TELEMETRY.md and PRIVACY.md. Install id: %s",
        g_endpoint, g_install_id ? g_install_id : "?");
    /* The log file may never be read; make the first-run notice visible on
     * stderr too, as PRIVACY.md promises a user-visible notice. */
    fprintf(stderr,
            "[Argus] Telemetry is ON (opt-in). Anonymous usage data is sent "
            "to %s — never hostnames, credentials or query text. Disable with "
            "TELEMETRY=0 / ARGUS_TELEMETRY=0. See PRIVACY.md.\n",
            g_endpoint);

    char *dir = g_path_get_dirname(path);
    g_mkdir_with_parents(dir, 0700);
    g_free(dir);
    g_file_set_contents(path, "1", -1, NULL);
    g_free(path);
}

/* ── JSON helpers (hand-built; inputs are controlled, still escaped) ──── */

static void json_append_escaped(GString *s, const char *val)
{
    g_string_append_c(s, '"');
    for (const char *p = val; p && *p; p++) {
        unsigned char c = (unsigned char)*p;
        switch (c) {
        case '"':  g_string_append(s, "\\\""); break;
        case '\\': g_string_append(s, "\\\\"); break;
        case '\n': g_string_append(s, "\\n");  break;
        case '\r': g_string_append(s, "\\r");  break;
        case '\t': g_string_append(s, "\\t");  break;
        default:
            if (c < 0x20)
                g_string_append_printf(s, "\\u%04x", c);
            else
                g_string_append_c(s, (char)c);
        }
    }
    g_string_append_c(s, '"');
}

static const char *rows_bucket(unsigned long n)
{
    if (n == 0)        return "0";
    if (n < 10)        return "1-9";
    if (n < 100)       return "10-99";
    if (n < 1000)      return "100-999";
    if (n < 10000)     return "1k-9k";
    if (n < 100000)    return "10k-99k";
    if (n < 1000000)   return "100k-999k";
    return "1M+";
}

/* ── Sender thread ───────────────────────────────────────────────────── */

/* argus_http_abort_fn: polled by libcurl during the POST. */
static int http_should_abort(void *ctx)
{
    (void)ctx;
    return g_atomic_int_get(&g_abort_http);
}

static void flush_batch(GPtrArray *events)
{
    if (!events || events->len == 0)
        return;
    /* Past the stop grace period nothing may be sent any more; a POST that
     * started earlier is being aborted through http_should_abort(). */
    if (g_atomic_int_get(&g_abort_http)) {
        ARGUS_LOG_DEBUG("Telemetry stopping (dropped %u event(s))",
                        events->len);
        return;
    }

    ensure_install_id();

    GString *body = g_string_new("{");
    g_string_append_printf(body, "\"schema_version\":%d,",
                           ARGUS_TELEMETRY_SCHEMA_VERSION);
    g_string_append(body, "\"install_id\":");
    json_append_escaped(body, g_install_id ? g_install_id : "");
    g_string_append(body, ",\"driver_version\":");
    json_append_escaped(body, g_driver_version);
    g_string_append(body, ",\"build_id\":");
    json_append_escaped(body, ARGUS_BUILD_ID);
    g_string_append(body, ",\"os\":");
    json_append_escaped(body, g_os);
    g_string_append(body, ",\"arch\":");
    json_append_escaped(body, g_arch);
    g_string_append(body, ",\"os_version\":");
    json_append_escaped(body, g_os_version);
    g_string_append(body, ",\"events\":[");
    for (guint i = 0; i < events->len; i++) {
        if (i) g_string_append_c(body, ',');
        g_string_append(body, (const char *)g_ptr_array_index(events, i));
    }
    g_string_append(body, "]}");

    int rc = argus_http_post_json(g_endpoint, body->str,
                                  ARGUS_TELEMETRY_HTTP_TIMEOUT,
                                  http_should_abort, NULL);
    if (rc != 0)
        ARGUS_LOG_DEBUG("Telemetry POST failed (dropped %u event(s))",
                        events->len);

    g_string_free(body, TRUE);
}

static gpointer sender_thread(gpointer data)
{
    (void)data;
    while (!g_atomic_int_get(&g_shutting_down)) {
        gpointer first = g_async_queue_timeout_pop(g_queue,
                                                   ARGUS_TELEMETRY_POLL_US);
        if (!first)
            continue;
        if (first == &g_stop_sentinel)
            break;

        GPtrArray *batch = g_ptr_array_new_with_free_func(g_free);
        g_ptr_array_add(batch, first);
        while (batch->len < ARGUS_TELEMETRY_BATCH_MAX) {
            gpointer e = g_async_queue_try_pop(g_queue);
            if (!e)
                break;
            if (e == &g_stop_sentinel) {
                g_atomic_int_set(&g_shutting_down, 1);
                break;
            }
            g_ptr_array_add(batch, e);
        }
        flush_batch(batch);
        g_ptr_array_free(batch, TRUE);
    }

    /* Final best-effort drain so the last session's events are not lost. */
    GPtrArray *tail = g_ptr_array_new_with_free_func(g_free);
    gpointer e;
    while ((e = g_async_queue_try_pop(g_queue)) != NULL &&
           tail->len < ARGUS_TELEMETRY_BATCH_MAX) {
        if (e == &g_stop_sentinel)
            continue;
        g_ptr_array_add(tail, e);
    }
    flush_batch(tail);
    g_ptr_array_free(tail, TRUE);

    /* Signalled before returning, so a stop() under the Windows loader lock
     * can learn that the thread is finished without waiting for it to exit
     * (thread exit needs that lock too). */
    g_mutex_lock(&g_sender_lock);
    g_sender_done = TRUE;
    g_cond_broadcast(&g_sender_cond);
    g_mutex_unlock(&g_sender_lock);
    return NULL;
}

/* Caller holds g_sender_lock. */
static void start_sender_locked(void)
{
    g_sender_done = FALSE;
    g_atomic_int_set(&g_abort_http, 0);
    g_atomic_int_set(&g_shutting_down, 0);
    g_sender = g_thread_try_new("argus-telemetry", sender_thread, NULL, NULL);
}

static void enqueue(char *event_json)
{
    if (!event_json)
        return;
    g_mutex_lock(&g_sender_lock);
    if (!g_queue || g_sender_abandoned ||
        g_atomic_int_get(&g_shutting_down) ||
        g_async_queue_length(g_queue) >= ARGUS_TELEMETRY_QUEUE_MAX) {
        g_mutex_unlock(&g_sender_lock);
        g_free(event_json);
        return;
    }
    /* Started on the first event and again after a stop(): a Driver Manager
     * that frees its environment handle and allocates a new one keeps the
     * driver loaded, and its telemetry keeps working. */
    if (!g_sender)
        start_sender_locked();
    if (!g_sender) {
        g_mutex_unlock(&g_sender_lock);
        g_free(event_json);
        return;
    }
    g_async_queue_push(g_queue, event_json);
    g_mutex_unlock(&g_sender_lock);
}

/* ── Public lifecycle ────────────────────────────────────────────────── */

/*
 * https, or a loopback host. Nothing else: an http:// endpoint on a real
 * network would put the event payload in clear on the wire.
 */
static bool endpoint_is_secure(const char *url)
{
    if (g_ascii_strncasecmp(url, "https://", 8) == 0) return true;
    if (g_ascii_strncasecmp(url, "http://", 7) != 0)  return false;

    const char *host = url + 7;
    size_t n = strcspn(host, ":/?#");
    return (n == 9 && g_ascii_strncasecmp(host, "localhost", 9) == 0) ||
           (n == 3 && strncmp(host, "::1", 3) == 0) ||
           (n == 5 && strncmp(host, "[::1]", 5) == 0) ||
           (n >= 8 && strncmp(host, "127.", 4) == 0);
}

void argus_telemetry_init(void)
{
    const char *env = getenv("ARGUS_TELEMETRY");
    if (env && *env) {
        if (strcmp(env, "0") == 0 || g_ascii_strcasecmp(env, "false") == 0 ||
            g_ascii_strcasecmp(env, "no") == 0 || g_ascii_strcasecmp(env, "off") == 0)
            g_mode = -1;
        else if (strcmp(env, "1") == 0 || g_ascii_strcasecmp(env, "true") == 0 ||
                 g_ascii_strcasecmp(env, "yes") == 0 || g_ascii_strcasecmp(env, "on") == 0)
            g_mode = 1;
    }

    const char *ep = getenv("ARGUS_TELEMETRY_ENDPOINT");
    if (!ep || !*ep)
        ep = ARGUS_TELEMETRY_ENDPOINT;
    /*
     * PRIVACY.md promises the events travel over TLS, and the environment
     * override could point them at a plain http:// collector, putting the
     * payload on the wire in clear. Anything but https is refused and
     * telemetry stays off rather than downgrading silently -- except on
     * loopback, where a local collector or a sidecar never puts the payload
     * on a network at all.
     */
    if (ep && *ep && !endpoint_is_secure(ep)) {
        ARGUS_LOG_WARN("Telemetry endpoint %s is neither https nor loopback; "
                       "telemetry stays off", ep);
        ep = "";
    }
    g_endpoint = g_strdup(ep ? ep : "");

    snprintf(g_driver_version, sizeof(g_driver_version), "%d.%d.%d",
             ARGUS_VERSION_MAJOR, ARGUS_VERSION_MINOR, ARGUS_VERSION_PATCH);

    detect_platform();
    g_queue = g_async_queue_new_full(g_free);

    /* The install id is created lazily, on the first event of an opted-in
     * connection (ensure_install_id) — never at library load. Writing a
     * persistent machine identifier for users who did not opt in would
     * contradict PRIVACY.md even if the id is never sent. */
}

/* Wait on g_sender_cond (g_sender_lock held) until the sender is done or
 * `deadline` (monotonic microseconds) passes. */
static void wait_sender_done_until(gint64 deadline)
{
    while (!g_sender_done &&
           g_cond_wait_until(&g_sender_cond, &g_sender_lock, deadline))
        ;
}

bool argus_telemetry_stop(bool may_wait)
{
    /* One stop at a time. Under the loader lock (may_wait false) never block
     * on another stopper either: it may be joining the sender, whose exit
     * needs the very lock this caller holds. */
    if (may_wait)
        g_mutex_lock(&g_stop_lock);
    else if (!g_mutex_trylock(&g_stop_lock))
        return false;

    g_mutex_lock(&g_sender_lock);
    GThread *t = g_sender;
    if (!t) {
        bool clean = !g_sender_abandoned;
        g_mutex_unlock(&g_sender_lock);
        g_mutex_unlock(&g_stop_lock);
        return clean;
    }

    /* Wake the sender out of its poll; it drains what is queued and goes. */
    g_atomic_int_set(&g_shutting_down, 1);
    g_async_queue_push(g_queue, &g_stop_sentinel);

    if (may_wait) {
        gint64 start = g_get_monotonic_time();
        /* Let a POST in flight finish on its own first ... */
        wait_sender_done_until(start +
                               ARGUS_TELEMETRY_STOP_GRACE_MS * G_TIME_SPAN_MILLISECOND);
        if (!g_sender_done) {
            /* ... then tell it to give up; libcurl notices within about a
             * second. Anything still queued is dropped by flush_batch(). */
            g_atomic_int_set(&g_abort_http, 1);
            wait_sender_done_until(start +
                                   ARGUS_TELEMETRY_STOP_WAIT_MS * G_TIME_SPAN_MILLISECOND);
        }
    } else {
        g_atomic_int_set(&g_abort_http, 1);
    }

    /* Joining is only allowed where the thread can actually exit: outside
     * DllMain. Once done it is a formality; if the sender is still not done
     * after the wait -- libcurl stuck somewhere it cannot poll the callback
     * -- the join is bounded by the HTTP timeout, which beats letting the
     * thread run on in code that is about to disappear. */
    bool joined = false;
    if (may_wait) {
        g_mutex_unlock(&g_sender_lock);
        g_thread_join(t);
        g_mutex_lock(&g_sender_lock);
        joined = true;
    } else {
        g_thread_unref(t);
        g_sender_abandoned = TRUE;
    }
    g_sender = NULL;
    if (joined) {
        /* Whatever the final drain left behind is dropped, the sentinel
         * included -- a restarted sender must not trip over a stale one.
         * Then ready for a lazy restart on the next event. */
        gpointer e;
        while ((e = g_async_queue_try_pop(g_queue)) != NULL)
            if (e != &g_stop_sentinel)
                g_free(e);
        g_atomic_int_set(&g_abort_http, 0);
        g_atomic_int_set(&g_shutting_down, 0);
    }
    g_mutex_unlock(&g_sender_lock);
    g_mutex_unlock(&g_stop_lock);
    return joined;
}

void argus_telemetry_shutdown(void)
{
    if (!argus_telemetry_stop(true))
        return;   /* an abandoned sender may still use all of this */
    g_mutex_lock(&g_sender_lock);
    if (g_queue) {
        g_async_queue_unref(g_queue);
        g_queue = NULL;
    }
    g_mutex_unlock(&g_sender_lock);
    g_free(g_endpoint);   g_endpoint = NULL;
    g_free(g_install_id); g_install_id = NULL;
}

bool argus_telemetry_active(const struct argus_dbc *dbc)
{
    if (g_mode == -1)              return false;   /* hard kill switch */
    if (!g_endpoint || !*g_endpoint) return false; /* nowhere to send */
    if (g_mode == 1)              return true;     /* machine-wide opt-in */
    return dbc && dbc->telemetry_enabled;          /* per-connection opt-in */
}

/* ── Emitters ────────────────────────────────────────────────────────── */

static const char *backend_name_of(const struct argus_dbc *dbc)
{
    if (dbc && dbc->backend && dbc->backend->name)
        return dbc->backend->name;
    if (dbc && dbc->backend_name)
        return dbc->backend_name;
    return "unknown";
}

void argus_telemetry_connect(const struct argus_dbc *dbc, bool success,
                             int attempts)
{
    if (!argus_telemetry_active(dbc))
        return;
    maybe_emit_notice();

    GString *e = g_string_new("{\"type\":\"connect\",\"backend\":");
    json_append_escaped(e, backend_name_of(dbc));
    g_string_append_printf(e, ",\"latency_ms\":%.1f,\"success\":%s,\"attempts\":%d}",
                           dbc->connect_time_ms, success ? "true" : "false",
                           attempts);
    enqueue(g_string_free(e, FALSE));
}

void argus_telemetry_statement(const struct argus_dbc *dbc, double execute_ms,
                               unsigned long rows_fetched, unsigned long errors)
{
    if (!argus_telemetry_active(dbc))
        return;

    GString *e = g_string_new("{\"type\":\"statement\",\"backend\":");
    json_append_escaped(e, backend_name_of(dbc));
    g_string_append_printf(e, ",\"execute_ms\":%.1f,\"rows_bucket\":", execute_ms);
    json_append_escaped(e, rows_bucket(rows_fetched));
    g_string_append_printf(e, ",\"errors\":%lu}", errors);
    enqueue(g_string_free(e, FALSE));
}

void argus_telemetry_error(const struct argus_dbc *dbc, const char *sqlstate,
                           long native_error)
{
    if (!argus_telemetry_active(dbc))
        return;

    char state[6] = "";
    char sqlclass[3] = "";
    if (sqlstate) {
        g_strlcpy(state, sqlstate, sizeof(state));
        sqlclass[0] = state[0];
        sqlclass[1] = state[1];
        sqlclass[2] = '\0';
    }

    GString *e = g_string_new("{\"type\":\"error\",\"backend\":");
    json_append_escaped(e, backend_name_of(dbc));
    g_string_append(e, ",\"sqlstate\":");
    json_append_escaped(e, state);
    g_string_append(e, ",\"sqlclass\":");
    json_append_escaped(e, sqlclass);
    g_string_append_printf(e, ",\"native\":%ld}", native_error);
    enqueue(g_string_free(e, FALSE));
}

void argus_telemetry_session_end(const struct argus_dbc *dbc)
{
    if (!argus_telemetry_active(dbc))
        return;

    GString *e = g_string_new("{\"type\":\"session\",\"backend\":");
    json_append_escaped(e, backend_name_of(dbc));
    g_string_append_printf(e, ",\"errors\":%lu}", dbc->errors_total);
    enqueue(g_string_free(e, FALSE));
}
