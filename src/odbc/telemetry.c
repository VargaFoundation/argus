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

/* ── Static state (initialized once at library load, before threads) ─── */

static int          g_mode = 0;        /* -1 forced off, 0 default, 1 forced on */
static char        *g_endpoint = NULL;
static char        *g_install_id = NULL;
static char         g_os[32] = "unknown";
static char         g_arch[32] = "unknown";
static char         g_os_version[64] = "";
static char         g_driver_version[24] = "";

static GAsyncQueue *g_queue = NULL;
static GThread     *g_sender = NULL;
static volatile gint g_shutting_down = 0;
static GOnce        g_sender_once = G_ONCE_INIT;

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

static void maybe_emit_notice(void)
{
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

static void flush_batch(GPtrArray *events)
{
    if (!events || events->len == 0)
        return;

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
                                  ARGUS_TELEMETRY_HTTP_TIMEOUT);
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
    return NULL;
}

static gpointer start_sender(gpointer data)
{
    (void)data;
    g_sender = g_thread_new("argus-telemetry", sender_thread, NULL);
    return NULL;
}

static void enqueue(char *event_json)
{
    if (!event_json)
        return;
    if (g_atomic_int_get(&g_shutting_down) ||
        g_async_queue_length(g_queue) >= ARGUS_TELEMETRY_QUEUE_MAX) {
        g_free(event_json);
        return;
    }
    g_once(&g_sender_once, start_sender, NULL);
    g_async_queue_push(g_queue, event_json);
}

/* ── Public lifecycle ────────────────────────────────────────────────── */

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
    g_endpoint = g_strdup(ep ? ep : "");

    snprintf(g_driver_version, sizeof(g_driver_version), "%d.%d.%d",
             ARGUS_VERSION_MAJOR, ARGUS_VERSION_MINOR, ARGUS_VERSION_PATCH);

    detect_platform();
    g_queue = g_async_queue_new_full(g_free);

    /* If hard-disabled, don't touch the filesystem at all. */
    if (g_mode != -1)
        load_or_create_install_id();
}

void argus_telemetry_shutdown(void)
{
    if (!g_queue)
        return;
    g_atomic_int_set(&g_shutting_down, 1);
    if (g_sender) {
        g_async_queue_push(g_queue, &g_stop_sentinel);
        g_thread_join(g_sender);
        g_sender = NULL;
    }
    g_async_queue_unref(g_queue);
    g_queue = NULL;
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
