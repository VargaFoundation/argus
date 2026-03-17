/*
 * Argus ODBC Driver — Connection Pool
 *
 * Pool connections by (host, port, backend, credentials) to avoid
 * expensive reconnection overhead. Thread-safe via GLib GMutex.
 */

#include "argus/handle.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <glib.h>

/* Maximum connections per pool key */
#define ARGUS_POOL_MAX_PER_KEY 8

/* Maximum total pool entries */
#define ARGUS_POOL_MAX_TOTAL 64

/* Pool entry: a cached backend connection */
typedef struct argus_pool_entry {
    char                   *host;
    int                     port;
    char                   *backend_name;
    char                   *username;
    const argus_backend_t  *backend;
    argus_backend_conn_t    conn;
    bool                    in_use;
    gint64                  last_used;  /* monotonic time */
} argus_pool_entry_t;

/* Global connection pool */
static struct {
    argus_pool_entry_t entries[ARGUS_POOL_MAX_TOTAL];
    int                count;
    GMutex             mutex;
    gsize              init_once;
} g_pool;

/* ── Internal: initialize pool (called once, thread-safe) ────── */

static void pool_ensure_init(void)
{
    if (g_once_init_enter(&g_pool.init_once)) {
        g_mutex_init(&g_pool.mutex);
        g_pool.count = 0;
        g_once_init_leave(&g_pool.init_once, 1);
    }
}

/* ── Internal: match a pool entry ────────────────────────────── */

static bool pool_entry_matches(const argus_pool_entry_t *entry,
                                const char *host, int port,
                                const char *backend_name,
                                const char *username)
{
    if (entry->port != port) return false;
    if (strcmp(entry->host, host) != 0) return false;
    if (strcmp(entry->backend_name, backend_name) != 0) return false;
    if (strcmp(entry->username, username ? username : "") != 0) return false;
    return true;
}

/* ── Public: try to acquire a pooled connection ──────────────── */

argus_backend_conn_t argus_pool_acquire(
    const char *host, int port,
    const char *backend_name,
    const char *username,
    const argus_backend_t **out_backend)
{
    pool_ensure_init();

    /* Collect stale connections to disconnect outside the mutex */
    typedef struct { const argus_backend_t *b; argus_backend_conn_t c; } stale_t;
    stale_t stale_list[ARGUS_POOL_MAX_TOTAL];
    int stale_count = 0;

    g_mutex_lock(&g_pool.mutex);

    for (int i = 0; i < g_pool.count; i++) {
        argus_pool_entry_t *e = &g_pool.entries[i];
        if (!e->in_use &&
            pool_entry_matches(e, host, port, backend_name, username)) {

            /* Liveness check: if backend supports is_alive, verify */
            if (e->backend && e->backend->is_alive &&
                !e->backend->is_alive(e->conn)) {
                ARGUS_LOG_DEBUG("Pool: stale connection to %s:%d, evicting",
                                host, port);
                /* Save for disconnect after unlock */
                if (stale_count < ARGUS_POOL_MAX_TOTAL) {
                    stale_list[stale_count].b = e->backend;
                    stale_list[stale_count].c = e->conn;
                    stale_count++;
                }
                free(e->host);
                free(e->backend_name);
                free(e->username);
                g_pool.count--;
                if (i < g_pool.count) {
                    memmove(&g_pool.entries[i],
                            &g_pool.entries[i + 1],
                            (size_t)(g_pool.count - i) * sizeof(argus_pool_entry_t));
                }
                i--;
                continue;
            }

            e->in_use = true;
            e->last_used = g_get_monotonic_time();
            if (out_backend) *out_backend = e->backend;
            ARGUS_LOG_DEBUG("Pool: reusing connection to %s:%d (backend=%s)",
                            host, port, backend_name);
            g_mutex_unlock(&g_pool.mutex);
            /* Disconnect stale connections found before the match */
            for (int s = 0; s < stale_count; s++) {
                if (stale_list[s].b && stale_list[s].b->disconnect)
                    stale_list[s].b->disconnect(stale_list[s].c);
            }
            return e->conn;
        }
    }

    g_mutex_unlock(&g_pool.mutex);

    /* Disconnect stale connections outside the mutex */
    for (int s = 0; s < stale_count; s++) {
        if (stale_list[s].b && stale_list[s].b->disconnect)
            stale_list[s].b->disconnect(stale_list[s].c);
    }
    return NULL;
}

/* ── Public: release a connection back to the pool ───────────── */

void argus_pool_release(
    const char *host, int port,
    const char *backend_name,
    const char *username,
    const argus_backend_t *backend,
    argus_backend_conn_t conn)
{
    pool_ensure_init();

    g_mutex_lock(&g_pool.mutex);

    /* Find this connection in the pool and mark not-in-use */
    for (int i = 0; i < g_pool.count; i++) {
        argus_pool_entry_t *e = &g_pool.entries[i];
        if (e->conn == conn && e->in_use) {
            e->in_use = false;
            e->last_used = g_get_monotonic_time();
            ARGUS_LOG_DEBUG("Pool: released connection to %s:%d", host, port);
            g_mutex_unlock(&g_pool.mutex);
            return;
        }
    }

    /* Not found: add new entry if there's space */
    if (g_pool.count < ARGUS_POOL_MAX_TOTAL) {
        /* Count entries for this key to enforce per-key limit */
        int key_count = 0;
        for (int i = 0; i < g_pool.count; i++) {
            if (pool_entry_matches(&g_pool.entries[i], host, port,
                                    backend_name, username))
                key_count++;
        }

        if (key_count < ARGUS_POOL_MAX_PER_KEY) {
            argus_pool_entry_t *e = &g_pool.entries[g_pool.count];
            e->host = strdup(host);
            e->backend_name = strdup(backend_name);
            e->username = strdup(username ? username : "");
            if (!e->host || !e->backend_name || !e->username) {
                free(e->host);
                free(e->backend_name);
                free(e->username);
                memset(e, 0, sizeof(*e));
                ARGUS_LOG_ERROR("Pool: strdup failed, disconnecting");
                g_mutex_unlock(&g_pool.mutex);
                if (backend && backend->disconnect && conn)
                    backend->disconnect(conn);
                return;
            }
            e->port = port;
            e->backend = backend;
            e->conn = conn;
            e->in_use = false;
            e->last_used = g_get_monotonic_time();
            g_pool.count++;
            ARGUS_LOG_DEBUG("Pool: cached connection to %s:%d (total=%d)",
                            host, port, g_pool.count);
            g_mutex_unlock(&g_pool.mutex);
            return;
        }
    }

    /* Pool is full or per-key limit reached: disconnect outside mutex */
    ARGUS_LOG_DEBUG("Pool: full, disconnecting %s:%d", host, port);
    g_mutex_unlock(&g_pool.mutex);

    if (backend && backend->disconnect && conn) {
        backend->disconnect(conn);
    }
}

/* ── Public: cleanup all pooled connections ──────────────────── */

void argus_pool_cleanup(void)
{
    if (!g_pool.init_once) return;

    g_mutex_lock(&g_pool.mutex);

    int kept = 0;
    for (int i = 0; i < g_pool.count; i++) {
        argus_pool_entry_t *e = &g_pool.entries[i];
        if (e->in_use) {
            /* Preserve in-use entries by compacting them */
            if (kept != i)
                g_pool.entries[kept] = *e;
            kept++;
            continue;
        }
        if (e->backend && e->conn) {
            e->backend->disconnect(e->conn);
        }
        free(e->host);
        free(e->backend_name);
        free(e->username);
    }
    g_pool.count = kept;

    g_mutex_unlock(&g_pool.mutex);
}

/* ── Public: evict idle connections older than max_idle_sec ──── */

void argus_pool_evict_idle(int max_idle_sec)
{
    if (!g_pool.init_once) return;

    g_mutex_lock(&g_pool.mutex);

    gint64 now = g_get_monotonic_time();
    gint64 max_idle_us = (gint64)max_idle_sec * G_USEC_PER_SEC;

    int i = 0;
    while (i < g_pool.count) {
        argus_pool_entry_t *e = &g_pool.entries[i];
        if (!e->in_use && (now - e->last_used) > max_idle_us) {
            ARGUS_LOG_DEBUG("Pool: evicting idle connection to %s:%d",
                            e->host, e->port);
            if (e->backend && e->conn)
                e->backend->disconnect(e->conn);
            free(e->host);
            free(e->backend_name);
            free(e->username);

            /* Shift remaining entries down */
            g_pool.count--;
            if (i < g_pool.count) {
                memmove(&g_pool.entries[i],
                        &g_pool.entries[i + 1],
                        (size_t)(g_pool.count - i) * sizeof(argus_pool_entry_t));
            }
        } else {
            i++;
        }
    }

    g_mutex_unlock(&g_pool.mutex);
}
