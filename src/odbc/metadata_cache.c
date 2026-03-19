/*
 * Metadata cache for SQLTables/SQLColumns results.
 *
 * BI tools (Tableau, Power BI) call SQLTables and SQLColumns repeatedly
 * during connection setup. Each call triggers a full SQL query to the backend.
 * This cache stores the results of these calls with a TTL, avoiding redundant
 * round-trips.
 *
 * Cache key: concatenation of function name + arguments.
 * Cache value: deep copy of the row_cache + column metadata.
 * TTL: 60 seconds (configurable via ARGUS_METADATA_CACHE_TTL_SEC).
 */
#include "argus/handle.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#define ARGUS_METADATA_CACHE_TTL_SEC 60

/* ── Cache entry ─────────────────────────────────────────────── */

typedef struct {
    argus_row_cache_t    row_cache;
    argus_column_desc_t *columns;
    int                  num_cols;
    gint64               created_at;   /* monotonic microseconds */
} cache_entry_t;

static void cache_entry_free(gpointer data)
{
    cache_entry_t *entry = (cache_entry_t *)data;
    if (!entry) return;

    argus_row_cache_free(&entry->row_cache);
    free(entry->columns);
    free(entry);
}

/* ── Deep-copy a row cache ───────────────────────────────────── */

static bool row_cache_deep_copy(argus_row_cache_t *dst,
                                 const argus_row_cache_t *src)
{
    memset(dst, 0, sizeof(*dst));
    if (src->num_rows == 0) {
        dst->exhausted = src->exhausted;
        dst->num_cols = src->num_cols;
        return true;
    }

    dst->rows = calloc(src->num_rows, sizeof(argus_row_t));
    if (!dst->rows) return false;

    dst->num_rows = src->num_rows;
    dst->capacity = src->num_rows;
    dst->num_cols = src->num_cols;
    dst->exhausted = src->exhausted;
    dst->current_row = 0;

    for (size_t r = 0; r < src->num_rows; r++) {
        const argus_row_t *src_row = &src->rows[r];
        argus_row_t *dst_row = &dst->rows[r];

        if (!src_row->cells) continue;

        dst_row->cells = calloc((size_t)src->num_cols, sizeof(argus_cell_t));
        if (!dst_row->cells) goto fail;

        for (int c = 0; c < src->num_cols; c++) {
            dst_row->cells[c].is_null = src_row->cells[c].is_null;
            dst_row->cells[c].data_len = src_row->cells[c].data_len;
            if (src_row->cells[c].data) {
                dst_row->cells[c].data = malloc(src_row->cells[c].data_len + 1);
                if (!dst_row->cells[c].data) goto fail;
                memcpy(dst_row->cells[c].data, src_row->cells[c].data,
                       src_row->cells[c].data_len + 1);
            }
        }
    }
    return true;

fail:
    argus_row_cache_free(dst);
    return false;
}

/* ── Build cache key ─────────────────────────────────────────── */

static char *build_cache_key(const char *func,
                              const char *a1, const char *a2,
                              const char *a3, const char *a4)
{
    char key[2048];
    snprintf(key, sizeof(key), "%s|%s|%s|%s|%s",
             func,
             a1 ? a1 : "",
             a2 ? a2 : "",
             a3 ? a3 : "",
             a4 ? a4 : "");
    return strdup(key);
}

/* ── Public API ──────────────────────────────────────────────── */

void argus_metadata_cache_init(argus_dbc_t *dbc)
{
    if (dbc->metadata_cache) return;
    dbc->metadata_cache = g_hash_table_new_full(
        g_str_hash, g_str_equal, g_free, cache_entry_free);
}

void argus_metadata_cache_free(argus_dbc_t *dbc)
{
    if (dbc->metadata_cache) {
        g_hash_table_destroy((GHashTable *)dbc->metadata_cache);
        dbc->metadata_cache = NULL;
    }
}

void argus_metadata_cache_clear(argus_dbc_t *dbc)
{
    if (dbc->metadata_cache) {
        g_hash_table_remove_all((GHashTable *)dbc->metadata_cache);
    }
}

/*
 * Look up a cached result for a catalog function.
 * If found and not expired, copies the cached result into stmt.
 * Returns true if a valid cache hit was found.
 */
bool argus_metadata_cache_lookup(argus_dbc_t *dbc, argus_stmt_t *stmt,
                                  const char *func,
                                  const char *a1, const char *a2,
                                  const char *a3, const char *a4)
{
    if (!dbc->metadata_cache) return false;

    GHashTable *ht = (GHashTable *)dbc->metadata_cache;

    char *key = build_cache_key(func, a1, a2, a3, a4);
    cache_entry_t *entry = (cache_entry_t *)g_hash_table_lookup(ht, key);

    if (!entry) {
        free(key);
        return false;
    }

    /* Check TTL */
    gint64 now = g_get_monotonic_time();
    gint64 age_sec = (now - entry->created_at) / G_USEC_PER_SEC;
    if (age_sec > ARGUS_METADATA_CACHE_TTL_SEC) {
        g_hash_table_remove(ht, key);
        free(key);
        ARGUS_LOG_DEBUG("Metadata cache expired for %s", func);
        return false;
    }

    free(key);

    /* Deep-copy cached data into stmt */
    if (!row_cache_deep_copy(&stmt->row_cache, &entry->row_cache))
        return false;

    if (argus_stmt_ensure_columns(stmt, entry->num_cols) != 0)
        return false;
    memcpy(stmt->columns, entry->columns,
           (size_t)entry->num_cols * sizeof(argus_column_desc_t));
    stmt->num_cols = entry->num_cols;
    stmt->metadata_fetched = true;
    stmt->executed = true;

    ARGUS_LOG_DEBUG("Metadata cache hit for %s (age=%llds)",
                    func, (long long)age_sec);
    return true;
}

/*
 * Store a catalog function result in the cache.
 * Deep-copies the stmt's row_cache and column metadata.
 */
void argus_metadata_cache_store(argus_dbc_t *dbc, argus_stmt_t *stmt,
                                 const char *func,
                                 const char *a1, const char *a2,
                                 const char *a3, const char *a4)
{
    if (!dbc->metadata_cache)
        argus_metadata_cache_init(dbc);

    GHashTable *ht = (GHashTable *)dbc->metadata_cache;

    cache_entry_t *entry = calloc(1, sizeof(cache_entry_t));
    if (!entry) return;

    if (!row_cache_deep_copy(&entry->row_cache, &stmt->row_cache)) {
        free(entry);
        return;
    }

    entry->num_cols = stmt->num_cols;
    entry->columns = calloc((size_t)stmt->num_cols, sizeof(argus_column_desc_t));
    if (!entry->columns) {
        argus_row_cache_free(&entry->row_cache);
        free(entry);
        return;
    }
    memcpy(entry->columns, stmt->columns,
           (size_t)stmt->num_cols * sizeof(argus_column_desc_t));

    entry->created_at = g_get_monotonic_time();

    char *key = build_cache_key(func, a1, a2, a3, a4);
    g_hash_table_replace(ht, key, entry);

    ARGUS_LOG_DEBUG("Metadata cache stored for %s", func);
}
