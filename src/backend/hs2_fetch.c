/*
 * hs2_fetch.c — shared columnar TRowSet parsing (see hs2_fetch.h).
 *
 * Two passes over the column-major TColumn union:
 *   1. size pass — computes an upper bound of each row's payload (strings and
 *      binary are exact; numerics use their maximum formatted width),
 *   2. fill pass — allocates ONE block per row (cell array + payloads,
 *      argus_row_alloc_block) and formats values into a per-row cursor.
 * The scroll cache's ownership transfer keeps working: a block row moves as
 * one unit and frees with one free.
 */
#include "hs2_fetch.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib-object.h>
#include "gen-c_glib/t_c_l_i_service_types.h"

/* Maximum formatted widths, NUL included. */
#define HS2_W_I8     8   /* "%d" on a byte    */
#define HS2_W_I16    8
#define HS2_W_I32   16
#define HS2_W_I64   24
#define HS2_W_DBL   32   /* "%.15g" */
#define HS2_W_BOOL   6   /* "false" */

static inline bool hs2_is_null(GByteArray *nulls, int r)
{
    if (!nulls || nulls->len == 0) return false;
    int byte_idx = r / 8;
    if (byte_idx >= (int)nulls->len) return false;
    return (nulls->data[byte_idx] >> (r % 8)) & 1;
}

/* ── Pass 1: accumulate each row's payload upper bound ─────────── */

static void size_column(TColumn *tcol, int num_rows, size_t *row_sz)
{
    if (tcol->__isset_stringVal && tcol->stringVal) {
        GPtrArray *v = tcol->stringVal->values;
        GByteArray *n = tcol->stringVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            if (hs2_is_null(n, r)) continue;
            gchar *val = g_ptr_array_index(v, r);
            if (val) row_sz[r] += strlen(val) + 1;
        }
        return;
    }
    if (tcol->__isset_binaryVal && tcol->binaryVal) {
        GPtrArray *v = tcol->binaryVal->values;
        GByteArray *n = tcol->binaryVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            if (hs2_is_null(n, r)) continue;
            GByteArray *val = g_ptr_array_index(v, r);
            if (val) row_sz[r] += (size_t)val->len * 2 + 1;
        }
        return;
    }

    /* Fixed-width kinds: charge every covered non-null row the max width. */
    size_t w = 0;
    GArray *v = NULL; GByteArray *n = NULL;
    if      (tcol->__isset_i32Val    && tcol->i32Val)    { w = HS2_W_I32;  v = tcol->i32Val->values;    n = tcol->i32Val->nulls; }
    else if (tcol->__isset_i64Val    && tcol->i64Val)    { w = HS2_W_I64;  v = tcol->i64Val->values;    n = tcol->i64Val->nulls; }
    else if (tcol->__isset_doubleVal && tcol->doubleVal) { w = HS2_W_DBL;  v = tcol->doubleVal->values; n = tcol->doubleVal->nulls; }
    else if (tcol->__isset_boolVal   && tcol->boolVal)   { w = HS2_W_BOOL; v = tcol->boolVal->values;   n = tcol->boolVal->nulls; }
    else if (tcol->__isset_byteVal   && tcol->byteVal)   { w = HS2_W_I8;   v = tcol->byteVal->values;   n = tcol->byteVal->nulls; }
    else if (tcol->__isset_i16Val    && tcol->i16Val)    { w = HS2_W_I16;  v = tcol->i16Val->values;    n = tcol->i16Val->nulls; }
    if (!v) return;
    for (int r = 0; r < num_rows && r < (int)v->len; r++)
        if (!hs2_is_null(n, r)) row_sz[r] += w;
}

/* ── Pass 2: fill one column into the pre-allocated blocks ─────── */

static void put_text(argus_cell_t *cell, char **cursor,
                     const char *text, size_t len)
{
    memcpy(*cursor, text, len);
    (*cursor)[len] = '\0';
    cell->data = *cursor;
    cell->data_len = len;
    *cursor += len + 1;
}

static void fill_column(TColumn *tcol, int col_idx, int num_rows,
                        argus_row_cache_t *cache, char **cursors)
{
    char buf[40];

    if (tcol->__isset_stringVal && tcol->stringVal) {
        GPtrArray *v = tcol->stringVal->values;
        GByteArray *n = tcol->stringVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            gchar *val = g_ptr_array_index(v, r);
            if (val) put_text(cell, &cursors[r], val, strlen(val));
        }
        return;
    }
    if (tcol->__isset_binaryVal && tcol->binaryVal) {
        GPtrArray *v = tcol->binaryVal->values;
        GByteArray *n = tcol->binaryVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            GByteArray *val = g_ptr_array_index(v, r);
            if (!val) continue;
            char *dst = cursors[r];
            for (guint i = 0; i < val->len; i++)
                snprintf(dst + i * 2, 3, "%02x", val->data[i]);
            dst[(size_t)val->len * 2] = '\0';
            cell->data = dst;
            cell->data_len = (size_t)val->len * 2;
            cursors[r] += (size_t)val->len * 2 + 1;
        }
        return;
    }

    if (tcol->__isset_i32Val && tcol->i32Val) {
        GArray *v = tcol->i32Val->values; GByteArray *n = tcol->i32Val->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            int len = snprintf(buf, sizeof(buf), "%d",
                               g_array_index(v, gint32, r));
            put_text(cell, &cursors[r], buf, (size_t)len);
        }
        return;
    }
    if (tcol->__isset_i64Val && tcol->i64Val) {
        GArray *v = tcol->i64Val->values; GByteArray *n = tcol->i64Val->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            int len = snprintf(buf, sizeof(buf), "%" G_GINT64_FORMAT,
                               g_array_index(v, gint64, r));
            put_text(cell, &cursors[r], buf, (size_t)len);
        }
        return;
    }
    if (tcol->__isset_doubleVal && tcol->doubleVal) {
        GArray *v = tcol->doubleVal->values; GByteArray *n = tcol->doubleVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            int len = snprintf(buf, sizeof(buf), "%.15g",
                               g_array_index(v, gdouble, r));
            put_text(cell, &cursors[r], buf, (size_t)len);
        }
        return;
    }
    if (tcol->__isset_boolVal && tcol->boolVal) {
        GArray *v = tcol->boolVal->values; GByteArray *n = tcol->boolVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            const char *s = g_array_index(v, gboolean, r) ? "true" : "false";
            put_text(cell, &cursors[r], s, strlen(s));
        }
        return;
    }
    if (tcol->__isset_byteVal && tcol->byteVal) {
        GArray *v = tcol->byteVal->values; GByteArray *n = tcol->byteVal->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            int len = snprintf(buf, sizeof(buf), "%d",
                               (int)g_array_index(v, gint8, r));
            put_text(cell, &cursors[r], buf, (size_t)len);
        }
        return;
    }
    if (tcol->__isset_i16Val && tcol->i16Val) {
        GArray *v = tcol->i16Val->values; GByteArray *n = tcol->i16Val->nulls;
        if (!v) return;
        for (int r = 0; r < num_rows && r < (int)v->len; r++) {
            argus_cell_t *cell = &cache->rows[r].cells[col_idx];
            if (hs2_is_null(n, r)) { cell->is_null = true; continue; }
            int len = snprintf(buf, sizeof(buf), "%d",
                               (int)g_array_index(v, gint16, r));
            put_text(cell, &cursors[r], buf, (size_t)len);
        }
        return;
    }
}

int argus_hs2_parse_rowset(GPtrArray *tcolumns, int ncols, int num_rows,
                           argus_row_cache_t *cache)
{
    if (num_rows <= 0 || ncols <= 0) return 0;

    size_t *row_sz = calloc((size_t)num_rows, sizeof(size_t));
    char  **cursors = calloc((size_t)num_rows, sizeof(char *));
    if (!row_sz || !cursors) { free(row_sz); free(cursors); return -1; }

    for (int c = 0; c < ncols && c < (int)tcolumns->len; c++)
        size_column(T_COLUMN(g_ptr_array_index(tcolumns, c)),
                    num_rows, row_sz);

    for (int r = 0; r < num_rows; r++) {
        cursors[r] = argus_row_alloc_block(&cache->rows[r], ncols, row_sz[r]);
        if (!cursors[r]) { free(row_sz); free(cursors); return -1; }
    }

    for (int c = 0; c < ncols && c < (int)tcolumns->len; c++)
        fill_column(T_COLUMN(g_ptr_array_index(tcolumns, c)),
                    c, num_rows, cache, cursors);

    free(row_sz);
    free(cursors);
    return 0;
}

/* ── Row count, row set to cache, status ───────────────────────── */

static int column_row_count(TColumn *tcol)
{
    if (tcol->__isset_stringVal && tcol->stringVal) {
        GPtrArray *v = tcol->stringVal->values;
        return v ? (int)v->len : 0;
    }
    if (tcol->__isset_binaryVal && tcol->binaryVal) {
        GPtrArray *v = tcol->binaryVal->values;
        return v ? (int)v->len : 0;
    }
    GArray *v = NULL;
    if      (tcol->__isset_i32Val    && tcol->i32Val)    v = tcol->i32Val->values;
    else if (tcol->__isset_i64Val    && tcol->i64Val)    v = tcol->i64Val->values;
    else if (tcol->__isset_doubleVal && tcol->doubleVal) v = tcol->doubleVal->values;
    else if (tcol->__isset_boolVal   && tcol->boolVal)   v = tcol->boolVal->values;
    else if (tcol->__isset_byteVal   && tcol->byteVal)   v = tcol->byteVal->values;
    else if (tcol->__isset_i16Val    && tcol->i16Val)    v = tcol->i16Val->values;
    return v ? (int)v->len : 0;
}

int argus_hs2_rowset_row_count(GPtrArray *tcolumns)
{
    int nrows = 0;
    if (!tcolumns) return 0;
    for (guint c = 0; c < tcolumns->len; c++) {
        int rc = column_row_count(T_COLUMN(g_ptr_array_index(tcolumns, c)));
        if (rc > nrows) nrows = rc;
    }
    return nrows;
}

int argus_hs2_rowset_to_cache(GPtrArray *tcolumns, argus_row_cache_t *cache,
                              int *num_cols)
{
    cache->num_rows = 0;
    if (!tcolumns || tcolumns->len == 0) return 0;

    int ncols = (int)tcolumns->len;
    if (num_cols) *num_cols = ncols;
    cache->num_cols = ncols;

    int nrows = argus_hs2_rowset_row_count(tcolumns);
    if (nrows == 0) return 0;

    cache->rows = calloc((size_t)nrows, sizeof(argus_row_t));
    if (!cache->rows) return -1;
    cache->num_rows = (size_t)nrows;
    cache->capacity = (size_t)nrows;

    return argus_hs2_parse_rowset(tcolumns, ncols, nrows, cache);
}

bool argus_hs2_status_ok(TStatus *status, char *errbuf, size_t errlen)
{
    if (!status) return true;

    TStatusCode code;
    g_object_get(status, "statusCode", &code, NULL);
    if (code == T_STATUS_CODE_SUCCESS_STATUS ||
        code == T_STATUS_CODE_SUCCESS_WITH_INFO_STATUS)
        return true;

    if (errbuf && errlen > 0) {
        char *emsg = NULL;
        g_object_get(status, "errorMessage", &emsg, NULL);
        const char *text = (emsg && *emsg) ? emsg
                         : code == T_STATUS_CODE_INVALID_HANDLE_STATUS
                           ? "Invalid operation handle"
                         : code == T_STATUS_CODE_STILL_EXECUTING_STATUS
                           ? "Operation still executing"
                           : "Server returned an error";
        g_strlcpy(errbuf, text, errlen);
        g_free(emsg);
    }
    return false;
}
