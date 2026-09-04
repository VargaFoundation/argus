/* SPDX-License-Identifier: Apache-2.0 */
#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/numtext.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <glib.h>

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);

/* ── Row cache implementation ─────────────────────────────────── */

void argus_row_cache_init(argus_row_cache_t *cache)
{
    memset(cache, 0, sizeof(*cache));
}

char *argus_row_alloc_block(argus_row_t *row, int num_cols,
                            size_t payload_bytes)
{
    size_t cells_sz = (size_t)num_cols * sizeof(argus_cell_t);
    char *blk = malloc(cells_sz + payload_bytes);
    if (!blk) return NULL;
    memset(blk, 0, cells_sz);
    row->cells = (argus_cell_t *)blk;
    row->block = true;
    return blk + cells_sz;
}

void argus_row_free(argus_row_t *row, int num_cols)
{
    if (!row->cells) return;
    if (!row->block) {
        for (int i = 0; i < num_cols; i++)
            free(row->cells[i].data);
    }
    free(row->cells);
    row->cells = NULL;
    row->block = false;
}

static void free_row(argus_row_t *row, int num_cols)
{
    argus_row_free(row, num_cols);
}

/* ── Binary cells: decode the engine's text encoding into bytes ── */

static int hex_nibble(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

int argus_cell_decode_hex(argus_cell_t *cell)
{
    if (!cell || cell->is_null || cell->native_kind == ARGUS_NATIVE_BINARY)
        return 0;
    if (cell->native_kind != ARGUS_NATIVE_NONE || (!cell->data && cell->data_len))
        return -1;

    const char *src = cell->data ? cell->data : "";
    size_t len = cell->data_len;
    if (len >= 2 && (src[0] == '0' || src[0] == '\\') &&
        (src[1] == 'x' || src[1] == 'X')) {
        src += 2;
        len -= 2;
    }
    if (len % 2) return -1;
    for (size_t i = 0; i < len; i++)
        if (hex_nibble(src[i]) < 0) return -1;

    /* Byte i comes from characters 2i and 2i+1 at or beyond it: writing
     * forward never overtakes the read cursor. */
    unsigned char *out = (unsigned char *)cell->data;
    size_t n = len / 2;
    for (size_t i = 0; i < n; i++)
        out[i] = (unsigned char)((hex_nibble(src[2 * i]) << 4) |
                                 hex_nibble(src[2 * i + 1]));
    if (cell->data) cell->data[n] = '\0';
    cell->data_len = n;
    cell->native_kind = ARGUS_NATIVE_BINARY;
    return 0;
}

static int b64_value(char c)
{
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+' || c == '-') return 62;   /* '-' and '_': the URL alphabet */
    if (c == '/' || c == '_') return 63;
    return -1;
}

int argus_cell_decode_base64(argus_cell_t *cell)
{
    if (!cell || cell->is_null || cell->native_kind == ARGUS_NATIVE_BINARY)
        return 0;
    if (cell->native_kind != ARGUS_NATIVE_NONE || (!cell->data && cell->data_len))
        return -1;

    const char *src = cell->data ? cell->data : "";
    size_t len = cell->data_len;
    while (len && src[len - 1] == '=') len--;      /* padding */
    if (len % 4 == 1) return -1;                   /* no such encoding */
    for (size_t i = 0; i < len; i++)
        if (b64_value(src[i]) < 0) return -1;

    /* Four characters become three bytes; the write cursor trails the read
     * cursor, so the decode is safe in place. */
    unsigned char *out = (unsigned char *)cell->data;
    size_t o = 0;
    unsigned acc = 0;
    int bits = 0;
    for (size_t i = 0; i < len; i++) {
        acc = (acc << 6) | (unsigned)b64_value(src[i]);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out[o++] = (unsigned char)((acc >> bits) & 0xFF);
        }
    }
    if (cell->data) cell->data[o] = '\0';
    cell->data_len = o;
    cell->native_kind = ARGUS_NATIVE_BINARY;
    return 0;
}

void argus_cache_decode_binary(argus_row_cache_t *cache,
                               const argus_column_desc_t *columns,
                               int num_cols, argus_binary_encoding_t enc)
{
    if (!cache || !cache->rows || !columns) return;

    int ncols = num_cols < cache->num_cols ? num_cols : cache->num_cols;
    for (int c = 0; c < ncols; c++) {
        SQLSMALLINT t = columns[c].sql_type;
        if (t != SQL_BINARY && t != SQL_VARBINARY && t != SQL_LONGVARBINARY)
            continue;
        for (size_t r = 0; r < cache->num_rows; r++) {
            if (!cache->rows[r].cells) continue;
            argus_cell_t *cell = &cache->rows[r].cells[c];
            if (cell->is_null) continue;
            switch (enc) {
            case ARGUS_BINARY_HEX:    argus_cell_decode_hex(cell);    break;
            case ARGUS_BINARY_BASE64: argus_cell_decode_base64(cell); break;
            case ARGUS_BINARY_RAW:
                /* Already the bytes; the cell just never said so, and a
                 * character target needs to know to render them as hex. */
                if (cell->native_kind == ARGUS_NATIVE_NONE)
                    cell->native_kind = ARGUS_NATIVE_BINARY;
                break;
            }
        }
    }
}

void argus_row_cache_free(argus_row_cache_t *cache)
{
    if (cache->rows) {
        for (size_t i = 0; i < cache->num_rows; i++) {
            free_row(&cache->rows[i], cache->num_cols);
        }
        free(cache->rows);
    }
    memset(cache, 0, sizeof(*cache));
}

/*
 * Empty the cache, including the row array itself.
 *
 * This used to keep the array and its capacity, on the theory that the next
 * batch could reuse it — but no backend ever did. Every one of them assigns
 * cache->rows unconditionally on entry to fetch_results (a fresh calloc, or a
 * hand-over from an operation that then nulls its own pointer), so the array
 * kept here was simply overwritten and leaked: one array per batch, 8 KB at
 * the default FetchBufferSize, ~8 MB per million rows fetched. Invisible on a
 * single-batch result and steady growth in a long-lived BI process.
 *
 * Freeing it here makes the rule uniform and unsurprising — the cache owns its
 * row array, and clearing the cache releases it — and every existing backend
 * becomes correct without touching any of them. The ones that grow the array
 * with realloc see rows == NULL and capacity == 0, which realloc handles as a
 * plain allocation. num_cols and the exhausted flag are still preserved: the
 * first is needed to free the cells above, and the second is the caller's
 * state, not the array's.
 */
void argus_row_cache_clear(argus_row_cache_t *cache)
{
    if (cache->rows) {
        for (size_t i = 0; i < cache->num_rows; i++) {
            free_row(&cache->rows[i], cache->num_cols);
        }
        free(cache->rows);
        cache->rows = NULL;
    }
    cache->num_rows    = 0;
    cache->capacity    = 0;
    cache->current_row = 0;
    /* Keep num_cols and the exhausted flag */
}

/* ── Internal: diagnostic for a failed backend fetch ─────────── */

/* Same preference order as SQLExecDirect: the backend's own SQLSTATE and
 * message when it has one, otherwise HY000 with a generic text. Leaves an
 * existing diagnostic (set by the backend itself) untouched. */
static void set_fetch_error(argus_stmt_t *stmt)
{
    argus_dbc_t *dbc = stmt->dbc;
    if (stmt->diag.count > 0) return;

    char errbuf[512];
    char sqlstate[6] = {0};
    if (dbc->backend->get_last_error_ex &&
        dbc->backend->get_last_error_ex(dbc->backend_conn, sqlstate,
                                        errbuf, sizeof(errbuf)) &&
        errbuf[0]) {
        char msg[600];
        snprintf(msg, sizeof(msg), "[Argus] %s", errbuf);
        argus_set_error(&stmt->diag, sqlstate[0] ? sqlstate : "HY000", msg, 0);
    } else if (dbc->backend->get_last_error &&
               dbc->backend->get_last_error(dbc->backend_conn,
                                            errbuf, sizeof(errbuf)) &&
               errbuf[0]) {
        char msg[600];
        snprintf(msg, sizeof(msg), "[Argus] %s", errbuf);
        argus_set_error(&stmt->diag, "HY000", msg, 0);
    } else {
        argus_set_error(&stmt->diag, "HY000",
                        "[Argus] Failed to fetch results", 0);
    }
}

/* ── Internal: fetch a batch from backend ─────────────────────── */

static SQLRETURN fetch_batch(argus_stmt_t *stmt)
{
    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->backend || !dbc->backend_conn) {
        return argus_set_error(&stmt->diag, "HY000",
                               "[Argus] No backend connection", 0);
    }

    argus_row_cache_clear(&stmt->row_cache);

    /* Use fetch_buffer_size if set, otherwise use default */
    int batch_size = (dbc->fetch_buffer_size > 0)
                     ? dbc->fetch_buffer_size
                     : ARGUS_DEFAULT_BATCH_SIZE;

    /* An SQLCancel raised while the previous batch was on the wire. */
    SQLRETURN canceled = argus_stmt_cancel_checkpoint(stmt);
    if (canceled != SQL_SUCCESS) return canceled;

    int num_cols = 0;
    int rc = dbc->backend->fetch_results(
        dbc->backend_conn, stmt->op,
        batch_size,
        &stmt->row_cache,
        stmt->columns, &num_cols);

    if (rc != 0) {
        /* A cancel sent while the batch was on the wire (backends whose
         * cancel is safe to send then) is reported as the cancel it is. */
        canceled = argus_stmt_cancel_checkpoint(stmt);
        if (canceled != SQL_SUCCESS) return canceled;
        set_fetch_error(stmt);
        return SQL_ERROR;
    }

    if (num_cols > 0 && !stmt->metadata_fetched) {
        if (argus_stmt_ensure_columns(stmt, num_cols) != 0)
            return SQL_ERROR;
        stmt->num_cols = num_cols;
        stmt->metadata_fetched = true;
    }

    if (stmt->row_cache.num_rows == 0) {
        stmt->row_cache.exhausted = true;
    }

    return SQL_SUCCESS;
}

/* ── Internal: convert cell to target type ────────────────────── */

/*
 * One conversion, from a row-cache cell to the application's buffer.
 *
 * Character and binary targets take the value in pieces: `gd` is the
 * SQLGetData continuation state — how many source units (UTF-8 bytes of a
 * text value, bytes of a binary one) earlier calls already delivered — and
 * this call advances it by what it copies. Bound columns pass NULL and
 * always receive the value from the start. Fixed-length targets ignore it.
 *
 * Numeric targets follow the ODBC conversion rules for character data:
 * 22018 when the text is not a number, 22003 when the value does not fit
 * the target, 01S07 when a fraction is dropped on the way to an integer.
 * A binary value converts to SQL_C_BINARY (the bytes) and to the character
 * types (as hex); anything else is 07006.
 */

static SQLRETURN err_out_of_range(argus_diag_t *diag)
{
    return argus_set_error(diag, "22003",
                           "[Argus] Numeric value out of range", 0);
}

static SQLRETURN err_not_numeric(argus_diag_t *diag)
{
    return argus_set_error(diag, "22018",
                           "[Argus] Invalid character value for cast specification", 0);
}

static SQLRETURN truncated(argus_diag_t *diag)
{
    argus_diag_push(diag, "01004", "[Argus] String data, right truncated", 0);
    return SQL_SUCCESS_WITH_INFO;
}

static bool is_blank(char c)
{
    return c == ' ' || c == '\t';
}

/* Where a continuation resumes in a value of src_len units. */
static size_t resume_at(const argus_getdata_state_t *gd, size_t src_len)
{
    size_t off = gd ? gd->offset : 0;
    return off < src_len ? off : src_len;
}

static void advance(argus_getdata_state_t *gd, size_t off, size_t copied,
                    bool all)
{
    if (!gd) return;
    gd->offset = off + copied;
    gd->done = all;
}

/* Text into a SQL_C_CHAR buffer, NUL-terminated. A chunk boundary never
 * splits a UTF-8 sequence when the buffer holds at least one whole
 * character. */
static SQLRETURN put_char_bytes(const char *src, size_t src_len,
                                SQLPOINTER target_value, SQLLEN buffer_length,
                                SQLLEN *str_len_or_ind,
                                argus_getdata_state_t *gd, argus_diag_t *diag)
{
    size_t off = resume_at(gd, src_len);
    size_t remaining = src_len - off;
    if (str_len_or_ind) *str_len_or_ind = (SQLLEN)remaining;

    size_t room = (target_value && buffer_length > 0)
                  ? (size_t)buffer_length - 1 : 0;
    size_t copy = remaining < room ? remaining : room;
    if (copy < remaining) {
        size_t cut = copy;
        while (cut > 0 && ((unsigned char)src[off + cut] & 0xC0) == 0x80)
            cut--;
        if (cut > 0) copy = cut;
    }
    if (target_value && buffer_length > 0) {
        memcpy(target_value, src + off, copy);
        ((char *)target_value)[copy] = '\0';
    }
    advance(gd, off, copy, copy == remaining);
    return copy < remaining ? truncated(diag) : SQL_SUCCESS;
}

/* Bytes into a SQL_C_BINARY buffer: no terminator, every byte counts. */
static SQLRETURN put_bytes(const char *src, size_t src_len,
                           SQLPOINTER target_value, SQLLEN buffer_length,
                           SQLLEN *str_len_or_ind,
                           argus_getdata_state_t *gd, argus_diag_t *diag)
{
    size_t off = resume_at(gd, src_len);
    size_t remaining = src_len - off;
    if (str_len_or_ind) *str_len_or_ind = (SQLLEN)remaining;

    size_t room = (target_value && buffer_length > 0)
                  ? (size_t)buffer_length : 0;
    size_t copy = remaining < room ? remaining : room;
    if (copy) memcpy(target_value, src + off, copy);
    advance(gd, off, copy, copy == remaining);
    return copy < remaining ? truncated(diag) : SQL_SUCCESS;
}

/* Bytes as hex digits, two per byte, into a SQL_C_CHAR or SQL_C_WCHAR
 * buffer; the continuation offset counts bytes, so a chunk always ends
 * between two digits of different bytes. */
static SQLRETURN put_hex(const unsigned char *src, size_t src_len, bool wide,
                         SQLPOINTER target_value, SQLLEN buffer_length,
                         SQLLEN *str_len_or_ind,
                         argus_getdata_state_t *gd, argus_diag_t *diag)
{
    static const char digits[] = "0123456789ABCDEF";
    size_t unit = wide ? sizeof(SQLWCHAR) : 1;
    size_t off = resume_at(gd, src_len);
    size_t remaining = src_len - off;
    if (str_len_or_ind) *str_len_or_ind = (SQLLEN)(remaining * 2 * unit);

    size_t room_units = (target_value && buffer_length >= (SQLLEN)unit)
                        ? (size_t)buffer_length / unit - 1 : 0;
    size_t copy = remaining < room_units / 2 ? remaining : room_units / 2;
    if (target_value && buffer_length >= (SQLLEN)unit) {
        if (wide) {
            SQLWCHAR *dst = (SQLWCHAR *)target_value;
            for (size_t i = 0; i < copy; i++) {
                dst[2 * i]     = (SQLWCHAR)digits[src[off + i] >> 4];
                dst[2 * i + 1] = (SQLWCHAR)digits[src[off + i] & 0x0F];
            }
            dst[2 * copy] = 0;
        } else {
            char *dst = (char *)target_value;
            for (size_t i = 0; i < copy; i++) {
                dst[2 * i]     = digits[src[off + i] >> 4];
                dst[2 * i + 1] = digits[src[off + i] & 0x0F];
            }
            dst[2 * copy] = '\0';
        }
    }
    advance(gd, off, copy, copy == remaining);
    return copy < remaining ? truncated(diag) : SQL_SUCCESS;
}

/* UTF-8 text as UTF-16 into a SQL_C_WCHAR buffer. Walks the text one code
 * point at a time so a chunk never ends inside a surrogate pair and the
 * continuation offset stays a UTF-8 byte count. The indicator is the
 * UTF-16 size of what remains; a continuation call reuses the count the
 * first call made instead of walking the rest of the value again. */
static SQLRETURN put_wchars(const char *src, size_t src_len,
                            SQLPOINTER target_value, SQLLEN buffer_length,
                            SQLLEN *str_len_or_ind,
                            argus_getdata_state_t *gd, argus_diag_t *diag)
{
    size_t off = resume_at(gd, src_len);
    const char *p = src + off, *end = src + src_len;
    SQLWCHAR *dst = (target_value && buffer_length >= (SQLLEN)sizeof(SQLWCHAR))
                    ? (SQLWCHAR *)target_value : NULL;
    size_t room = dst ? (size_t)buffer_length / sizeof(SQLWCHAR) - 1 : 0;
    bool counted = gd && off > 0 && gd->wchar_left >= 0;
    size_t total = counted ? (size_t)gd->wchar_left : 0;
    size_t written = 0, consumed = 0;
    bool copying = true;

    while (p < end && (copying || !counted)) {
        gunichar c = g_utf8_get_char_validated(p, (gssize)(end - p));
        if (c == (gunichar)-1 || c == (gunichar)-2)
            return argus_set_error(diag, "22018", "[Argus] Invalid UTF-8 data", 0);
        const char *next = g_utf8_next_char(p);
        size_t need = (c >= 0x10000) ? 2 : 1;
        if (copying) {
            if (written + need <= room) {
                if (need == 2) {
                    c -= 0x10000;
                    dst[written++] = (SQLWCHAR)(0xD800 | (c >> 10));
                    dst[written++] = (SQLWCHAR)(0xDC00 | (c & 0x3FF));
                } else {
                    dst[written++] = (SQLWCHAR)c;
                }
                consumed = (size_t)(next - (src + off));
            } else {
                copying = false;
            }
        }
        if (!counted) total += need;
        p = next;
    }
    if (dst) dst[written] = 0;
    if (str_len_or_ind)
        *str_len_or_ind = (SQLLEN)(total * sizeof(SQLWCHAR));
    if (gd) {
        gd->offset = off + consumed;
        gd->wchar_left = (long)(total - written);
        gd->done = (written == total);
    }
    return written < total ? truncated(diag) : SQL_SUCCESS;
}

/* ── Integers ── */

/* An integer on its way to a C integer target: the sign and magnitude of
 * the integer part, and whether a non-zero fraction was dropped. */
typedef struct int_value {
    bool               negative;
    unsigned long long magnitude;
    bool               fractional;
} int_value_t;

static SQLRETURN int_from_double(double v, int_value_t *r, argus_diag_t *diag)
{
    if (isnan(v) || !(v < 18446744073709551616.0 && v > -18446744073709551616.0))
        return err_out_of_range(diag);
    double t = trunc(v);
    r->negative = t < 0;
    r->magnitude = (unsigned long long)fabs(t);
    r->fractional = (t != v);
    return SQL_SUCCESS;
}

static void int_from_i64(long long v, int_value_t *r)
{
    r->negative = v < 0;
    r->magnitude = v < 0 ? (unsigned long long)(-(v + 1)) + 1ULL
                         : (unsigned long long)v;
    r->fractional = false;
}

/* A number as the engines print one: blanks, a sign, digits, an optional
 * fraction and exponent. Boolean text ("true"/"false") counts as 1/0 so a
 * BOOLEAN column reads into any integer type. */
static SQLRETURN int_from_text(const char *s, int_value_t *r, argus_diag_t *diag)
{
    const char *p = s;
    while (is_blank(*p)) p++;
    if (g_ascii_strcasecmp(p, "true") == 0 || g_ascii_strcasecmp(p, "false") == 0) {
        r->negative = false;
        r->magnitude = (*p == 't' || *p == 'T') ? 1 : 0;
        r->fractional = false;
        return SQL_SUCCESS;
    }

    const char *start = p;
    r->negative = false;
    r->magnitude = 0;
    r->fractional = false;
    if (*p == '-') { r->negative = true; p++; }
    else if (*p == '+') p++;

    size_t ndigits = 0;
    bool overflow = false;
    for (; *p >= '0' && *p <= '9'; p++, ndigits++) {
        unsigned d = (unsigned)(*p - '0');
        if (r->magnitude > (ULLONG_MAX - d) / 10) overflow = true;
        else r->magnitude = r->magnitude * 10 + d;
    }
    const char *frac = NULL;
    size_t nfrac = 0;
    if (*p == '.') {
        p++;
        frac = p;
        while (*p >= '0' && *p <= '9') p++;
        nfrac = (size_t)(p - frac);
    }
    if (ndigits == 0 && nfrac == 0) return err_not_numeric(diag);

    bool has_exp = false;
    if (*p == 'e' || *p == 'E') {
        const char *q = p + 1;
        if (*q == '+' || *q == '-') q++;
        if (!(*q >= '0' && *q <= '9')) return err_not_numeric(diag);
        while (*q >= '0' && *q <= '9') q++;
        p = q;
        has_exp = true;
    }
    while (is_blank(*p)) p++;
    if (*p) return err_not_numeric(diag);

    /* "1.0E10" is how Hive prints a double: let the double parser place
     * the point, then truncate. */
    if (has_exp) return int_from_double(argus_strtod(start, NULL), r, diag);

    if (overflow) return err_out_of_range(diag);
    for (size_t i = 0; i < nfrac; i++)
        if (frac[i] != '0') { r->fractional = true; break; }
    return SQL_SUCCESS;
}

static bool is_int_target(SQLSMALLINT t)
{
    switch (t) {
    case SQL_C_SLONG: case SQL_C_LONG: case SQL_C_ULONG:
    case SQL_C_SSHORT: case SQL_C_SHORT: case SQL_C_USHORT:
    case SQL_C_STINYINT: case SQL_C_TINYINT: case SQL_C_UTINYINT:
    case SQL_C_SBIGINT: case SQL_C_UBIGINT: case SQL_C_BIT:
        return true;
    default:
        return false;
    }
}

/* Range-check and store an integer in its C target. The bounds are the
 * magnitudes allowed on each side of zero. */
static SQLRETURN store_int(const int_value_t *r, SQLSMALLINT target_type,
                           SQLPOINTER target_value, SQLLEN *str_len_or_ind,
                           argus_diag_t *diag)
{
    unsigned long long max_pos, max_neg;
    size_t size;
    switch (target_type) {
    case SQL_C_SLONG: case SQL_C_LONG:
        max_pos = 2147483647ULL; max_neg = 2147483648ULL; size = 4; break;
    case SQL_C_ULONG:
        max_pos = 4294967295ULL; max_neg = 0; size = 4; break;
    case SQL_C_SSHORT: case SQL_C_SHORT:
        max_pos = 32767; max_neg = 32768; size = 2; break;
    case SQL_C_USHORT:
        max_pos = 65535; max_neg = 0; size = 2; break;
    case SQL_C_STINYINT: case SQL_C_TINYINT:
        max_pos = 127; max_neg = 128; size = 1; break;
    case SQL_C_UTINYINT:
        max_pos = 255; max_neg = 0; size = 1; break;
    case SQL_C_SBIGINT:
        max_pos = 9223372036854775807ULL; max_neg = 9223372036854775808ULL; size = 8; break;
    case SQL_C_UBIGINT:
        max_pos = ULLONG_MAX; max_neg = 0; size = 8; break;
    case SQL_C_BIT:
        max_pos = 1; max_neg = 0; size = 1; break;
    default:
        return argus_set_error(diag, "HY003", "[Argus] Program type out of range", 0);
    }

    if (r->negative && r->magnitude) {
        if (r->magnitude > max_neg) return err_out_of_range(diag);
    } else if (r->magnitude > max_pos) {
        return err_out_of_range(diag);
    }

    if (target_value) {
        /* Two's complement of the magnitude, truncated to the target width. */
        unsigned long long u = r->negative ? 0ULL - r->magnitude : r->magnitude;
        switch (size) {
        case 1: *(uint8_t *)target_value = (uint8_t)u; break;
        case 2: *(uint16_t *)target_value = (uint16_t)u; break;
        case 4: *(uint32_t *)target_value = (uint32_t)u; break;
        default: *(uint64_t *)target_value = (uint64_t)u; break;
        }
    }
    if (str_len_or_ind) *str_len_or_ind = (SQLLEN)size;
    if (r->fractional) {
        argus_diag_push(diag, "01S07", "[Argus] Fractional truncation", 0);
        return SQL_SUCCESS_WITH_INFO;
    }
    return SQL_SUCCESS;
}

/* ── Floating point ── */

static SQLRETURN double_from_text(const char *s, double *out, argus_diag_t *diag)
{
    char *end = NULL;
    errno = 0;
    double v = argus_strtod(s, &end);
    if (end == s) return err_not_numeric(diag);
    while (is_blank(*end)) end++;
    if (*end) return err_not_numeric(diag);
    if (errno == ERANGE && isinf(v)) return err_out_of_range(diag);
    *out = v;
    return SQL_SUCCESS;
}

static SQLRETURN store_double(double v, SQLSMALLINT target_type,
                              SQLPOINTER target_value, SQLLEN *str_len_or_ind,
                              argus_diag_t *diag)
{
    if (target_type == SQL_C_FLOAT) {
        float f = (float)v;
        if (isinf(f) && !isinf(v)) return err_out_of_range(diag);
        if (target_value) *(SQLREAL *)target_value = f;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLREAL);
    } else {
        if (target_value) *(SQLDOUBLE *)target_value = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLDOUBLE);
    }
    return SQL_SUCCESS;
}

/* ── SQL_NUMERIC_STRUCT ── */

/* (hi:lo) = (hi:lo) * 10 + digit; false on 128-bit overflow. */
static bool u128_mul10_add(unsigned long long *hi, unsigned long long *lo,
                           unsigned digit)
{
    unsigned long long lo_x10, carry;
#if defined(__GNUC__) || defined(__clang__)
    __extension__ typedef unsigned __int128 u128;
    u128 full = (u128)*lo * 10;
    lo_x10 = (unsigned long long)full;
    carry  = (unsigned long long)(full >> 64);
#else
    /* 64x64->128 via 32-bit halves (MSVC) */
    unsigned long long a_lo = *lo & 0xFFFFFFFFULL;
    unsigned long long a_hi = *lo >> 32;
    unsigned long long r0 = a_lo * 10;
    unsigned long long r1 = a_hi * 10 + (r0 >> 32);
    lo_x10 = (r0 & 0xFFFFFFFFULL) | ((r1 & 0xFFFFFFFFULL) << 32);
    carry  = r1 >> 32;
#endif
    if (*hi > (0xFFFFFFFFFFFFFFFFULL - carry) / 10) return false;
    *hi = *hi * 10 + carry;
    *lo = lo_x10 + digit;
    if (*lo < lo_x10) (*hi)++;
    return true;
}

/* Decimal text — digits, an optional point, an optional exponent — to the
 * little-endian 128-bit SQL_NUMERIC_STRUCT. The value keeps its own scale
 * ("1.50" has scale 2, "1E3" is 1000 with scale 0); the ODBC descriptor
 * scale is not applied. More than 38 significant digits is 22003. */
static SQLRETURN numeric_from_text(const char *s, SQL_NUMERIC_STRUCT *num,
                                   argus_diag_t *diag)
{
    memset(num, 0, sizeof(*num));
    const char *p = s;
    while (is_blank(*p)) p++;
    num->sign = 1;
    if (*p == '-') { num->sign = 0; p++; }
    else if (*p == '+') p++;

    unsigned long long lo = 0, hi = 0;
    int scale = 0, digits = 0;
    bool point = false, any = false;
    for (;; p++) {
        if (*p == '.') {
            if (point) return err_not_numeric(diag);
            point = true;
            continue;
        }
        if (*p < '0' || *p > '9') break;
        any = true;
        unsigned d = (unsigned)(*p - '0');
        if (point) scale++;
        if (digits == 0 && d == 0) continue;          /* leading zero */
        if (++digits > 38 || !u128_mul10_add(&hi, &lo, d))
            return err_out_of_range(diag);
    }
    if (!any) return err_not_numeric(diag);

    if (*p == 'e' || *p == 'E') {
        p++;
        int esign = 1, exp = 0;
        if (*p == '-') { esign = -1; p++; }
        else if (*p == '+') p++;
        if (!(*p >= '0' && *p <= '9')) return err_not_numeric(diag);
        for (; *p >= '0' && *p <= '9'; p++)
            if (exp < 1000) exp = exp * 10 + (*p - '0');
        scale -= esign * exp;
    }
    while (is_blank(*p)) p++;
    if (*p) return err_not_numeric(diag);

    /* A positive exponent beyond the fraction becomes trailing zeros. */
    while (scale < 0) {
        if (digits && (++digits > 38 || !u128_mul10_add(&hi, &lo, 0)))
            return err_out_of_range(diag);
        scale++;
    }
    if (scale > 127) return err_out_of_range(diag);
    if (digits == 0) scale = 0;                       /* zero is 0 */

    int precision = digits > scale ? digits : scale;
    num->precision = (SQLCHAR)(precision > 0 ? precision : 1);
    num->scale = (SQLSCHAR)scale;
    memcpy(num->val, &lo, 8);
    memcpy(num->val + 8, &hi, 8);
    return SQL_SUCCESS;
}

/* ── Date and time ── */

/* "YYYY-MM-DD", "HH:MM[:SS[.fraction]]", or both joined by a space or 'T',
 * followed by an optional zone the driver reads past but does not apply
 * ('Z', "+05:00", "-0800", " UTC", " Europe/Paris"): the application gets
 * the wall-clock time the engine printed. `fraction` is in nanoseconds. */
static bool parse_datetime(const char *s, SQL_TIMESTAMP_STRUCT *ts,
                           bool *has_date, bool *has_time)
{
    const char *p = s;
    while (is_blank(*p)) p++;
    memset(ts, 0, sizeof(*ts));
    *has_date = *has_time = false;

    unsigned y, mo, d, h, mi, sec = 0;
    int n = 0;
    if (sscanf(p, "%4u-%2u-%2u%n", &y, &mo, &d, &n) == 3 && n > 0) {
        ts->year = (SQLSMALLINT)y;
        ts->month = (SQLUSMALLINT)mo;
        ts->day = (SQLUSMALLINT)d;
        *has_date = true;
        p += n;
        if (*p == ' ' || *p == 'T') p++;
    }
    n = 0;
    if (sscanf(p, "%2u:%2u%n", &h, &mi, &n) == 2 && n > 0) {
        p += n;
        if (*p == ':') {
            n = 0;
            if (sscanf(p, ":%2u%n", &sec, &n) != 1 || n == 0) return false;
            p += n;
        }
        ts->hour = (SQLUSMALLINT)h;
        ts->minute = (SQLUSMALLINT)mi;
        ts->second = (SQLUSMALLINT)sec;
        *has_time = true;
        if (*p == '.') {
            p++;
            SQLUINTEGER frac = 0;
            int digits = 0;
            while (*p >= '0' && *p <= '9') {
                if (digits < 9) {
                    frac = frac * 10 + (SQLUINTEGER)(*p - '0');
                    digits++;
                }
                p++;
            }
            if (digits == 0) return false;
            for (; digits < 9; digits++) frac *= 10;
            ts->fraction = frac;
        }
    }
    if (!*has_date && !*has_time) return false;

    /* Zone suffix: 'Z' or a numeric offset may touch the time; a zone name
     * ("UTC", "Europe/Paris", or a legacy one like "Japan") follows a blank,
     * which is how every engine prints it and what keeps "10:20x" invalid. */
    bool spaced = is_blank(*p);
    while (is_blank(*p)) p++;
    if (*p == 'Z' || *p == 'z') {
        p++;
    } else if (*p == '+' || *p == '-') {
        p++;
        if (!(*p >= '0' && *p <= '9')) return false;
        while (*p >= '0' && *p <= '9') p++;
        if (*p == ':') p++;
        while (*p >= '0' && *p <= '9') p++;
    } else if (spaced && g_ascii_isalpha(*p)) {
        while (g_ascii_isalnum(*p) || *p == '/' || *p == '_' || *p == '-' ||
               *p == '+' || *p == ':')
            p++;
    }
    while (is_blank(*p)) p++;
    return *p == '\0';
}

static bool datetime_in_range(const SQL_TIMESTAMP_STRUCT *ts, bool has_date,
                              bool has_time)
{
    if (has_date && (ts->month < 1 || ts->month > 12 || ts->day < 1 || ts->day > 31))
        return false;
    if (has_time && (ts->hour > 23 || ts->minute > 59 || ts->second > 59))
        return false;
    return true;
}

static SQLRETURN err_datetime(argus_diag_t *diag, const char *what)
{
    char msg[80];
    snprintf(msg, sizeof(msg), "[Argus] Invalid %s value", what);
    return argus_set_error(diag, "22007", msg, 0);
}

static SQLRETURN convert_cell_to_target(
    const argus_cell_t *cell,
    SQLSMALLINT target_type,
    SQLPOINTER target_value,
    SQLLEN buffer_length,
    SQLLEN *str_len_or_ind,
    argus_getdata_state_t *gd,
    argus_diag_t *diag)
{
    if (cell->is_null) {
        /*
         * With no indicator there is nowhere to say "this is NULL", and the
         * application would read whatever its buffer already held as though
         * it were the value -- a zero, a blank, 1970-01-01. ODBC calls that
         * 22002 rather than success, so the caller finds out.
         */
        if (!str_len_or_ind)
            return argus_set_error(diag, "22002",
                                   "[Argus] Indicator variable required but "
                                   "not supplied for a NULL value", 0);
        *str_len_or_ind = SQL_NULL_DATA;
        return SQL_SUCCESS;
    }

    /* A binary value: the bytes, or their hex rendering. */
    if (cell->native_kind == ARGUS_NATIVE_BINARY) {
        const char *bytes = cell->data ? cell->data : "";
        switch (target_type) {
        case SQL_C_BINARY:
        case SQL_C_DEFAULT:
            return put_bytes(bytes, cell->data_len, target_value, buffer_length,
                             str_len_or_ind, gd, diag);
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
            return put_hex((const unsigned char *)bytes, cell->data_len,
                           target_type == SQL_C_WCHAR, target_value,
                           buffer_length, str_len_or_ind, gd, diag);
        default:
            return argus_set_error(diag, "07006",
                                   "[Argus] Restricted data type attribute violation", 0);
        }
    }

    /* Typed fast path: a cell carrying a native value converts straight to
     * a numeric C type, skipping the text round-trip. Other targets take
     * the text form, made from the native value when the backend gave
     * none. */
    const char *text = cell->data;
    size_t text_len = cell->data ? cell->data_len : 0;
    char tmp[64];
    if (cell->native_kind == ARGUS_NATIVE_I64 ||
        cell->native_kind == ARGUS_NATIVE_F64) {
        bool is_i64 = cell->native_kind == ARGUS_NATIVE_I64;
        if (is_int_target(target_type)) {
            int_value_t iv;
            if (is_i64) {
                int_from_i64(cell->native.i64, &iv);
            } else {
                SQLRETURN r = int_from_double(cell->native.f64, &iv, diag);
                if (r != SQL_SUCCESS) return r;
            }
            return store_int(&iv, target_type, target_value, str_len_or_ind, diag);
        }
        if (target_type == SQL_C_FLOAT || target_type == SQL_C_DOUBLE)
            return store_double(is_i64 ? (double)cell->native.i64 : cell->native.f64,
                                target_type, target_value, str_len_or_ind, diag);
        if (!text) {
            text_len = is_i64
                ? (size_t)snprintf(tmp, sizeof(tmp), "%lld", (long long)cell->native.i64)
                : argus_dtoa_shortest(tmp, sizeof(tmp), cell->native.f64);
            text = tmp;
        }
    }
    if (!text) text = "";

    switch (target_type) {
    case SQL_C_CHAR:
    case SQL_C_DEFAULT:
        return put_char_bytes(text, text_len, target_value, buffer_length,
                              str_len_or_ind, gd, diag);

    case SQL_C_WCHAR:
        return put_wchars(text, text_len, target_value, buffer_length,
                          str_len_or_ind, gd, diag);

    case SQL_C_BINARY:
        /* Character data to a binary target is copied byte for byte; the
         * engine's encoding of a BINARY column is decoded by the backend,
         * never guessed from the text here. */
        return put_bytes(text, text_len, target_value, buffer_length,
                         str_len_or_ind, gd, diag);

    case SQL_C_SLONG: case SQL_C_LONG: case SQL_C_ULONG:
    case SQL_C_SSHORT: case SQL_C_SHORT: case SQL_C_USHORT:
    case SQL_C_STINYINT: case SQL_C_TINYINT: case SQL_C_UTINYINT:
    case SQL_C_SBIGINT: case SQL_C_UBIGINT: case SQL_C_BIT: {
        int_value_t iv;
        SQLRETURN r = int_from_text(text, &iv, diag);
        if (r != SQL_SUCCESS) return r;
        return store_int(&iv, target_type, target_value, str_len_or_ind, diag);
    }

    case SQL_C_FLOAT:
    case SQL_C_DOUBLE: {
        /* double_from_text only writes on success and the caller returns
         * on anything else, but the compiler cannot see that through the
         * call and warns at -O2. */
        double v = 0;
        SQLRETURN r = double_from_text(text, &v, diag);
        if (r != SQL_SUCCESS) return r;
        return store_double(v, target_type, target_value, str_len_or_ind, diag);
    }

    case SQL_C_TYPE_DATE:
    case SQL_C_DATE: {
        SQL_TIMESTAMP_STRUCT ts;
        bool has_date, has_time;
        if (!parse_datetime(text, &ts, &has_date, &has_time) || !has_date ||
            !datetime_in_range(&ts, has_date, has_time))
            return err_datetime(diag, "date");
        if (target_value) {
            SQL_DATE_STRUCT *date = (SQL_DATE_STRUCT *)target_value;
            date->year = ts.year;
            date->month = ts.month;
            date->day = ts.day;
        }
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_DATE_STRUCT);
        if (has_time && (ts.hour || ts.minute || ts.second || ts.fraction)) {
            argus_diag_push(diag, "01S07", "[Argus] Fractional truncation", 0);
            return SQL_SUCCESS_WITH_INFO;
        }
        return SQL_SUCCESS;
    }

    case SQL_C_TYPE_TIME:
    case SQL_C_TIME: {
        SQL_TIMESTAMP_STRUCT ts;
        bool has_date, has_time;
        if (!parse_datetime(text, &ts, &has_date, &has_time) || !has_time ||
            !datetime_in_range(&ts, has_date, has_time))
            return err_datetime(diag, "time");
        if (target_value) {
            SQL_TIME_STRUCT *time = (SQL_TIME_STRUCT *)target_value;
            time->hour = ts.hour;
            time->minute = ts.minute;
            time->second = ts.second;
        }
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_TIME_STRUCT);
        if (ts.fraction) {
            argus_diag_push(diag, "01S07", "[Argus] Fractional truncation", 0);
            return SQL_SUCCESS_WITH_INFO;
        }
        return SQL_SUCCESS;
    }

    case SQL_C_TYPE_TIMESTAMP:
    case SQL_C_TIMESTAMP: {
        SQL_TIMESTAMP_STRUCT ts;
        bool has_date, has_time;
        if (!parse_datetime(text, &ts, &has_date, &has_time) ||
            !datetime_in_range(&ts, has_date, has_time))
            return err_datetime(diag, "timestamp");
        if (!has_date) {
            /* A time alone takes today's date, as ODBC specifies. */
            GDateTime *now = g_date_time_new_now_local();
            if (now) {
                ts.year = (SQLSMALLINT)g_date_time_get_year(now);
                ts.month = (SQLUSMALLINT)g_date_time_get_month(now);
                ts.day = (SQLUSMALLINT)g_date_time_get_day_of_month(now);
                g_date_time_unref(now);
            }
        }
        if (target_value)
            *(SQL_TIMESTAMP_STRUCT *)target_value = ts;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_TIMESTAMP_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_NUMERIC: {
        SQL_NUMERIC_STRUCT num;
        SQLRETURN r = numeric_from_text(text, &num, diag);
        if (r != SQL_SUCCESS) return r;
        if (target_value)
            *(SQL_NUMERIC_STRUCT *)target_value = num;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_NUMERIC_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_GUID: {
        /*
         * Parse UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
         * into SQLGUID structure.
         */
        SQLGUID guid;
        memset(&guid, 0, sizeof(guid));
        unsigned int d1, d2, d3;
        unsigned int d4[8];
        int n = sscanf(text,
            "%8x-%4x-%4x-%2x%2x-%2x%2x%2x%2x%2x%2x",
            &d1, &d2, &d3,
            &d4[0], &d4[1], &d4[2], &d4[3],
            &d4[4], &d4[5], &d4[6], &d4[7]);
        if (n != 11) {
            return argus_set_error(diag, "22018",
                                   "[Argus] Invalid UUID/GUID format", 0);
        }
        guid.Data1 = (DWORD)d1;
        guid.Data2 = (WORD)d2;
        guid.Data3 = (WORD)d3;
        for (int i = 0; i < 8; i++)
            guid.Data4[i] = (BYTE)d4[i];
        if (target_value)
            *(SQLGUID *)target_value = guid;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLGUID);
        return SQL_SUCCESS;
    }

    case SQL_C_INTERVAL_YEAR:
    case SQL_C_INTERVAL_MONTH:
    case SQL_C_INTERVAL_DAY:
    case SQL_C_INTERVAL_HOUR:
    case SQL_C_INTERVAL_MINUTE:
    case SQL_C_INTERVAL_SECOND:
    case SQL_C_INTERVAL_YEAR_TO_MONTH:
    case SQL_C_INTERVAL_DAY_TO_HOUR:
    case SQL_C_INTERVAL_DAY_TO_MINUTE:
    case SQL_C_INTERVAL_DAY_TO_SECOND:
    case SQL_C_INTERVAL_HOUR_TO_MINUTE:
    case SQL_C_INTERVAL_HOUR_TO_SECOND:
    case SQL_C_INTERVAL_MINUTE_TO_SECOND: {
        if (str_len_or_ind)
            *str_len_or_ind = (SQLLEN)sizeof(SQL_INTERVAL_STRUCT);
        if (!target_value || buffer_length < (SQLLEN)sizeof(SQL_INTERVAL_STRUCT))
            return SQL_SUCCESS;

        SQL_INTERVAL_STRUCT *iv = (SQL_INTERVAL_STRUCT *)target_value;
        memset(iv, 0, sizeof(*iv));

        const char *s = text;
        int sign = SQL_FALSE;
        if (*s == '-') { sign = SQL_TRUE; s++; }
        else if (*s == '+') { s++; }
        iv->interval_sign = (SQLSMALLINT)sign;

        unsigned int v1 = 0, v2 = 0, v3 = 0, v4 = 0;
        unsigned int frac = 0;

        switch (target_type) {
        case SQL_C_INTERVAL_YEAR:
            iv->interval_type = SQL_IS_YEAR;
            sscanf(s, "%u", &v1);
            iv->intval.year_month.year = (SQLUINTEGER)v1;
            break;
        case SQL_C_INTERVAL_MONTH:
            iv->interval_type = SQL_IS_MONTH;
            sscanf(s, "%u", &v1);
            iv->intval.year_month.month = (SQLUINTEGER)v1;
            break;
        case SQL_C_INTERVAL_YEAR_TO_MONTH:
            iv->interval_type = SQL_IS_YEAR_TO_MONTH;
            sscanf(s, "%u-%u", &v1, &v2);
            iv->intval.year_month.year = (SQLUINTEGER)v1;
            iv->intval.year_month.month = (SQLUINTEGER)v2;
            break;
        case SQL_C_INTERVAL_DAY:
            iv->interval_type = SQL_IS_DAY;
            sscanf(s, "%u", &v1);
            iv->intval.day_second.day = (SQLUINTEGER)v1;
            break;
        case SQL_C_INTERVAL_HOUR:
            iv->interval_type = SQL_IS_HOUR;
            sscanf(s, "%u", &v1);
            iv->intval.day_second.hour = (SQLUINTEGER)v1;
            break;
        case SQL_C_INTERVAL_MINUTE:
            iv->interval_type = SQL_IS_MINUTE;
            sscanf(s, "%u", &v1);
            iv->intval.day_second.minute = (SQLUINTEGER)v1;
            break;
        case SQL_C_INTERVAL_SECOND:
            iv->interval_type = SQL_IS_SECOND;
            sscanf(s, "%u.%u", &v1, &frac);
            iv->intval.day_second.second = (SQLUINTEGER)v1;
            iv->intval.day_second.fraction = (SQLUINTEGER)frac;
            break;
        case SQL_C_INTERVAL_DAY_TO_HOUR:
            iv->interval_type = SQL_IS_DAY_TO_HOUR;
            sscanf(s, "%u %u", &v1, &v2);
            iv->intval.day_second.day = (SQLUINTEGER)v1;
            iv->intval.day_second.hour = (SQLUINTEGER)v2;
            break;
        case SQL_C_INTERVAL_DAY_TO_MINUTE:
            iv->interval_type = SQL_IS_DAY_TO_MINUTE;
            sscanf(s, "%u %u:%u", &v1, &v2, &v3);
            iv->intval.day_second.day = (SQLUINTEGER)v1;
            iv->intval.day_second.hour = (SQLUINTEGER)v2;
            iv->intval.day_second.minute = (SQLUINTEGER)v3;
            break;
        case SQL_C_INTERVAL_DAY_TO_SECOND:
            iv->interval_type = SQL_IS_DAY_TO_SECOND;
            sscanf(s, "%u %u:%u:%u.%u", &v1, &v2, &v3, &v4, &frac);
            iv->intval.day_second.day = (SQLUINTEGER)v1;
            iv->intval.day_second.hour = (SQLUINTEGER)v2;
            iv->intval.day_second.minute = (SQLUINTEGER)v3;
            iv->intval.day_second.second = (SQLUINTEGER)v4;
            iv->intval.day_second.fraction = (SQLUINTEGER)frac;
            break;
        case SQL_C_INTERVAL_HOUR_TO_MINUTE:
            iv->interval_type = SQL_IS_HOUR_TO_MINUTE;
            sscanf(s, "%u:%u", &v1, &v2);
            iv->intval.day_second.hour = (SQLUINTEGER)v1;
            iv->intval.day_second.minute = (SQLUINTEGER)v2;
            break;
        case SQL_C_INTERVAL_HOUR_TO_SECOND:
            iv->interval_type = SQL_IS_HOUR_TO_SECOND;
            sscanf(s, "%u:%u:%u.%u", &v1, &v2, &v3, &frac);
            iv->intval.day_second.hour = (SQLUINTEGER)v1;
            iv->intval.day_second.minute = (SQLUINTEGER)v2;
            iv->intval.day_second.second = (SQLUINTEGER)v3;
            iv->intval.day_second.fraction = (SQLUINTEGER)frac;
            break;
        case SQL_C_INTERVAL_MINUTE_TO_SECOND:
            iv->interval_type = SQL_IS_MINUTE_TO_SECOND;
            sscanf(s, "%u:%u.%u", &v1, &v2, &frac);
            iv->intval.day_second.minute = (SQLUINTEGER)v1;
            iv->intval.day_second.second = (SQLUINTEGER)v2;
            iv->intval.day_second.fraction = (SQLUINTEGER)frac;
            break;
        default:
            break;
        }
        return SQL_SUCCESS;
    }

    default:
        return argus_set_error(diag, "HY003",
                               "[Argus] Program type out of range", 0);
    }
}

/* ── Internal: build full scroll cache for static cursors ─────── */

static SQLRETURN build_scroll_cache(argus_stmt_t *stmt)
{
    if (stmt->scroll_cached) return SQL_SUCCESS;

    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->backend || !dbc->backend_conn) {
        return argus_set_error(&stmt->diag, "HY000",
                               "[Argus] No backend connection", 0);
    }

    /* Start with reasonable capacity */
    size_t capacity = 1024;
    argus_row_t *all_rows = calloc(capacity, sizeof(argus_row_t));
    if (!all_rows) {
        return argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }
    size_t total = 0;

    int batch_size = (dbc->fetch_buffer_size > 0)
                     ? dbc->fetch_buffer_size
                     : ARGUS_DEFAULT_BATCH_SIZE;

    /* Bound the materialisation: a static cursor buffers the whole result, so
     * an unbounded SELECT would exhaust memory. Past the cap, fail cleanly with
     * an actionable diagnostic rather than OOM the process. */
    size_t max_rows = (dbc->max_scroll_rows > 0)
                      ? (size_t)dbc->max_scroll_rows
                      : (size_t)ARGUS_DEFAULT_MAX_SCROLL_ROWS;

    while (1) {
        argus_row_cache_clear(&stmt->row_cache);

        /* Batches keep coming until the result is exhausted: an SQLCancel
         * from another thread is answered between two of them. */
        if (argus_stmt_cancel_checkpoint(stmt) != SQL_SUCCESS) {
            for (size_t i = 0; i < total; i++)
                argus_row_free(&all_rows[i], stmt->num_cols);
            free(all_rows);
            return SQL_ERROR;
        }

        int num_cols = 0;
        int rc = dbc->backend->fetch_results(
            dbc->backend_conn, stmt->op,
            batch_size,
            &stmt->row_cache,
            stmt->columns, &num_cols);

        if (rc != 0) {
            /* Free partially built cache */
            for (size_t i = 0; i < total; i++)
                argus_row_free(&all_rows[i], stmt->num_cols);
            free(all_rows);
            if (argus_stmt_cancel_checkpoint(stmt) != SQL_SUCCESS)
                return SQL_ERROR;
            set_fetch_error(stmt);
            return SQL_ERROR;
        }

        if (num_cols > 0 && !stmt->metadata_fetched) {
            if (argus_stmt_ensure_columns(stmt, num_cols) != 0) {
                free(all_rows);
                return SQL_ERROR;
            }
            stmt->num_cols = num_cols;
            stmt->metadata_fetched = true;
        }

        if (stmt->row_cache.num_rows == 0) break;

        /* Enforce the materialisation cap before growing further. */
        if (total + stmt->row_cache.num_rows > max_rows) {
            for (size_t i = 0; i < total; i++)
                argus_row_free(&all_rows[i], stmt->num_cols);
            free(all_rows);
            char msg[192];
            snprintf(msg, sizeof(msg),
                     "[Argus] Result set exceeds the static-cursor limit of %zu "
                     "rows (MaxScrollRows); use a forward-only cursor for this "
                     "query", max_rows);
            return argus_set_error(&stmt->diag, "HY001", msg, 0);
        }

        /* Copy rows into scroll cache */
        if (total + stmt->row_cache.num_rows > capacity) {
            while (total + stmt->row_cache.num_rows > capacity)
                capacity *= 2;
            argus_row_t *new_rows = realloc(all_rows,
                                             capacity * sizeof(argus_row_t));
            if (!new_rows) {
                for (size_t i = 0; i < total; i++)
                    argus_row_free(&all_rows[i], stmt->num_cols);
                free(all_rows);
                return argus_set_error(&stmt->diag, "HY001",
                                       "[Argus] Memory allocation failed", 0);
            }
            all_rows = new_rows;
        }

        for (size_t i = 0; i < stmt->row_cache.num_rows; i++) {
            /* Move rows (transfer ownership of cells) */
            all_rows[total + i] = stmt->row_cache.rows[i];
            stmt->row_cache.rows[i].cells = NULL;
        }
        total += stmt->row_cache.num_rows;
        stmt->row_cache.num_rows = 0;
    }

    /* Store the scroll cache */
    stmt->scroll_rows = all_rows;
    stmt->scroll_row_count = total;
    stmt->scroll_position = 0;
    stmt->scroll_cached = true;
    stmt->fetch_started = true;

    return SQL_SUCCESS;
}

/* ── Internal: deliver scroll cache row to bound columns ─────── */

/*
 * Where row `rowset_idx` of the current rowset writes, for one bound column.
 *
 * ODBC has two layouts and they offset differently:
 *   column-wise (SQL_BIND_BY_COLUMN, the default) — each column is its own
 *     array, so the value advances by the column's buffer length and the
 *     indicator by sizeof(SQLLEN);
 *   row-wise — each row is one application struct, SQL_ATTR_ROW_BIND_TYPE is
 *     that struct's size, and BOTH pointers advance by it.
 *
 * Using the column-wise arithmetic on a row-wise binding writes inside the
 * application's buffer but at the wrong offset: no crash, no diagnostic, just
 * wrong data. Hence one helper, used by every fetch path.
 */
static void resolve_bind_target(const argus_stmt_t *stmt,
                                const argus_col_binding_t *bind,
                                SQLULEN rowset_idx,
                                SQLPOINTER *out_target,
                                SQLLEN **out_ind)
{
    SQLPOINTER target = bind->target_value;
    SQLLEN    *ind    = bind->str_len_or_ind;

    if (rowset_idx > 0) {
        size_t value_stride, ind_stride;

        if (stmt->row_bind_type == SQL_BIND_BY_COLUMN) {
            value_stride = (size_t)bind->buffer_length;
            ind_stride   = sizeof(SQLLEN);
        } else {
            value_stride = ind_stride = (size_t)stmt->row_bind_type;
        }

        if (target) target = (char *)target + rowset_idx * value_stride;
        if (ind)    ind    = (SQLLEN *)((char *)ind + rowset_idx * ind_stride);
    }

    *out_target = target;
    *out_ind    = ind;
}

static SQLRETURN deliver_scroll_row(argus_stmt_t *stmt, size_t row_idx,
                                     SQLULEN rowset_idx)
{
    if (row_idx >= stmt->scroll_row_count) return SQL_NO_DATA;

    argus_row_t *row = &stmt->scroll_rows[row_idx];
    SQLRETURN final_ret = SQL_SUCCESS;

    for (int col = 0; col < stmt->num_cols && col < stmt->bindings_capacity; col++) {
        if (!stmt->bindings[col].bound) continue;

        argus_col_binding_t *bind = &stmt->bindings[col];
        argus_cell_t *cell = &row->cells[col];

        SQLPOINTER target = NULL;
        SQLLEN *ind_ptr = NULL;
        resolve_bind_target(stmt, bind, rowset_idx, &target, &ind_ptr);

        SQLRETURN ret = convert_cell_to_target(
            cell, bind->target_type,
            target, bind->buffer_length,
            ind_ptr, NULL, &stmt->diag);

        if (ret == SQL_SUCCESS_WITH_INFO)
            final_ret = SQL_SUCCESS_WITH_INFO;
        else if (ret == SQL_ERROR)
            return SQL_ERROR;
    }

    stmt->rows_fetched_total++;
    return final_ret;
}

/* ── Internal: fetch a single row into bound columns ──────────── */

static SQLRETURN fetch_single_row(argus_stmt_t *stmt, SQLULEN rowset_idx)
{
    /* SQL_ATTR_MAX_ROWS, or a guardrail's ceiling, whichever is smaller. */
    SQLULEN limit = argus_stmt_effective_max_rows(stmt);
    if (limit > 0 && stmt->rows_fetched_total >= limit) {
        stmt->row_count = (SQLLEN)stmt->rows_fetched_total;
        return SQL_NO_DATA;
    }

    /* Check if we need to fetch a new batch */
    if (!stmt->fetch_started ||
        stmt->row_cache.current_row >= stmt->row_cache.num_rows) {

        if (stmt->row_cache.exhausted && stmt->fetch_started) {
            stmt->row_count = (SQLLEN)stmt->rows_fetched_total;
            return SQL_NO_DATA;
        }

        SQLRETURN rc = fetch_batch(stmt);
        if (rc != SQL_SUCCESS) return rc;

        stmt->fetch_started = true;
        stmt->row_cache.current_row = 0;

        if (stmt->row_cache.num_rows == 0) {
            stmt->row_count = (SQLLEN)stmt->rows_fetched_total;
            return SQL_NO_DATA;
        }
    }

    /* Get current row */
    size_t row_idx = stmt->row_cache.current_row;
    argus_row_t *row = &stmt->row_cache.rows[row_idx];
    stmt->row_cache.current_row++;

    /* Transfer data to bound columns */
    SQLRETURN final_ret = SQL_SUCCESS;
    for (int col = 0; col < stmt->num_cols && col < stmt->bindings_capacity; col++) {
        if (!stmt->bindings[col].bound) continue;

        argus_col_binding_t *bind = &stmt->bindings[col];
        argus_cell_t *cell = &row->cells[col];

        /* Block cursors (row_array_size > 1) write row rowset_idx of the
         * rowset; the layout decides the arithmetic. */
        SQLPOINTER target = NULL;
        SQLLEN *ind_ptr = NULL;
        resolve_bind_target(stmt, bind, rowset_idx, &target, &ind_ptr);

        SQLRETURN ret = convert_cell_to_target(
            cell, bind->target_type,
            target, bind->buffer_length,
            ind_ptr, NULL, &stmt->diag);

        if (ret == SQL_SUCCESS_WITH_INFO)
            final_ret = SQL_SUCCESS_WITH_INFO;
        else if (ret == SQL_ERROR)
            return SQL_ERROR;
    }

    stmt->rows_fetched_total++;
    return final_ret;
}

/* ── ODBC API: SQLFetch ──────────────────────────────────────── */

SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    /* Reset SQLGetData multi-call state on new row */
    argus_getdata_reset(&stmt->getdata);

    if (!stmt->executed) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY010",
                               "[Argus] Function sequence error: not executed",
                               0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    /* An SQLCancel that arrived during the previous fetch ends the cursor
     * now, rather than after the rows still buffered from that fetch. */
    SQLRETURN canceled = argus_stmt_cancel_checkpoint(stmt);
    if (canceled != SQL_SUCCESS) {
        ARGUS_STMT_UNLOCK(stmt);
        return canceled;
    }

    SQLULEN array_size = stmt->row_array_size > 0 ? stmt->row_array_size : 1;
    SQLULEN rows_fetched = 0;
    SQLULEN status_filled = 0;   /* row status slots already written */
    SQLRETURN final_ret = SQL_SUCCESS;

    for (SQLULEN i = 0; i < array_size; i++) {
        SQLRETURN ret = fetch_single_row(stmt, i);

        if (ret == SQL_NO_DATA) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_NOROW;
            status_filled = i + 1;
            break;
        }

        if (ret == SQL_ERROR) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_ERROR;
            status_filled = i + 1;
            final_ret = SQL_ERROR;
            break;
        }

        if (stmt->row_status_ptr)
            stmt->row_status_ptr[i] = SQL_ROW_SUCCESS;
        if (ret == SQL_SUCCESS_WITH_INFO) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_SUCCESS_WITH_INFO;
            final_ret = SQL_SUCCESS_WITH_INFO;
        }
        rows_fetched++;
        status_filled = i + 1;
    }

    /* Fill remaining status slots with SQL_ROW_NOROW (without erasing the
     * SQL_ROW_ERROR slot of a failed row) */
    if (stmt->row_status_ptr) {
        for (SQLULEN i = status_filled; i < array_size; i++)
            stmt->row_status_ptr[i] = SQL_ROW_NOROW;
    }

    /* Update rows fetched pointer */
    if (stmt->rows_fetched_ptr)
        *(stmt->rows_fetched_ptr) = rows_fetched;

    ARGUS_STMT_UNLOCK(stmt);

    /* An error on the first row of the rowset must surface as SQL_ERROR.
     * Reporting SQL_NO_DATA here would turn a dropped connection or a
     * server-side failure into a clean, silently truncated result set. */
    if (rows_fetched == 0)
        return final_ret == SQL_ERROR ? SQL_ERROR : SQL_NO_DATA;

    return final_ret;
}

/* ── ODBC API: SQLFetchScroll ────────────────────────────────── */

SQLRETURN SQL_API SQLFetchScroll(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT FetchOrientation,
    SQLLEN      FetchOffset)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    /* Forward-only cursor: only SQL_FETCH_NEXT allowed */
    if (stmt->cursor_type == SQL_CURSOR_FORWARD_ONLY ||
        stmt->cursor_type == 0) {
        if (FetchOrientation != SQL_FETCH_NEXT) {
            return argus_set_error(&stmt->diag, "HY106",
                                   "[Argus] Fetch type out of range "
                                   "(forward-only cursor)", 0);
        }
        return SQLFetch(StatementHandle);
    }

    /* Static cursor: build full scroll cache on first call */
    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    if (!stmt->executed) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY010",
                               "[Argus] Function sequence error: not executed", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    SQLRETURN canceled = argus_stmt_cancel_checkpoint(stmt);
    if (canceled != SQL_SUCCESS) {
        ARGUS_STMT_UNLOCK(stmt);
        return canceled;
    }

    if (!stmt->scroll_cached) {
        SQLRETURN rc = build_scroll_cache(stmt);
        if (rc != SQL_SUCCESS) {
            ARGUS_STMT_UNLOCK(stmt);
            return rc;
        }
    }

    /* Compute new position based on orientation */
    long long new_pos;
    size_t total = stmt->scroll_row_count;

    switch (FetchOrientation) {
    case SQL_FETCH_NEXT:
        new_pos = (long long)stmt->scroll_position;
        break;
    case SQL_FETCH_PRIOR:
        new_pos = (long long)stmt->scroll_position - 2;
        break;
    case SQL_FETCH_FIRST:
        new_pos = 0;
        break;
    case SQL_FETCH_LAST:
        new_pos = (long long)total - 1;
        break;
    case SQL_FETCH_ABSOLUTE:
        if (FetchOffset > 0)
            new_pos = FetchOffset - 1;
        else if (FetchOffset < 0)
            new_pos = (long long)total + FetchOffset;
        else
            new_pos = -1; /* before start */
        break;
    case SQL_FETCH_RELATIVE:
        new_pos = (long long)stmt->scroll_position - 1 + FetchOffset;
        break;
    case SQL_FETCH_BOOKMARK:
        ARGUS_STMT_UNLOCK(stmt);
        return argus_set_error(&stmt->diag, "HYC00",
                               "[Argus] Bookmarks not supported", 0);
    default:
        ARGUS_STMT_UNLOCK(stmt);
        return argus_set_error(&stmt->diag, "HY106",
                               "[Argus] Fetch type out of range", 0);
    }

    /* Bounds check */
    if (new_pos < 0 || (total == 0) || (size_t)new_pos >= total) {
        stmt->scroll_position = (new_pos < 0) ? 0 : total;
        if (stmt->rows_fetched_ptr) *stmt->rows_fetched_ptr = 0;
        if (stmt->row_status_ptr) stmt->row_status_ptr[0] = SQL_ROW_NOROW;
        ARGUS_STMT_UNLOCK(stmt);
        return SQL_NO_DATA;
    }

    /* Reset GetData state */
    argus_getdata_reset(&stmt->getdata);

    /* Fetch rows for the rowset */
    SQLULEN array_size = stmt->row_array_size > 0 ? stmt->row_array_size : 1;
    SQLULEN rows_fetched = 0;
    SQLRETURN final_ret = SQL_SUCCESS;

    for (SQLULEN i = 0; i < array_size; i++) {
        size_t idx = (size_t)new_pos + i;
        if (idx >= total) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_NOROW;
            break;
        }

        SQLRETURN ret = deliver_scroll_row(stmt, idx, i);
        if (ret == SQL_ERROR) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_ERROR;
            final_ret = SQL_ERROR;
            break;
        }
        if (stmt->row_status_ptr) {
            stmt->row_status_ptr[i] = (ret == SQL_SUCCESS_WITH_INFO)
                ? SQL_ROW_SUCCESS_WITH_INFO : SQL_ROW_SUCCESS;
        }
        if (ret == SQL_SUCCESS_WITH_INFO)
            final_ret = SQL_SUCCESS_WITH_INFO;
        rows_fetched++;
    }

    /* Fill remaining status with NOROW */
    if (stmt->row_status_ptr) {
        for (SQLULEN i = rows_fetched; i < array_size; i++)
            stmt->row_status_ptr[i] = SQL_ROW_NOROW;
    }

    /* Record rowset bounds (for SQLSetPos SQL_POSITION / SQL_REFRESH) */
    stmt->scroll_rowset_start = (size_t)new_pos;

    /* Update position to after the fetched rows */
    stmt->scroll_position = (size_t)new_pos + rows_fetched;

    /* Also update row_cache.current_row for GetData compatibility */
    if (rows_fetched > 0 && (size_t)new_pos < total) {
        /* Point row_cache at the last fetched scroll row for GetData */
        stmt->row_cache.current_row = (size_t)new_pos + rows_fetched;
    }

    if (stmt->rows_fetched_ptr)
        *stmt->rows_fetched_ptr = rows_fetched;

    ARGUS_STMT_UNLOCK(stmt);
    return rows_fetched == 0 ? SQL_NO_DATA : final_ret;
}

/* ── ODBC API: SQLGetData ────────────────────────────────────── */

SQLRETURN SQL_API SQLGetData(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLSMALLINT  TargetType,
    SQLPOINTER   TargetValue,
    SQLLEN       BufferLength,
    SQLLEN      *StrLen_or_Ind)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    if (ColumnNumber < 1 || ColumnNumber > (SQLUSMALLINT)stmt->num_cols) {
        SQLRETURN err = argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid column number", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    /* Current row is one behind current_row pointer */
    size_t row_idx = stmt->row_cache.current_row - 1;
    if (row_idx >= stmt->row_cache.num_rows) {
        SQLRETURN err = argus_set_error(&stmt->diag, "24000",
                               "[Argus] Invalid cursor state", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    argus_row_t *row = &stmt->row_cache.rows[row_idx];
    argus_cell_t *cell = &row->cells[ColumnNumber - 1];

    /* Character and binary targets may take the value in pieces: the
     * continuation state follows the column, and once the whole value has
     * gone out the next call on that column reports SQL_NO_DATA, which is
     * what ends an application's "until SQL_NO_DATA" loop. Reading the same
     * column again into a fixed-length type stays possible. */
    bool piecewise = (TargetType == SQL_C_CHAR || TargetType == SQL_C_DEFAULT ||
                      TargetType == SQL_C_WCHAR || TargetType == SQL_C_BINARY);
    if (piecewise && BufferLength < 0) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY090",
                               "[Argus] Invalid string or buffer length", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    argus_getdata_state_t *gd = &stmt->getdata;
    if (gd->col != ColumnNumber) {
        argus_getdata_reset(gd);
        gd->col = ColumnNumber;
    }
    if (piecewise && gd->done && !cell->is_null) {
        ARGUS_STMT_UNLOCK(stmt);
        return SQL_NO_DATA;
    }

    SQLRETURN ret = convert_cell_to_target(cell, TargetType, TargetValue,
                                            BufferLength, StrLen_or_Ind,
                                            piecewise ? gd : NULL,
                                            &stmt->diag);

    ARGUS_STMT_UNLOCK(stmt);
    return ret;
}

/* ── ODBC API: SQLBindCol ────────────────────────────────────── */

SQLRETURN SQL_API SQLBindCol(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLSMALLINT  TargetType,
    SQLPOINTER   TargetValue,
    SQLLEN       BufferLength,
    SQLLEN      *StrLen_or_Ind)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (ColumnNumber < 1) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid column number", 0);
    }

    int idx = ColumnNumber - 1;

    if (!TargetValue) {
        /* Unbind */
        if (idx < stmt->bindings_capacity)
            stmt->bindings[idx].bound = false;
        return SQL_SUCCESS;
    }

    /* Ensure bindings array is large enough */
    if (argus_stmt_ensure_bindings(stmt, ColumnNumber) != 0) {
        return argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }

    stmt->bindings[idx].target_type    = TargetType;
    stmt->bindings[idx].target_value   = TargetValue;
    stmt->bindings[idx].buffer_length  = BufferLength;
    stmt->bindings[idx].str_len_or_ind = StrLen_or_Ind;
    stmt->bindings[idx].bound          = true;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLNumResultCols ──────────────────────────────── */

SQLRETURN SQL_API SQLNumResultCols(
    SQLHSTMT     StatementHandle,
    SQLSMALLINT *ColumnCount)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    if (ColumnCount)
        *ColumnCount = (SQLSMALLINT)stmt->num_cols;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLDescribeCol ────────────────────────────────── */

SQLRETURN SQL_API SQLDescribeCol(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLCHAR     *ColumnName,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *NameLengthPtr,
    SQLSMALLINT *DataTypePtr,
    SQLULEN     *ColumnSizePtr,
    SQLSMALLINT *DecimalDigitsPtr,
    SQLSMALLINT *NullablePtr)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (ColumnNumber < 1 || ColumnNumber > (SQLUSMALLINT)stmt->num_cols) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid column number", 0);
    }

    const argus_column_desc_t *col = &stmt->columns[ColumnNumber - 1];

    if (ColumnName) {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->name, ColumnName, BufferLength);
        if (NameLengthPtr) *NameLengthPtr = len;
    } else if (NameLengthPtr) {
        *NameLengthPtr = col->name_len;
    }

    if (DataTypePtr)      *DataTypePtr      = col->sql_type;
    if (ColumnSizePtr) {
        SQLULEN cs = col->column_size;
        /* Provide default sizes when backend returns 0 */
        if (cs == 0) {
            switch (col->sql_type) {
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:  cs = 65535;  break;
            case SQL_CHAR:         cs = 1;      break;
            case SQL_INTEGER:      cs = 10;     break;
            case SQL_BIGINT:       cs = 19;     break;
            case SQL_SMALLINT:     cs = 5;      break;
            case SQL_TINYINT:      cs = 3;      break;
            case SQL_FLOAT:
            case SQL_DOUBLE:       cs = 15;     break;
            case SQL_REAL:         cs = 7;      break;
            case SQL_DECIMAL:
            case SQL_NUMERIC:      cs = 38;     break;
            case SQL_TYPE_DATE:    cs = 10;     break;
            case SQL_TYPE_TIMESTAMP: cs = 26;   break;
            case SQL_TYPE_TIME:    cs = 8;      break;
            case SQL_BIT:          cs = 1;      break;
            case SQL_BINARY:
            case SQL_VARBINARY:    cs = 8000;   break;
            case SQL_GUID:         cs = 36;     break;
            default:               cs = 255;    break;
            }
        }
        *ColumnSizePtr = cs;
    }
    if (DecimalDigitsPtr) *DecimalDigitsPtr  = col->decimal_digits;
    if (NullablePtr)      *NullablePtr       = col->nullable;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLColAttribute ───────────────────────────────── */

SQLRETURN SQL_API SQLColAttribute(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER   CharacterAttribute,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN      *NumericAttribute)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (ColumnNumber < 1 || ColumnNumber > (SQLUSMALLINT)stmt->num_cols) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid column number", 0);
    }

    const argus_column_desc_t *col = &stmt->columns[ColumnNumber - 1];

    switch (FieldIdentifier) {
    case SQL_DESC_NAME:
    case SQL_COLUMN_NAME:
    case SQL_DESC_LABEL: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->name,
            (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_TYPE:
    case SQL_COLUMN_TYPE:
        if (NumericAttribute) *NumericAttribute = col->sql_type;
        return SQL_SUCCESS;

    case SQL_DESC_LENGTH:
    case SQL_COLUMN_LENGTH:
    case SQL_DESC_OCTET_LENGTH:
    case SQL_DESC_DISPLAY_SIZE:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)col->column_size;
        return SQL_SUCCESS;

    case SQL_DESC_PRECISION:
    case SQL_COLUMN_PRECISION:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)col->column_size;
        return SQL_SUCCESS;

    case SQL_DESC_SCALE:
    case SQL_COLUMN_SCALE:
        if (NumericAttribute) *NumericAttribute = col->decimal_digits;
        return SQL_SUCCESS;

    case SQL_DESC_NULLABLE:
    case SQL_COLUMN_NULLABLE:
        if (NumericAttribute) *NumericAttribute = col->nullable;
        return SQL_SUCCESS;

    case SQL_DESC_UNSIGNED:
        if (NumericAttribute) *NumericAttribute = SQL_FALSE;
        return SQL_SUCCESS;

    case SQL_DESC_AUTO_UNIQUE_VALUE:
        if (NumericAttribute) *NumericAttribute = SQL_FALSE;
        return SQL_SUCCESS;

    case SQL_DESC_SEARCHABLE:
        if (NumericAttribute) *NumericAttribute = SQL_PRED_SEARCHABLE;
        return SQL_SUCCESS;

    case SQL_DESC_UPDATABLE:
        if (NumericAttribute) *NumericAttribute = SQL_ATTR_READONLY;
        return SQL_SUCCESS;

    case SQL_DESC_CASE_SENSITIVE:
        if (NumericAttribute) *NumericAttribute = SQL_TRUE;
        return SQL_SUCCESS;

    case SQL_DESC_FIXED_PREC_SCALE:
        if (NumericAttribute) *NumericAttribute = SQL_FALSE;
        return SQL_SUCCESS;

    case SQL_DESC_TYPE_NAME: {
        const char *type_name;
        switch (col->sql_type) {
        case SQL_VARCHAR:   type_name = "VARCHAR"; break;
        case SQL_INTEGER:   type_name = "INTEGER"; break;
        case SQL_BIGINT:    type_name = "BIGINT"; break;
        case SQL_SMALLINT:  type_name = "SMALLINT"; break;
        case SQL_TINYINT:   type_name = "TINYINT"; break;
        case SQL_FLOAT:     type_name = "FLOAT"; break;
        case SQL_DOUBLE:    type_name = "DOUBLE"; break;
        case SQL_TYPE_TIMESTAMP: type_name = "TIMESTAMP"; break;
        case SQL_TYPE_DATE: type_name = "DATE"; break;
        case SQL_BIT:       type_name = "BOOLEAN"; break;
        case SQL_DECIMAL:   type_name = "DECIMAL"; break;
        case SQL_BINARY:    type_name = "BINARY"; break;
        case SQL_GUID:      type_name = "GUID"; break;
        default:            type_name = "VARCHAR"; break;
        }
        SQLSMALLINT len = argus_copy_string(
            type_name, (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_BASE_TABLE_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->table_name,
            (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_SCHEMA_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->schema_name,
            (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_CATALOG_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->catalog_name,
            (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_BASE_COLUMN_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->name,
            (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_LITERAL_PREFIX:
    case SQL_DESC_LITERAL_SUFFIX:
    case SQL_DESC_LOCAL_TYPE_NAME:
        /* Return empty string */
        if (CharacterAttribute && BufferLength > 0)
            ((SQLCHAR *)CharacterAttribute)[0] = '\0';
        if (StringLength) *StringLength = 0;
        return SQL_SUCCESS;

    case SQL_DESC_COUNT:
        if (NumericAttribute) *NumericAttribute = stmt->num_cols;
        return SQL_SUCCESS;

    case SQL_DESC_NUM_PREC_RADIX:
        if (NumericAttribute) {
            switch (col->sql_type) {
            case SQL_INTEGER:
            case SQL_BIGINT:
            case SQL_SMALLINT:
            case SQL_TINYINT:
                *NumericAttribute = 10;
                break;
            case SQL_FLOAT:
            case SQL_DOUBLE:
            case SQL_REAL:
                *NumericAttribute = 2;
                break;
            default:
                *NumericAttribute = 0;
                break;
            }
        }
        return SQL_SUCCESS;

    default:
        if (NumericAttribute) *NumericAttribute = 0;
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLCloseCursor ────────────────────────────────── */

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    if (!stmt->executed) {
        SQLRETURN ret = argus_set_error(&stmt->diag, "24000",
                                        "[Argus] Invalid cursor state", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return ret;
    }

    argus_stmt_close_cursor(stmt);
    ARGUS_STMT_UNLOCK(stmt);
    return SQL_SUCCESS;
}

/* ── ODBC 2.x: SQLColAttributes (without 'e') ────────────────── */

SQLRETURN SQL_API SQLColAttributes(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER   CharacterAttribute,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN      *NumericAttribute)
{
    /*
     * ODBC 2.x SQLColAttributes maps to ODBC 3.x SQLColAttribute.
     * Field identifiers are the same for the fields we support.
     */
    return SQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier,
                           CharacterAttribute, BufferLength, StringLength,
                           NumericAttribute);
}

/* ── ODBC 2.x: SQLExtendedFetch ──────────────────────────────── */

SQLRETURN SQL_API SQLExtendedFetch(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT FetchOrientation,
    SQLLEN       FetchOffset,
    SQLULEN     *RowCountPtr,
    SQLUSMALLINT *RowStatusArray)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    /* Temporarily set rows_fetched_ptr and row_status_ptr for the call */
    SQLULEN *saved_rfp = stmt->rows_fetched_ptr;
    SQLUSMALLINT *saved_rsp = stmt->row_status_ptr;
    SQLULEN rows_fetched = 0;

    stmt->rows_fetched_ptr = &rows_fetched;
    stmt->row_status_ptr = RowStatusArray;

    SQLRETURN ret = SQLFetchScroll(StatementHandle,
                                    (SQLSMALLINT)FetchOrientation,
                                    FetchOffset);

    stmt->rows_fetched_ptr = saved_rfp;
    stmt->row_status_ptr = saved_rsp;

    if (RowCountPtr) *RowCountPtr = rows_fetched;

    return ret;
}

/* ── SQLSetPos (stub / minimal positioning) ──────────────────── */

SQLRETURN SQL_API SQLSetPos(
    SQLHSTMT     StatementHandle,
    SQLSETPOSIROW RowNumber,
    SQLUSMALLINT Operation,
    SQLUSMALLINT LockType)
{
    (void)LockType;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    if (Operation == SQL_POSITION) {
        /* Position the cursor on a row within the current rowset. RowNumber is
         * 1-based within the rowset; 0 leaves the position unchanged. */
        if (stmt->scroll_cached && RowNumber > 0) {
            size_t target = stmt->scroll_rowset_start + (size_t)(RowNumber - 1);
            if (target >= stmt->scroll_position ||
                target >= stmt->scroll_row_count) {
                return argus_set_error(&stmt->diag, "HY107",
                                       "[Argus] Row value out of range", 0);
            }
            /* Point GetData at the chosen row. */
            stmt->row_cache.current_row = target;
            return SQL_SUCCESS;
        }
        return SQL_SUCCESS;
    }

    if (Operation == SQL_REFRESH) {
        /* Re-deliver the current rowset (or a single row of it) into the bound
         * application buffers from the static scroll cache. */
        if (!stmt->scroll_cached) {
            return argus_set_error(&stmt->diag, "HYC00",
                "[Argus] SQLSetPos SQL_REFRESH requires a static cursor", 0);
        }

        size_t start = stmt->scroll_rowset_start;
        size_t end   = stmt->scroll_position;   /* exclusive */
        if (end > stmt->scroll_row_count) end = stmt->scroll_row_count;

        /* SQL_REFRESH must not advance the fetch counters. */
        unsigned long saved_total = stmt->rows_fetched_total;
        SQLRETURN final_ret = SQL_SUCCESS;

        if (RowNumber > 0) {
            size_t abs_row = start + (size_t)(RowNumber - 1);
            if (abs_row >= end) {
                return argus_set_error(&stmt->diag, "HY107",
                                       "[Argus] Row value out of range", 0);
            }
            final_ret = deliver_scroll_row(stmt, abs_row,
                                           (SQLULEN)(RowNumber - 1));
        } else {
            SQLULEN slot = 0;
            for (size_t r = start; r < end; r++, slot++) {
                SQLRETURN ret = deliver_scroll_row(stmt, r, slot);
                if (ret == SQL_ERROR) { final_ret = SQL_ERROR; break; }
                if (ret == SQL_SUCCESS_WITH_INFO)
                    final_ret = SQL_SUCCESS_WITH_INFO;
            }
        }

        stmt->rows_fetched_total = saved_total;
        return final_ret;
    }

    return argus_set_error(&stmt->diag, "HYC00",
                           "[Argus] SQLSetPos operation not supported", 0);
}

/* ── SQLBulkOperations ───────────────────────────────────────────
 * Bulk/positioned writes (SQL_ADD, *_BY_BOOKMARK) require a positioned-update
 * context (target table + row bookmarks) that a query-oriented driver over
 * read-mostly analytic engines does not maintain. Writes are issued as direct
 * INSERT/UPDATE/DELETE statements instead. We validate the operation code and
 * return the conformant "optional feature not implemented" (HYC00). */
SQLRETURN SQL_API SQLBulkOperations(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT Operation)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;
    argus_diag_clear(&stmt->diag);

    const char *what;
    switch (Operation) {
    case SQL_ADD:                what = "SQL_ADD"; break;
    case SQL_UPDATE_BY_BOOKMARK: what = "SQL_UPDATE_BY_BOOKMARK"; break;
    case SQL_DELETE_BY_BOOKMARK: what = "SQL_DELETE_BY_BOOKMARK"; break;
    case SQL_FETCH_BY_BOOKMARK:  what = "SQL_FETCH_BY_BOOKMARK"; break;
    default:
        return argus_set_error(&stmt->diag, "HY092",
                               "[Argus] Invalid SQLBulkOperations operation", 0);
    }

    char msg[256];
    snprintf(msg, sizeof(msg),
             "[Argus] SQLBulkOperations(%s) not supported; issue a direct "
             "INSERT/UPDATE/DELETE statement instead", what);
    return argus_set_error(&stmt->diag, "HYC00", msg, 0);
}

/* ── ODBC 2.x: SQLSetScrollOptions (stub) ────────────────────── */

/*
 * The ODBC 2.x way of asking for a scrollable cursor, which older tools
 * (and the driver manager, on their behalf) still use. It was refused with
 * HYC00 even for the one combination the driver does support — a read-only
 * static cursor — so those tools fell back to forward-only for no reason.
 * It is expressed in terms of the 3.x attributes, as the specification has
 * it: KeysetSize selects the cursor type, RowsetSize is the rowset size.
 */
SQLRETURN SQL_API SQLSetScrollOptions(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT Concurrency,
    SQLLEN       KeysetSize,
    SQLUSMALLINT RowsetSize)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;
    argus_diag_clear(&stmt->diag);

    /* Only a read-only cursor exists here: the driver never writes back. */
    if (Concurrency != SQL_CONCUR_READ_ONLY)
        return argus_set_error(&stmt->diag, "HYC00",
                               "[Argus] only SQL_CONCUR_READ_ONLY is supported",
                               0);

    SQLULEN cursor;
    switch (KeysetSize) {
    case SQL_SCROLL_FORWARD_ONLY: cursor = SQL_CURSOR_FORWARD_ONLY; break;
    case SQL_SCROLL_STATIC:       cursor = SQL_CURSOR_STATIC;       break;
    default:
        /* SQL_SCROLL_KEYSET_DRIVEN, SQL_SCROLL_DYNAMIC and an explicit
         * keyset size all need a cursor this driver does not have. */
        return argus_set_error(&stmt->diag, "HYC00",
                               "[Argus] only forward-only and static cursors "
                               "are supported", 0);
    }

    if (RowsetSize == 0)
        return argus_set_error(&stmt->diag, "HY107",
                               "[Argus] row value out of range", 0);

    stmt->cursor_type = cursor;
    stmt->row_array_size = RowsetSize;
    return SQL_SUCCESS;
}

