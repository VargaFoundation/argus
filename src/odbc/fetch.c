#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include <glib.h>

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);
extern int argus_hex_decode(const char *hex, size_t hex_len,
                             unsigned char *out, size_t out_capacity);

/* ── Row cache implementation ─────────────────────────────────── */

void argus_row_cache_init(argus_row_cache_t *cache)
{
    memset(cache, 0, sizeof(*cache));
}

static void free_row(argus_row_t *row, int num_cols)
{
    if (!row->cells) return;
    for (int i = 0; i < num_cols; i++) {
        free(row->cells[i].data);
    }
    free(row->cells);
    row->cells = NULL;
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

void argus_row_cache_clear(argus_row_cache_t *cache)
{
    if (cache->rows) {
        for (size_t i = 0; i < cache->num_rows; i++) {
            free_row(&cache->rows[i], cache->num_cols);
        }
    }
    cache->num_rows    = 0;
    cache->current_row = 0;
    /* Keep allocated capacity, num_cols, and exhausted flag */
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

    int num_cols = 0;
    int rc = dbc->backend->fetch_results(
        dbc->backend_conn, stmt->op,
        batch_size,
        &stmt->row_cache,
        stmt->columns, &num_cols);

    if (rc != 0) {
        if (stmt->diag.count == 0) {
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Failed to fetch results", 0);
        }
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

static SQLRETURN convert_cell_to_target(
    const argus_cell_t *cell,
    SQLSMALLINT target_type,
    SQLPOINTER target_value,
    SQLLEN buffer_length,
    SQLLEN *str_len_or_ind,
    argus_diag_t *diag)
{
    if (cell->is_null) {
        if (str_len_or_ind)
            *str_len_or_ind = SQL_NULL_DATA;
        return SQL_SUCCESS;
    }

    switch (target_type) {
    case SQL_C_CHAR:
    case SQL_C_DEFAULT: {
        size_t data_len = cell->data_len;
        if (str_len_or_ind)
            *str_len_or_ind = (SQLLEN)data_len;

        if (target_value && buffer_length > 0) {
            size_t copy = data_len < (size_t)(buffer_length - 1)
                          ? data_len : (size_t)(buffer_length - 1);
            memcpy(target_value, cell->data, copy);
            ((char *)target_value)[copy] = '\0';

            if (data_len >= (size_t)buffer_length) {
                /* Data truncated */
                argus_diag_push(diag, "01004",
                                "[Argus] String data, right truncated", 0);
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        return SQL_SUCCESS;
    }

    case SQL_C_SLONG:
    case SQL_C_LONG: {
        errno = 0;
        long val = strtol(cell->data, NULL, 10);
        if (errno) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLINTEGER *)target_value = (SQLINTEGER)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLINTEGER);
        return SQL_SUCCESS;
    }

    case SQL_C_SSHORT:
    case SQL_C_SHORT: {
        errno = 0;
        long val = strtol(cell->data, NULL, 10);
        if (errno || val < -32768 || val > 32767) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLSMALLINT *)target_value = (SQLSMALLINT)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    }

    case SQL_C_STINYINT:
    case SQL_C_TINYINT: {
        errno = 0;
        long val = strtol(cell->data, NULL, 10);
        if (errno || val < -128 || val > 127) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLSCHAR *)target_value = (SQLSCHAR)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLSCHAR);
        return SQL_SUCCESS;
    }

    case SQL_C_SBIGINT: {
        errno = 0;
        long long val = strtoll(cell->data, NULL, 10);
        if (errno) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLBIGINT *)target_value = (SQLBIGINT)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLBIGINT);
        return SQL_SUCCESS;
    }

    case SQL_C_FLOAT: {
        errno = 0;
        float val = strtof(cell->data, NULL);
        if (errno) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLREAL *)target_value = val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLREAL);
        return SQL_SUCCESS;
    }

    case SQL_C_DOUBLE: {
        errno = 0;
        double val = strtod(cell->data, NULL);
        if (errno) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLDOUBLE *)target_value = val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLDOUBLE);
        return SQL_SUCCESS;
    }

    case SQL_C_BIT: {
        unsigned char bit_val;
        if (g_ascii_strcasecmp(cell->data, "true") == 0) {
            bit_val = 1;
        } else if (g_ascii_strcasecmp(cell->data, "false") == 0) {
            bit_val = 0;
        } else {
            bit_val = strtol(cell->data, NULL, 10) ? 1 : 0;
        }
        if (target_value)
            *(unsigned char *)target_value = bit_val;
        if (str_len_or_ind)
            *str_len_or_ind = 1;
        return SQL_SUCCESS;
    }

    case SQL_C_WCHAR: {
        /* UTF-8 to UTF-16 conversion using GLib */
        GError *err = NULL;
        glong items_written = 0;
        gunichar2 *utf16 = g_utf8_to_utf16(
            cell->data, (glong)cell->data_len,
            NULL, &items_written, &err);

        if (!utf16) {
            if (err) {
                argus_set_error(diag, "22018",
                                "[Argus] Invalid UTF-8 data", 0);
                g_error_free(err);
            }
            return SQL_ERROR;
        }

        /* str_len_or_ind = total byte count of UTF-16 data (excl NUL) */
        size_t utf16_bytes = (size_t)items_written * sizeof(SQLWCHAR);
        if (str_len_or_ind)
            *str_len_or_ind = (SQLLEN)utf16_bytes;

        if (target_value && buffer_length > 0 &&
            buffer_length < (SQLLEN)sizeof(SQLWCHAR)) {
            /* Buffer too small for even one character + NUL */
            g_free(utf16);
            argus_diag_push(diag, "01004",
                            "[Argus] String data, right truncated", 0);
            return SQL_SUCCESS_WITH_INFO;
        }

        if (target_value && buffer_length >= (SQLLEN)sizeof(SQLWCHAR)) {
            size_t max_chars = (size_t)(buffer_length / (SQLLEN)sizeof(SQLWCHAR)) - 1;
            size_t copy_chars = (size_t)items_written < max_chars
                                ? (size_t)items_written : max_chars;
            SQLWCHAR *dst = (SQLWCHAR *)target_value;
            memcpy(dst, utf16, copy_chars * sizeof(SQLWCHAR));
            dst[copy_chars] = 0;

            if ((size_t)items_written > max_chars) {
                g_free(utf16);
                argus_diag_push(diag, "01004",
                                "[Argus] String data, right truncated", 0);
                return SQL_SUCCESS_WITH_INFO;
            }
        }

        g_free(utf16);
        return SQL_SUCCESS;
    }

    /* Unsigned integer types */
    case SQL_C_ULONG: {
        errno = 0;
        unsigned long val = strtoul(cell->data, NULL, 10);
        if (errno) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLUINTEGER *)target_value = (SQLUINTEGER)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;
    }

    case SQL_C_USHORT: {
        errno = 0;
        unsigned long val = strtoul(cell->data, NULL, 10);
        if (errno || val > 65535) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLUSMALLINT *)target_value = (SQLUSMALLINT)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLUSMALLINT);
        return SQL_SUCCESS;
    }

    case SQL_C_UTINYINT: {
        errno = 0;
        unsigned long val = strtoul(cell->data, NULL, 10);
        if (errno || val > 255) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLCHAR *)target_value = (SQLCHAR)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLCHAR);
        return SQL_SUCCESS;
    }

    case SQL_C_UBIGINT: {
        errno = 0;
        unsigned long long val = strtoull(cell->data, NULL, 10);
        if (errno) {
            return argus_set_error(diag, "22003",
                                   "[Argus] Numeric value out of range", 0);
        }
        if (target_value)
            *(SQLUBIGINT *)target_value = (SQLUBIGINT)val;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQLUBIGINT);
        return SQL_SUCCESS;
    }

    /* Date/Time types */
    case SQL_C_TYPE_DATE: {
        /* Parse "YYYY-MM-DD" */
        SQL_DATE_STRUCT date;
        if (sscanf(cell->data, "%4hd-%2hu-%2hu",
                   &date.year, &date.month, &date.day) != 3) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Invalid date format", 0);
        }
        if (date.month < 1 || date.month > 12 ||
            date.day < 1 || date.day > 31) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Date value out of range", 0);
        }
        if (target_value)
            *(SQL_DATE_STRUCT *)target_value = date;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_DATE_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_TYPE_TIME: {
        /* Parse "HH:MM:SS" */
        SQL_TIME_STRUCT time;
        if (sscanf(cell->data, "%2hu:%2hu:%2hu",
                   &time.hour, &time.minute, &time.second) != 3) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Invalid time format", 0);
        }
        if (time.hour > 23 || time.minute > 59 || time.second > 59) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Time value out of range", 0);
        }
        if (target_value)
            *(SQL_TIME_STRUCT *)target_value = time;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_TIME_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_TYPE_TIMESTAMP: {
        /* Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.fff...fffffffff" */
        SQL_TIMESTAMP_STRUCT ts;
        memset(&ts, 0, sizeof(ts));
        int n = sscanf(cell->data, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu",
                       &ts.year, &ts.month, &ts.day,
                       &ts.hour, &ts.minute, &ts.second);
        if (n < 6) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Invalid timestamp format", 0);
        }
        /* Parse fractional seconds and normalize to nanoseconds.
         * ODBC SQL_TIMESTAMP_STRUCT.fraction is in nanoseconds (0-999999999).
         * We must count the digits to scale correctly:
         *   ".1"   -> 100000000 ns
         *   ".12"  -> 120000000 ns
         *   ".123" -> 123000000 ns
         *   ".123456789" -> 123456789 ns */
        const char *dot = strchr(cell->data, '.');
        if (dot) {
            dot++;
            SQLUINTEGER frac = 0;
            int digits = 0;
            while (*dot >= '0' && *dot <= '9' && digits < 9) {
                frac = frac * 10 + (SQLUINTEGER)(*dot - '0');
                dot++;
                digits++;
            }
            /* Pad to 9 digits (nanoseconds) */
            for (int pad = digits; pad < 9; pad++)
                frac *= 10;
            ts.fraction = frac;
        }
        if (ts.month < 1 || ts.month > 12 ||
            ts.day < 1 || ts.day > 31 ||
            ts.hour > 23 || ts.minute > 59 || ts.second > 59) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Timestamp value out of range", 0);
        }
        if (target_value)
            *(SQL_TIMESTAMP_STRUCT *)target_value = ts;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_TIMESTAMP_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_NUMERIC: {
        /* Parse decimal string to SQL_NUMERIC_STRUCT */
        SQL_NUMERIC_STRUCT num;
        memset(&num, 0, sizeof(num));

        /* Parse sign and skip whitespace */
        const char *p = cell->data;
        while (*p == ' ') p++;
        num.sign = (*p == '-') ? 0 : 1;
        if (*p == '-' || *p == '+') p++;

        /* Parse digits and build 128-bit little-endian value.
         * Use two 64-bit halves for MSVC portability (__uint128_t
         * is a GCC/Clang extension not available on Windows). */
        unsigned long long lo = 0, hi = 0;
        int scale = 0;
        int total_digits = 0;
        bool past_decimal = false;
        while (*p) {
            if (*p == '.') {
                past_decimal = true;
            } else if (*p >= '0' && *p <= '9') {
                unsigned digit = (unsigned)(*p - '0');
                /* Multiply (hi:lo) by 10 and add digit */
                unsigned long long lo_x10, carry;
#if defined(__GNUC__) || defined(__clang__)
                {
                    unsigned __int128 full = (unsigned __int128)lo * 10;
                    lo_x10 = (unsigned long long)full;
                    carry   = (unsigned long long)(full >> 64);
                }
#else
                /* Manual 64×64→128 via 32-bit halves (MSVC path) */
                {
                    unsigned long long a_lo = lo & 0xFFFFFFFFULL;
                    unsigned long long a_hi = lo >> 32;
                    unsigned long long r0 = a_lo * 10;
                    unsigned long long r1 = a_hi * 10 + (r0 >> 32);
                    lo_x10 = (r0 & 0xFFFFFFFFULL) | ((r1 & 0xFFFFFFFFULL) << 32);
                    carry  = r1 >> 32;
                }
#endif
                /* Check for 128-bit overflow: hi*10 + carry must fit */
                if (hi > (0xFFFFFFFFFFFFFFFFULL - carry) / 10) {
                    return argus_set_error(diag, "22003",
                                           "[Argus] Numeric value out of range", 0);
                }
                hi = hi * 10 + carry;
                lo = lo_x10 + digit;
                if (lo < lo_x10) hi++;  /* addition carry */
                total_digits++;
                if (past_decimal) scale++;
            }
            p++;
        }

        /* Store in little-endian format */
        num.precision = (SQLCHAR)(total_digits > 0 ? total_digits : 1);
        num.scale = (SQLSCHAR)scale;
        memcpy(num.val, &lo, 8);
        memcpy(num.val + 8, &hi, 8);

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
        int n = sscanf(cell->data,
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

    case SQL_C_BINARY: {
        /*
         * Backends return binary data as hex strings ("48656C6C6F").
         * Detect hex encoding and decode, otherwise copy raw bytes.
         */
        bool is_hex = (cell->data_len >= 2 && cell->data_len % 2 == 0);
        if (is_hex) {
            for (size_t i = 0; i < cell->data_len; i++) {
                char c = cell->data[i];
                if (!((c >= '0' && c <= '9') ||
                      (c >= 'A' && c <= 'F') ||
                      (c >= 'a' && c <= 'f'))) {
                    is_hex = false;
                    break;
                }
            }
        }

        if (is_hex) {
            size_t decoded_len = cell->data_len / 2;
            if (str_len_or_ind)
                *str_len_or_ind = (SQLLEN)decoded_len;

            if (target_value && buffer_length > 0) {
                size_t copy = decoded_len < (size_t)buffer_length
                              ? decoded_len : (size_t)buffer_length;
                /* Decode only the bytes that fit */
                argus_hex_decode(cell->data, copy * 2,
                                 (unsigned char *)target_value, copy);

                if (decoded_len > (size_t)buffer_length) {
                    argus_diag_push(diag, "01004",
                                    "[Argus] Binary data truncated", 0);
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
        } else {
            /* Raw binary data — no hex encoding */
            if (str_len_or_ind)
                *str_len_or_ind = (SQLLEN)cell->data_len;

            if (target_value && buffer_length > 0) {
                size_t copy = cell->data_len < (size_t)buffer_length
                              ? cell->data_len : (size_t)buffer_length;
                memcpy(target_value, cell->data, copy);

                if (cell->data_len > (size_t)buffer_length) {
                    argus_diag_push(diag, "01004",
                                    "[Argus] Binary data truncated", 0);
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
        }
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

        const char *s = cell->data;
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
        /* Fall back to string conversion */
        if (str_len_or_ind)
            *str_len_or_ind = (SQLLEN)cell->data_len;
        if (target_value && buffer_length > 0) {
            size_t copy = cell->data_len < (size_t)(buffer_length - 1)
                          ? cell->data_len : (size_t)(buffer_length - 1);
            memcpy(target_value, cell->data, copy);
            ((char *)target_value)[copy] = '\0';
        }
        return SQL_SUCCESS;
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

    while (1) {
        argus_row_cache_clear(&stmt->row_cache);
        int num_cols = 0;
        int rc = dbc->backend->fetch_results(
            dbc->backend_conn, stmt->op,
            batch_size,
            &stmt->row_cache,
            stmt->columns, &num_cols);

        if (rc != 0) {
            /* Free partially built cache */
            for (size_t i = 0; i < total; i++) {
                if (all_rows[i].cells) {
                    for (int c = 0; c < stmt->num_cols; c++)
                        free(all_rows[i].cells[c].data);
                    free(all_rows[i].cells);
                }
            }
            free(all_rows);
            if (stmt->diag.count == 0)
                argus_set_error(&stmt->diag, "HY000",
                                "[Argus] Failed to fetch results", 0);
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

        /* Copy rows into scroll cache */
        if (total + stmt->row_cache.num_rows > capacity) {
            while (total + stmt->row_cache.num_rows > capacity)
                capacity *= 2;
            argus_row_t *new_rows = realloc(all_rows,
                                             capacity * sizeof(argus_row_t));
            if (!new_rows) {
                for (size_t i = 0; i < total; i++) {
                    if (all_rows[i].cells) {
                        for (int c = 0; c < stmt->num_cols; c++)
                            free(all_rows[i].cells[c].data);
                        free(all_rows[i].cells);
                    }
                }
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

        SQLPOINTER target = bind->target_value;
        SQLLEN *ind_ptr = bind->str_len_or_ind;
        if (rowset_idx > 0 && target) {
            target = (char *)target + rowset_idx * bind->buffer_length;
            if (ind_ptr)
                ind_ptr = (SQLLEN *)((char *)ind_ptr +
                           rowset_idx * sizeof(SQLLEN));
        }

        SQLRETURN ret = convert_cell_to_target(
            cell, bind->target_type,
            target, bind->buffer_length,
            ind_ptr, &stmt->diag);

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
    /* Check SQL_ATTR_MAX_ROWS limit */
    if (stmt->max_rows > 0 && stmt->rows_fetched_total >= stmt->max_rows) {
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

        /*
         * For block cursors (row_array_size > 1), offset the target
         * pointer by rowset_idx * buffer_length for column-wise binding.
         */
        SQLPOINTER target = bind->target_value;
        SQLLEN *ind_ptr = bind->str_len_or_ind;
        if (rowset_idx > 0 && target) {
            target = (char *)target + rowset_idx * bind->buffer_length;
            if (ind_ptr)
                ind_ptr = (SQLLEN *)((char *)ind_ptr +
                           rowset_idx * sizeof(SQLLEN));
        }

        SQLRETURN ret = convert_cell_to_target(
            cell, bind->target_type,
            target, bind->buffer_length,
            ind_ptr, &stmt->diag);

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
    stmt->getdata_col = 0;
    stmt->getdata_offset = 0;

    if (!stmt->executed) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY010",
                               "[Argus] Function sequence error: not executed",
                               0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    SQLULEN array_size = stmt->row_array_size > 0 ? stmt->row_array_size : 1;
    SQLULEN rows_fetched = 0;
    SQLRETURN final_ret = SQL_SUCCESS;

    for (SQLULEN i = 0; i < array_size; i++) {
        SQLRETURN ret = fetch_single_row(stmt, i);

        if (ret == SQL_NO_DATA) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_NOROW;
            break;
        }

        if (ret == SQL_ERROR) {
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[i] = SQL_ROW_ERROR;
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
    }

    /* Fill remaining status slots with SQL_ROW_NOROW */
    if (stmt->row_status_ptr) {
        for (SQLULEN i = rows_fetched; i < array_size; i++)
            stmt->row_status_ptr[i] = SQL_ROW_NOROW;
    }

    /* Update rows fetched pointer */
    if (stmt->rows_fetched_ptr)
        *(stmt->rows_fetched_ptr) = rows_fetched;

    ARGUS_STMT_UNLOCK(stmt);

    if (rows_fetched == 0)
        return SQL_NO_DATA;

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
    stmt->getdata_col = 0;
    stmt->getdata_offset = 0;

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

    /* If column changed, reset offset */
    if (stmt->getdata_col != ColumnNumber) {
        stmt->getdata_col = ColumnNumber;
        stmt->getdata_offset = 0;
    }

    /* Multi-call support for character/binary data */
    if (cell->is_null) {
        if (StrLen_or_Ind)
            *StrLen_or_Ind = SQL_NULL_DATA;
        stmt->getdata_offset = 0;
        ARGUS_STMT_UNLOCK(stmt);
        return SQL_SUCCESS;
    }

    if ((TargetType == SQL_C_CHAR || TargetType == SQL_C_DEFAULT ||
         TargetType == SQL_C_BINARY || TargetType == SQL_C_WCHAR) &&
        stmt->getdata_offset > 0) {
        /* Continuation call — return remaining data from offset */
        size_t data_len = cell->data_len;
        size_t remaining = (stmt->getdata_offset < data_len)
                           ? data_len - stmt->getdata_offset : 0;

        if (remaining == 0) {
            if (StrLen_or_Ind) *StrLen_or_Ind = 0;
            ARGUS_STMT_UNLOCK(stmt);
            return SQL_NO_DATA;
        }

        if (TargetType == SQL_C_BINARY) {
            if (StrLen_or_Ind)
                *StrLen_or_Ind = (SQLLEN)remaining;
            if (TargetValue && BufferLength > 0) {
                size_t copy = remaining < (size_t)BufferLength
                              ? remaining : (size_t)BufferLength;
                memcpy(TargetValue,
                       cell->data + stmt->getdata_offset, copy);
                stmt->getdata_offset += copy;
                if (remaining > (size_t)BufferLength) {
                    argus_diag_push(&stmt->diag, "01004",
                                    "[Argus] Binary data truncated", 0);
                    ARGUS_STMT_UNLOCK(stmt);
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            ARGUS_STMT_UNLOCK(stmt);
            return SQL_SUCCESS;
        }

        /* SQL_C_CHAR / SQL_C_DEFAULT continuation */
        if (StrLen_or_Ind)
            *StrLen_or_Ind = (SQLLEN)remaining;
        if (TargetValue && BufferLength > 0) {
            size_t copy = remaining < (size_t)(BufferLength - 1)
                          ? remaining : (size_t)(BufferLength - 1);
            memcpy(TargetValue,
                   cell->data + stmt->getdata_offset, copy);
            ((char *)TargetValue)[copy] = '\0';
            stmt->getdata_offset += copy;
            if (remaining >= (size_t)BufferLength) {
                argus_diag_push(&stmt->diag, "01004",
                                "[Argus] String data, right truncated", 0);
                ARGUS_STMT_UNLOCK(stmt);
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        ARGUS_STMT_UNLOCK(stmt);
        return SQL_SUCCESS;
    }

    /* First call — use standard conversion */
    SQLRETURN ret = convert_cell_to_target(cell, TargetType, TargetValue,
                                            BufferLength, StrLen_or_Ind,
                                            &stmt->diag);

    /* Track offset for multi-call if data was truncated */
    if (ret == SQL_SUCCESS_WITH_INFO &&
        (TargetType == SQL_C_CHAR || TargetType == SQL_C_DEFAULT ||
         TargetType == SQL_C_BINARY || TargetType == SQL_C_WCHAR)) {
        if (BufferLength > 1)
            stmt->getdata_offset = (size_t)(BufferLength - 1);
        else if (BufferLength > 0)
            stmt->getdata_offset = (size_t)BufferLength;
    }

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

    if (!stmt->executed) {
        return argus_set_error(&stmt->diag, "24000",
                               "[Argus] Invalid cursor state", 0);
    }

    argus_stmt_reset(stmt);
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
        /* Position the cursor within the scroll cache */
        if (stmt->scroll_cached && RowNumber > 0) {
            size_t target = (size_t)(RowNumber - 1);
            if (target < stmt->scroll_row_count) {
                stmt->scroll_position = target + 1;
                return SQL_SUCCESS;
            }
            return argus_set_error(&stmt->diag, "HY109",
                                   "[Argus] Invalid cursor position", 0);
        }
        return SQL_SUCCESS;
    }

    return argus_set_error(&stmt->diag, "HYC00",
                           "[Argus] SQLSetPos operation not supported", 0);
}

/* ── SQLBulkOperations (stub) ────────────────────────────────── */

SQLRETURN SQL_API SQLBulkOperations(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT Operation)
{
    (void)Operation;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    return argus_set_error(&stmt->diag, "HYC00",
                           "[Argus] SQLBulkOperations not supported", 0);
}

/* ── ODBC 2.x: SQLSetScrollOptions (stub) ────────────────────── */

SQLRETURN SQL_API SQLSetScrollOptions(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT Concurrency,
    SQLLEN       KeysetSize,
    SQLUSMALLINT RowsetSize)
{
    (void)Concurrency;
    (void)KeysetSize;
    (void)RowsetSize;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    return argus_set_error(&stmt->diag, "HYC00",
                           "[Argus] SQLSetScrollOptions not supported", 0);
}

