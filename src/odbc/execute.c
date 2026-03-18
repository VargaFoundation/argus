#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <float.h>
#include <glib.h>

extern char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len);

/* ── Internal: count parameter markers in a SQL string ────────── */

static int count_param_markers(const char *sql)
{
    int count = 0;
    bool in_single_quote = false;
    bool in_double_quote = false;

    for (const char *p = sql; *p; p++) {
        if (*p == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
        } else if (*p == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
        } else if (*p == '?' && !in_single_quote && !in_double_quote) {
            count++;
        }
    }
    return count;
}

/* ── Internal: escape a string for SQL (single-quote escaping) ── */

static char *sql_escape_string(const char *value, size_t len)
{
    /* Reject embedded NUL bytes to prevent SQL injection via truncation */
    for (size_t i = 0; i < len; i++) {
        if (value[i] == '\0') return NULL;
    }

    /* Count characters that need escaping to determine output size */
    size_t num_special = 0;
    for (size_t i = 0; i < len; i++) {
        if (value[i] == '\'' || value[i] == '\\') num_special++;
    }

    /* Output: quote + escaped_data + quote + NUL */
    size_t out_len = 1 + len + num_special + 1 + 1;
    char *out = malloc(out_len);
    if (!out) return NULL;

    char *dst = out;
    *dst++ = '\'';
    for (size_t i = 0; i < len; i++) {
        if (value[i] == '\'') {
            *dst++ = '\'';
            *dst++ = '\'';
        } else if (value[i] == '\\') {
            *dst++ = '\\';
            *dst++ = '\\';
        } else {
            *dst++ = value[i];
        }
    }
    *dst++ = '\'';
    *dst = '\0';
    return out;
}

/* ── Internal: render a bound parameter as a SQL literal ──────── */

static char *render_param(const argus_param_binding_t *param)
{
    if (!param->bound) return NULL;

    /* Check for SQL_NULL_DATA */
    if (param->str_len_or_ind &&
        *param->str_len_or_ind == SQL_NULL_DATA) {
        return strdup("NULL");
    }

    switch (param->value_type) {
    case SQL_C_CHAR:
    case SQL_C_DEFAULT: {
        const char *str = (const char *)param->value;
        size_t len;
        if (param->str_len_or_ind && *param->str_len_or_ind >= 0)
            len = (size_t)*param->str_len_or_ind;
        else
            len = strlen(str);
        return sql_escape_string(str, len); /* NULL if embedded NUL byte */
    }

    case SQL_C_WCHAR: {
        /* UTF-16 input - convert to UTF-8 using GLib, then escape */
        const gunichar2 *wstr = (const gunichar2 *)param->value;
        glong wlen;
        if (param->str_len_or_ind && *param->str_len_or_ind >= 0)
            wlen = (glong)(*param->str_len_or_ind / (SQLLEN)sizeof(SQLWCHAR));
        else {
            wlen = 0;
            while (wstr[wlen]) wlen++;
        }
        GError *err = NULL;
        glong bytes_written = 0;
        gchar *utf8 = g_utf16_to_utf8(wstr, wlen, NULL, &bytes_written, &err);
        if (!utf8) {
            if (err) g_error_free(err);
            return strdup("NULL");
        }
        char *escaped = sql_escape_string(utf8, (size_t)bytes_written);
        g_free(utf8);
        return escaped;
    }

    case SQL_C_SLONG:
    case SQL_C_LONG: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", *(const SQLINTEGER *)param->value);
        return strdup(buf);
    }

    case SQL_C_SSHORT:
    case SQL_C_SHORT: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", (int)*(const SQLSMALLINT *)param->value);
        return strdup(buf);
    }

    case SQL_C_STINYINT:
    case SQL_C_TINYINT: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", (int)*(const SQLSCHAR *)param->value);
        return strdup(buf);
    }

    case SQL_C_SBIGINT: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%lld", (long long)*(const SQLBIGINT *)param->value);
        return strdup(buf);
    }

    case SQL_C_ULONG: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%u", *(const SQLUINTEGER *)param->value);
        return strdup(buf);
    }

    case SQL_C_USHORT: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%u", (unsigned)*(const SQLUSMALLINT *)param->value);
        return strdup(buf);
    }

    case SQL_C_UTINYINT: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%u", (unsigned)*(const SQLCHAR *)param->value);
        return strdup(buf);
    }

    case SQL_C_UBIGINT: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%llu", (unsigned long long)*(const SQLUBIGINT *)param->value);
        return strdup(buf);
    }

    case SQL_C_FLOAT: {
        char buf[64];
        snprintf(buf, sizeof(buf), "%.7g", (double)*(const SQLREAL *)param->value);
        return strdup(buf);
    }

    case SQL_C_DOUBLE: {
        char buf[64];
        snprintf(buf, sizeof(buf), "%.15g", *(const SQLDOUBLE *)param->value);
        return strdup(buf);
    }

    case SQL_C_BIT: {
        return strdup(*(const unsigned char *)param->value ? "1" : "0");
    }

    case SQL_C_TYPE_DATE: {
        const SQL_DATE_STRUCT *d = (const SQL_DATE_STRUCT *)param->value;
        char buf[32];
        snprintf(buf, sizeof(buf), "DATE '%04d-%02u-%02u'",
                 d->year, d->month, d->day);
        return strdup(buf);
    }

    case SQL_C_TYPE_TIME: {
        const SQL_TIME_STRUCT *t = (const SQL_TIME_STRUCT *)param->value;
        char buf[32];
        snprintf(buf, sizeof(buf), "TIME '%02u:%02u:%02u'",
                 t->hour, t->minute, t->second);
        return strdup(buf);
    }

    case SQL_C_TYPE_TIMESTAMP: {
        const SQL_TIMESTAMP_STRUCT *ts =
            (const SQL_TIMESTAMP_STRUCT *)param->value;
        char buf[64];
        if (ts->fraction > 0) {
            snprintf(buf, sizeof(buf),
                     "TIMESTAMP '%04d-%02u-%02u %02u:%02u:%02u.%u'",
                     ts->year, ts->month, ts->day,
                     ts->hour, ts->minute, ts->second, ts->fraction);
        } else {
            snprintf(buf, sizeof(buf),
                     "TIMESTAMP '%04d-%02u-%02u %02u:%02u:%02u'",
                     ts->year, ts->month, ts->day,
                     ts->hour, ts->minute, ts->second);
        }
        return strdup(buf);
    }

    case SQL_C_BINARY: {
        /* Render as hex literal: X'AABB...' */
        size_t len;
        if (param->str_len_or_ind && *param->str_len_or_ind >= 0)
            len = (size_t)*param->str_len_or_ind;
        else
            len = (size_t)param->buffer_length;
        char *out = malloc(2 + len * 2 + 1 + 1);
        if (!out) return NULL;
        char *dst = out;
        *dst++ = 'X';
        *dst++ = '\'';
        const unsigned char *src = (const unsigned char *)param->value;
        for (size_t i = 0; i < len; i++) {
            static const char hex[] = "0123456789ABCDEF";
            *dst++ = hex[src[i] >> 4];
            *dst++ = hex[src[i] & 0x0F];
        }
        *dst++ = '\'';
        *dst = '\0';
        return out;
    }

    case SQL_C_NUMERIC: {
        /* Convert SQL_NUMERIC_STRUCT 128-bit LE value to decimal string */
        const SQL_NUMERIC_STRUCT *ns =
            (const SQL_NUMERIC_STRUCT *)param->value;
        unsigned long long low = 0, high = 0;
        memcpy(&low, ns->val, 8);
        memcpy(&high, ns->val + 8, 8);

        /* Build digit string from 128-bit value */
        char digits[42];
        int dpos = 0;
        if (low == 0 && high == 0) {
            digits[dpos++] = '0';
        } else {
            /* Divide 128-bit value by 10 repeatedly */
            unsigned long long h = high, l = low;
            while (h > 0 || l > 0) {
                /* 128-bit division by 10 */
                unsigned long long rem = h % 10;
                h = h / 10;
                unsigned long long tmp = (rem << 32) | (l >> 32);
                unsigned long long q_hi32 = tmp / 10;
                rem = tmp % 10;
                tmp = (rem << 32) | (l & 0xFFFFFFFFULL);
                unsigned long long q_lo32 = tmp / 10;
                rem = tmp % 10;
                l = (q_hi32 << 32) | q_lo32;
                digits[dpos++] = (char)('0' + rem);
            }
        }
        /* digits[] is reversed; build output */
        char buf[64];
        char *dst = buf;
        if (!ns->sign) *dst++ = '-';
        int scale = (int)ns->scale;
        if (scale >= dpos) {
            *dst++ = '0';
            *dst++ = '.';
            for (int z = 0; z < scale - dpos; z++) *dst++ = '0';
            for (int i = dpos - 1; i >= 0; i--) *dst++ = digits[i];
        } else {
            for (int i = dpos - 1; i >= 0; i--) {
                if (i == scale - 1 && scale > 0) *dst++ = '.';
                *dst++ = digits[i];
            }
        }
        *dst = '\0';
        return strdup(buf);
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
    case SQL_C_INTERVAL_MINUTE_TO_SECOND:
        /* Interval types not supported — return NULL to propagate HYC00 */
        return NULL;

    default:
        /* Treat as string */
        if (param->value) {
            const char *str = (const char *)param->value;
            return sql_escape_string(str, strlen(str));
        }
        return strdup("NULL");
    }
}

/* ── Internal: substitute ? markers with bound parameter values ── */

static char *substitute_params(const char *sql,
                                const argus_param_binding_t *params,
                                int num_params,
                                argus_diag_t *diag)
{
    int marker_count = count_param_markers(sql);
    if (marker_count == 0) return strdup(sql);

    if (marker_count > num_params) {
        argus_set_error(diag, "07002",
                        "[Argus] COUNT field incorrect: "
                        "fewer parameters bound than markers", 0);
        return NULL;
    }

    /* Render all parameter values */
    char **rendered = calloc((size_t)marker_count, sizeof(char *));
    if (!rendered) return NULL;

    for (int i = 0; i < marker_count; i++) {
        if (!params[i].bound) {
            argus_set_error(diag, "07002",
                            "[Argus] Parameter not bound", 0);
            for (int j = 0; j < i; j++) free(rendered[j]);
            free(rendered);
            return NULL;
        }
        rendered[i] = render_param(&params[i]);
        if (!rendered[i]) {
            argus_set_error(diag, "HYC00",
                            "[Argus] Unsupported parameter type or "
                            "invalid parameter value", 0);
            for (int j = 0; j < i; j++) free(rendered[j]);
            free(rendered);
            return NULL;
        }
    }

    /* Calculate output size */
    size_t out_size = strlen(sql) + 1;
    for (int i = 0; i < marker_count; i++) {
        out_size += strlen(rendered[i]); /* -1 for ? + rendered len */
    }

    char *out = malloc(out_size);
    if (!out) {
        for (int i = 0; i < marker_count; i++) free(rendered[i]);
        free(rendered);
        return NULL;
    }

    /* Build output string */
    char *dst = out;
    int param_idx = 0;
    bool in_single_quote = false;
    bool in_double_quote = false;

    for (const char *p = sql; *p; p++) {
        if (*p == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
            *dst++ = *p;
        } else if (*p == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            *dst++ = *p;
        } else if (*p == '?' && !in_single_quote && !in_double_quote
                   && param_idx < marker_count) {
            size_t rlen = strlen(rendered[param_idx]);
            memcpy(dst, rendered[param_idx], rlen);
            dst += rlen;
            param_idx++;
        } else {
            *dst++ = *p;
        }
    }
    *dst = '\0';

    for (int i = 0; i < marker_count; i++) free(rendered[i]);
    free(rendered);
    return out;
}

/* ── Internal: execute a query on the backend ────────────────── */

static SQLRETURN do_execute(argus_stmt_t *stmt, const char *query)
{
    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->connected || !dbc->backend) {
        return argus_set_error(&stmt->diag, "08003",
                               "[Argus] Connection not open", 0);
    }

    /* Reset previous execution state */
    if (stmt->op) {
        dbc->backend->close_operation(dbc->backend_conn, stmt->op);
        stmt->op = NULL;
    }
    stmt->executed          = false;
    stmt->num_cols          = 0;
    stmt->metadata_fetched  = false;
    stmt->fetch_started     = false;
    stmt->row_count         = -1;
    stmt->rows_fetched_total = 0;
    argus_row_cache_clear(&stmt->row_cache);

    /* Log query (truncate if very long) */
    if (strlen(query) > 100) {
        ARGUS_LOG_DEBUG("Executing query: %.100s...", query);
    } else {
        ARGUS_LOG_DEBUG("Executing query: %s", query);
    }

    /* Propagate query timeout to backend if set */
    if (stmt->query_timeout > 0 && dbc->query_timeout_sec == 0)
        dbc->query_timeout_sec = (int)stmt->query_timeout;

    /* Execute via backend with timing */
    gint64 exec_start = g_get_monotonic_time();
    int rc = dbc->backend->execute(dbc->backend_conn, query, &stmt->op);
    gint64 exec_end = g_get_monotonic_time();
    stmt->execute_time_ms = (double)(exec_end - exec_start) / 1000.0;

    if (rc != 0) {
        ARGUS_LOG_ERROR("Query execution failed: rc=%d, query=%.100s (%.1f ms)",
                        rc, query, stmt->execute_time_ms);
        stmt->errors_total++;
        if (dbc) dbc->errors_total++;
        if (stmt->diag.count == 0) {
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Backend execution failed", 0);
        }
        return SQL_ERROR;
    }

    stmt->executed = true;
    ARGUS_LOG_DEBUG("Query executed successfully (%.1f ms)",
                    stmt->execute_time_ms);

    /* Try to get result metadata */
    if (dbc->backend->get_result_metadata) {
        int ncols = 0;
        rc = dbc->backend->get_result_metadata(
            dbc->backend_conn, stmt->op,
            stmt->columns, &ncols);
        if (rc == 0 && ncols > 0) {
            if (ncols > ARGUS_MAX_COLUMNS)
                ncols = ARGUS_MAX_COLUMNS;
            stmt->num_cols = ncols;
            stmt->metadata_fetched = true;
            ARGUS_LOG_TRACE("Retrieved metadata: %d columns", ncols);
        }
    }

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLExecDirect ─────────────────────────────────── */

SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *StatementText,
    SQLINTEGER TextLength)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    if (!StatementText) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY009",
                               "[Argus] NULL statement text", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    char *query = argus_str_dup(StatementText, TextLength);
    if (!query) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    /* Store the query */
    free(stmt->query);
    stmt->query = query;

    /* Substitute bound parameters if any */
    SQLRETURN ret;
    if (stmt->num_param_bindings > 0) {
        char *resolved = substitute_params(
            query, stmt->param_bindings,
            stmt->num_param_bindings, &stmt->diag);
        if (!resolved) {
            ARGUS_STMT_UNLOCK(stmt);
            return SQL_ERROR;
        }

        ret = do_execute(stmt, resolved);
        free(resolved);
    } else {
        ret = do_execute(stmt, query);
    }

    ARGUS_STMT_UNLOCK(stmt);
    return ret;
}

/* ── ODBC API: SQLPrepare ────────────────────────────────────── */

SQLRETURN SQL_API SQLPrepare(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *StatementText,
    SQLINTEGER TextLength)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    if (!StatementText) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY009",
                               "[Argus] NULL statement text", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    char *query = argus_str_dup(StatementText, TextLength);
    if (!query) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    free(stmt->query);
    stmt->query    = query;
    stmt->prepared = true;
    stmt->executed = false;

    ARGUS_STMT_UNLOCK(stmt);
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLExecute ────────────────────────────────────── */

SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    if (!stmt->query || !stmt->prepared) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HY010",
                               "[Argus] No prepared statement", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    /* Substitute bound parameters if any */
    SQLRETURN ret;
    if (stmt->num_param_bindings > 0) {
        char *resolved = substitute_params(
            stmt->query, stmt->param_bindings,
            stmt->num_param_bindings, &stmt->diag);
        if (!resolved) {
            ARGUS_STMT_UNLOCK(stmt);
            return SQL_ERROR;
        }

        ret = do_execute(stmt, resolved);
        free(resolved);
    } else {
        ret = do_execute(stmt, stmt->query);
    }

    ARGUS_STMT_UNLOCK(stmt);
    return ret;
}

/* ── ODBC API: SQLRowCount ───────────────────────────────────── */

SQLRETURN SQL_API SQLRowCount(
    SQLHSTMT StatementHandle,
    SQLLEN  *RowCount)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (RowCount)
        *RowCount = stmt->row_count;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLNativeSql ──────────────────────────────────── */

SQLRETURN SQL_API SQLNativeSql(
    SQLHDBC     ConnectionHandle,
    SQLCHAR    *InStatementText,
    SQLINTEGER  TextLength1,
    SQLCHAR    *OutStatementText,
    SQLINTEGER  BufferLength,
    SQLINTEGER *TextLength2Ptr)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    /* Pass-through: Hive SQL is the native form */
    size_t src_len;
    if (TextLength1 == SQL_NTS)
        src_len = InStatementText ? strlen((const char *)InStatementText) : 0;
    else
        src_len = (size_t)TextLength1;

    if (TextLength2Ptr)
        *TextLength2Ptr = (SQLINTEGER)src_len;

    if (OutStatementText && BufferLength > 0) {
        size_t copy = src_len < (size_t)(BufferLength - 1)
                      ? src_len : (size_t)(BufferLength - 1);
        if (InStatementText)
            memcpy(OutStatementText, InStatementText, copy);
        OutStatementText[copy] = '\0';
    }

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLCancel ─────────────────────────────────────── */

SQLRETURN SQL_API SQLCancel(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    ARGUS_STMT_LOCK(stmt);
    argus_diag_clear(&stmt->diag);

    /* Check if there's an active operation to cancel */
    if (!stmt->op || !stmt->executed) {
        /* Nothing to cancel */
        ARGUS_STMT_UNLOCK(stmt);
        return SQL_SUCCESS;
    }

    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->backend || !dbc->backend->cancel) {
        SQLRETURN err = argus_set_error(&stmt->diag, "HYC00",
                               "[Argus] Cancel not supported by backend", 0);
        ARGUS_STMT_UNLOCK(stmt);
        return err;
    }

    ARGUS_LOG_INFO("Cancelling statement operation");

    /* Call backend cancel function */
    int rc = dbc->backend->cancel(dbc->backend_conn, stmt->op);
    ARGUS_STMT_UNLOCK(stmt);

    if (rc != 0) {
        ARGUS_LOG_ERROR("Cancel operation failed: rc=%d", rc);
        return argus_set_error(&stmt->diag, "HY008",
                               "[Argus] Operation cancelled", 0);
    }

    ARGUS_LOG_DEBUG("Operation cancelled successfully");
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLMoreResults ────────────────────────────────── */

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    /* No multiple result sets — clean up state to prevent stale data */
    stmt->executed = false;
    stmt->num_cols = 0;
    stmt->metadata_fetched = false;
    stmt->fetch_started = false;
    stmt->getdata_col = 0;
    stmt->getdata_offset = 0;

    return SQL_NO_DATA;
}

/* ── ODBC API: SQLParamData (stub) ───────────────────────────── */

SQLRETURN SQL_API SQLParamData(
    SQLHSTMT   StatementHandle,
    SQLPOINTER *Value)
{
    (void)Value;
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;
    return argus_set_not_implemented(&stmt->diag, "SQLParamData");
}

/* ── ODBC API: SQLPutData (stub) ─────────────────────────────── */

SQLRETURN SQL_API SQLPutData(
    SQLHSTMT   StatementHandle,
    SQLPOINTER Data,
    SQLLEN     StrLen_or_Ind)
{
    (void)Data;
    (void)StrLen_or_Ind;
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;
    return argus_set_not_implemented(&stmt->diag, "SQLPutData");
}

/* ── ODBC API: SQLNumParams ──────────────────────────────────── */

SQLRETURN SQL_API SQLNumParams(
    SQLHSTMT     StatementHandle,
    SQLSMALLINT *ParameterCountPtr)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    if (ParameterCountPtr) {
        if (stmt->query)
            *ParameterCountPtr = (SQLSMALLINT)count_param_markers(stmt->query);
        else
            *ParameterCountPtr = 0;
    }
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLBindParameter ──────────────────────────────── */

SQLRETURN SQL_API SQLBindParameter(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ParameterNumber,
    SQLSMALLINT  InputOutputType,
    SQLSMALLINT  ValueType,
    SQLSMALLINT  ParameterType,
    SQLULEN      ColumnSize,
    SQLSMALLINT  DecimalDigits,
    SQLPOINTER   ParameterValuePtr,
    SQLLEN       BufferLength,
    SQLLEN      *StrLen_or_IndPtr)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    /* Only input parameters supported (read-only driver) */
    if (InputOutputType != SQL_PARAM_INPUT) {
        return argus_set_error(&stmt->diag, "HY105",
                               "[Argus] Only SQL_PARAM_INPUT is supported", 0);
    }

    if (ParameterNumber < 1 || ParameterNumber > ARGUS_MAX_PARAMS) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid parameter number", 0);
    }

    int idx = ParameterNumber - 1;

    /* Unbind if ParameterValuePtr is NULL and not SQL_NULL_DATA */
    if (!ParameterValuePtr && !(StrLen_or_IndPtr &&
                                 *StrLen_or_IndPtr == SQL_NULL_DATA)) {
        stmt->param_bindings[idx].bound = false;
        /* Recalculate num_param_bindings */
        while (stmt->num_param_bindings > 0 &&
               !stmt->param_bindings[stmt->num_param_bindings - 1].bound)
            stmt->num_param_bindings--;
        return SQL_SUCCESS;
    }

    argus_param_binding_t *bind = &stmt->param_bindings[idx];
    bind->io_type        = InputOutputType;
    bind->value_type     = ValueType;
    bind->param_type     = ParameterType;
    bind->column_size    = ColumnSize;
    bind->decimal_digits = DecimalDigits;
    bind->value          = ParameterValuePtr;
    bind->buffer_length  = BufferLength;
    bind->str_len_or_ind = StrLen_or_IndPtr;
    bind->bound          = true;

    if (ParameterNumber > stmt->num_param_bindings)
        stmt->num_param_bindings = ParameterNumber;

    ARGUS_LOG_DEBUG("Bound parameter %d: value_type=%d, param_type=%d",
                    ParameterNumber, ValueType, ParameterType);

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLDescribeParam ──────────────────────────────── */

SQLRETURN SQL_API SQLDescribeParam(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ParameterNumber,
    SQLSMALLINT *DataTypePtr,
    SQLULEN     *ParameterSizePtr,
    SQLSMALLINT *DecimalDigitsPtr,
    SQLSMALLINT *NullablePtr)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (ParameterNumber < 1) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid parameter number", 0);
    }

    /*
     * Return a generic description (SQL_VARCHAR) since we cannot
     * determine parameter types without server-side prepare support.
     */
    if (DataTypePtr)      *DataTypePtr      = SQL_VARCHAR;
    if (ParameterSizePtr) *ParameterSizePtr = 255;
    if (DecimalDigitsPtr) *DecimalDigitsPtr  = 0;
    if (NullablePtr)      *NullablePtr       = SQL_NULLABLE;

    return SQL_SUCCESS;
}
