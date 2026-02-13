#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);

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
        long val = strtol(cell->data, NULL, 10);
        if (target_value)
            *(unsigned char *)target_value = val ? 1 : 0;
        if (str_len_or_ind)
            *str_len_or_ind = 1;
        return SQL_SUCCESS;
    }

    case SQL_C_WCHAR: {
        /* UTF-8 to UTF-16LE conversion */
        if (str_len_or_ind)
            *str_len_or_ind = (SQLLEN)(cell->data_len * 2);

        if (target_value && buffer_length >= 2) {
            /* Simple conversion: treat as UTF-8 to wide char */
            size_t max_chars = (size_t)(buffer_length / 2) - 1;
            size_t copy = cell->data_len < max_chars ? cell->data_len : max_chars;
            SQLWCHAR *dst = (SQLWCHAR *)target_value;
            for (size_t i = 0; i < copy; i++)
                dst[i] = (SQLWCHAR)(unsigned char)cell->data[i];
            dst[copy] = 0;

            if (cell->data_len > max_chars) {
                argus_diag_push(diag, "01004",
                                "[Argus] String data, right truncated", 0);
                return SQL_SUCCESS_WITH_INFO;
            }
        }
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
        if (target_value)
            *(SQL_TIME_STRUCT *)target_value = time;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_TIME_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_TYPE_TIMESTAMP: {
        /* Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.fff" */
        SQL_TIMESTAMP_STRUCT ts;
        memset(&ts, 0, sizeof(ts));
        int n = sscanf(cell->data, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                       &ts.year, &ts.month, &ts.day,
                       &ts.hour, &ts.minute, &ts.second, &ts.fraction);
        if (n < 6) {
            return argus_set_error(diag, "22007",
                                   "[Argus] Invalid timestamp format", 0);
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

        /* Parse digits and build 128-bit little-endian value */
        unsigned long long low = 0, high = 0;
        int scale = 0;
        bool past_decimal = false;
        while (*p) {
            if (*p == '.') {
                past_decimal = true;
            } else if (*p >= '0' && *p <= '9') {
                int digit = *p - '0';
                /* Multiply by 10 and add digit */
                unsigned long long new_low = low * 10 + digit;
                unsigned long long new_high = high * 10;
                if (new_low < low) new_high++; /* carry */
                low = new_low;
                high = new_high;
                if (past_decimal) scale++;
            }
            p++;
        }

        /* Store in little-endian format */
        num.precision = 38;
        num.scale = (SQLSCHAR)scale;
        memcpy(num.val, &low, 8);
        memcpy(num.val + 8, &high, 8);

        if (target_value)
            *(SQL_NUMERIC_STRUCT *)target_value = num;
        if (str_len_or_ind)
            *str_len_or_ind = sizeof(SQL_NUMERIC_STRUCT);
        return SQL_SUCCESS;
    }

    case SQL_C_BINARY: {
        /* Raw binary data copy */
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

/* ── ODBC API: SQLFetch ──────────────────────────────────────── */

SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (!stmt->executed) {
        return argus_set_error(&stmt->diag, "HY010",
                               "[Argus] Function sequence error: not executed",
                               0);
    }

    /* Check if we need to fetch a new batch */
    if (!stmt->fetch_started ||
        stmt->row_cache.current_row >= stmt->row_cache.num_rows) {

        if (stmt->row_cache.exhausted && stmt->fetch_started)
            return SQL_NO_DATA;

        SQLRETURN rc = fetch_batch(stmt);
        if (rc != SQL_SUCCESS) return rc;

        stmt->fetch_started = true;
        stmt->row_cache.current_row = 0;

        if (stmt->row_cache.num_rows == 0)
            return SQL_NO_DATA;
    }

    /* Get current row */
    size_t row_idx = stmt->row_cache.current_row;
    argus_row_t *row = &stmt->row_cache.rows[row_idx];
    stmt->row_cache.current_row++;

    /* Transfer data to bound columns */
    SQLRETURN final_ret = SQL_SUCCESS;
    for (int col = 0; col < stmt->num_cols && col < ARGUS_MAX_COLUMNS; col++) {
        if (!stmt->bindings[col].bound) continue;

        argus_col_binding_t *bind = &stmt->bindings[col];
        argus_cell_t *cell = &row->cells[col];

        SQLRETURN ret = convert_cell_to_target(
            cell, bind->target_type,
            bind->target_value, bind->buffer_length,
            bind->str_len_or_ind, &stmt->diag);

        if (ret == SQL_SUCCESS_WITH_INFO)
            final_ret = SQL_SUCCESS_WITH_INFO;
        else if (ret == SQL_ERROR)
            return SQL_ERROR;
    }

    /* Update rows fetched pointer */
    if (stmt->rows_fetched_ptr)
        *(stmt->rows_fetched_ptr) = 1;

    return final_ret;
}

/* ── ODBC API: SQLFetchScroll ────────────────────────────────── */

SQLRETURN SQL_API SQLFetchScroll(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT FetchOrientation,
    SQLLEN      FetchOffset)
{
    (void)FetchOffset;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    /* Only support SQL_FETCH_NEXT (forward-only cursor) */
    if (FetchOrientation != SQL_FETCH_NEXT) {
        return argus_set_error(&stmt->diag, "HY106",
                               "[Argus] Only SQL_FETCH_NEXT is supported", 0);
    }

    return SQLFetch(StatementHandle);
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

    argus_diag_clear(&stmt->diag);

    if (ColumnNumber < 1 || ColumnNumber > (SQLUSMALLINT)stmt->num_cols) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid column number", 0);
    }

    /* Current row is one behind current_row pointer */
    size_t row_idx = stmt->row_cache.current_row - 1;
    if (row_idx >= stmt->row_cache.num_rows) {
        return argus_set_error(&stmt->diag, "24000",
                               "[Argus] Invalid cursor state", 0);
    }

    argus_row_t *row = &stmt->row_cache.rows[row_idx];
    argus_cell_t *cell = &row->cells[ColumnNumber - 1];

    return convert_cell_to_target(cell, TargetType, TargetValue,
                                   BufferLength, StrLen_or_Ind,
                                   &stmt->diag);
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

    if (ColumnNumber < 1 || ColumnNumber > ARGUS_MAX_COLUMNS) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid column number", 0);
    }

    int idx = ColumnNumber - 1;

    if (!TargetValue) {
        /* Unbind */
        stmt->bindings[idx].bound = false;
        return SQL_SUCCESS;
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
    if (ColumnSizePtr)    *ColumnSizePtr     = col->column_size;
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
        default:            type_name = "VARCHAR"; break;
        }
        SQLSMALLINT len = argus_copy_string(
            type_name, (SQLCHAR *)CharacterAttribute, BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_CATALOG_NAME:
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
    (void)ParameterNumber;
    (void)InputOutputType;
    (void)ValueType;
    (void)ParameterType;
    (void)ColumnSize;
    (void)DecimalDigits;
    (void)ParameterValuePtr;
    (void)BufferLength;
    (void)StrLen_or_IndPtr;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;
    return argus_set_not_implemented(&stmt->diag, "SQLBindParameter");
}
