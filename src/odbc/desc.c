/*
 * Argus ODBC Driver — Descriptor implementation
 *
 * Implements implicit descriptors (IRD, IPD, ARD, APD) backed by
 * the statement's existing column metadata and bindings.
 * Provides SQLGetDescField and SQLSetDescField.
 */

#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <string.h>
#include <stdlib.h>

/* argus_desc_t and its signature now live in handle.h — descriptors are real,
 * distinct handles (see the descriptor block there). */

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);

/* ── Descriptor handle resolution ─────────────────────────────────
 * The four descriptor entry points accept a real descriptor handle or, for
 * Driver-Manager backward compatibility, a raw statement handle. An implicit
 * descriptor (or an explicit one associated with a statement) resolves to that
 * statement, and field access flows through the statement's data. A standalone
 * explicit descriptor — allocated but not yet associated — has no statement, so
 * field access flows through its own record array. */

/* Grow a standalone explicit descriptor's record array to hold `n` records,
 * zero-filling the new tail and keeping the active-ARD statement view in sync. */
static int desc_ensure_records(argus_desc_t *d, int n)
{
    if (n <= d->record_capacity) return 0;
    int cap = n < 8 ? 8 : n;
    argus_col_binding_t *p = realloc(
        d->records, (size_t)cap * sizeof(argus_col_binding_t));
    if (!p) return -1;
    memset(p + d->record_capacity, 0,
           (size_t)(cap - d->record_capacity) * sizeof(argus_col_binding_t));
    d->records = p;
    d->record_capacity = cap;
    if (d->stmt && d->stmt->active_ard == d) {
        d->stmt->bindings = p;
        d->stmt->bindings_capacity = cap;
    }
    return 0;
}

/* ── ODBC API: SQLGetDescField ───────────────────────────────── */

SQLRETURN SQL_API SQLGetDescField(
    SQLHDESC    DescriptorHandle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT FieldIdentifier,
    SQLPOINTER  Value,
    SQLINTEGER  BufferLength,
    SQLINTEGER *StringLength)
{
    argus_stmt_t *stmt = argus_desc_stmt((SQLHANDLE)DescriptorHandle);

    /* A standalone explicit descriptor (allocated, not yet associated) has no
     * statement; answer from its own records. Only the header count and the
     * per-record application fields are meaningful for it. */
    if (!stmt) {
        if (!argus_valid_desc((SQLHANDLE)DescriptorHandle))
            return SQL_INVALID_HANDLE;
        argus_desc_t *d = (argus_desc_t *)DescriptorHandle;
        if (RecNumber == 0 && FieldIdentifier == SQL_DESC_COUNT) {
            if (Value) *(SQLSMALLINT *)Value = (SQLSMALLINT)d->record_capacity;
            if (StringLength) *StringLength = sizeof(SQLSMALLINT);
            return SQL_SUCCESS;
        }
        if (RecNumber >= 1 && RecNumber <= d->record_capacity && d->records) {
            const argus_col_binding_t *b = &d->records[RecNumber - 1];
            if (FieldIdentifier == SQL_DESC_DATA_PTR) {
                if (Value) *(SQLPOINTER *)Value = b->target_value;
                if (StringLength) *StringLength = sizeof(SQLPOINTER);
                return SQL_SUCCESS;
            }
            if (FieldIdentifier == SQL_DESC_TYPE ||
                FieldIdentifier == SQL_DESC_CONCISE_TYPE) {
                if (Value) *(SQLSMALLINT *)Value = b->target_type;
                if (StringLength) *StringLength = sizeof(SQLSMALLINT);
                return SQL_SUCCESS;
            }
            if (FieldIdentifier == SQL_DESC_OCTET_LENGTH) {
                if (Value) *(SQLLEN *)Value = b->buffer_length;
                if (StringLength) *StringLength = sizeof(SQLLEN);
                return SQL_SUCCESS;
            }
        }
        if (Value && BufferLength >= (SQLINTEGER)sizeof(SQLINTEGER))
            *(SQLINTEGER *)Value = 0;
        if (StringLength) *StringLength = 0;
        return SQL_SUCCESS;
    }

    /* Header fields (RecNumber == 0) */
    if (RecNumber == 0) {
        switch (FieldIdentifier) {
        case SQL_DESC_COUNT:
            if (Value) *(SQLSMALLINT *)Value = (SQLSMALLINT)stmt->num_cols;
            if (StringLength) *StringLength = sizeof(SQLSMALLINT);
            return SQL_SUCCESS;

        case SQL_DESC_ALLOC_TYPE:
            if (Value) *(SQLSMALLINT *)Value = SQL_DESC_ALLOC_AUTO;
            if (StringLength) *StringLength = sizeof(SQLSMALLINT);
            return SQL_SUCCESS;

        default:
            if (Value && BufferLength >= (SQLINTEGER)sizeof(SQLINTEGER))
                *(SQLINTEGER *)Value = 0;
            if (StringLength) *StringLength = sizeof(SQLINTEGER);
            return SQL_SUCCESS;
        }
    }

    /* Record fields (RecNumber >= 1) */
    if (RecNumber < 1 || RecNumber > (SQLSMALLINT)stmt->num_cols) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid descriptor index", 0);
    }

    const argus_column_desc_t *col = &stmt->columns[RecNumber - 1];

    switch (FieldIdentifier) {
    case SQL_DESC_NAME:
    case SQL_DESC_LABEL: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->name,
            (SQLCHAR *)Value, (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = (SQLINTEGER)len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_TYPE:
    case SQL_DESC_CONCISE_TYPE:
        if (Value) *(SQLSMALLINT *)Value = col->sql_type;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;

    case SQL_DESC_LENGTH:
    case SQL_DESC_OCTET_LENGTH:
    case SQL_DESC_DISPLAY_SIZE:
        if (Value) *(SQLULEN *)Value = col->column_size;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_DESC_PRECISION:
        if (Value) *(SQLSMALLINT *)Value = (SQLSMALLINT)col->column_size;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;

    case SQL_DESC_SCALE:
        if (Value) *(SQLSMALLINT *)Value = col->decimal_digits;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;

    case SQL_DESC_NULLABLE:
        if (Value) *(SQLSMALLINT *)Value = col->nullable;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;

    case SQL_DESC_UNSIGNED:
        if (Value) *(SQLSMALLINT *)Value = SQL_FALSE;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;

    case SQL_DESC_AUTO_UNIQUE_VALUE:
        if (Value) *(SQLINTEGER *)Value = SQL_FALSE;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    case SQL_DESC_UPDATABLE:
        if (Value) *(SQLSMALLINT *)Value = SQL_ATTR_READONLY;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;

    case SQL_DESC_SEARCHABLE:
        if (Value) *(SQLINTEGER *)Value = SQL_PRED_SEARCHABLE;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    case SQL_DESC_CASE_SENSITIVE:
        if (Value) *(SQLINTEGER *)Value = SQL_TRUE;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    case SQL_DESC_FIXED_PREC_SCALE:
        if (Value) *(SQLINTEGER *)Value = SQL_FALSE;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
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
            type_name, (SQLCHAR *)Value, (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = (SQLINTEGER)len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_BASE_TABLE_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->table_name,
            (SQLCHAR *)Value, (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = (SQLINTEGER)len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_SCHEMA_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->schema_name,
            (SQLCHAR *)Value, (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = (SQLINTEGER)len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_CATALOG_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->catalog_name,
            (SQLCHAR *)Value, (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = (SQLINTEGER)len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_BASE_COLUMN_NAME: {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->name,
            (SQLCHAR *)Value, (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = (SQLINTEGER)len;
        return SQL_SUCCESS;
    }

    case SQL_DESC_LITERAL_PREFIX:
    case SQL_DESC_LITERAL_SUFFIX:
    case SQL_DESC_LOCAL_TYPE_NAME:
        /* Return empty string */
        if (Value && BufferLength > 0) ((SQLCHAR *)Value)[0] = '\0';
        if (StringLength) *StringLength = 0;
        return SQL_SUCCESS;

    case SQL_DESC_NUM_PREC_RADIX:
        if (Value) {
            switch (col->sql_type) {
            case SQL_INTEGER:
            case SQL_BIGINT:
            case SQL_SMALLINT:
            case SQL_TINYINT:
                *(SQLINTEGER *)Value = 10;
                break;
            case SQL_FLOAT:
            case SQL_DOUBLE:
            case SQL_REAL:
                *(SQLINTEGER *)Value = 2;
                break;
            default:
                *(SQLINTEGER *)Value = 0;
                break;
            }
        }
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    default:
        if (Value && BufferLength >= (SQLINTEGER)sizeof(SQLINTEGER))
            *(SQLINTEGER *)Value = 0;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLSetDescField ───────────────────────────────── */

SQLRETURN SQL_API SQLSetDescField(
    SQLHDESC    DescriptorHandle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT FieldIdentifier,
    SQLPOINTER  Value,
    SQLINTEGER  BufferLength)
{
    (void)BufferLength; /* No string descriptor fields handled yet */

    argus_stmt_t *stmt = argus_desc_stmt((SQLHANDLE)DescriptorHandle);

    /* A standalone explicit descriptor stores field changes in its own records
     * until it is associated with a statement. */
    if (!stmt) {
        if (!argus_valid_desc((SQLHANDLE)DescriptorHandle))
            return SQL_INVALID_HANDLE;
        argus_desc_t *d = (argus_desc_t *)DescriptorHandle;
        if (FieldIdentifier == SQL_DESC_COUNT) return SQL_SUCCESS;
        if (RecNumber >= 1) {
            if (desc_ensure_records(d, RecNumber) != 0)
                return argus_set_error(&d->diag, "HY001",
                                       "[Argus] Memory allocation failed", 0);
            argus_col_binding_t *b = &d->records[RecNumber - 1];
            switch (FieldIdentifier) {
            case SQL_DESC_DATA_PTR:
                b->target_value = Value;
                b->bound = (Value != NULL);
                break;
            case SQL_DESC_TYPE:
            case SQL_DESC_CONCISE_TYPE:
                b->target_type = (SQLSMALLINT)(intptr_t)Value;
                break;
            case SQL_DESC_OCTET_LENGTH:
                b->buffer_length = (SQLLEN)(intptr_t)Value;
                break;
            case SQL_DESC_OCTET_LENGTH_PTR:
            case SQL_DESC_INDICATOR_PTR:
                b->str_len_or_ind = (SQLLEN *)Value;
                break;
            default:
                break;
            }
        }
        return SQL_SUCCESS;
    }

    /*
     * For implicit descriptors routed through a statement handle,
     * we accept basic ARD/APD field modifications that BI tools need.
     * IRD fields remain read-only.
     */
    switch (FieldIdentifier) {
    case SQL_DESC_COUNT:
        /* Allow setting descriptor count (ARD/APD) */
        return SQL_SUCCESS;

    case SQL_DESC_TYPE:
    case SQL_DESC_CONCISE_TYPE:
    case SQL_DESC_DATA_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_LENGTH:
    case SQL_DESC_PRECISION:
    case SQL_DESC_SCALE:
    case SQL_DESC_OCTET_LENGTH:
        /*
         * Accept these for ARD/APD. We route actual binding
         * through SQLBindCol/SQLBindParameter, so store minimally.
         */
        if (RecNumber >= 1) {
            if (argus_stmt_ensure_bindings(stmt, RecNumber) != 0)
                return SQL_ERROR;
            if (FieldIdentifier == SQL_DESC_DATA_PTR) {
                int idx = RecNumber - 1;
                if (Value) {
                    stmt->bindings[idx].target_value = Value;
                    stmt->bindings[idx].bound = true;
                } else {
                    stmt->bindings[idx].bound = false;
                }
            }
            if (FieldIdentifier == SQL_DESC_TYPE ||
                FieldIdentifier == SQL_DESC_CONCISE_TYPE) {
                int idx = RecNumber - 1;
                stmt->bindings[idx].target_type =
                    (SQLSMALLINT)(intptr_t)Value;
            }
        }
        return SQL_SUCCESS;

    default:
        return argus_set_error(&stmt->diag, "HY016",
                               "[Argus] Cannot modify an implementation row descriptor",
                               0);
    }
}

/* ── ODBC API: SQLSetDescRec ─────────────────────────────────── */

SQLRETURN SQL_API SQLSetDescRec(
    SQLHDESC     DescriptorHandle,
    SQLSMALLINT  RecNumber,
    SQLSMALLINT  Type,
    SQLSMALLINT  SubType,
    SQLLEN       Length,
    SQLSMALLINT  Precision,
    SQLSMALLINT  Scale,
    SQLPOINTER   DataPtr,
    SQLLEN      *StringLengthPtr,
    SQLLEN      *IndicatorPtr)
{
    (void)SubType;
    (void)Precision;
    (void)Scale;

    argus_stmt_t *stmt = argus_desc_stmt((SQLHANDLE)DescriptorHandle);

    /* Standalone explicit descriptor: write the record into its own array. */
    if (!stmt) {
        if (!argus_valid_desc((SQLHANDLE)DescriptorHandle))
            return SQL_INVALID_HANDLE;
        argus_desc_t *d = (argus_desc_t *)DescriptorHandle;
        if (RecNumber < 1)
            return argus_set_error(&d->diag, "07009",
                                   "[Argus] Invalid descriptor index", 0);
        if (desc_ensure_records(d, RecNumber) != 0)
            return argus_set_error(&d->diag, "HY001",
                                   "[Argus] Memory allocation failed", 0);
        argus_col_binding_t *b = &d->records[RecNumber - 1];
        b->target_type    = Type;
        b->target_value   = DataPtr;
        b->buffer_length  = Length;
        b->str_len_or_ind = StringLengthPtr;
        b->bound          = (DataPtr != NULL);
        (void)IndicatorPtr;
        return SQL_SUCCESS;
    }

    if (RecNumber < 1) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid descriptor index", 0);
    }

    if (argus_stmt_ensure_bindings(stmt, RecNumber) != 0) {
        return argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }

    int idx = RecNumber - 1;

    /* Update ARD binding */
    stmt->bindings[idx].target_type    = Type;
    stmt->bindings[idx].target_value   = DataPtr;
    stmt->bindings[idx].buffer_length  = Length;
    stmt->bindings[idx].str_len_or_ind = StringLengthPtr;
    stmt->bindings[idx].bound          = (DataPtr != NULL);

    (void)IndicatorPtr; /* We use StringLengthPtr as combined indicator */

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLGetDescRec ─────────────────────────────────── */

SQLRETURN SQL_API SQLGetDescRec(
    SQLHDESC     DescriptorHandle,
    SQLSMALLINT  RecNumber,
    SQLCHAR     *Name,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLengthPtr,
    SQLSMALLINT *TypePtr,
    SQLSMALLINT *SubTypePtr,
    SQLLEN      *LengthPtr,
    SQLSMALLINT *PrecisionPtr,
    SQLSMALLINT *ScalePtr,
    SQLSMALLINT *NullablePtr)
{
    argus_stmt_t *stmt = argus_desc_stmt((SQLHANDLE)DescriptorHandle);
    if (!stmt) {
        /* A standalone explicit descriptor has no column metadata to report. */
        if (!argus_valid_desc((SQLHANDLE)DescriptorHandle))
            return SQL_INVALID_HANDLE;
        return SQL_NO_DATA;
    }

    if (RecNumber < 1 || RecNumber > (SQLSMALLINT)stmt->num_cols) {
        return SQL_NO_DATA;
    }

    const argus_column_desc_t *col = &stmt->columns[RecNumber - 1];

    if (Name) {
        SQLSMALLINT len = argus_copy_string(
            (const char *)col->name, Name, BufferLength);
        if (StringLengthPtr) *StringLengthPtr = len;
    } else if (StringLengthPtr) {
        *StringLengthPtr = col->name_len;
    }

    if (TypePtr)      *TypePtr      = col->sql_type;
    if (SubTypePtr)   *SubTypePtr   = 0;
    if (LengthPtr)    *LengthPtr    = (SQLLEN)col->column_size;
    if (PrecisionPtr) *PrecisionPtr = (SQLSMALLINT)col->column_size;
    if (ScalePtr)     *ScalePtr     = col->decimal_digits;
    if (NullablePtr)  *NullablePtr  = col->nullable;

    return SQL_SUCCESS;
}
