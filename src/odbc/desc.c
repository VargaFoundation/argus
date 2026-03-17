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

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);

/* ── Descriptor signature and types ──────────────────────────── */

#define ARGUS_DESC_SIGNATURE 0x41524758U  /* 'ARGX' - unique descriptor signature */

typedef enum {
    ARGUS_DESC_IRD = 0,  /* Implementation Row Descriptor */
    ARGUS_DESC_IPD,      /* Implementation Parameter Descriptor */
    ARGUS_DESC_ARD,      /* Application Row Descriptor */
    ARGUS_DESC_APD       /* Application Parameter Descriptor */
} argus_desc_type_t;

/*
 * Lightweight descriptor handle: just points back to the parent
 * statement and carries a type tag. All data lives in argus_stmt_t.
 */
typedef struct argus_desc {
    unsigned int      signature;
    argus_desc_type_t type;
    argus_stmt_t     *stmt;
    argus_diag_t      diag;
} argus_desc_t;

/* ── Descriptor handle retrieval ─────────────────────────────── */

/*
 * We allocate descriptors on-demand as part of SQLGetStmtAttr.
 * For now, SQLGetDescField works with the statement directly.
 */

/* ── ODBC API: SQLGetDescField ───────────────────────────────── */

SQLRETURN SQL_API SQLGetDescField(
    SQLHDESC    DescriptorHandle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT FieldIdentifier,
    SQLPOINTER  Value,
    SQLINTEGER  BufferLength,
    SQLINTEGER *StringLength)
{
    /*
     * The Driver Manager may pass a descriptor handle directly.
     * Since we don't allocate explicit descriptors, this is called
     * when the DM maps descriptor operations. We handle the most
     * common fields that BI tools query.
     *
     * The DescriptorHandle for implicit descriptors will actually
     * be a statement handle (the DM routes calls accordingly).
     * We detect this via the signature.
     */
    argus_stmt_t *stmt = NULL;

    /* Check if it's actually a statement handle (implicit descriptor) */
    if (argus_valid_stmt((SQLHANDLE)DescriptorHandle)) {
        stmt = (argus_stmt_t *)DescriptorHandle;
    } else {
        /* Possibly an explicit descriptor - not supported */
        return SQL_INVALID_HANDLE;
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
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_CATALOG_NAME:
    case SQL_DESC_BASE_TABLE_NAME:
    case SQL_DESC_BASE_COLUMN_NAME:
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

    if (!argus_valid_stmt((SQLHANDLE)DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    argus_stmt_t *stmt = (argus_stmt_t *)DescriptorHandle;

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
        if (RecNumber >= 1 && RecNumber <= ARGUS_MAX_COLUMNS) {
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

    if (!argus_valid_stmt((SQLHANDLE)DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    argus_stmt_t *stmt = (argus_stmt_t *)DescriptorHandle;

    if (RecNumber < 1 || RecNumber > ARGUS_MAX_COLUMNS) {
        return argus_set_error(&stmt->diag, "07009",
                               "[Argus] Invalid descriptor index", 0);
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
    argus_stmt_t *stmt = NULL;
    if (argus_valid_stmt((SQLHANDLE)DescriptorHandle)) {
        stmt = (argus_stmt_t *)DescriptorHandle;
    } else {
        return SQL_INVALID_HANDLE;
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
