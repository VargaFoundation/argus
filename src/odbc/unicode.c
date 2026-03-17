/*
 * Argus ODBC Driver — Unicode (W) function wrappers
 *
 * These functions accept UTF-16 (SQLWCHAR*) strings, convert to UTF-8,
 * call the ANSI implementation, and convert results back to UTF-16.
 * Conversion uses GLib g_utf16_to_utf8() / g_utf8_to_utf16().
 */

#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <stdlib.h>
#include <string.h>
#include <glib.h>

/* ── Helper: convert SQLWCHAR* (UTF-16) to UTF-8 char* ────────── */

static char *wchar_to_utf8(const SQLWCHAR *wstr, SQLSMALLINT len_chars)
{
    if (!wstr) return NULL;

    glong items_read = 0;
    glong n_chars;

    if (len_chars == SQL_NTS) {
        /* Find NUL terminator */
        n_chars = 0;
        while (wstr[n_chars]) n_chars++;
    } else {
        n_chars = (glong)(len_chars);
    }

    GError *err = NULL;
    gchar *utf8 = g_utf16_to_utf8(
        (const gunichar2 *)wstr, n_chars,
        &items_read, NULL, &err);

    if (err) {
        g_error_free(err);
        return NULL;
    }
    return utf8;
}

/* ── Helper: convert UTF-8 to SQLWCHAR* (UTF-16) ─────────────── */

static SQLSMALLINT utf8_to_wchar(const SQLCHAR *utf8, SQLSMALLINT utf8_len,
                                   SQLWCHAR *out, SQLSMALLINT out_buf_len)
{
    if (!utf8) {
        if (out && out_buf_len > 0) out[0] = 0;
        return 0;
    }

    glong src_len;
    if (utf8_len == SQL_NTS)
        src_len = -1;
    else
        src_len = (glong)utf8_len;

    GError *err = NULL;
    glong items_written = 0;
    gunichar2 *utf16 = g_utf8_to_utf16(
        (const gchar *)utf8, src_len,
        NULL, &items_written, &err);

    if (err) {
        g_error_free(err);
        if (out && out_buf_len > 0) out[0] = 0;
        return 0;
    }

    SQLSMALLINT total_chars = (SQLSMALLINT)items_written;

    if (out && out_buf_len > 0) {
        SQLSMALLINT max_chars = (SQLSMALLINT)((size_t)out_buf_len / sizeof(SQLWCHAR)) - 1;
        if (max_chars < 0) max_chars = 0;
        SQLSMALLINT copy = total_chars < max_chars ? total_chars : max_chars;
        memcpy(out, utf16, (size_t)copy * sizeof(SQLWCHAR));
        out[copy] = 0;
    }

    g_free(utf16);
    return total_chars;
}

/* ── SQLDriverConnectW ───────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLDriverConnectW(
    SQLHDBC      ConnectionHandle,
    SQLHWND      WindowHandle,
    SQLWCHAR    *InConnectionString,
    SQLSMALLINT  StringLength1,
    SQLWCHAR    *OutConnectionString,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength2Ptr,
    SQLUSMALLINT DriverCompletion)
{
    char *in_utf8 = wchar_to_utf8(InConnectionString, StringLength1);

    /* Call ANSI version with UTF-8 buffer for output */
    SQLCHAR out_buf[4096];
    SQLSMALLINT out_len = 0;

    SQLRETURN ret = SQLDriverConnect(
        ConnectionHandle, WindowHandle,
        (SQLCHAR *)in_utf8, in_utf8 ? SQL_NTS : 0,
        out_buf, sizeof(out_buf), &out_len,
        DriverCompletion);

    g_free(in_utf8);

    /* Convert output to UTF-16 */
    if (OutConnectionString && BufferLength > 0) {
        SQLSMALLINT wlen = utf8_to_wchar(out_buf, out_len,
                                           OutConnectionString, BufferLength);
        if (StringLength2Ptr) *StringLength2Ptr = wlen;
    } else if (StringLength2Ptr) {
        /* Measure character count by converting without output buffer */
        SQLSMALLINT wlen = utf8_to_wchar(out_buf, out_len, NULL, 0);
        *StringLength2Ptr = wlen;
    }

    return ret;
}

/* ── SQLExecDirectW ──────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLExecDirectW(
    SQLHSTMT  StatementHandle,
    SQLWCHAR *StatementText,
    SQLINTEGER TextLength)
{
    char *utf8 = NULL;
    if (TextLength == SQL_NTS) {
        utf8 = wchar_to_utf8(StatementText, SQL_NTS);
    } else {
        /* TextLength is in characters; wchar_to_utf8 expects glong count */
        if (!StatementText) {
            utf8 = NULL;
        } else {
            GError *err = NULL;
            utf8 = (char *)g_utf16_to_utf8(
                (const gunichar2 *)StatementText, (glong)TextLength,
                NULL, NULL, &err);
            if (err) { g_error_free(err); utf8 = NULL; }
        }
    }

    SQLRETURN ret = SQLExecDirect(
        StatementHandle,
        (SQLCHAR *)utf8,
        utf8 ? SQL_NTS : 0);

    g_free(utf8);
    return ret;
}

/* ── SQLPrepareW ─────────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLPrepareW(
    SQLHSTMT  StatementHandle,
    SQLWCHAR *StatementText,
    SQLINTEGER TextLength)
{
    char *utf8 = NULL;
    if (TextLength == SQL_NTS) {
        utf8 = wchar_to_utf8(StatementText, SQL_NTS);
    } else {
        if (!StatementText) {
            utf8 = NULL;
        } else {
            GError *err = NULL;
            utf8 = (char *)g_utf16_to_utf8(
                (const gunichar2 *)StatementText, (glong)TextLength,
                NULL, NULL, &err);
            if (err) { g_error_free(err); utf8 = NULL; }
        }
    }

    SQLRETURN ret = SQLPrepare(
        StatementHandle,
        (SQLCHAR *)utf8,
        utf8 ? SQL_NTS : 0);

    g_free(utf8);
    return ret;
}

/* ── SQLGetDiagRecW ──────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetDiagRecW(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLWCHAR   *Sqlstate,
    SQLINTEGER *NativeError,
    SQLWCHAR   *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength)
{
    SQLCHAR state_buf[6];
    SQLCHAR msg_buf[1024];
    SQLSMALLINT msg_len = 0;

    SQLRETURN ret = SQLGetDiagRec(
        HandleType, Handle, RecNumber,
        state_buf, NativeError,
        msg_buf, sizeof(msg_buf), &msg_len);

    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (Sqlstate) {
            utf8_to_wchar(state_buf, SQL_NTS,
                           Sqlstate, 6 * (SQLSMALLINT)sizeof(SQLWCHAR));
        }
        if (MessageText && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                msg_buf, msg_len,
                MessageText, BufferLength);
            if (TextLength)
                *TextLength = wlen; /* ODBC spec: character count */
        } else if (TextLength) {
            SQLSMALLINT wlen = utf8_to_wchar(
                msg_buf, msg_len, NULL, 0);
            *TextLength = wlen;
        }
    }

    return ret;
}

/* ── SQLColAttributeW ────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLColAttributeW(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER   CharacterAttribute,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN      *NumericAttribute)
{
    /* For numeric fields, just pass through */
    switch (FieldIdentifier) {
    case SQL_DESC_NAME:
    case SQL_COLUMN_NAME:
    case SQL_DESC_LABEL:
    case SQL_DESC_TYPE_NAME:
    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_CATALOG_NAME:
    case SQL_DESC_LITERAL_PREFIX:
    case SQL_DESC_LITERAL_SUFFIX:
    case SQL_DESC_LOCAL_TYPE_NAME: {
        /* String attribute: get ANSI, convert to UTF-16 */
        SQLCHAR ansi_buf[512];
        SQLSMALLINT ansi_len = 0;

        SQLRETURN ret = SQLColAttribute(
            StatementHandle, ColumnNumber, FieldIdentifier,
            ansi_buf, sizeof(ansi_buf), &ansi_len,
            NumericAttribute);

        if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) &&
            CharacterAttribute && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                ansi_buf, ansi_len,
                (SQLWCHAR *)CharacterAttribute, BufferLength);
            if (StringLength)
                *StringLength = (SQLSMALLINT)(wlen * (SQLSMALLINT)sizeof(SQLWCHAR));
        } else if (StringLength) {
            *StringLength = (SQLSMALLINT)(ansi_len * (SQLSMALLINT)sizeof(SQLWCHAR));
        }
        return ret;
    }
    default:
        /* Numeric attribute: pass through directly */
        return SQLColAttribute(
            StatementHandle, ColumnNumber, FieldIdentifier,
            CharacterAttribute, BufferLength, StringLength,
            NumericAttribute);
    }
}

/* ── SQLDescribeColW ─────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLDescribeColW(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLWCHAR    *ColumnName,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *NameLengthPtr,
    SQLSMALLINT *DataTypePtr,
    SQLULEN     *ColumnSizePtr,
    SQLSMALLINT *DecimalDigitsPtr,
    SQLSMALLINT *NullablePtr)
{
    SQLCHAR name_buf[ARGUS_MAX_COLUMN_NAME];
    SQLSMALLINT name_len = 0;

    SQLRETURN ret = SQLDescribeCol(
        StatementHandle, ColumnNumber,
        name_buf, sizeof(name_buf), &name_len,
        DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);

    if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) &&
        ColumnName && BufferLength > 0) {
        SQLSMALLINT wlen = utf8_to_wchar(
            name_buf, name_len,
            ColumnName, BufferLength);
        if (NameLengthPtr) *NameLengthPtr = wlen;
    } else if (NameLengthPtr) {
        *NameLengthPtr = name_len;
    }

    return ret;
}

/* ── SQLGetInfoW ─────────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetInfoW(
    SQLHDBC      ConnectionHandle,
    SQLUSMALLINT InfoType,
    SQLPOINTER   InfoValue,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength)
{
    /*
     * Determine if InfoType returns a string or numeric.
     * String info types have values < 100 typically, but
     * we check known string types explicitly.
     */
    switch (InfoType) {
    case SQL_DRIVER_NAME:
    case SQL_DRIVER_VER:
    case SQL_DRIVER_ODBC_VER:
    case SQL_ODBC_VER:
    case SQL_DATA_SOURCE_NAME:
    case SQL_SERVER_NAME:
    case SQL_DATABASE_NAME:
    case SQL_DBMS_NAME:
    case SQL_DBMS_VER:
    case SQL_IDENTIFIER_QUOTE_CHAR:
    case SQL_CATALOG_NAME_SEPARATOR:
    case SQL_CATALOG_TERM:
    case SQL_SCHEMA_TERM:
    case SQL_TABLE_TERM:
    case SQL_PROCEDURE_TERM:
    case SQL_SEARCH_PATTERN_ESCAPE:
    case SQL_LIKE_ESCAPE_CLAUSE:
    case SQL_SPECIAL_CHARACTERS:
    case SQL_COLUMN_ALIAS:
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
    case SQL_EXPRESSIONS_IN_ORDERBY:
    case SQL_MULT_RESULT_SETS:
    case SQL_MULTIPLE_ACTIVE_TXN:
    case SQL_OUTER_JOINS:
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
    case SQL_NEED_LONG_DATA_LEN:
    case SQL_ACCESSIBLE_TABLES:
    case SQL_ACCESSIBLE_PROCEDURES:
    case SQL_KEYWORDS:
    case SQL_USER_NAME:
    case SQL_ROW_UPDATES:
    case SQL_DESCRIBE_PARAMETER:
    case SQL_INTEGRITY:
    case SQL_CATALOG_NAME: {
        /* Get ANSI string then convert to UTF-16 */
        SQLCHAR ansi_buf[4096];
        SQLSMALLINT ansi_len = 0;

        SQLRETURN ret = SQLGetInfo(
            ConnectionHandle, InfoType,
            ansi_buf, sizeof(ansi_buf), &ansi_len);

        if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) &&
            InfoValue && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                ansi_buf, ansi_len,
                (SQLWCHAR *)InfoValue, BufferLength);
            if (StringLength)
                *StringLength = (SQLSMALLINT)(wlen * (SQLSMALLINT)sizeof(SQLWCHAR));
        } else if (StringLength) {
            *StringLength = (SQLSMALLINT)(ansi_len * (SQLSMALLINT)sizeof(SQLWCHAR));
        }
        return ret;
    }
    default:
        /* Numeric: pass through */
        return SQLGetInfo(ConnectionHandle, InfoType,
                          InfoValue, BufferLength, StringLength);
    }
}

/* ── SQLTablesW ──────────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLTablesW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,    SQLSMALLINT NameLength3,
    SQLWCHAR  *TableType,    SQLSMALLINT NameLength4)
{
    char *cat  = wchar_to_utf8(CatalogName, NameLength1);
    char *sch  = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl  = wchar_to_utf8(TableName,   NameLength3);
    char *type = wchar_to_utf8(TableType,    NameLength4);

    SQLRETURN ret = SQLTables(
        StatementHandle,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0,
        (SQLCHAR *)type, type ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    g_free(type);
    return ret;
}

/* ── SQLColumnsW ─────────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLColumnsW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,    SQLSMALLINT NameLength3,
    SQLWCHAR  *ColumnName,   SQLSMALLINT NameLength4)
{
    char *cat  = wchar_to_utf8(CatalogName, NameLength1);
    char *sch  = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl  = wchar_to_utf8(TableName,   NameLength3);
    char *col  = wchar_to_utf8(ColumnName,  NameLength4);

    SQLRETURN ret = SQLColumns(
        StatementHandle,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0,
        (SQLCHAR *)col, col ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    g_free(col);
    return ret;
}

/* ── SQLConnectW ─────────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLConnectW(
    SQLHDBC   ConnectionHandle,
    SQLWCHAR *ServerName,      SQLSMALLINT NameLength1,
    SQLWCHAR *UserName,        SQLSMALLINT NameLength2,
    SQLWCHAR *Authentication,  SQLSMALLINT NameLength3)
{
    char *srv  = wchar_to_utf8(ServerName,     NameLength1);
    char *user = wchar_to_utf8(UserName,       NameLength2);
    char *auth = wchar_to_utf8(Authentication, NameLength3);

    SQLRETURN ret = SQLConnect(
        ConnectionHandle,
        (SQLCHAR *)srv,  srv  ? SQL_NTS : 0,
        (SQLCHAR *)user, user ? SQL_NTS : 0,
        (SQLCHAR *)auth, auth ? SQL_NTS : 0);

    g_free(srv);
    g_free(user);
    g_free(auth);
    return ret;
}

/* ── SQLNativeSqlW ───────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLNativeSqlW(
    SQLHDBC    ConnectionHandle,
    SQLWCHAR  *InStatementText,  SQLINTEGER TextLength1,
    SQLWCHAR  *OutStatementText, SQLINTEGER BufferLength,
    SQLINTEGER *TextLength2Ptr)
{
    char *utf8_in = NULL;
    if (TextLength1 == SQL_NTS) {
        utf8_in = wchar_to_utf8(InStatementText, SQL_NTS);
    } else if (InStatementText) {
        GError *err = NULL;
        utf8_in = (char *)g_utf16_to_utf8(
            (const gunichar2 *)InStatementText, (glong)TextLength1,
            NULL, NULL, &err);
        if (err) { g_error_free(err); utf8_in = NULL; }
    }

    SQLCHAR out_buf[4096];
    SQLINTEGER out_len = 0;

    SQLRETURN ret = SQLNativeSql(
        ConnectionHandle,
        (SQLCHAR *)utf8_in, utf8_in ? SQL_NTS : 0,
        out_buf, sizeof(out_buf), &out_len);

    g_free(utf8_in);

    if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) &&
        OutStatementText && BufferLength > 0) {
        SQLSMALLINT wout = utf8_to_wchar(
            out_buf, (SQLSMALLINT)out_len,
            OutStatementText, (SQLSMALLINT)BufferLength);
        if (TextLength2Ptr)
            *TextLength2Ptr = (SQLINTEGER)wout; /* character count */
    } else if (TextLength2Ptr) {
        SQLSMALLINT wout = utf8_to_wchar(
            out_buf, (SQLSMALLINT)out_len, NULL, 0);
        *TextLength2Ptr = (SQLINTEGER)wout;
    }

    return ret;
}

/* ── SQLGetDiagFieldW ────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetDiagFieldW(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT DiagIdentifier,
    SQLPOINTER  DiagInfo,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength)
{
    /* For string diag fields, convert; for others, pass through */
    switch (DiagIdentifier) {
    case SQL_DIAG_SQLSTATE:
    case SQL_DIAG_MESSAGE_TEXT:
    case SQL_DIAG_CLASS_ORIGIN:
    case SQL_DIAG_SUBCLASS_ORIGIN:
    case SQL_DIAG_CONNECTION_NAME:
    case SQL_DIAG_SERVER_NAME: {
        SQLCHAR ansi_buf[1024];
        SQLSMALLINT ansi_len = 0;

        SQLRETURN ret = SQLGetDiagField(
            HandleType, Handle, RecNumber, DiagIdentifier,
            ansi_buf, sizeof(ansi_buf), &ansi_len);

        if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) &&
            DiagInfo && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                ansi_buf, ansi_len,
                (SQLWCHAR *)DiagInfo, BufferLength);
            if (StringLength)
                *StringLength = (SQLSMALLINT)(wlen * (SQLSMALLINT)sizeof(SQLWCHAR));
        } else if (StringLength) {
            *StringLength = (SQLSMALLINT)(ansi_len * (SQLSMALLINT)sizeof(SQLWCHAR));
        }
        return ret;
    }
    default:
        return SQLGetDiagField(HandleType, Handle, RecNumber,
                                DiagIdentifier, DiagInfo,
                                BufferLength, StringLength);
    }
}

/* ── SQLSetConnectAttrW ──────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLSetConnectAttrW(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength)
{
    /* String attributes need conversion */
    if (Attribute == SQL_ATTR_CURRENT_CATALOG && Value &&
        (StringLength > 0 || StringLength == SQL_NTS)) {
        SQLSMALLINT wlen;
        if (StringLength == SQL_NTS)
            wlen = SQL_NTS;
        else
            wlen = (SQLSMALLINT)(StringLength / (SQLINTEGER)sizeof(SQLWCHAR));
        char *utf8 = wchar_to_utf8((SQLWCHAR *)Value, wlen);
        SQLRETURN ret = SQLSetConnectAttr(
            ConnectionHandle, Attribute,
            (SQLPOINTER)utf8, utf8 ? SQL_NTS : 0);
        g_free(utf8);
        return ret;
    }
    return SQLSetConnectAttr(ConnectionHandle, Attribute, Value, StringLength);
}

/* ── SQLGetConnectAttrW ──────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetConnectAttrW(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
    if (Attribute == SQL_ATTR_CURRENT_CATALOG) {
        SQLCHAR ansi_buf[512];
        SQLINTEGER ansi_len = 0;

        SQLRETURN ret = SQLGetConnectAttr(
            ConnectionHandle, Attribute,
            ansi_buf, sizeof(ansi_buf), &ansi_len);

        if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) &&
            Value && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                ansi_buf, (SQLSMALLINT)ansi_len,
                (SQLWCHAR *)Value, (SQLSMALLINT)BufferLength);
            if (StringLength)
                *StringLength = (SQLINTEGER)(wlen * (SQLINTEGER)sizeof(SQLWCHAR));
        } else if (StringLength) {
            *StringLength = (SQLINTEGER)(ansi_len * (SQLINTEGER)sizeof(SQLWCHAR));
        }
        return ret;
    }
    return SQLGetConnectAttr(ConnectionHandle, Attribute,
                              Value, BufferLength, StringLength);
}

/* ── SQLGetTypeInfoW ─────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetTypeInfoW(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT DataType)
{
    /* No string parameters to convert */
    return SQLGetTypeInfo(StatementHandle, DataType);
}

/* ── SQLStatisticsW ──────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLStatisticsW(
    SQLHSTMT     StatementHandle,
    SQLWCHAR    *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR    *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR    *TableName,    SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique,
    SQLUSMALLINT Reserved)
{
    char *cat = wchar_to_utf8(CatalogName, NameLength1);
    char *sch = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl = wchar_to_utf8(TableName,   NameLength3);

    SQLRETURN ret = SQLStatistics(
        StatementHandle,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0,
        Unique, Reserved);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    return ret;
}

/* ── SQLSpecialColumnsW ──────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLSpecialColumnsW(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT IdentifierType,
    SQLWCHAR    *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR    *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR    *TableName,    SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope,
    SQLUSMALLINT Nullable)
{
    char *cat = wchar_to_utf8(CatalogName, NameLength1);
    char *sch = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl = wchar_to_utf8(TableName,   NameLength3);

    SQLRETURN ret = SQLSpecialColumns(
        StatementHandle, IdentifierType,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0,
        Scope, Nullable);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    return ret;
}

/* ── SQLPrimaryKeysW ─────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLPrimaryKeysW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,    SQLSMALLINT NameLength3)
{
    char *cat = wchar_to_utf8(CatalogName, NameLength1);
    char *sch = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl = wchar_to_utf8(TableName,   NameLength3);

    SQLRETURN ret = SQLPrimaryKeys(
        StatementHandle,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    return ret;
}

/* ── SQLForeignKeysW ─────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLForeignKeysW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *PKCatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *PKSchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *PKTableName,    SQLSMALLINT NameLength3,
    SQLWCHAR  *FKCatalogName,  SQLSMALLINT NameLength4,
    SQLWCHAR  *FKSchemaName,   SQLSMALLINT NameLength5,
    SQLWCHAR  *FKTableName,    SQLSMALLINT NameLength6)
{
    char *pkcat = wchar_to_utf8(PKCatalogName, NameLength1);
    char *pksch = wchar_to_utf8(PKSchemaName,  NameLength2);
    char *pktbl = wchar_to_utf8(PKTableName,   NameLength3);
    char *fkcat = wchar_to_utf8(FKCatalogName, NameLength4);
    char *fksch = wchar_to_utf8(FKSchemaName,  NameLength5);
    char *fktbl = wchar_to_utf8(FKTableName,   NameLength6);

    SQLRETURN ret = SQLForeignKeys(
        StatementHandle,
        (SQLCHAR *)pkcat, pkcat ? SQL_NTS : 0,
        (SQLCHAR *)pksch, pksch ? SQL_NTS : 0,
        (SQLCHAR *)pktbl, pktbl ? SQL_NTS : 0,
        (SQLCHAR *)fkcat, fkcat ? SQL_NTS : 0,
        (SQLCHAR *)fksch, fksch ? SQL_NTS : 0,
        (SQLCHAR *)fktbl, fktbl ? SQL_NTS : 0);

    g_free(pkcat);
    g_free(pksch);
    g_free(pktbl);
    g_free(fkcat);
    g_free(fksch);
    g_free(fktbl);
    return ret;
}

/* ── SQLProceduresW ──────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLProceduresW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *ProcName,     SQLSMALLINT NameLength3)
{
    char *cat  = wchar_to_utf8(CatalogName, NameLength1);
    char *sch  = wchar_to_utf8(SchemaName,  NameLength2);
    char *proc = wchar_to_utf8(ProcName,    NameLength3);

    SQLRETURN ret = SQLProcedures(
        StatementHandle,
        (SQLCHAR *)cat,  cat  ? SQL_NTS : 0,
        (SQLCHAR *)sch,  sch  ? SQL_NTS : 0,
        (SQLCHAR *)proc, proc ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(proc);
    return ret;
}

/* ── SQLProcedureColumnsW ────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLProcedureColumnsW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *ProcName,     SQLSMALLINT NameLength3,
    SQLWCHAR  *ColumnName,   SQLSMALLINT NameLength4)
{
    char *cat  = wchar_to_utf8(CatalogName, NameLength1);
    char *sch  = wchar_to_utf8(SchemaName,  NameLength2);
    char *proc = wchar_to_utf8(ProcName,    NameLength3);
    char *col  = wchar_to_utf8(ColumnName,  NameLength4);

    SQLRETURN ret = SQLProcedureColumns(
        StatementHandle,
        (SQLCHAR *)cat,  cat  ? SQL_NTS : 0,
        (SQLCHAR *)sch,  sch  ? SQL_NTS : 0,
        (SQLCHAR *)proc, proc ? SQL_NTS : 0,
        (SQLCHAR *)col,  col  ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(proc);
    g_free(col);
    return ret;
}

/* ── SQLTablePrivilegesW ─────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLTablePrivilegesW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,    SQLSMALLINT NameLength3)
{
    char *cat = wchar_to_utf8(CatalogName, NameLength1);
    char *sch = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl = wchar_to_utf8(TableName,   NameLength3);

    SQLRETURN ret = SQLTablePrivileges(
        StatementHandle,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    return ret;
}

/* ── SQLColumnPrivilegesW ────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLColumnPrivilegesW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName,  SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,   SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,    SQLSMALLINT NameLength3,
    SQLWCHAR  *ColumnName,   SQLSMALLINT NameLength4)
{
    char *cat = wchar_to_utf8(CatalogName, NameLength1);
    char *sch = wchar_to_utf8(SchemaName,  NameLength2);
    char *tbl = wchar_to_utf8(TableName,   NameLength3);
    char *col = wchar_to_utf8(ColumnName,  NameLength4);

    SQLRETURN ret = SQLColumnPrivileges(
        StatementHandle,
        (SQLCHAR *)cat, cat ? SQL_NTS : 0,
        (SQLCHAR *)sch, sch ? SQL_NTS : 0,
        (SQLCHAR *)tbl, tbl ? SQL_NTS : 0,
        (SQLCHAR *)col, col ? SQL_NTS : 0);

    g_free(cat);
    g_free(sch);
    g_free(tbl);
    g_free(col);
    return ret;
}

/* ── SQLSetStmtAttrW ────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLSetStmtAttrW(
    SQLHSTMT   StatementHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength)
{
    /* Statement attributes are all numeric — pass through */
    return SQLSetStmtAttr(StatementHandle, Attribute, Value, StringLength);
}

/* ── SQLGetStmtAttrW ────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetStmtAttrW(
    SQLHSTMT   StatementHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
    /* Statement attributes are all numeric — pass through */
    return SQLGetStmtAttr(StatementHandle, Attribute, Value,
                          BufferLength, StringLength);
}

/* ── SQLSetEnvAttrW ─────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLSetEnvAttrW(
    SQLHENV    EnvironmentHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength)
{
    /* Environment attributes are all numeric — pass through */
    return SQLSetEnvAttr(EnvironmentHandle, Attribute, Value, StringLength);
}

/* ── SQLGetEnvAttrW ─────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetEnvAttrW(
    SQLHENV    EnvironmentHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
    /* Environment attributes are all numeric — pass through */
    return SQLGetEnvAttr(EnvironmentHandle, Attribute, Value,
                         BufferLength, StringLength);
}

/* ── SQLErrorW ───────────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLErrorW(
    SQLHENV   EnvironmentHandle,
    SQLHDBC   ConnectionHandle,
    SQLHSTMT  StatementHandle,
    SQLWCHAR *Sqlstate,
    SQLINTEGER *NativeError,
    SQLWCHAR *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength)
{
    SQLCHAR state_buf[6];
    SQLCHAR msg_buf[1024];
    SQLSMALLINT msg_len = 0;

    SQLRETURN ret = SQLError(
        EnvironmentHandle, ConnectionHandle, StatementHandle,
        state_buf, NativeError,
        msg_buf, sizeof(msg_buf), &msg_len);

    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (Sqlstate) {
            utf8_to_wchar(state_buf, SQL_NTS,
                           Sqlstate, 6 * (SQLSMALLINT)sizeof(SQLWCHAR));
        }
        if (MessageText && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                msg_buf, msg_len,
                MessageText, BufferLength);
            if (TextLength)
                *TextLength = wlen; /* ODBC spec: character count */
        } else if (TextLength) {
            SQLSMALLINT wlen = utf8_to_wchar(
                msg_buf, msg_len, NULL, 0);
            *TextLength = wlen;
        }
    }

    return ret;
}

/* ── SQLGetCursorNameW ───────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLGetCursorNameW(
    SQLHSTMT    StatementHandle,
    SQLWCHAR   *CursorName,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr)
{
    SQLCHAR name_buf[256];
    SQLSMALLINT name_len = 0;

    SQLRETURN ret = SQLGetCursorName(StatementHandle,
                                      name_buf, sizeof(name_buf), &name_len);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        SQLSMALLINT wlen = utf8_to_wchar(name_buf, name_len,
                                          CursorName, BufferLength);
        if (NameLengthPtr) *NameLengthPtr = wlen;
    }
    return ret;
}

/* ── SQLSetCursorNameW ───────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLSetCursorNameW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CursorName,
    SQLSMALLINT NameLength)
{
    char *utf8 = wchar_to_utf8(CursorName, NameLength);
    SQLRETURN ret = SQLSetCursorName(StatementHandle,
                                      (SQLCHAR *)utf8,
                                      utf8 ? SQL_NTS : 0);
    g_free(utf8);
    return ret;
}

/* ── SQLBrowseConnectW ───────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLBrowseConnectW(
    SQLHDBC     ConnectionHandle,
    SQLWCHAR   *InConnectionString,
    SQLSMALLINT StringLength1,
    SQLWCHAR   *OutConnectionString,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength2Ptr)
{
    char *in_utf8 = wchar_to_utf8(InConnectionString, StringLength1);

    SQLCHAR out_buf[2048];
    SQLSMALLINT out_len = 0;

    SQLRETURN ret = SQLBrowseConnect(
        ConnectionHandle,
        (SQLCHAR *)in_utf8, in_utf8 ? SQL_NTS : 0,
        out_buf, sizeof(out_buf), &out_len);

    g_free(in_utf8);

    if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO ||
         ret == SQL_NEED_DATA) &&
        OutConnectionString && BufferLength > 0) {
        SQLSMALLINT wlen = utf8_to_wchar(out_buf, out_len,
                                          OutConnectionString, BufferLength);
        if (StringLength2Ptr) *StringLength2Ptr = wlen;
    } else if (StringLength2Ptr) {
        *StringLength2Ptr = 0;
    }

    return ret;
}

/* ── SQLNativeSqlW ───────────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLNativeSqlW(
    SQLHDBC     ConnectionHandle,
    SQLWCHAR   *InStatementText,
    SQLINTEGER  TextLength1,
    SQLWCHAR   *OutStatementText,
    SQLINTEGER  BufferLength,
    SQLINTEGER *TextLength2Ptr)
{
    char *in_utf8 = wchar_to_utf8(InStatementText,
                                    TextLength1 == SQL_NTS ? SQL_NTS
                                    : (SQLSMALLINT)TextLength1);

    SQLCHAR out_buf[4096];
    SQLINTEGER out_len = 0;

    SQLRETURN ret = SQLNativeSql(
        ConnectionHandle,
        (SQLCHAR *)in_utf8, in_utf8 ? SQL_NTS : 0,
        out_buf, sizeof(out_buf), &out_len);

    g_free(in_utf8);

    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (OutStatementText && BufferLength > 0) {
            SQLSMALLINT wlen = utf8_to_wchar(
                out_buf, (SQLSMALLINT)(out_len < 4096 ? out_len : 4095),
                OutStatementText, (SQLSMALLINT)BufferLength);
            if (TextLength2Ptr) *TextLength2Ptr = (SQLINTEGER)wlen;
        } else if (TextLength2Ptr) {
            *TextLength2Ptr = 0;
        }
    }

    return ret;
}
