#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/compat.h"
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);

/* ── ODBC API: SQLSetEnvAttr ─────────────────────────────────── */

SQLRETURN SQL_API SQLSetEnvAttr(
    SQLHENV    EnvironmentHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength)
{
    (void)StringLength;

    argus_env_t *env = (argus_env_t *)EnvironmentHandle;
    if (!argus_valid_env(env)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&env->diag);

    switch (Attribute) {
    case SQL_ATTR_ODBC_VERSION:
        env->odbc_version = (SQLINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_CONNECTION_POOLING:
        env->connection_pooling = (SQLINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_CP_MATCH:
        /* Accept but ignore */
        return SQL_SUCCESS;

    case SQL_ATTR_OUTPUT_NTS:
        /* Always return NTS */
        return SQL_SUCCESS;

    default:
        return argus_set_error(&env->diag, "HY092",
                               "[Argus] Invalid attribute", 0);
    }
}

/* ── ODBC API: SQLGetEnvAttr ─────────────────────────────────── */

SQLRETURN SQL_API SQLGetEnvAttr(
    SQLHENV    EnvironmentHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
    (void)BufferLength;

    argus_env_t *env = (argus_env_t *)EnvironmentHandle;
    if (!argus_valid_env(env)) return SQL_INVALID_HANDLE;

    switch (Attribute) {
    case SQL_ATTR_ODBC_VERSION:
        if (Value) *(SQLINTEGER *)Value = env->odbc_version;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_CONNECTION_POOLING:
        if (Value) *(SQLINTEGER *)Value = env->connection_pooling;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_OUTPUT_NTS:
        if (Value) *(SQLINTEGER *)Value = SQL_TRUE;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;

    default:
        return argus_set_error(&env->diag, "HY092",
                               "[Argus] Invalid attribute", 0);
    }
}

/* ── ODBC API: SQLSetConnectAttr ─────────────────────────────── */

SQLRETURN SQL_API SQLSetConnectAttr(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    switch (Attribute) {
    case SQL_ATTR_LOGIN_TIMEOUT:
        dbc->login_timeout = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_CONNECTION_TIMEOUT:
        dbc->connection_timeout = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_ACCESS_MODE:
        dbc->access_mode = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_AUTOCOMMIT:
        dbc->autocommit = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_CURRENT_CATALOG:
        if (Value && StringLength > 0) {
            free(dbc->current_catalog);
            dbc->current_catalog = strndup((const char *)Value,
                                            (size_t)StringLength);
        } else if (Value) {
            free(dbc->current_catalog);
            dbc->current_catalog = strdup((const char *)Value);
        }
        return SQL_SUCCESS;

    case SQL_ATTR_ASYNC_ENABLE:
    case SQL_ATTR_METADATA_ID:
    case SQL_ATTR_QUIET_MODE:
    case SQL_ATTR_TRACE:
    case SQL_ATTR_TRACEFILE:
    case SQL_ATTR_TRANSLATE_LIB:
    case SQL_ATTR_TRANSLATE_OPTION:
    case SQL_ATTR_PACKET_SIZE:
        /* Accept but ignore */
        return SQL_SUCCESS;

    default:
        /* Accept unknown attributes silently for compatibility */
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLGetConnectAttr ─────────────────────────────── */

SQLRETURN SQL_API SQLGetConnectAttr(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    switch (Attribute) {
    case SQL_ATTR_LOGIN_TIMEOUT:
        if (Value) *(SQLUINTEGER *)Value = dbc->login_timeout;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_CONNECTION_TIMEOUT:
        if (Value) *(SQLUINTEGER *)Value = dbc->connection_timeout;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_ACCESS_MODE:
        if (Value) *(SQLUINTEGER *)Value = dbc->access_mode;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_AUTOCOMMIT:
        if (Value) *(SQLUINTEGER *)Value = dbc->autocommit;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_CURRENT_CATALOG: {
        const char *cat = dbc->current_catalog
                          ? dbc->current_catalog
                          : (dbc->database ? dbc->database : "default");
        SQLSMALLINT len = argus_copy_string(cat, (SQLCHAR *)Value,
                                             (SQLSMALLINT)BufferLength);
        if (StringLength) *StringLength = len;
        return SQL_SUCCESS;
    }

    case SQL_ATTR_CONNECTION_DEAD:
        if (Value) *(SQLUINTEGER *)Value = dbc->connected
                                            ? SQL_CD_FALSE : SQL_CD_TRUE;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;

    case SQL_ATTR_ASYNC_ENABLE:
        if (Value) *(SQLUINTEGER *)Value = SQL_ASYNC_ENABLE_OFF;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;

    default:
        /* Return 0 for unknown attributes */
        if (Value && BufferLength >= (SQLINTEGER)sizeof(SQLUINTEGER))
            *(SQLUINTEGER *)Value = 0;
        if (StringLength) *StringLength = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLSetStmtAttr ────────────────────────────────── */

SQLRETURN SQL_API SQLSetStmtAttr(
    SQLHSTMT   StatementHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength)
{
    (void)StringLength;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    switch (Attribute) {
    case SQL_ATTR_MAX_ROWS:
        stmt->max_rows = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_QUERY_TIMEOUT:
        stmt->query_timeout = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_ROW_ARRAY_SIZE:
        stmt->row_array_size = (SQLULEN)(uintptr_t)Value;
        if (stmt->row_array_size == 0) stmt->row_array_size = 1;
        return SQL_SUCCESS;

    case SQL_ATTR_ROWS_FETCHED_PTR:
        stmt->rows_fetched_ptr = (SQLULEN *)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_ROW_STATUS_PTR:
        stmt->row_status_ptr = (SQLUSMALLINT *)Value;
        return SQL_SUCCESS;

    case SQL_ATTR_CURSOR_TYPE:
    case SQL_ATTR_CONCURRENCY:
    case SQL_ATTR_CURSOR_SCROLLABLE:
    case SQL_ATTR_CURSOR_SENSITIVITY:
    case SQL_ATTR_USE_BOOKMARKS:
    case SQL_ATTR_NOSCAN:
    case SQL_ATTR_RETRIEVE_DATA:
    case SQL_ATTR_MAX_LENGTH:
    case SQL_ATTR_METADATA_ID:
    case SQL_ATTR_ASYNC_ENABLE:
    case SQL_ATTR_PARAM_BIND_TYPE:
    case SQL_ATTR_PARAMSET_SIZE:
    case SQL_ATTR_PARAM_STATUS_PTR:
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
    case SQL_ATTR_ROW_BIND_TYPE:
        /* Accept but ignore */
        return SQL_SUCCESS;

    default:
        /* Accept unknown attributes silently */
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLGetStmtAttr ────────────────────────────────── */

SQLRETURN SQL_API SQLGetStmtAttr(
    SQLHSTMT   StatementHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
    (void)BufferLength;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    switch (Attribute) {
    case SQL_ATTR_MAX_ROWS:
        if (Value) *(SQLULEN *)Value = stmt->max_rows;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_QUERY_TIMEOUT:
        if (Value) *(SQLULEN *)Value = stmt->query_timeout;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_ROW_ARRAY_SIZE:
        if (Value) *(SQLULEN *)Value = stmt->row_array_size;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_ROWS_FETCHED_PTR:
        if (Value) *(SQLULEN **)Value = stmt->rows_fetched_ptr;
        if (StringLength) *StringLength = sizeof(SQLULEN *);
        return SQL_SUCCESS;

    case SQL_ATTR_ROW_STATUS_PTR:
        if (Value) *(SQLUSMALLINT **)Value = stmt->row_status_ptr;
        if (StringLength) *StringLength = sizeof(SQLUSMALLINT *);
        return SQL_SUCCESS;

    case SQL_ATTR_CURSOR_TYPE:
        if (Value) *(SQLULEN *)Value = SQL_CURSOR_FORWARD_ONLY;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_CONCURRENCY:
        if (Value) *(SQLULEN *)Value = SQL_CONCUR_READ_ONLY;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_CURSOR_SCROLLABLE:
        if (Value) *(SQLULEN *)Value = SQL_NONSCROLLABLE;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_CURSOR_SENSITIVITY:
        if (Value) *(SQLULEN *)Value = SQL_UNSPECIFIED;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_ROW_NUMBER:
        if (Value) {
            if (stmt->row_cache.current_row > 0)
                *(SQLULEN *)Value = (SQLULEN)stmt->row_cache.current_row;
            else
                *(SQLULEN *)Value = 0;
        }
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_IMP_ROW_DESC:
    case SQL_ATTR_IMP_PARAM_DESC:
    case SQL_ATTR_APP_ROW_DESC:
    case SQL_ATTR_APP_PARAM_DESC:
        /* Descriptor handles not supported */
        if (Value) *(SQLPOINTER *)Value = NULL;
        return SQL_SUCCESS;

    case SQL_ATTR_USE_BOOKMARKS:
        if (Value) *(SQLULEN *)Value = SQL_UB_OFF;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_ASYNC_ENABLE:
        if (Value) *(SQLULEN *)Value = SQL_ASYNC_ENABLE_OFF;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_NOSCAN:
        if (Value) *(SQLULEN *)Value = SQL_NOSCAN_OFF;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    case SQL_ATTR_ROW_BIND_TYPE:
        if (Value) *(SQLULEN *)Value = SQL_BIND_BY_COLUMN;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;

    default:
        if (Value && BufferLength >= (SQLINTEGER)sizeof(SQLULEN))
            *(SQLULEN *)Value = 0;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLEndTran ────────────────────────────────────── */

SQLRETURN SQL_API SQLEndTran(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT CompletionType)
{
    (void)CompletionType;

    /* Hive doesn't support transactions - just return success */
    switch (HandleType) {
    case SQL_HANDLE_ENV:
        if (!argus_valid_env(Handle)) return SQL_INVALID_HANDLE;
        return SQL_SUCCESS;
    case SQL_HANDLE_DBC:
        if (!argus_valid_dbc(Handle)) return SQL_INVALID_HANDLE;
        return SQL_SUCCESS;
    default:
        return SQL_ERROR;
    }
}

/* ── ODBC API: SQLGetCursorName ──────────────────────────────── */

SQLRETURN SQL_API SQLGetCursorName(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CursorName,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    SQLSMALLINT len = argus_copy_string("ARGUS_CURSOR",
                                         CursorName, BufferLength);
    if (NameLengthPtr) *NameLengthPtr = len;
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLSetCursorName ──────────────────────────────── */

SQLRETURN SQL_API SQLSetCursorName(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CursorName,
    SQLSMALLINT NameLength)
{
    (void)CursorName;
    (void)NameLength;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    /* Accept but ignore - we use forward-only cursors */
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLCopyDesc (stub) ────────────────────────────── */

SQLRETURN SQL_API SQLCopyDesc(
    SQLHDESC SourceDescHandle,
    SQLHDESC TargetDescHandle)
{
    (void)SourceDescHandle;
    (void)TargetDescHandle;
    return SQL_ERROR;
}
