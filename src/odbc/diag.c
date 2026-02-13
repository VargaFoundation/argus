#include "argus/error.h"
#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/log.h"
#include <string.h>
#include <stdio.h>

/* ── Internal diagnostic functions ────────────────────────────── */

void argus_diag_clear(argus_diag_t *diag)
{
    diag->count = 0;
    memset(diag->header_sqlstate, 0, sizeof(diag->header_sqlstate));
    diag->return_code = SQL_SUCCESS;
}

void argus_diag_push(argus_diag_t *diag,
                     const char *sqlstate,
                     const char *message,
                     SQLINTEGER native_error)
{
    if (diag->count >= ARGUS_MAX_DIAG_RECORDS) return;

    argus_diag_record_t *rec = &diag->records[diag->count];
    memset(rec, 0, sizeof(*rec));

    if (sqlstate)
        strncpy((char *)rec->sqlstate, sqlstate, ARGUS_MAX_SQLSTATE_LEN - 1);
    if (message)
        snprintf((char *)rec->message, ARGUS_MAX_MESSAGE_LEN, "%s", message);
    rec->native_error = native_error;

    /* First record also sets the header SQLSTATE */
    if (diag->count == 0 && sqlstate)
        strncpy((char *)diag->header_sqlstate, sqlstate,
                ARGUS_MAX_SQLSTATE_LEN - 1);

    diag->count++;
}

SQLRETURN argus_diag_get_rec(const argus_diag_t *diag,
                             SQLSMALLINT rec_number,
                             SQLCHAR *sqlstate,
                             SQLINTEGER *native_error,
                             SQLCHAR *message,
                             SQLSMALLINT buffer_length,
                             SQLSMALLINT *text_length)
{
    if (rec_number < 1 || rec_number > diag->count)
        return SQL_NO_DATA;

    const argus_diag_record_t *rec = &diag->records[rec_number - 1];

    if (sqlstate)
        memcpy(sqlstate, rec->sqlstate, ARGUS_MAX_SQLSTATE_LEN);

    if (native_error)
        *native_error = rec->native_error;

    SQLSMALLINT msg_len = (SQLSMALLINT)strlen((const char *)rec->message);
    if (text_length)
        *text_length = msg_len;

    if (message && buffer_length > 0) {
        SQLSMALLINT copy_len = msg_len < (buffer_length - 1)
                               ? msg_len : (buffer_length - 1);
        memcpy(message, rec->message, (size_t)copy_len);
        message[copy_len] = '\0';
    }

    return SQL_SUCCESS;
}

SQLRETURN argus_set_error(argus_diag_t *diag,
                          const char *sqlstate,
                          const char *message,
                          SQLINTEGER native_error)
{
    argus_diag_clear(diag);
    argus_diag_push(diag, sqlstate, message, native_error);
    diag->return_code = SQL_ERROR;
    ARGUS_LOG_ERROR("SQLSTATE=%s, native=%d, msg=%s",
                    sqlstate ? sqlstate : "(null)",
                    (int)native_error,
                    message ? message : "(null)");
    return SQL_ERROR;
}

SQLRETURN argus_set_not_implemented(argus_diag_t *diag,
                                    const char *func_name)
{
    char msg[ARGUS_MAX_MESSAGE_LEN];
    snprintf(msg, sizeof(msg),
             "[Argus] %s: Optional feature not implemented", func_name);
    return argus_set_error(diag, "HYC00", msg, 0);
}

/* ── Helper to get diag from any handle type ─────────────────── */

static argus_diag_t *get_diag_for_handle(SQLSMALLINT handle_type,
                                          SQLHANDLE handle)
{
    switch (handle_type) {
    case SQL_HANDLE_ENV:
        if (argus_valid_env(handle))
            return &((argus_env_t *)handle)->diag;
        break;
    case SQL_HANDLE_DBC:
        if (argus_valid_dbc(handle))
            return &((argus_dbc_t *)handle)->diag;
        break;
    case SQL_HANDLE_STMT:
        if (argus_valid_stmt(handle))
            return &((argus_stmt_t *)handle)->diag;
        break;
    }
    return NULL;
}

/* ── ODBC API: SQLGetDiagRec ─────────────────────────────────── */

SQLRETURN SQL_API SQLGetDiagRec(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLCHAR    *Sqlstate,
    SQLINTEGER *NativeError,
    SQLCHAR    *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength)
{
    argus_diag_t *diag = get_diag_for_handle(HandleType, Handle);
    if (!diag) return SQL_INVALID_HANDLE;

    return argus_diag_get_rec(diag, RecNumber, Sqlstate, NativeError,
                              MessageText, BufferLength, TextLength);
}

/* ── ODBC API: SQLGetDiagField ───────────────────────────────── */

SQLRETURN SQL_API SQLGetDiagField(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT DiagIdentifier,
    SQLPOINTER  DiagInfo,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength)
{
    argus_diag_t *diag = get_diag_for_handle(HandleType, Handle);
    if (!diag) return SQL_INVALID_HANDLE;

    /* Header fields (RecNumber == 0) */
    if (RecNumber == 0) {
        switch (DiagIdentifier) {
        case SQL_DIAG_NUMBER:
            if (DiagInfo) *(SQLINTEGER *)DiagInfo = diag->count;
            return SQL_SUCCESS;

        case SQL_DIAG_RETURNCODE:
            if (DiagInfo) *(SQLRETURN *)DiagInfo = diag->return_code;
            return SQL_SUCCESS;

        default:
            return SQL_ERROR;
        }
    }

    /* Record fields */
    if (RecNumber < 1 || RecNumber > diag->count)
        return SQL_NO_DATA;

    const argus_diag_record_t *rec = &diag->records[RecNumber - 1];

    switch (DiagIdentifier) {
    case SQL_DIAG_SQLSTATE:
        if (DiagInfo) {
            strncpy((char *)DiagInfo, (const char *)rec->sqlstate,
                    BufferLength > 0 ? (size_t)BufferLength : 0);
        }
        if (StringLength)
            *StringLength = (SQLSMALLINT)strlen((const char *)rec->sqlstate);
        return SQL_SUCCESS;

    case SQL_DIAG_NATIVE:
        if (DiagInfo) *(SQLINTEGER *)DiagInfo = rec->native_error;
        return SQL_SUCCESS;

    case SQL_DIAG_MESSAGE_TEXT: {
        SQLSMALLINT msg_len = (SQLSMALLINT)strlen((const char *)rec->message);
        if (StringLength) *StringLength = msg_len;
        if (DiagInfo && BufferLength > 0) {
            SQLSMALLINT copy = msg_len < (BufferLength - 1) ? msg_len : (BufferLength - 1);
            memcpy(DiagInfo, rec->message, (size_t)copy);
            ((char *)DiagInfo)[copy] = '\0';
        }
        return SQL_SUCCESS;
    }

    default:
        return SQL_ERROR;
    }
}

/* ── ODBC API: SQLError (ODBC 2.x compat) ───────────────────── */

SQLRETURN SQL_API SQLError(
    SQLHENV   EnvironmentHandle,
    SQLHDBC   ConnectionHandle,
    SQLHSTMT  StatementHandle,
    SQLCHAR  *Sqlstate,
    SQLINTEGER *NativeError,
    SQLCHAR  *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength)
{
    /* Find the most specific handle that's valid */
    argus_diag_t *diag = NULL;
    if (StatementHandle && argus_valid_stmt(StatementHandle))
        diag = &((argus_stmt_t *)StatementHandle)->diag;
    else if (ConnectionHandle && argus_valid_dbc(ConnectionHandle))
        diag = &((argus_dbc_t *)ConnectionHandle)->diag;
    else if (EnvironmentHandle && argus_valid_env(EnvironmentHandle))
        diag = &((argus_env_t *)EnvironmentHandle)->diag;

    if (!diag) return SQL_INVALID_HANDLE;

    /* Return first record and clear it */
    if (diag->count == 0)
        return SQL_NO_DATA;

    SQLRETURN ret = argus_diag_get_rec(diag, 1, Sqlstate, NativeError,
                                        MessageText, BufferLength, TextLength);

    /* Shift records down (ODBC 2.x consumes the record) */
    if (diag->count > 1) {
        memmove(&diag->records[0], &diag->records[1],
                (size_t)(diag->count - 1) * sizeof(argus_diag_record_t));
    }
    diag->count--;

    return ret;
}
