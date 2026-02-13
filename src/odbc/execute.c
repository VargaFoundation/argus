#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>

extern char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len);

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
    stmt->executed        = false;
    stmt->num_cols        = 0;
    stmt->metadata_fetched = false;
    stmt->fetch_started   = false;
    stmt->row_count       = -1;
    argus_row_cache_clear(&stmt->row_cache);

    /* Log query (truncate if very long) */
    if (strlen(query) > 100) {
        ARGUS_LOG_DEBUG("Executing query: %.100s...", query);
    } else {
        ARGUS_LOG_DEBUG("Executing query: %s", query);
    }

    /* Execute via backend */
    int rc = dbc->backend->execute(dbc->backend_conn, query, &stmt->op);
    if (rc != 0) {
        ARGUS_LOG_ERROR("Query execution failed: rc=%d, query=%.100s", rc, query);
        if (stmt->diag.count == 0) {
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Backend execution failed", 0);
        }
        return SQL_ERROR;
    }

    stmt->executed = true;
    ARGUS_LOG_DEBUG("Query executed successfully");

    /* Try to get result metadata */
    if (dbc->backend->get_result_metadata) {
        int ncols = 0;
        rc = dbc->backend->get_result_metadata(
            dbc->backend_conn, stmt->op,
            stmt->columns, &ncols);
        if (rc == 0 && ncols > 0) {
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

    argus_diag_clear(&stmt->diag);

    if (!StatementText) {
        return argus_set_error(&stmt->diag, "HY009",
                               "[Argus] NULL statement text", 0);
    }

    char *query = argus_str_dup(StatementText, TextLength);
    if (!query) {
        return argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }

    /* Store the query */
    free(stmt->query);
    stmt->query = query;

    return do_execute(stmt, query);
}

/* ── ODBC API: SQLPrepare ────────────────────────────────────── */

SQLRETURN SQL_API SQLPrepare(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *StatementText,
    SQLINTEGER TextLength)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (!StatementText) {
        return argus_set_error(&stmt->diag, "HY009",
                               "[Argus] NULL statement text", 0);
    }

    char *query = argus_str_dup(StatementText, TextLength);
    if (!query) {
        return argus_set_error(&stmt->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }

    free(stmt->query);
    stmt->query    = query;
    stmt->prepared = true;
    stmt->executed = false;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLExecute ────────────────────────────────────── */

SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);

    if (!stmt->query || !stmt->prepared) {
        return argus_set_error(&stmt->diag, "HY010",
                               "[Argus] No prepared statement", 0);
    }

    return do_execute(stmt, stmt->query);
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

    argus_diag_clear(&stmt->diag);

    /* Check if there's an active operation to cancel */
    if (!stmt->op || !stmt->executed) {
        /* Nothing to cancel */
        return SQL_SUCCESS;
    }

    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->backend || !dbc->backend->cancel) {
        return argus_set_error(&stmt->diag, "HYC00",
                               "[Argus] Cancel not supported by backend", 0);
    }

    ARGUS_LOG_INFO("Cancelling statement operation");

    /* Call backend cancel function */
    int rc = dbc->backend->cancel(dbc->backend_conn, stmt->op);
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

    /* Hive doesn't support multiple result sets */
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

    /* We don't support parameterized queries yet */
    if (ParameterCountPtr)
        *ParameterCountPtr = 0;
    return SQL_SUCCESS;
}
