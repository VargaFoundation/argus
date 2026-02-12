#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <stdlib.h>
#include <string.h>

/* ── Internal allocation functions ────────────────────────────── */

SQLRETURN argus_alloc_env(argus_env_t **out)
{
    argus_env_t *env = calloc(1, sizeof(argus_env_t));
    if (!env) return SQL_ERROR;

    env->signature          = ARGUS_ENV_SIGNATURE;
    env->odbc_version       = SQL_OV_ODBC3;
    env->connection_pooling = SQL_CP_OFF;
    argus_diag_clear(&env->diag);

    *out = env;
    return SQL_SUCCESS;
}

SQLRETURN argus_alloc_dbc(argus_env_t *env, argus_dbc_t **out)
{
    if (!argus_valid_env(env)) return SQL_INVALID_HANDLE;

    argus_dbc_t *dbc = calloc(1, sizeof(argus_dbc_t));
    if (!dbc) return SQL_ERROR;

    dbc->signature          = ARGUS_DBC_SIGNATURE;
    dbc->env                = env;
    dbc->connected          = false;
    dbc->login_timeout      = 0;
    dbc->connection_timeout = 0;
    dbc->access_mode        = SQL_MODE_READ_WRITE;
    dbc->autocommit         = SQL_AUTOCOMMIT_ON;
    argus_diag_clear(&dbc->diag);

    *out = dbc;
    return SQL_SUCCESS;
}

SQLRETURN argus_alloc_stmt(argus_dbc_t *dbc, argus_stmt_t **out)
{
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;
    if (!dbc->connected) {
        argus_set_error(&dbc->diag, "08003",
                        "[Argus] Connection not open", 0);
        return SQL_ERROR;
    }

    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    if (!stmt) return SQL_ERROR;

    stmt->signature       = ARGUS_STMT_SIGNATURE;
    stmt->dbc             = dbc;
    stmt->row_count       = -1;
    stmt->row_array_size  = 1;
    argus_diag_clear(&stmt->diag);
    argus_row_cache_init(&stmt->row_cache);

    *out = stmt;
    return SQL_SUCCESS;
}

/* ── Deallocation ─────────────────────────────────────────────── */

SQLRETURN argus_free_env(argus_env_t *env)
{
    if (!argus_valid_env(env)) return SQL_INVALID_HANDLE;
    env->signature = 0;
    free(env);
    return SQL_SUCCESS;
}

SQLRETURN argus_free_dbc(argus_dbc_t *dbc)
{
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    if (dbc->connected) {
        argus_set_error(&dbc->diag, "HY010",
                        "[Argus] Connection still open; call SQLDisconnect first",
                        0);
        return SQL_ERROR;
    }

    dbc->signature = 0;
    free(dbc->host);
    free(dbc->username);
    free(dbc->password);
    free(dbc->database);
    free(dbc->auth_mechanism);
    free(dbc->backend_name);
    free(dbc->current_catalog);
    free(dbc);
    return SQL_SUCCESS;
}

void argus_stmt_reset(argus_stmt_t *stmt)
{
    /* Close backend operation if active */
    if (stmt->op && stmt->dbc && stmt->dbc->backend) {
        stmt->dbc->backend->close_operation(
            stmt->dbc->backend_conn, stmt->op);
        stmt->op = NULL;
    }

    free(stmt->query);
    stmt->query           = NULL;
    stmt->prepared        = false;
    stmt->executed        = false;
    stmt->num_cols        = 0;
    stmt->metadata_fetched = false;
    stmt->fetch_started   = false;
    stmt->row_count       = -1;

    argus_row_cache_free(&stmt->row_cache);
    argus_row_cache_init(&stmt->row_cache);
}

SQLRETURN argus_free_stmt(argus_stmt_t *stmt)
{
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_stmt_reset(stmt);
    stmt->signature = 0;
    free(stmt);
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLAllocHandle ─────────────────────────────────── */

SQLRETURN SQL_API SQLAllocHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   InputHandle,
    SQLHANDLE  *OutputHandle)
{
    if (!OutputHandle) return SQL_ERROR;
    *OutputHandle = SQL_NULL_HANDLE;

    switch (HandleType) {
    case SQL_HANDLE_ENV:
        return argus_alloc_env((argus_env_t **)OutputHandle);

    case SQL_HANDLE_DBC:
        return argus_alloc_dbc((argus_env_t *)InputHandle,
                               (argus_dbc_t **)OutputHandle);

    case SQL_HANDLE_STMT:
        return argus_alloc_stmt((argus_dbc_t *)InputHandle,
                                (argus_stmt_t **)OutputHandle);

    default:
        return SQL_ERROR;
    }
}

/* ── ODBC API: SQLFreeHandle ──────────────────────────────────── */

SQLRETURN SQL_API SQLFreeHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle)
{
    switch (HandleType) {
    case SQL_HANDLE_ENV:
        return argus_free_env((argus_env_t *)Handle);
    case SQL_HANDLE_DBC:
        return argus_free_dbc((argus_dbc_t *)Handle);
    case SQL_HANDLE_STMT:
        return argus_free_stmt((argus_stmt_t *)Handle);
    default:
        return SQL_ERROR;
    }
}

/* ── ODBC API: SQLFreeStmt ────────────────────────────────────── */

SQLRETURN SQL_API SQLFreeStmt(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT Option)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    switch (Option) {
    case SQL_CLOSE:
        /* Close cursor / reset for re-execution */
        argus_stmt_reset(stmt);
        return SQL_SUCCESS;

    case SQL_DROP:
        return argus_free_stmt(stmt);

    case SQL_UNBIND:
        memset(stmt->bindings, 0, sizeof(stmt->bindings));
        return SQL_SUCCESS;

    case SQL_RESET_PARAMS:
        /* No-op for now (no parameter binding support yet) */
        return SQL_SUCCESS;

    default:
        return argus_set_error(&stmt->diag, "HY092",
                               "[Argus] Invalid option for SQLFreeStmt", 0);
    }
}
