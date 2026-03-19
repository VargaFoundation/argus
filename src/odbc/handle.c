#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/compat.h"
#include "argus/log.h"
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
    g_mutex_init(&dbc->mutex);
    dbc->connected          = false;
    dbc->login_timeout      = 0;
    dbc->connection_timeout = 0;
    dbc->access_mode        = SQL_MODE_READ_WRITE;
    dbc->autocommit         = SQL_AUTOCOMMIT_ON;
    argus_diag_clear(&dbc->diag);

    /* Initialize SSL/TLS defaults */
    dbc->ssl_enabled        = false;
    dbc->ssl_verify         = true;  /* Verify SSL certs by default */

    /* Initialize additional connection parameters */
    dbc->fetch_buffer_size  = 0;     /* 0 means use backend default */
    dbc->retry_count        = 0;     /* No retries by default */
    dbc->retry_delay_sec    = 2;     /* 2 second delay between retries */
    dbc->socket_timeout_sec = 0;     /* 0 means no timeout */
    dbc->connect_timeout_sec = 0;
    dbc->query_timeout_sec  = 0;
    dbc->log_level          = -1;    /* -1 means not set (use global) */

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
    g_mutex_init(&stmt->mutex);
    stmt->row_count       = -1;
    stmt->row_array_size  = 1;
    stmt->paramset_size   = 1;
    stmt->param_bind_type = SQL_PARAM_BIND_BY_COLUMN;
    argus_diag_clear(&stmt->diag);
    argus_row_cache_init(&stmt->row_cache);

    /* Pre-allocate columns and bindings for common case */
    if (argus_stmt_ensure_columns(stmt, 64) != 0 ||
        argus_stmt_ensure_bindings(stmt, 64) != 0) {
        free(stmt->columns);
        free(stmt->bindings);
        free(stmt);
        return SQL_ERROR;
    }

    *out = stmt;
    return SQL_SUCCESS;
}

/* ── Dynamic column/binding capacity ─────────────────────────── */

int argus_stmt_ensure_columns(argus_stmt_t *stmt, int ncols)
{
    if (ncols <= stmt->columns_capacity) return 0;
    int cap = ncols < 64 ? 64 : ncols;
    argus_column_desc_t *p = realloc(
        stmt->columns, (size_t)cap * sizeof(argus_column_desc_t));
    if (!p) return -1;
    memset(p + stmt->columns_capacity, 0,
           (size_t)(cap - stmt->columns_capacity) * sizeof(argus_column_desc_t));
    stmt->columns = p;
    stmt->columns_capacity = cap;
    return 0;
}

int argus_stmt_ensure_bindings(argus_stmt_t *stmt, int ncols)
{
    if (ncols <= stmt->bindings_capacity) return 0;
    int cap = ncols < 64 ? 64 : ncols;
    argus_col_binding_t *p = realloc(
        stmt->bindings, (size_t)cap * sizeof(argus_col_binding_t));
    if (!p) return -1;
    memset(p + stmt->bindings_capacity, 0,
           (size_t)(cap - stmt->bindings_capacity) * sizeof(argus_col_binding_t));
    stmt->bindings = p;
    stmt->bindings_capacity = cap;
    return 0;
}

/* ── Deallocation ─────────────────────────────────────────────── */

SQLRETURN argus_free_env(argus_env_t *env)
{
    if (!argus_valid_env(env)) return SQL_INVALID_HANDLE;
    argus_pool_cleanup();
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

    g_mutex_clear(&dbc->mutex);
    dbc->signature = 0;
    free(dbc->host);
    free(dbc->username);
    argus_secure_free(dbc->password);
    free(dbc->database);
    free(dbc->auth_mechanism);
    free(dbc->backend_name);
    free(dbc->current_catalog);

    /* Free SSL/TLS fields */
    free(dbc->ssl_cert_file);
    free(dbc->ssl_key_file);
    free(dbc->ssl_ca_file);

    /* Free additional connection parameters */
    free(dbc->app_name);
    free(dbc->http_path);
    free(dbc->log_file);

    /* Free browse buffer */
    free(dbc->browse_buf);

    /* Free metadata cache */
    argus_metadata_cache_free(dbc);

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

    /* Save num_cols before clearing for scroll cache cleanup */
    int saved_num_cols = stmt->num_cols;

    free(stmt->query);
    stmt->query           = NULL;
    stmt->prepared        = false;
    stmt->executed        = false;
    stmt->num_cols        = 0;
    stmt->metadata_fetched = false;
    stmt->fetch_started   = false;
    stmt->row_count       = -1;
    stmt->getdata_col     = 0;
    stmt->getdata_offset  = 0;

    argus_row_cache_free(&stmt->row_cache);
    argus_row_cache_init(&stmt->row_cache);

    /* Free scroll cache */
    if (stmt->scroll_rows) {
        int nc = saved_num_cols > 0 ? saved_num_cols
                                     : stmt->row_cache.num_cols;
        for (size_t i = 0; i < stmt->scroll_row_count; i++) {
            argus_row_t *row = &stmt->scroll_rows[i];
            if (row->cells) {
                for (int c = 0; c < nc; c++)
                    free(row->cells[c].data);
                free(row->cells);
            }
        }
        free(stmt->scroll_rows);
        stmt->scroll_rows = NULL;
    }
    stmt->scroll_row_count = 0;
    stmt->scroll_position  = 0;
    stmt->scroll_cached    = false;

    /* Reset async state */
    stmt->async_state = ARGUS_ASYNC_IDLE;
    free(stmt->async_query);
    stmt->async_query = NULL;

    /* Reset DAE state */
    stmt->dae_state = ARGUS_DAE_IDLE;
    stmt->dae_current_param = -1;
    if (stmt->dae_buffer) {
        g_byte_array_free(stmt->dae_buffer, TRUE);
        stmt->dae_buffer = NULL;
    }

    /* Reset parameter bindings */
    memset(stmt->param_bindings, 0, sizeof(stmt->param_bindings));
    stmt->num_param_bindings = 0;
    stmt->paramset_size = 1;

    /* Reset column bindings (keep allocation) */
    if (stmt->bindings && stmt->bindings_capacity > 0)
        memset(stmt->bindings, 0,
               (size_t)stmt->bindings_capacity * sizeof(argus_col_binding_t));
}

SQLRETURN argus_free_stmt(argus_stmt_t *stmt)
{
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    /* Log metrics at INFO level before cleanup */
    if (stmt->rows_fetched_total > 0 || stmt->execute_time_ms > 0) {
        ARGUS_LOG_INFO("Statement metrics: execute=%.1f ms, "
                       "rows_fetched=%lu, errors=%lu",
                       stmt->execute_time_ms,
                       stmt->rows_fetched_total,
                       stmt->errors_total);
    }

    argus_stmt_reset(stmt);
    g_mutex_clear(&stmt->mutex);
    free(stmt->columns);
    free(stmt->bindings);
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

/* ── ODBC 2.x compatibility: SQLAllocEnv ──────────────────────── */

SQLRETURN SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle)
{
    return SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,
                          (SQLHANDLE *)EnvironmentHandle);
}

/* ── ODBC 2.x compatibility: SQLFreeEnv ──────────────────────── */

SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle)
{
    return SQLFreeHandle(SQL_HANDLE_ENV, (SQLHANDLE)EnvironmentHandle);
}

/* ── ODBC 2.x compatibility: SQLAllocConnect ─────────────────── */

SQLRETURN SQL_API SQLAllocConnect(
    SQLHENV  EnvironmentHandle,
    SQLHDBC *ConnectionHandle)
{
    return SQLAllocHandle(SQL_HANDLE_DBC, (SQLHANDLE)EnvironmentHandle,
                          (SQLHANDLE *)ConnectionHandle);
}

/* ── ODBC 2.x compatibility: SQLFreeConnect ──────────────────── */

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle)
{
    return SQLFreeHandle(SQL_HANDLE_DBC, (SQLHANDLE)ConnectionHandle);
}

/* ── ODBC 2.x compatibility: SQLAllocStmt ────────────────────── */

SQLRETURN SQL_API SQLAllocStmt(
    SQLHDBC   ConnectionHandle,
    SQLHSTMT *StatementHandle)
{
    return SQLAllocHandle(SQL_HANDLE_STMT, (SQLHANDLE)ConnectionHandle,
                          (SQLHANDLE *)StatementHandle);
}

/* ── ODBC 2.x compatibility: SQLTransact ─────────────────────── */

SQLRETURN SQL_API SQLTransact(
    SQLHENV EnvironmentHandle,
    SQLHDBC ConnectionHandle,
    SQLUSMALLINT CompletionType)
{
    if (ConnectionHandle && ConnectionHandle != SQL_NULL_HDBC)
        return SQLEndTran(SQL_HANDLE_DBC, (SQLHANDLE)ConnectionHandle,
                          (SQLSMALLINT)CompletionType);
    if (EnvironmentHandle && EnvironmentHandle != SQL_NULL_HENV)
        return SQLEndTran(SQL_HANDLE_ENV, (SQLHANDLE)EnvironmentHandle,
                          (SQLSMALLINT)CompletionType);
    return SQL_INVALID_HANDLE;
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
        if (stmt->bindings && stmt->bindings_capacity > 0)
            memset(stmt->bindings, 0,
                   (size_t)stmt->bindings_capacity * sizeof(argus_col_binding_t));
        return SQL_SUCCESS;

    case SQL_RESET_PARAMS:
        memset(stmt->param_bindings, 0, sizeof(stmt->param_bindings));
        stmt->num_param_bindings = 0;
        return SQL_SUCCESS;

    default:
        return argus_set_error(&stmt->diag, "HY092",
                               "[Argus] Invalid option for SQLFreeStmt", 0);
    }
}
