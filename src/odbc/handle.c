#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/compat.h"
#include "argus/log.h"
#include "argus/obs_hooks.h"
#include "argus/telemetry.h"
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
    dbc->telemetry_enabled  = false; /* opt-in; off unless TELEMETRY=1 */

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
    stmt->row_bind_type   = SQL_BIND_BY_COLUMN;
    stmt->noscan          = SQL_NOSCAN_OFF;   /* the driver translates escapes */
    stmt->paramset_size   = 1;
    stmt->param_bind_type = SQL_PARAM_BIND_BY_COLUMN;
    argus_diag_clear(&stmt->diag);
    argus_row_cache_init(&stmt->row_cache);

    /* Guardrail taps: cap rows / duration when the application has not set
     * stricter limits itself (the open build is a no-op). */
    {
        unsigned long g_rows = 0, g_timeout_ms = 0;
        if (argus_obs_hook_guards(dbc, &g_rows, &g_timeout_ms)) {
            if (g_rows > 0 && stmt->max_rows == 0)
                stmt->max_rows = (SQLULEN)g_rows;
            if (g_timeout_ms > 0 && stmt->query_timeout == 0)
                stmt->query_timeout = (SQLULEN)((g_timeout_ms + 999) / 1000);
        }
    }

    /* The four implicit descriptors are distinct handles that view this
     * statement's data. active_ard starts as the implicit ARD; associating an
     * explicit one later re-points it (and stmt->bindings) at that. */
    stmt->desc_ard.signature = ARGUS_DESC_SIGNATURE;
    stmt->desc_ard.type = ARGUS_DESC_ARD;
    stmt->desc_ard.stmt = stmt;
    stmt->desc_apd.signature = ARGUS_DESC_SIGNATURE;
    stmt->desc_apd.type = ARGUS_DESC_APD;
    stmt->desc_apd.stmt = stmt;
    stmt->desc_ird.signature = ARGUS_DESC_SIGNATURE;
    stmt->desc_ird.type = ARGUS_DESC_IRD;
    stmt->desc_ird.stmt = stmt;
    stmt->desc_ipd.signature = ARGUS_DESC_SIGNATURE;
    stmt->desc_ipd.type = ARGUS_DESC_IPD;
    stmt->desc_ipd.stmt = stmt;
    stmt->active_ard = &stmt->desc_ard;

    /* Pre-allocate columns and bindings for common case */
    if (argus_stmt_ensure_columns(stmt, 64) != 0 ||
        argus_stmt_ensure_bindings(stmt, 64) != 0) {
        free(stmt->columns);
        free(stmt->bindings);
        free(stmt);
        return SQL_ERROR;
    }

    /* The implicit ARD owns the array ensure_bindings just made active. */
    stmt->implicit_bindings = stmt->bindings;
    stmt->implicit_bindings_capacity = stmt->bindings_capacity;

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

    /* Keep whichever ARD owns this array in sync with the realloc, so its
     * `records` pointer never dangles: the explicit one when associated, else
     * the statement's own implicit array. */
    if (stmt->active_ard && stmt->active_ard->is_explicit) {
        stmt->active_ard->records = p;
        stmt->active_ard->record_capacity = cap;
    } else {
        stmt->implicit_bindings = p;
        stmt->implicit_bindings_capacity = cap;
    }
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
    free(dbc->krb_service_name);
    free(dbc->krb_host_fqdn);
    free(dbc->krb_realm);
    free(dbc->backend_name);
    free(dbc->current_catalog);
    free(dbc->obs_connstr);
    free(dbc->connected_host);

    /* Free SSL/TLS fields */
    free(dbc->ssl_cert_file);
    free(dbc->ssl_key_file);
    free(dbc->ssl_ca_file);

    /* Free additional connection parameters */
    free(dbc->app_name);
    free(dbc->http_path);
    free(dbc->log_file);
    free(dbc->oauth_token_url);
    free(dbc->oauth_client_id);
    free(dbc->oauth_client_secret);
    free(dbc->oauth_scope);
    free(dbc->oauth_device_url);
    free(dbc->oauth_auth_url);
    free(dbc->oauth_issuer);
    free(dbc->bq_project);
    free(dbc->bq_location);
    free(dbc->bq_endpoint);
    free(dbc->bq_token_url);
    free(dbc->bq_audience);
    free(dbc->bq_scope);
    free(dbc->bq_key_file);
    argus_secure_free(dbc->bq_access_token);

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

    /* Reset async state. A worker thread may still be running an execute and
     * owns async_query and the execution fields, so it must be joined before
     * anything here is torn down. */
    if (stmt->async_thread) {
        g_thread_join(stmt->async_thread);
        stmt->async_thread = NULL;
    }
    stmt->async_state = ARGUS_ASYNC_IDLE;
    g_atomic_int_set(&stmt->async_done, 0);
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
        /* Observability tap: one aggregate event per statement handle.
         * Only the SQLSTATE is reported for errors, never the message text. */
        argus_obs_hook_statement(
            stmt->dbc,
            stmt->dbc->backend ? stmt->dbc->backend->name
                               : stmt->dbc->backend_name,
            stmt->query,
            stmt->execute_time_ms,
            stmt->rows_fetched_total,
            0,
            stmt->errors_total > 0 && stmt->diag.count > 0
                ? (const char *)stmt->diag.records[0].sqlstate
                : "00000");
        argus_telemetry_statement(stmt->dbc, stmt->execute_time_ms,
                                  stmt->rows_fetched_total, stmt->errors_total);
    }

    argus_stmt_reset(stmt);
    g_mutex_clear(&stmt->mutex);
    free(stmt->cursor_name);
    free(stmt->columns);
    /* Free the statement's own array, not stmt->bindings, which may currently
     * point at an explicitly-associated descriptor the application still owns
     * and will free with SQLFreeHandle(SQL_HANDLE_DESC). */
    free(stmt->implicit_bindings);
    stmt->signature = 0;
    free(stmt);
    return SQL_SUCCESS;
}

/* ── Explicit descriptors ─────────────────────────────────────────
 * An application allocates these on a connection and may associate one as a
 * statement's ARD or APD via SQLSetStmtAttr. It carries its own record array;
 * the implicit descriptors, by contrast, are embedded in the statement. */
SQLRETURN argus_alloc_desc(argus_dbc_t *dbc, argus_desc_t **out)
{
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;
    if (!out) return SQL_ERROR;
    *out = NULL;

    argus_desc_t *desc = calloc(1, sizeof(argus_desc_t));
    if (!desc) return SQL_ERROR;

    desc->signature   = ARGUS_DESC_SIGNATURE;
    /* Explicitly-allocated descriptors are application descriptors; ODBC lets
     * one serve as either an ARD or an APD depending on where it is set. ARD is
     * the neutral default until associated. */
    desc->type        = ARGUS_DESC_ARD;
    desc->is_explicit = true;
    desc->dbc         = dbc;
    argus_diag_clear(&desc->diag);

    *out = desc;
    return SQL_SUCCESS;
}

SQLRETURN argus_free_desc(argus_desc_t *desc)
{
    if (!argus_valid_desc(desc)) return SQL_INVALID_HANDLE;

    /* If this descriptor is still associated with a statement as its active
     * ARD, detach it first so the statement reverts to its own implicit array
     * rather than reading freed memory. */
    argus_stmt_t *stmt = desc->stmt;
    if (stmt && stmt->active_ard == desc) {
        stmt->bindings          = stmt->implicit_bindings;
        stmt->bindings_capacity = stmt->implicit_bindings_capacity;
        stmt->active_ard        = &stmt->desc_ard;
    }

    free(desc->records);
    desc->signature = 0;
    free(desc);
    return SQL_SUCCESS;
}

argus_stmt_t *argus_desc_stmt(SQLHANDLE handle)
{
    if (argus_valid_desc(handle))
        return ((argus_desc_t *)handle)->stmt;
    /* The Driver Manager may still route descriptor calls through the statement
     * handle for implicit descriptors — accept that for compatibility. */
    if (argus_valid_stmt(handle))
        return (argus_stmt_t *)handle;
    return NULL;
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

    case SQL_HANDLE_DESC:
        return argus_alloc_desc((argus_dbc_t *)InputHandle,
                                (argus_desc_t **)OutputHandle);

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
    case SQL_HANDLE_DESC:
        return argus_free_desc((argus_desc_t *)Handle);
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
