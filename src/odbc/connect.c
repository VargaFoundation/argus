#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* External string helper declarations */
extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);
extern char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len);
extern char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len);

/* ── Internal: perform the actual connection ─────────────────── */

static SQLRETURN do_connect(argus_dbc_t *dbc)
{
    /* Default backend to hive */
    const char *backend_name = dbc->backend_name ? dbc->backend_name : "hive";
    const argus_backend_t *backend = argus_backend_find(backend_name);
    if (!backend) {
        char msg[256];
        snprintf(msg, sizeof(msg),
                 "[Argus] Unknown backend: %s", backend_name);
        return argus_set_error(&dbc->diag, "HY000", msg, 0);
    }

    dbc->backend = backend;

    const char *host = dbc->host ? dbc->host : "localhost";
    int port = dbc->port > 0 ? dbc->port : 10000;
    const char *user = dbc->username ? dbc->username : "";
    const char *pass = dbc->password ? dbc->password : "";
    const char *db   = dbc->database ? dbc->database : "default";
    const char *auth = dbc->auth_mechanism ? dbc->auth_mechanism : "NOSASL";

    int rc = backend->connect(dbc, host, port, user, pass, db, auth,
                              &dbc->backend_conn);
    if (rc != 0) {
        /* Backend should have set diagnostics */
        if (dbc->diag.count == 0) {
            argus_set_error(&dbc->diag, "08001",
                            "[Argus] Failed to connect to backend", 0);
        }
        dbc->backend = NULL;
        return SQL_ERROR;
    }

    dbc->connected = true;
    return SQL_SUCCESS;
}

/* ── ODBC API: SQLDriverConnect ──────────────────────────────── */

SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC      ConnectionHandle,
    SQLHWND      WindowHandle,
    SQLCHAR     *InConnectionString,
    SQLSMALLINT  StringLength1,
    SQLCHAR     *OutConnectionString,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength2Ptr,
    SQLUSMALLINT DriverCompletion)
{
    (void)WindowHandle;
    (void)DriverCompletion;

    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    if (dbc->connected) {
        return argus_set_error(&dbc->diag, "08002",
                               "[Argus] Already connected", 0);
    }

    /* Get the connection string */
    char *conn_str = argus_str_dup_short(InConnectionString, StringLength1);
    if (!conn_str) {
        return argus_set_error(&dbc->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }

    /* Parse connection string */
    argus_conn_params_t params;
    argus_conn_params_init(&params);
    if (argus_conn_params_parse(&params, conn_str) != 0) {
        free(conn_str);
        argus_conn_params_free(&params);
        return argus_set_error(&dbc->diag, "HY000",
                               "[Argus] Failed to parse connection string", 0);
    }

    /* Extract parameters */
    const char *v;

    v = argus_conn_params_get(&params, "HOST");
    if (!v) v = argus_conn_params_get(&params, "SERVER");
    if (v) { free(dbc->host); dbc->host = strdup(v); }

    v = argus_conn_params_get(&params, "PORT");
    if (v) dbc->port = atoi(v);

    v = argus_conn_params_get(&params, "UID");
    if (!v) v = argus_conn_params_get(&params, "USERNAME");
    if (!v) v = argus_conn_params_get(&params, "USER");
    if (v) { free(dbc->username); dbc->username = strdup(v); }

    v = argus_conn_params_get(&params, "PWD");
    if (!v) v = argus_conn_params_get(&params, "PASSWORD");
    if (v) { free(dbc->password); dbc->password = strdup(v); }

    v = argus_conn_params_get(&params, "DATABASE");
    if (!v) v = argus_conn_params_get(&params, "SCHEMA");
    if (v) { free(dbc->database); dbc->database = strdup(v); }

    v = argus_conn_params_get(&params, "AUTHMECH");
    if (!v) v = argus_conn_params_get(&params, "AUTH");
    if (v) { free(dbc->auth_mechanism); dbc->auth_mechanism = strdup(v); }

    v = argus_conn_params_get(&params, "BACKEND");
    if (!v) v = argus_conn_params_get(&params, "DRIVER_TYPE");
    if (v) { free(dbc->backend_name); dbc->backend_name = strdup(v); }

    argus_conn_params_free(&params);

    /* Connect */
    SQLRETURN ret = do_connect(dbc);

    /* Build output connection string */
    if (OutConnectionString && BufferLength > 0) {
        SQLSMALLINT out_len = argus_copy_string(conn_str,
                                                 OutConnectionString,
                                                 BufferLength);
        if (StringLength2Ptr) *StringLength2Ptr = out_len;
    } else if (StringLength2Ptr) {
        *StringLength2Ptr = (SQLSMALLINT)strlen(conn_str);
    }

    free(conn_str);
    return ret;
}

/* ── ODBC API: SQLConnect ────────────────────────────────────── */

SQLRETURN SQL_API SQLConnect(
    SQLHDBC   ConnectionHandle,
    SQLCHAR  *ServerName,    SQLSMALLINT NameLength1,
    SQLCHAR  *UserName,      SQLSMALLINT NameLength2,
    SQLCHAR  *Authentication, SQLSMALLINT NameLength3)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    if (dbc->connected) {
        return argus_set_error(&dbc->diag, "08002",
                               "[Argus] Already connected", 0);
    }

    /* ServerName is the DSN name; we treat it as host for now */
    char *server = argus_str_dup_short(ServerName, NameLength1);
    if (server) { free(dbc->host); dbc->host = server; }

    char *user = argus_str_dup_short(UserName, NameLength2);
    if (user) { free(dbc->username); dbc->username = user; }

    char *pass = argus_str_dup_short(Authentication, NameLength3);
    if (pass) { free(dbc->password); dbc->password = pass; }

    return do_connect(dbc);
}

/* ── ODBC API: SQLDisconnect ─────────────────────────────────── */

SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    if (!dbc->connected) {
        return argus_set_error(&dbc->diag, "08003",
                               "[Argus] Not connected", 0);
    }

    if (dbc->backend && dbc->backend_conn) {
        dbc->backend->disconnect(dbc->backend_conn);
    }

    dbc->backend_conn = NULL;
    dbc->backend      = NULL;
    dbc->connected    = false;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLBrowseConnect (stub) ───────────────────────── */

SQLRETURN SQL_API SQLBrowseConnect(
    SQLHDBC     ConnectionHandle,
    SQLCHAR    *InConnectionString,  SQLSMALLINT StringLength1,
    SQLCHAR    *OutConnectionString, SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength2Ptr)
{
    (void)InConnectionString;
    (void)StringLength1;
    (void)OutConnectionString;
    (void)BufferLength;
    (void)StringLength2Ptr;

    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;
    return argus_set_not_implemented(&dbc->diag, "SQLBrowseConnect");
}
