#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef _WIN32
#define strcasecmp _stricmp
#include <windows.h>
#define sleep_seconds(n) Sleep((n) * 1000)
#else
#include <strings.h>  /* for strcasecmp */
#include <unistd.h>
#define sleep_seconds(n) sleep(n)
#endif

/* External string helper declarations */
extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);
extern char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len);
extern char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len);

/* ── Internal: perform the actual connection ─────────────────── */

static SQLRETURN do_connect(argus_dbc_t *dbc)
{
    /* Default backend depends on what was compiled in */
#ifdef ARGUS_HAS_THRIFT_BACKENDS
    const char *default_backend = "hive";
    int default_port = 10000;
#elif defined(ARGUS_HAS_TRINO)
    const char *default_backend = "trino";
    int default_port = 8080;
#else
    const char *default_backend = "";
    int default_port = 0;
#endif

    const char *backend_name = dbc->backend_name ? dbc->backend_name : default_backend;
    const argus_backend_t *backend = argus_backend_find(backend_name);
    if (!backend) {
        char msg[256];
        snprintf(msg, sizeof(msg),
                 "[Argus] Unknown backend: %s", backend_name);
        ARGUS_LOG_ERROR("Unknown backend: %s", backend_name);
        return argus_set_error(&dbc->diag, "HY000", msg, 0);
    }

    dbc->backend = backend;

    const char *host = dbc->host ? dbc->host : "localhost";
    int port = dbc->port > 0 ? dbc->port : default_port;
    const char *user = dbc->username ? dbc->username : "";
    const char *pass = dbc->password ? dbc->password : "";
    const char *db   = dbc->database ? dbc->database : "default";
    const char *auth = dbc->auth_mechanism ? dbc->auth_mechanism : "NOSASL";

    /* Retry logic: try up to (1 + retry_count) times */
    int max_attempts = 1 + (dbc->retry_count > 0 ? dbc->retry_count : 0);
    int rc = -1;

    for (int attempt = 1; attempt <= max_attempts; attempt++) {
        if (attempt > 1) {
            ARGUS_LOG_INFO("Retry attempt %d/%d after %d second(s)",
                           attempt, max_attempts, dbc->retry_delay_sec);
            argus_diag_clear(&dbc->diag);
            sleep_seconds(dbc->retry_delay_sec);
        }

        ARGUS_LOG_INFO("Connecting to %s backend at %s:%d (user=%s, db=%s, auth=%s) [attempt %d/%d]",
                       backend_name, host, port, user, db, auth, attempt, max_attempts);

        rc = backend->connect(dbc, host, port, user, pass, db, auth,
                              &dbc->backend_conn);
        if (rc == 0) {
            /* Success */
            ARGUS_LOG_INFO("Connected successfully to %s backend at %s:%d (attempt %d/%d)",
                           backend_name, host, port, attempt, max_attempts);
            dbc->connected = true;
            return SQL_SUCCESS;
        }

        /* Connection failed */
        ARGUS_LOG_WARN("Connection failed: backend=%s, host=%s:%d, rc=%d (attempt %d/%d)",
                       backend_name, host, port, rc, attempt, max_attempts);
    }

    /* All retry attempts exhausted */
    ARGUS_LOG_ERROR("Connection failed after %d attempt(s): backend=%s, host=%s:%d, rc=%d",
                    max_attempts, backend_name, host, port, rc);
    if (dbc->diag.count == 0) {
        argus_set_error(&dbc->diag, "08001",
                        "[Argus] Failed to connect to backend", 0);
    }
    dbc->backend = NULL;
    return SQL_ERROR;
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

    /* SSL/TLS parameters */
    v = argus_conn_params_get(&params, "SSL");
    if (!v) v = argus_conn_params_get(&params, "USESSL");
    if (v) {
        dbc->ssl_enabled = (strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 ||
                            strcasecmp(v, "yes") == 0);
    }

    v = argus_conn_params_get(&params, "SSLCERTFILE");
    if (v) { free(dbc->ssl_cert_file); dbc->ssl_cert_file = strdup(v); }

    v = argus_conn_params_get(&params, "SSLKEYFILE");
    if (v) { free(dbc->ssl_key_file); dbc->ssl_key_file = strdup(v); }

    v = argus_conn_params_get(&params, "SSLCAFILE");
    if (!v) v = argus_conn_params_get(&params, "TRUSTEDCERTS");
    if (v) { free(dbc->ssl_ca_file); dbc->ssl_ca_file = strdup(v); }

    v = argus_conn_params_get(&params, "SSLVERIFY");
    if (v) {
        dbc->ssl_verify = (strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 ||
                           strcasecmp(v, "yes") == 0);
    }

    /* Logging parameters */
    v = argus_conn_params_get(&params, "LOGLEVEL");
    if (v) dbc->log_level = atoi(v);

    v = argus_conn_params_get(&params, "LOGFILE");
    if (v) { free(dbc->log_file); dbc->log_file = strdup(v); }

    /* Additional connection parameters */
    v = argus_conn_params_get(&params, "APPLICATIONNAME");
    if (!v) v = argus_conn_params_get(&params, "APPNAME");
    if (v) { free(dbc->app_name); dbc->app_name = strdup(v); }

    v = argus_conn_params_get(&params, "FETCHBUFFERSIZE");
    if (v) dbc->fetch_buffer_size = atoi(v);

    v = argus_conn_params_get(&params, "SOCKETTIMEOUT");
    if (v) dbc->socket_timeout_sec = atoi(v);

    v = argus_conn_params_get(&params, "CONNECTTIMEOUT");
    if (v) dbc->connect_timeout_sec = atoi(v);

    v = argus_conn_params_get(&params, "QUERYTIMEOUT");
    if (v) dbc->query_timeout_sec = atoi(v);

    v = argus_conn_params_get(&params, "RETRYCOUNT");
    if (v) dbc->retry_count = atoi(v);

    v = argus_conn_params_get(&params, "RETRYDELAY");
    if (v) dbc->retry_delay_sec = atoi(v);

    v = argus_conn_params_get(&params, "HTTPPATH");
    if (v) { free(dbc->http_path); dbc->http_path = strdup(v); }

    v = argus_conn_params_get(&params, "TRINOPROTOCOL");
    if (!v) v = argus_conn_params_get(&params, "TRINO_PROTOCOL");
    if (v) {
        if (strcmp(v, "v2") == 0 || strcmp(v, "2") == 0)
            dbc->trino_protocol_version = 2;
        else
            dbc->trino_protocol_version = 1;
    }

    /* Apply logging settings if specified */
    if (dbc->log_level >= 0) {
        argus_log_set_level(dbc->log_level);
    }
    if (dbc->log_file) {
        argus_log_set_file(dbc->log_file);
    }

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

    ARGUS_LOG_INFO("Disconnecting from %s backend",
                   dbc->backend ? dbc->backend->name : "unknown");

    if (dbc->backend && dbc->backend_conn) {
        dbc->backend->disconnect(dbc->backend_conn);
    }

    dbc->backend_conn = NULL;
    dbc->backend      = NULL;
    dbc->connected    = false;

    ARGUS_LOG_DEBUG("Disconnected successfully");
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
