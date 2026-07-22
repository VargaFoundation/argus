#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"
#include "argus/compat.h"
#include "argus/log.h"
#include "argus/obs_hooks.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

#ifdef _WIN32
#include <windows.h>
#define sleep_seconds(n) Sleep((n) * 1000)
#else
#include <unistd.h>
#define sleep_seconds(n) sleep(n)
#endif

/* External string helper declarations */
extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);
extern char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len);
extern char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len);
extern bool argus_resolve_dsn(argus_dbc_t *dbc, const char *dsn_name);

/* ── Internal: redacted connection string for the observability taps ──
 * Copy of "k=v;k=v" with the value of any secret-bearing key (its name
 * contains PWD, PASSWORD, SECRET or TOKEN — deliberately over-broad, so an
 * endpoint URL may be masked but a secret never survives) replaced by "***".
 * The taps (argus/obs_hooks.h) only ever see this copy, never the raw string. */
static char *obs_redact_connstr(const char *s)
{
    if (!s) return NULL;
    GString *out = g_string_sized_new(strlen(s));
    const char *p = s;
    while (*p) {
        const char *pair_end = strchr(p, ';');
        if (!pair_end) pair_end = p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(pair_end - p));
        if (eq) {
            char key[64];
            size_t klen = (size_t)(eq - p) < sizeof(key) - 1
                              ? (size_t)(eq - p) : sizeof(key) - 1;
            for (size_t i = 0; i < klen; i++)
                key[i] = (char)g_ascii_toupper(p[i]);
            key[klen] = '\0';
            g_string_append_len(out, p, eq - p + 1);
            if (strstr(key, "PWD") || strstr(key, "PASSWORD") ||
                strstr(key, "SECRET") || strstr(key, "TOKEN"))
                g_string_append(out, "***");
            else
                g_string_append_len(out, eq + 1, pair_end - eq - 1);
        } else {
            g_string_append_len(out, p, pair_end - p);
        }
        if (*pair_end) g_string_append_c(out, ';');
        p = *pair_end ? pair_end + 1 : pair_end;
    }
    return g_string_free(out, FALSE);
}

/* ── Internal: multi-host + secret helpers for do_connect ────── */

/* Hosts a HOST=h1,h2,h3 failover list may carry (bare IPv6 addresses are not
 * supported in the list form — use a single HOST for IPv6). */
#define ARGUS_MAX_HOSTS 16

/* Split "host[:port]" into host_out; *port_inout only changes when a numeric
 * :port suffix is present. */
static void split_host_port(const char *entry, char *host_out, size_t out_size,
                            int *port_inout)
{
    g_strlcpy(host_out, entry, out_size);
    char *colon = strrchr(host_out, ':');
    if (colon && colon[1] &&
        strspn(colon + 1, "0123456789") == strlen(colon + 1)) {
        *colon = '\0';
        *port_inout = atoi(colon + 1);
    }
}

/* Resolve a ${scheme:ref} secret reference in place via the tap. The resolved
 * value replaces the field in process memory only — it is never written back
 * to a DSN or config file. No-op when there is no reference or no resolver. */
static void obs_resolve_secret_field(char **field)
{
    if (!field || !*field || !strstr(*field, "${")) return;
    char *resolved = argus_obs_hook_resolve_secret(*field);
    if (resolved && strcmp(resolved, *field) != 0) {
        argus_secure_free(*field);
        *field = resolved;
    } else {
        free(resolved);
    }
}

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

    /* Resolve ${scheme:ref} secret references at connect time (tap; the open
     * build is a no-op). */
    obs_resolve_secret_field(&dbc->password);
    obs_resolve_secret_field(&dbc->oauth_client_secret);

    const char *host_csv = dbc->host ? dbc->host : "localhost";
    int port = dbc->port > 0 ? dbc->port : default_port;
    const char *user = dbc->username ? dbc->username : "";
    const char *pass = dbc->password ? dbc->password : "";
    const char *db   = dbc->database ? dbc->database : "default";
    const char *auth = dbc->auth_mechanism ? dbc->auth_mechanism : "NOSASL";

    /* Per-BI-tool fetch preset when neither the DSN nor the app set one (tap). */
    if (dbc->fetch_buffer_size == 0 && dbc->app_name) {
        long preset = argus_obs_hook_fetch_preset(dbc->app_name);
        if (preset > 0) {
            dbc->fetch_buffer_size = (int)preset;
            ARGUS_LOG_DEBUG("Fetch preset for '%s': %ld rows",
                            dbc->app_name, preset);
        }
    }

    /* HOST may be a comma-separated failover list: "h1[:p1],h2[:p2],...".
     * One host is the common case and behaves exactly as before. */
    char **hosts = g_strsplit(host_csv, ",", -1);
    int nhosts = 0;
    while (hosts[nhosts]) { g_strstrip(hosts[nhosts]); nhosts++; }
    if (nhosts > ARGUS_MAX_HOSTS) nhosts = ARGUS_MAX_HOSTS;
    if (nhosts == 0 || !*hosts[0]) {
        g_strfreev(hosts);
        hosts = g_strsplit("localhost", ",", -1);
        nhosts = 1;
    }

    /* Try pool first if connection pooling is enabled (first host's key) */
    if (dbc->env && dbc->env->connection_pooling != SQL_CP_OFF) {
        char phost[256];
        int pport = port;
        split_host_port(hosts[0], phost, sizeof(phost), &pport);
        const argus_backend_t *pooled_backend = NULL;
        argus_backend_conn_t pooled_conn = argus_pool_acquire(
            phost, pport, backend_name, user, &pooled_backend);
        if (pooled_conn) {
            dbc->backend_conn = pooled_conn;
            dbc->backend = pooled_backend;
            dbc->connected = true;
            dbc->pooled = true;
            dbc->connect_time_ms = 0.0;
            free(dbc->connected_host);
            dbc->connected_host = strdup(phost);
            dbc->connected_port = pport;
            ARGUS_LOG_INFO("Acquired pooled connection to %s:%d", phost, pport);
            argus_obs_hook_connect(dbc, dbc->obs_connstr, backend_name, phost,
                                   user, 1, dbc->connect_time_ms);
            g_strfreev(hosts);
            return SQL_SUCCESS;
        }
    }

    /* Retry logic: up to (1 + retry_count) rounds; within a round, fail over
     * across every host (a tap may reorder; the driver guarantees each host
     * is tried at most once per round). */
    int max_attempts = 1 + (dbc->retry_count > 0 ? dbc->retry_count : 0);
    int rc = -1;
    gint64 connect_start = g_get_monotonic_time();
    char chosen[256] = "";
    int chosen_port = port;

    for (int attempt = 1; attempt <= max_attempts && rc != 0; attempt++) {
        if (attempt > 1) {
            ARGUS_LOG_INFO("Retry attempt %d/%d after %d second(s)",
                           attempt, max_attempts, dbc->retry_delay_sec);
            argus_diag_clear(&dbc->diag);
            sleep_seconds(dbc->retry_delay_sec);
        }

        gboolean tried[ARGUS_MAX_HOSTS] = { FALSE };
        for (int h = 0; h < nhosts && rc != 0; h++) {
            int idx = argus_obs_hook_pick_host(dbc, host_csv, nhosts);
            if (idx < 0 || idx >= nhosts || tried[idx]) {
                idx = -1;
                for (int k = 0; k < nhosts; k++)
                    if (!tried[k]) { idx = k; break; }
                if (idx < 0) break;
            }
            tried[idx] = TRUE;

            char hbuf[256];
            int hport = port;
            split_host_port(hosts[idx], hbuf, sizeof(hbuf), &hport);

            ARGUS_LOG_INFO("Connecting to %s backend at %s:%d (user=%s, db=%s, auth=%s) [attempt %d/%d]",
                           backend_name, hbuf, hport, user, db, auth, attempt, max_attempts);

            rc = backend->connect(dbc, hbuf, hport, user, pass, db, auth,
                                  &dbc->backend_conn);
            argus_obs_hook_host_result(dbc, host_csv, idx, rc == 0);
            if (rc == 0) {
                g_strlcpy(chosen, hbuf, sizeof(chosen));
                chosen_port = hport;
            } else {
                ARGUS_LOG_WARN("Connection failed: backend=%s, host=%s:%d, rc=%d (attempt %d/%d)",
                               backend_name, hbuf, hport, rc, attempt, max_attempts);
            }
        }
    }
    g_strfreev(hosts);

    if (rc == 0) {
        /* Success */
        gint64 connect_end = g_get_monotonic_time();
        dbc->connect_time_ms = (double)(connect_end - connect_start) / 1000.0;
        ARGUS_LOG_INFO("Connected successfully to %s backend at %s:%d (%.1f ms)",
                       backend_name, chosen, chosen_port, dbc->connect_time_ms);
        dbc->connected = true;
        free(dbc->connected_host);
        dbc->connected_host = strdup(chosen);
        dbc->connected_port = chosen_port;
        argus_obs_hook_connect(dbc, dbc->obs_connstr, backend_name, chosen,
                               user, 1, dbc->connect_time_ms);
        return SQL_SUCCESS;
    }

    /* All retry attempts exhausted */
    ARGUS_LOG_ERROR("Connection failed after %d attempt(s): backend=%s, host=%s, rc=%d",
                    max_attempts, backend_name, host_csv, rc);
    if (dbc->diag.count == 0) {
        argus_set_error(&dbc->diag, "08001",
                        "[Argus] Failed to connect to backend", 0);
    }
    argus_obs_hook_connect(dbc, dbc->obs_connstr, backend_name, host_csv,
                           user, 0, 0.0);
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

    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    if (dbc->connected) {
        return argus_set_error(&dbc->diag, "08002",
                               "[Argus] Already connected", 0);
    }

    /* Reject SQL_DRIVER_PROMPT — we have no UI dialog */
    if (DriverCompletion == SQL_DRIVER_PROMPT) {
        return argus_set_error(&dbc->diag, "HY000",
                               "[Argus] Driver does not support prompting", 0);
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

    /* If DSN= is specified, resolve from odbc.ini first */
    v = argus_conn_params_get(&params, "DSN");
    if (v && *v) {
        argus_resolve_dsn(dbc, v);
    }

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

    /* Kerberos SPN overrides (optional) */
    v = argus_conn_params_get(&params, "KRBSERVICENAME");
    if (!v) v = argus_conn_params_get(&params, "SERVICEPRINCIPALNAME");
    if (v) { free(dbc->krb_service_name); dbc->krb_service_name = strdup(v); }

    v = argus_conn_params_get(&params, "KRBHOSTFQDN");
    if (!v) v = argus_conn_params_get(&params, "KRBHOST");
    if (v) { free(dbc->krb_host_fqdn); dbc->krb_host_fqdn = strdup(v); }

    v = argus_conn_params_get(&params, "KRBREALM");
    if (!v) v = argus_conn_params_get(&params, "REALM");
    if (v) { free(dbc->krb_realm); dbc->krb_realm = strdup(v); }

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

    v = argus_conn_params_get(&params, "MAXSCROLLROWS");
    if (v) dbc->max_scroll_rows = atol(v);

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

    /* OAuth2 client-credentials (M2M) parameters (Trino) */
    v = argus_conn_params_get(&params, "OAUTH2TOKENENDPOINT");
    if (!v) v = argus_conn_params_get(&params, "TOKENURI");
    if (!v) v = argus_conn_params_get(&params, "TOKENURL");
    if (v) { free(dbc->oauth_token_url); dbc->oauth_token_url = strdup(v); }

    v = argus_conn_params_get(&params, "OAUTH2CLIENTID");
    if (!v) v = argus_conn_params_get(&params, "CLIENTID");
    if (v) { free(dbc->oauth_client_id); dbc->oauth_client_id = strdup(v); }

    v = argus_conn_params_get(&params, "OAUTH2CLIENTSECRET");
    if (!v) v = argus_conn_params_get(&params, "CLIENTSECRET");
    if (v) { free(dbc->oauth_client_secret); dbc->oauth_client_secret = strdup(v); }

    v = argus_conn_params_get(&params, "OAUTH2SCOPE");
    if (!v) v = argus_conn_params_get(&params, "SCOPE");
    if (v) { free(dbc->oauth_scope); dbc->oauth_scope = strdup(v); }

    v = argus_conn_params_get(&params, "OAUTH2DEVICEENDPOINT");
    if (!v) v = argus_conn_params_get(&params, "DEVICEAUTHURI");
    if (!v) v = argus_conn_params_get(&params, "DEVICEAUTHURL");
    if (v) { free(dbc->oauth_device_url); dbc->oauth_device_url = strdup(v); }

    v = argus_conn_params_get(&params, "OAUTH2AUTHENDPOINT");
    if (!v) v = argus_conn_params_get(&params, "AUTHURI");
    if (!v) v = argus_conn_params_get(&params, "AUTHURL");
    if (v) { free(dbc->oauth_auth_url); dbc->oauth_auth_url = strdup(v); }

    v = argus_conn_params_get(&params, "OAUTH2ISSUER");
    if (!v) v = argus_conn_params_get(&params, "OIDCISSUER");
    if (!v) v = argus_conn_params_get(&params, "ISSUER");
    if (v) { free(dbc->oauth_issuer); dbc->oauth_issuer = strdup(v); }

    /* BigQuery parameters — every Google URL is overridable so the driver
     * works against sovereign clouds (S3NS) and the emulator. */
    v = argus_conn_params_get(&params, "PROJECT");
    if (!v) v = argus_conn_params_get(&params, "BQPROJECT");
    if (!v) v = argus_conn_params_get(&params, "PROJECTID");
    if (v) { free(dbc->bq_project); dbc->bq_project = strdup(v); }

    v = argus_conn_params_get(&params, "BQLOCATION");
    if (!v) v = argus_conn_params_get(&params, "LOCATION");
    if (v) { free(dbc->bq_location); dbc->bq_location = strdup(v); }

    v = argus_conn_params_get(&params, "BQENDPOINT");
    if (v) { free(dbc->bq_endpoint); dbc->bq_endpoint = strdup(v); }

    v = argus_conn_params_get(&params, "BQTOKENENDPOINT");
    if (v) { free(dbc->bq_token_url); dbc->bq_token_url = strdup(v); }

    v = argus_conn_params_get(&params, "BQAUDIENCE");
    if (v) { free(dbc->bq_audience); dbc->bq_audience = strdup(v); }

    v = argus_conn_params_get(&params, "BQSCOPE");
    if (v) { free(dbc->bq_scope); dbc->bq_scope = strdup(v); }

    v = argus_conn_params_get(&params, "BQKEYFILE");
    if (!v) v = argus_conn_params_get(&params, "KEYFILEPATH");
    if (v) { free(dbc->bq_key_file); dbc->bq_key_file = strdup(v); }

    v = argus_conn_params_get(&params, "ACCESSTOKEN");
    if (!v) v = argus_conn_params_get(&params, "BQACCESSTOKEN");
    if (v) {
        argus_secure_free(dbc->bq_access_token);
        dbc->bq_access_token = strdup(v);
    }

    v = argus_conn_params_get(&params, "TRINOPROTOCOL");
    if (!v) v = argus_conn_params_get(&params, "TRINO_PROTOCOL");
    if (v) {
        if (strcmp(v, "v2") == 0 || strcmp(v, "2") == 0)
            dbc->trino_protocol_version = 2;
        else
            dbc->trino_protocol_version = 1;
    }

    /* Pool configuration keywords */
    {
        int pool_mpk = -1, pool_mt = -1, pool_it = -1, pool_ttl = -1;
        v = argus_conn_params_get(&params, "POOLMAXPERKEY");
        if (v) pool_mpk = atoi(v);
        v = argus_conn_params_get(&params, "POOLMAXTOTAL");
        if (v) pool_mt = atoi(v);
        v = argus_conn_params_get(&params, "POOLIDLETIMEOUT");
        if (v) pool_it = atoi(v);
        v = argus_conn_params_get(&params, "POOLTTL");
        if (v) pool_ttl = atoi(v);
        if (pool_mpk > 0 || pool_mt > 0 || pool_it >= 0 || pool_ttl >= 0)
            argus_pool_configure(pool_mpk, pool_mt, pool_it, pool_ttl);
    }

    /* Apply logging settings if specified */
    if (dbc->log_level >= 0) {
        argus_log_set_level(dbc->log_level);
    }
    if (dbc->log_file) {
        argus_log_set_file(dbc->log_file);
    }

    argus_conn_params_free(&params);

    /* The observability taps get a redacted copy — never the raw string. */
    free(dbc->obs_connstr);
    dbc->obs_connstr = obs_redact_connstr(conn_str);

    /* For SQL_DRIVER_COMPLETE[_REQUIRED], verify HOST is present */
    if ((DriverCompletion == SQL_DRIVER_COMPLETE ||
         DriverCompletion == SQL_DRIVER_COMPLETE_REQUIRED) && !dbc->host) {
        free(conn_str);
        return argus_set_error(&dbc->diag, "01S00",
                               "[Argus] HOST parameter required", 0);
    }

    /* Connect */
    SQLRETURN ret = do_connect(dbc);

    /* Build output connection string with password masked */
    if (OutConnectionString && BufferLength > 0) {
        /* Mask PWD= value in connection string for security */
        char *masked = strdup(conn_str);
        if (masked) {
            char *p = masked;
            while (*p) {
                if ((p == masked || *(p - 1) == ';') &&
                    (strncasecmp(p, "PWD=", 4) == 0 ||
                     strncasecmp(p, "PASSWORD=", 9) == 0)) {
                    char *val = strchr(p, '=');
                    if (val) {
                        val++;
                        char *val_end = strchr(val, ';');
                        if (!val_end) val_end = val + strlen(val);
                        for (char *c = val; c < val_end; c++)
                            *c = '*';
                        p = val_end;
                        continue;
                    }
                }
                p++;
            }
            SQLSMALLINT out_len = argus_copy_string(masked,
                                                     OutConnectionString,
                                                     BufferLength);
            if (StringLength2Ptr) *StringLength2Ptr = out_len;
            free(masked);
        }
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

    /* Try resolving ServerName as a DSN first */
    char *server = argus_str_dup_short(ServerName, NameLength1);
    if (server) {
        if (!argus_resolve_dsn(dbc, server)) {
            /* Not a DSN — treat as literal hostname */
            free(dbc->host);
            dbc->host = server;
        } else {
            free(server);
        }
    }

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

    argus_obs_hook_disconnect(dbc);

    if (dbc->backend && dbc->backend_conn) {
        /* Return to pool if pooling is enabled */
        if (dbc->env && dbc->env->connection_pooling != SQL_CP_OFF) {
            /* Release under the key the connection was acquired with: HOST
             * may be a failover list, so use the concrete connected host. */
            const char *host = dbc->connected_host ? dbc->connected_host
                               : (dbc->host ? dbc->host : "localhost");
            int port = dbc->connected_port > 0 ? dbc->connected_port
                                               : dbc->port;
            const char *user = dbc->username ? dbc->username : "";
            const char *bname = dbc->backend_name ? dbc->backend_name : "";
            argus_pool_release(host, port, bname, user,
                               dbc->backend, dbc->backend_conn);
        } else {
            dbc->backend->disconnect(dbc->backend_conn);
        }
    }

    dbc->backend_conn = NULL;
    dbc->backend      = NULL;
    dbc->connected    = false;
    dbc->pooled       = false;

    ARGUS_LOG_DEBUG("Disconnected successfully");
    return SQL_SUCCESS;
}

/* ── Internal: merge connection string keywords into dbc->browse_buf ── */

static void browse_merge(argus_dbc_t *dbc, const char *in_str)
{
    argus_conn_params_t incoming;
    argus_conn_params_init(&incoming);
    argus_conn_params_parse(&incoming, in_str);

    /* Parse existing accumulated keywords */
    argus_conn_params_t existing;
    argus_conn_params_init(&existing);
    if (dbc->browse_buf && *dbc->browse_buf)
        argus_conn_params_parse(&existing, dbc->browse_buf);

    /* Merge incoming into existing (incoming wins on conflict) */
    for (int i = 0; i < incoming.count; i++) {
        bool found = false;
        for (int j = 0; j < existing.count; j++) {
            if (strcasecmp(existing.params[j].key,
                           incoming.params[i].key) == 0) {
                free(existing.params[j].value);
                existing.params[j].value = strdup(incoming.params[i].value);
                found = true;
                break;
            }
        }
        if (!found) {
            /* Add new key */
            if (existing.count >= existing.capacity) {
                int new_cap = existing.capacity ? existing.capacity * 2 : 8;
                argus_conn_param_t *new_p = realloc(
                    existing.params,
                    (size_t)new_cap * sizeof(argus_conn_param_t));
                if (!new_p) continue;
                existing.params = new_p;
                existing.capacity = new_cap;
            }
            existing.params[existing.count].key =
                strdup(incoming.params[i].key);
            existing.params[existing.count].value =
                strdup(incoming.params[i].value);
            existing.count++;
        }
    }

    /* Rebuild browse_buf from merged params */
    free(dbc->browse_buf);
    size_t buf_size = 1;
    for (int i = 0; i < existing.count; i++)
        buf_size += strlen(existing.params[i].key) + 1 +
                    strlen(existing.params[i].value) + 1;
    dbc->browse_buf = malloc(buf_size);
    if (dbc->browse_buf) {
        char *dst = dbc->browse_buf;
        for (int i = 0; i < existing.count; i++) {
            if (i > 0) *dst++ = ';';
            size_t kl = strlen(existing.params[i].key);
            memcpy(dst, existing.params[i].key, kl);
            dst += kl;
            *dst++ = '=';
            size_t vl = strlen(existing.params[i].value);
            memcpy(dst, existing.params[i].value, vl);
            dst += vl;
        }
        *dst = '\0';
    }

    argus_conn_params_free(&incoming);
    argus_conn_params_free(&existing);
}

/* ── ODBC API: SQLBrowseConnect ──────────────────────────────── */

SQLRETURN SQL_API SQLBrowseConnect(
    SQLHDBC     ConnectionHandle,
    SQLCHAR    *InConnectionString,  SQLSMALLINT StringLength1,
    SQLCHAR    *OutConnectionString, SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength2Ptr)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    if (dbc->connected) {
        return argus_set_error(&dbc->diag, "08002",
                               "[Argus] Already connected", 0);
    }

    /* Merge incoming keywords with previously accumulated ones */
    char *in_str = argus_str_dup_short(InConnectionString, StringLength1);
    if (!in_str) {
        return argus_set_error(&dbc->diag, "HY001",
                               "[Argus] Memory allocation failed", 0);
    }
    browse_merge(dbc, in_str);
    free(in_str);

    /* Check which required keywords are present */
    argus_conn_params_t merged;
    argus_conn_params_init(&merged);
    if (dbc->browse_buf)
        argus_conn_params_parse(&merged, dbc->browse_buf);

    /* Required keywords and their descriptions */
    static const struct { const char *key; const char *alt; const char *desc; } required[] = {
        { "HOST",    "SERVER",      "Server hostname" },
        { "PORT",    NULL,          "Server port number" },
        { "BACKEND", "DRIVER_TYPE", "Backend type (hive,impala,trino,phoenix,kudu)" },
    };
    static const int num_required = 3;

    /* Build list of missing keywords */
    char missing_buf[512];
    char *mp = missing_buf;
    int missing_count = 0;

    for (int i = 0; i < num_required; i++) {
        const char *val = argus_conn_params_get(&merged, required[i].key);
        if ((!val || !*val) && required[i].alt)
            val = argus_conn_params_get(&merged, required[i].alt);
        if (!val || !*val) {
            int n = snprintf(mp, (size_t)(missing_buf + sizeof(missing_buf) - mp),
                             "%s%s:%s=?",
                             missing_count > 0 ? ";" : "",
                             required[i].key, required[i].desc);
            if (n > 0) mp += n;
            missing_count++;
        }
    }

    argus_conn_params_free(&merged);

    if (missing_count > 0) {
        /* Return SQL_NEED_DATA with browse result listing missing keywords */
        *mp = '\0';
        SQLSMALLINT out_len = (SQLSMALLINT)strlen(missing_buf);
        if (OutConnectionString && BufferLength > 0) {
            SQLSMALLINT copy = out_len < (BufferLength - 1)
                               ? out_len : (SQLSMALLINT)(BufferLength - 1);
            memcpy(OutConnectionString, missing_buf, (size_t)copy);
            OutConnectionString[copy] = '\0';
        }
        if (StringLength2Ptr) *StringLength2Ptr = out_len;
        return SQL_NEED_DATA;
    }

    /* All required keywords present — connect via SQLDriverConnect */
    SQLRETURN ret = SQLDriverConnect(
        ConnectionHandle, NULL,
        (SQLCHAR *)dbc->browse_buf,
        (SQLSMALLINT)strlen(dbc->browse_buf),
        OutConnectionString, BufferLength,
        StringLength2Ptr, SQL_DRIVER_NOPROMPT);

    /* Clean up browse buffer on success or error */
    free(dbc->browse_buf);
    dbc->browse_buf = NULL;

    return ret;
}
