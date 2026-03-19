/*
 * DSN / odbc.ini resolver for SQLConnect.
 *
 * When SQLConnect receives a ServerName, we first check if it matches
 * a DSN in odbc.ini (via SQLGetPrivateProfileString from ODBC DM, or
 * by reading ~/.odbc.ini / /etc/odbc.ini directly with GKeyFile).
 *
 * If a DSN is found, its key-value pairs are applied to the DBC handle.
 * If no DSN is found, ServerName is treated as a literal hostname.
 */
#include "argus/handle.h"
#include "argus/compat.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

/* Forward declarations */
extern char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len);

/* ── Internal: apply a single DSN key-value pair to DBC ─────── */

static void apply_dsn_param(argus_dbc_t *dbc, const char *key, const char *val)
{
    if (!key || !val || !*val) return;

    if (strcasecmp(key, "HOST") == 0 || strcasecmp(key, "SERVER") == 0 ||
        strcasecmp(key, "ServerName") == 0) {
        free(dbc->host);
        dbc->host = strdup(val);
    } else if (strcasecmp(key, "PORT") == 0) {
        dbc->port = atoi(val);
    } else if (strcasecmp(key, "UID") == 0 || strcasecmp(key, "USERNAME") == 0 ||
               strcasecmp(key, "USER") == 0) {
        free(dbc->username);
        dbc->username = strdup(val);
    } else if (strcasecmp(key, "PWD") == 0 || strcasecmp(key, "PASSWORD") == 0) {
        free(dbc->password);
        dbc->password = strdup(val);
    } else if (strcasecmp(key, "DATABASE") == 0 || strcasecmp(key, "SCHEMA") == 0) {
        free(dbc->database);
        dbc->database = strdup(val);
    } else if (strcasecmp(key, "BACKEND") == 0 || strcasecmp(key, "DRIVER_TYPE") == 0) {
        free(dbc->backend_name);
        dbc->backend_name = strdup(val);
    } else if (strcasecmp(key, "SSL") == 0 || strcasecmp(key, "USESSL") == 0) {
        dbc->ssl_enabled = (strcmp(val, "1") == 0 ||
                            strcasecmp(val, "true") == 0 ||
                            strcasecmp(val, "yes") == 0);
    } else if (strcasecmp(key, "SSLCERTFILE") == 0) {
        free(dbc->ssl_cert_file);
        dbc->ssl_cert_file = strdup(val);
    } else if (strcasecmp(key, "SSLKEYFILE") == 0) {
        free(dbc->ssl_key_file);
        dbc->ssl_key_file = strdup(val);
    } else if (strcasecmp(key, "SSLCAFILE") == 0 || strcasecmp(key, "TRUSTEDCERTS") == 0) {
        free(dbc->ssl_ca_file);
        dbc->ssl_ca_file = strdup(val);
    } else if (strcasecmp(key, "SSLVERIFY") == 0) {
        dbc->ssl_verify = (strcmp(val, "1") == 0 ||
                           strcasecmp(val, "true") == 0 ||
                           strcasecmp(val, "yes") == 0);
    } else if (strcasecmp(key, "AUTHMECH") == 0 || strcasecmp(key, "AUTH") == 0) {
        free(dbc->auth_mechanism);
        dbc->auth_mechanism = strdup(val);
    } else if (strcasecmp(key, "RETRYCOUNT") == 0) {
        dbc->retry_count = atoi(val);
    } else if (strcasecmp(key, "RETRYDELAY") == 0) {
        dbc->retry_delay_sec = atoi(val);
    } else if (strcasecmp(key, "CONNECTTIMEOUT") == 0) {
        dbc->connect_timeout_sec = atoi(val);
    } else if (strcasecmp(key, "QUERYTIMEOUT") == 0) {
        dbc->query_timeout_sec = atoi(val);
    } else if (strcasecmp(key, "FETCHBUFFERSIZE") == 0) {
        dbc->fetch_buffer_size = atoi(val);
    } else if (strcasecmp(key, "LOGLEVEL") == 0) {
        dbc->log_level = atoi(val);
    } else if (strcasecmp(key, "LOGFILE") == 0) {
        free(dbc->log_file);
        dbc->log_file = strdup(val);
    }
}

/* ── Internal: try loading DSN from a specific ini file ──────── */

static bool load_dsn_from_file(argus_dbc_t *dbc, const char *dsn_name,
                                const char *ini_path)
{
    GKeyFile *kf = g_key_file_new();
    if (!g_key_file_load_from_file(kf, ini_path, G_KEY_FILE_NONE, NULL)) {
        g_key_file_free(kf);
        return false;
    }

    if (!g_key_file_has_group(kf, dsn_name)) {
        g_key_file_free(kf);
        return false;
    }

    ARGUS_LOG_DEBUG("DSN '%s' found in %s", dsn_name, ini_path);

    gsize n_keys = 0;
    gchar **keys = g_key_file_get_keys(kf, dsn_name, &n_keys, NULL);
    if (keys) {
        for (gsize i = 0; i < n_keys; i++) {
            gchar *val = g_key_file_get_string(kf, dsn_name, keys[i], NULL);
            if (val) {
                apply_dsn_param(dbc, keys[i], val);
                g_free(val);
            }
        }
        g_strfreev(keys);
    }

    g_key_file_free(kf);
    return true;
}

/* ── Public: resolve DSN name to connection parameters ──────── */

bool argus_resolve_dsn(argus_dbc_t *dbc, const char *dsn_name)
{
    if (!dsn_name || !*dsn_name) return false;

    /* Try user-level odbc.ini first, then system-level */
    const char *home = g_get_home_dir();
    if (home) {
        char user_ini[1024];
        snprintf(user_ini, sizeof(user_ini), "%s/.odbc.ini", home);
        if (load_dsn_from_file(dbc, dsn_name, user_ini))
            return true;
    }

    /* Also check ODBCINI environment variable */
    const char *env_ini = g_getenv("ODBCINI");
    if (env_ini && load_dsn_from_file(dbc, dsn_name, env_ini))
        return true;

    /* System-level */
    if (load_dsn_from_file(dbc, dsn_name, "/etc/odbc.ini"))
        return true;

    return false;
}
