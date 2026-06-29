#include "trino_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <unistd.h>

/* Forward declarations for the OAuth2 token-refresh path. */
static int trino_refresh_oauth_token(trino_conn_t *conn);
static int trino_fetch_device_token(trino_conn_t *conn, char **out_token);

/* ── Helper: Apply SSL and timeout settings to curl ─────────────── */

static void trino_apply_curl_settings(trino_conn_t *conn, CURL *curl)
{
    /* SSL/TLS settings */
    if (conn->ssl_enabled) {
        if (conn->ssl_verify) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        } else {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        }

        if (conn->ssl_cert_file) {
            curl_easy_setopt(curl, CURLOPT_SSLCERT, conn->ssl_cert_file);
        }
        if (conn->ssl_key_file) {
            curl_easy_setopt(curl, CURLOPT_SSLKEY, conn->ssl_key_file);
        }
        if (conn->ssl_ca_file) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, conn->ssl_ca_file);
        }
    }

    /* Timeout settings */
    if (conn->connect_timeout_sec > 0) {
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long)conn->connect_timeout_sec);
    }
    if (conn->query_timeout_sec > 0) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)conn->query_timeout_sec);
    }

    /* Authentication. Bearer (JWT/OAuth2) is applied as a default header at
     * connect time; Basic and Negotiate are set on the easy handle here because
     * curl_easy_reset() wiped them at the start of each request. */
    switch (conn->auth_mode) {
    case TRINO_AUTH_BASIC: {
        char userpwd[512];
        snprintf(userpwd, sizeof(userpwd), "%s:%s",
                 conn->user ? conn->user : "",
                 conn->password ? conn->password : "");
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd);
        break;
    }
    case TRINO_AUTH_NEGOTIATE:
        /* Kerberos/SPNEGO via the ambient credential cache (kinit). */
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_NEGOTIATE);
        curl_easy_setopt(curl, CURLOPT_USERPWD, ":");
        break;
    case TRINO_AUTH_BEARER:
    case TRINO_AUTH_NONE:
    default:
        break;
    }
}

/* ── CURL write callback ─────────────────────────────────────── */

size_t trino_curl_write_cb(void *contents, size_t size, size_t nmemb,
                           void *userp)
{
    size_t total = size * nmemb;
    trino_response_t *resp = (trino_response_t *)userp;

    char *ptr = realloc(resp->data, resp->size + total + 1);
    if (!ptr) return 0;

    resp->data = ptr;
    memcpy(resp->data + resp->size, contents, total);
    resp->size += total;
    resp->data[resp->size] = '\0';

    return total;
}

/* ── HTTP helper: POST ───────────────────────────────────────── */

int trino_http_post(trino_conn_t *conn, const char *url, const char *body,
                    trino_response_t *resp)
{
    CURL *curl = conn->curl;

    curl_easy_reset(curl);
    trino_apply_curl_settings(conn, curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, trino_curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);

    resp->data = NULL;
    resp->size = 0;

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK)
        return -1;

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    /* OAuth2 (M2M) access token may have expired; refresh once and retry. */
    if (http_code == 401 && conn->oauth_m2m &&
        trino_refresh_oauth_token(conn) == 0) {
        free(resp->data);
        resp->data = NULL;
        resp->size = 0;
        curl_easy_reset(curl);
        trino_apply_curl_settings(conn, curl);
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, trino_curl_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
            return -1;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    }

    if (http_code >= 400)
        return -1;

    return 0;
}

/* ── HTTP helper: GET ────────────────────────────────────────── */

int trino_http_get(trino_conn_t *conn, const char *url,
                   trino_response_t *resp)
{
    CURL *curl = conn->curl;

    curl_easy_reset(curl);
    trino_apply_curl_settings(conn, curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, trino_curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);

    resp->data = NULL;
    resp->size = 0;

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK)
        return -1;

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    /* OAuth2 (M2M) access token may have expired; refresh once and retry. */
    if (http_code == 401 && conn->oauth_m2m &&
        trino_refresh_oauth_token(conn) == 0) {
        free(resp->data);
        resp->data = NULL;
        resp->size = 0;
        curl_easy_reset(curl);
        trino_apply_curl_settings(conn, curl);
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, trino_curl_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
            return -1;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    }

    if (http_code >= 400)
        return -1;

    return 0;
}

/* ── HTTP helper: DELETE ─────────────────────────────────────── */

int trino_http_delete(trino_conn_t *conn, const char *url)
{
    CURL *curl = conn->curl;

    curl_easy_reset(curl);
    trino_apply_curl_settings(conn, curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);

    CURLcode res = curl_easy_perform(curl);
    return (res == CURLE_OK) ? 0 : -1;
}

/* ── OAuth2 client-credentials: fetch an access token from the IdP ─── */

static int trino_fetch_oauth_token(trino_conn_t *conn, char **out_token)
{
    *out_token = NULL;
    if (!conn->oauth_token_url || !conn->oauth_client_id ||
        !conn->oauth_client_secret)
        return -1;

    CURL *c = curl_easy_init();
    if (!c) return -1;

    /* Build a client_secret_post body (widely accepted: Keycloak/Okta/Auth0). */
    char *eid = curl_easy_escape(c, conn->oauth_client_id, 0);
    char *esec = curl_easy_escape(c, conn->oauth_client_secret, 0);
    char *escope = conn->oauth_scope ? curl_easy_escape(c, conn->oauth_scope, 0) : NULL;
    char body[2048];
    if (escope)
        snprintf(body, sizeof(body),
                 "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
                 eid ? eid : "", esec ? esec : "", escope);
    else
        snprintf(body, sizeof(body),
                 "grant_type=client_credentials&client_id=%s&client_secret=%s",
                 eid ? eid : "", esec ? esec : "");

    struct curl_slist *hdrs = curl_slist_append(
        NULL, "Content-Type: application/x-www-form-urlencoded");
    trino_response_t resp = {0};

    curl_easy_setopt(c, CURLOPT_URL, conn->oauth_token_url);
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, trino_curl_write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);
    if (conn->ssl_enabled) {
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, conn->ssl_verify ? 1L : 0L);
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, conn->ssl_verify ? 2L : 0L);
        if (conn->ssl_ca_file) curl_easy_setopt(c, CURLOPT_CAINFO, conn->ssl_ca_file);
    }
    if (conn->connect_timeout_sec > 0)
        curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, (long)conn->connect_timeout_sec);

    CURLcode res = curl_easy_perform(c);
    long http_code = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);

    curl_free(eid);
    curl_free(esec);
    if (escope) curl_free(escope);
    curl_slist_free_all(hdrs);

    int ret = -1;
    if (res == CURLE_OK && http_code >= 200 && http_code < 300 && resp.data) {
        JsonParser *parser = json_parser_new();
        if (json_parser_load_from_data(parser, resp.data, (gssize)resp.size, NULL)) {
            JsonNode *root = json_parser_get_root(parser);
            if (root && JSON_NODE_HOLDS_OBJECT(root)) {
                JsonObject *obj = json_node_get_object(root);
                if (json_object_has_member(obj, "access_token")) {
                    const char *tok = json_object_get_string_member(obj, "access_token");
                    if (tok && *tok) { *out_token = strdup(tok); ret = 0; }
                }
            }
        }
        g_object_unref(parser);
    } else {
        ARGUS_LOG_ERROR("Trino OAuth2 token request failed (curl=%d, http=%ld)",
                        (int)res, http_code);
    }

    free(resp.data);
    curl_easy_cleanup(c);
    return ret;
}

/* POST an x-www-form-urlencoded body to an IdP endpoint, return parsed JSON. */
static JsonParser *trino_oauth_form_post(trino_conn_t *conn, const char *url,
                                         const char *body, long *http_code)
{
    CURL *c = curl_easy_init();
    if (!c) return NULL;
    struct curl_slist *hdrs = curl_slist_append(
        NULL, "Content-Type: application/x-www-form-urlencoded");
    trino_response_t resp = {0};
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, trino_curl_write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);
    if (conn->ssl_enabled) {
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, conn->ssl_verify ? 1L : 0L);
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, conn->ssl_verify ? 2L : 0L);
        if (conn->ssl_ca_file) curl_easy_setopt(c, CURLOPT_CAINFO, conn->ssl_ca_file);
    }
    if (conn->connect_timeout_sec > 0)
        curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, (long)conn->connect_timeout_sec);
    CURLcode res = curl_easy_perform(c);
    long code = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
    if (http_code) *http_code = code;
    JsonParser *p = NULL;
    if (res == CURLE_OK && resp.data) {
        p = json_parser_new();
        if (!json_parser_load_from_data(p, resp.data, (gssize)resp.size, NULL)) {
            g_object_unref(p); p = NULL;
        }
    }
    free(resp.data);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(c);
    return p;
}

/* ── OAuth2 device authorization grant (RFC 8628) ────────────────
 * For headless/no-browser logins: request a device + user code, surface the
 * verification URL to the operator, then poll the token endpoint until the
 * user authorizes. Needs a device endpoint, token endpoint and client id. */
static int trino_fetch_device_token(trino_conn_t *conn, char **out_token)
{
    *out_token = NULL;
    if (!conn->oauth_device_url || !conn->oauth_token_url || !conn->oauth_client_id)
        return -1;

    CURL *e = curl_easy_init();
    if (!e) return -1;
    char *eid = curl_easy_escape(e, conn->oauth_client_id, 0);
    char *escope = conn->oauth_scope ? curl_easy_escape(e, conn->oauth_scope, 0) : NULL;

    char body[2048];
    if (escope)
        snprintf(body, sizeof(body), "client_id=%s&scope=%s", eid ? eid : "", escope);
    else
        snprintf(body, sizeof(body), "client_id=%s", eid ? eid : "");

    long code = 0;
    JsonParser *p = trino_oauth_form_post(conn, conn->oauth_device_url, body, &code);
    if (!p || code < 200 || code >= 300) {
        ARGUS_LOG_ERROR("Trino device authorization request failed (http=%ld)", code);
        if (p) g_object_unref(p);
        curl_free(eid); if (escope) curl_free(escope); curl_easy_cleanup(e);
        return -1;
    }
    JsonObject *o = json_node_get_object(json_parser_get_root(p));
    const char *device_code = json_object_has_member(o, "device_code")
        ? json_object_get_string_member(o, "device_code") : NULL;
    const char *user_code = json_object_has_member(o, "user_code")
        ? json_object_get_string_member(o, "user_code") : NULL;
    const char *vuri = json_object_has_member(o, "verification_uri")
        ? json_object_get_string_member(o, "verification_uri")
        : (json_object_has_member(o, "verification_url")
           ? json_object_get_string_member(o, "verification_url") : NULL);
    const char *vuri_full = json_object_has_member(o, "verification_uri_complete")
        ? json_object_get_string_member(o, "verification_uri_complete") : NULL;
    int interval = json_object_has_member(o, "interval")
        ? (int)json_object_get_int_member(o, "interval") : 5;
    int expires = json_object_has_member(o, "expires_in")
        ? (int)json_object_get_int_member(o, "expires_in") : 300;
    char *dc = device_code ? strdup(device_code) : NULL;

    /* Surface the verification prompt (ODBC has no UI — use stderr + the log). */
    fprintf(stderr,
            "\n[Argus][Trino] To sign in, open %s and enter code: %s\n\n",
            vuri_full ? vuri_full : (vuri ? vuri : "(your IdP device page)"),
            user_code ? user_code : "(see IdP)");
    ARGUS_LOG_WARN("Trino device-code auth: open %s and enter %s",
                   vuri ? vuri : "(idp)", user_code ? user_code : "");
    g_object_unref(p);

    if (!dc) { curl_free(eid); if (escope) curl_free(escope); curl_easy_cleanup(e); return -1; }

    char *edc = curl_easy_escape(e, dc, 0);
    char pbody[2048];
    snprintf(pbody, sizeof(pbody),
             "grant_type=urn:ietf:params:oauth:grant-type:device_code"
             "&device_code=%s&client_id=%s", edc ? edc : "", eid ? eid : "");

    int waited = 0, ret = -1;
    if (interval < 1) interval = 1;
    while (waited < expires) {
        sleep((unsigned)interval);
        waited += interval;
        long c2 = 0;
        JsonParser *tp = trino_oauth_form_post(conn, conn->oauth_token_url, pbody, &c2);
        if (!tp) continue;
        JsonObject *to = json_node_get_object(json_parser_get_root(tp));
        if (json_object_has_member(to, "access_token")) {
            const char *tok = json_object_get_string_member(to, "access_token");
            if (tok && *tok) { *out_token = strdup(tok); ret = 0; g_object_unref(tp); break; }
        }
        const char *err = json_object_has_member(to, "error")
            ? json_object_get_string_member(to, "error") : NULL;
        if (err && strcmp(err, "authorization_pending") == 0) {
            /* keep polling */
        } else if (err && strcmp(err, "slow_down") == 0) {
            interval += 5;
        } else if (err) {
            ARGUS_LOG_ERROR("Trino device token error: %s", err);
            g_object_unref(tp); break;
        }
        g_object_unref(tp);
    }

    free(dc); curl_free(edc); curl_free(eid); if (escope) curl_free(escope);
    curl_easy_cleanup(e);
    return ret;
}

/* ── (Re)build the default request headers from connection state ── */

static void trino_build_default_headers(trino_conn_t *conn)
{
    if (conn->default_headers) {
        curl_slist_free_all(conn->default_headers);
        conn->default_headers = NULL;
    }

    char header_buf[2048];

    snprintf(header_buf, sizeof(header_buf), "X-Trino-User: %s", conn->user);
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    snprintf(header_buf, sizeof(header_buf), "X-Trino-Catalog: %s", conn->catalog);
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    snprintf(header_buf, sizeof(header_buf), "X-Trino-Schema: %s", conn->schema);
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    if (conn->app_name && conn->app_name[0]) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-Source: %s", conn->app_name);
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }

    if (conn->protocol_version == 2) {
        conn->default_headers = curl_slist_append(
            conn->default_headers,
            "X-Trino-Client-Capabilities: CLIENT_OUTCOME_URI");
    }

    if (conn->auth_mode == TRINO_AUTH_BEARER && conn->password) {
        snprintf(header_buf, sizeof(header_buf),
                 "Authorization: Bearer %s", conn->password);
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }
}

/* ── Re-fetch an OAuth2 (M2M) access token and refresh the header ── */

static int trino_refresh_oauth_token(trino_conn_t *conn)
{
    if (!conn->oauth_m2m) return -1;

    char *tok = NULL;
    if (trino_fetch_oauth_token(conn, &tok) != 0 || !tok)
        return -1;

    free(conn->password);
    conn->password = tok;
    trino_build_default_headers(conn);
    ARGUS_LOG_INFO("Trino: OAuth2 access token refreshed after 401");
    return 0;
}

/* ── Connect to Trino ────────────────────────────────────────── */

int trino_connect(argus_dbc_t *dbc,
                  const char *host, int port,
                  const char *username, const char *password,
                  const char *database, const char *auth_mechanism,
                  argus_backend_conn_t *out_conn)
{
    trino_conn_t *conn = calloc(1, sizeof(trino_conn_t));
    if (!conn) {
        argus_set_error(&dbc->diag, "HY001",
                        "[Argus][Trino] Memory allocation failed", 0);
        return -1;
    }

    /* Copy SSL/TLS settings from DBC */
    conn->ssl_enabled = dbc->ssl_enabled;
    conn->ssl_verify = dbc->ssl_verify;
    if (dbc->ssl_cert_file) conn->ssl_cert_file = strdup(dbc->ssl_cert_file);
    if (dbc->ssl_key_file) conn->ssl_key_file = strdup(dbc->ssl_key_file);
    if (dbc->ssl_ca_file) conn->ssl_ca_file = strdup(dbc->ssl_ca_file);

    /* Copy timeout settings */
    conn->connect_timeout_sec = dbc->connect_timeout_sec;
    conn->query_timeout_sec = dbc->query_timeout_sec;

    /* Copy protocol version (default to v1 if not set) */
    conn->protocol_version = dbc->trino_protocol_version > 0
                             ? dbc->trino_protocol_version : 1;

    /* Build base URL (use https:// if SSL enabled) */
    char url_buf[512];
    const char *scheme = conn->ssl_enabled ? "https" : "http";
    snprintf(url_buf, sizeof(url_buf), "%s://%s:%d", scheme, host, port);
    conn->base_url = strdup(url_buf);

    ARGUS_LOG_DEBUG("Trino base URL: %s (SSL=%d)", conn->base_url, conn->ssl_enabled);

    conn->user = strdup(username && *username ? username : "argus");
    conn->catalog = strdup(database && *database ? database : "hive");
    conn->schema = strdup("default");

    /* Determine authentication mode from AuthMech (+ credentials). */
    if (password && *password) conn->password = strdup(password);
    if (auth_mechanism &&
        (strcasecmp(auth_mechanism, "GSSAPI") == 0 ||
         strcasecmp(auth_mechanism, "KERBEROS") == 0 ||
         strcasecmp(auth_mechanism, "SPNEGO") == 0 ||
         strcasecmp(auth_mechanism, "NEGOTIATE") == 0)) {
        conn->auth_mode = TRINO_AUTH_NEGOTIATE;
    } else if (auth_mechanism &&
        (strcasecmp(auth_mechanism, "JWT") == 0 ||
         strcasecmp(auth_mechanism, "BEARER") == 0 ||
         strcasecmp(auth_mechanism, "OAUTH2") == 0 ||
         strcasecmp(auth_mechanism, "OAUTH") == 0 ||
         strcasecmp(auth_mechanism, "CLIENT_CREDENTIALS") == 0 ||
         strcasecmp(auth_mechanism, "DEVICE_CODE") == 0 ||
         strcasecmp(auth_mechanism, "DEVICE") == 0)) {
        conn->auth_mode = TRINO_AUTH_BEARER;   /* static token in PWD, or fetched below */
    } else if (conn->password &&
        (!auth_mechanism ||
         strcasecmp(auth_mechanism, "BASIC") == 0 ||
         strcasecmp(auth_mechanism, "LDAP") == 0 ||
         strcasecmp(auth_mechanism, "PLAIN") == 0 ||
         strcasecmp(auth_mechanism, "PASSWORD") == 0 ||
         /* default mech is NOSASL; a password present implies Basic for Trino */
         strcasecmp(auth_mechanism, "NOSASL") == 0)) {
        conn->auth_mode = TRINO_AUTH_BASIC;
    } else {
        conn->auth_mode = TRINO_AUTH_NONE;
    }

    if ((conn->auth_mode == TRINO_AUTH_BASIC ||
         conn->auth_mode == TRINO_AUTH_BEARER) && !conn->ssl_enabled) {
        ARGUS_LOG_WARN("Trino: sending credentials over plain HTTP (no SSL) — "
                       "Trino normally requires TLS for password/token auth");
    }

    /* Initialize CURL */
    conn->curl = curl_easy_init();
    if (!conn->curl) {
        argus_set_error(&dbc->diag, "08001",
                        "[Argus][Trino] Failed to initialize HTTP client", 0);
        free(conn->base_url);
        free(conn->user);
        free(conn->password);
        free(conn->catalog);
        free(conn->schema);
        free(conn);
        return -1;
    }

    /* OAuth2 client-credentials (M2M): obtain an access token from the IdP and
     * use it as the Bearer token. Triggered when an OAuth2-family AuthMech is
     * set together with the token endpoint + client credentials. */
    if (conn->auth_mode == TRINO_AUTH_BEARER &&
        dbc->oauth_token_url && dbc->oauth_client_id && dbc->oauth_client_secret) {
        /* Retain the params so the access token can be refreshed on a 401. */
        conn->oauth_m2m = true;
        conn->oauth_token_url = strdup(dbc->oauth_token_url);
        conn->oauth_client_id = strdup(dbc->oauth_client_id);
        conn->oauth_client_secret = strdup(dbc->oauth_client_secret);
        if (dbc->oauth_scope) conn->oauth_scope = strdup(dbc->oauth_scope);

        char *tok = NULL;
        if (trino_fetch_oauth_token(conn, &tok) != 0 || !tok) {
            argus_set_error(&dbc->diag, "08001",
                "[Argus][Trino] OAuth2 client-credentials token request failed", 0);
            curl_easy_cleanup(conn->curl);
            free(conn->base_url);
            free(conn->user);
            free(conn->password);
            free(conn->catalog);
            free(conn->schema);
            free(conn);
            return -1;
        }
        free(conn->password);
        conn->password = tok;   /* fetched access token, used as Bearer below */
        ARGUS_LOG_INFO("Trino: obtained OAuth2 access token via client-credentials");
    }
    /* OAuth2 device authorization grant (RFC 8628): headless interactive login.
     * Triggered when a device endpoint + token endpoint + client id are set. */
    else if (conn->auth_mode == TRINO_AUTH_BEARER &&
             dbc->oauth_device_url && dbc->oauth_token_url && dbc->oauth_client_id) {
        conn->oauth_device_url = strdup(dbc->oauth_device_url);
        conn->oauth_token_url = strdup(dbc->oauth_token_url);
        conn->oauth_client_id = strdup(dbc->oauth_client_id);
        if (dbc->oauth_scope) conn->oauth_scope = strdup(dbc->oauth_scope);

        char *tok = NULL;
        if (trino_fetch_device_token(conn, &tok) != 0 || !tok) {
            argus_set_error(&dbc->diag, "08001",
                "[Argus][Trino] OAuth2 device-code authorization failed", 0);
            curl_easy_cleanup(conn->curl);
            free(conn->base_url); free(conn->user); free(conn->password);
            free(conn->catalog); free(conn->schema);
            free(conn->oauth_device_url); free(conn->oauth_token_url);
            free(conn->oauth_client_id); free(conn->oauth_scope);
            free(conn);
            return -1;
        }
        free(conn->password);
        conn->password = tok;
        ARGUS_LOG_INFO("Trino: obtained OAuth2 access token via device-code grant");
    }

    /* Retain the application name so headers can be rebuilt on token refresh. */
    if (dbc->app_name && dbc->app_name[0])
        conn->app_name = strdup(dbc->app_name);

    /* Build the default request headers (X-Trino-*, optional Bearer token). */
    trino_build_default_headers(conn);

    if (conn->auth_mode == TRINO_AUTH_BEARER && conn->password)
        ARGUS_LOG_DEBUG("Trino: JWT/OAuth2 bearer token auth enabled");
    else if (conn->auth_mode == TRINO_AUTH_NEGOTIATE)
        ARGUS_LOG_DEBUG("Trino: Kerberos/SPNEGO (Negotiate) auth enabled");
    else if (conn->auth_mode == TRINO_AUTH_BASIC)
        ARGUS_LOG_DEBUG("Trino: HTTP Basic (password) auth enabled");

    /* Verify connectivity with a lightweight request */
    trino_response_t resp = {0};
    char stmt_url[1024];
    snprintf(stmt_url, sizeof(stmt_url), "%s/v1/statement", conn->base_url);

    if (trino_http_post(conn, stmt_url, "SELECT 1", &resp) != 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "[Argus][Trino] Failed to connect to %s:%d", host, port);
        argus_set_error(&dbc->diag, "08001", msg, 0);
        free(resp.data);
        curl_slist_free_all(conn->default_headers);
        curl_easy_cleanup(conn->curl);
        free(conn->base_url);
        free(conn->user);
        free(conn->password);
        free(conn->catalog);
        free(conn->schema);
        free(conn);
        return -1;
    }

    /* Cancel the test query to clean up */
    if (resp.data) {
        JsonParser *parser = json_parser_new();
        if (json_parser_load_from_data(parser, resp.data, -1, NULL)) {
            JsonNode *root = json_parser_get_root(parser);
            JsonObject *obj = json_node_get_object(root);
            if (json_object_has_member(obj, "id")) {
                const char *qid = json_object_get_string_member(obj, "id");
                char cancel_url[1024];
                snprintf(cancel_url, sizeof(cancel_url),
                         "%s/v1/query/%s", conn->base_url, qid);
                trino_http_delete(conn, cancel_url);
            }
        }
        g_object_unref(parser);
    }
    free(resp.data);

    *out_conn = conn;
    return 0;
}

/* ── Liveness check ──────────────────────────────────────────── */

bool trino_is_alive(argus_backend_conn_t raw_conn)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn || !conn->curl || !conn->base_url) return false;

    char url[1024];
    snprintf(url, sizeof(url), "%s/v1/info", conn->base_url);

    trino_response_t resp = {0};
    int rc = trino_http_get(conn, url, &resp);
    free(resp.data);
    return (rc == 0);
}

/* ── Disconnect from Trino ───────────────────────────────────── */

void trino_disconnect(argus_backend_conn_t raw_conn)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn) return;

    if (conn->default_headers)
        curl_slist_free_all(conn->default_headers);
    if (conn->curl)
        curl_easy_cleanup(conn->curl);

    free(conn->base_url);
    free(conn->user);
    free(conn->password);
    free(conn->catalog);
    free(conn->schema);
    free(conn->app_name);

    /* Free OAuth2 (M2M) refresh params */
    free(conn->oauth_token_url);
    free(conn->oauth_client_id);
    free(conn->oauth_client_secret);
    free(conn->oauth_scope);
    free(conn->oauth_device_url);

    /* Free SSL/TLS fields */
    free(conn->ssl_cert_file);
    free(conn->ssl_key_file);
    free(conn->ssl_ca_file);

    free(conn);
}

/* ── Server error message capture ────────────────────────────── */

void trino_capture_error(trino_conn_t *conn, JsonObject *obj)
{
    if (!conn || !obj) return;
    conn->last_error[0] = '\0';
    if (!json_object_has_member(obj, "error")) return;

    JsonObject *err = json_object_get_object_member(obj, "error");
    if (err && json_object_has_member(err, "message")) {
        const char *m = json_object_get_string_member(err, "message");
        if (m && *m) {
            strncpy(conn->last_error, m, sizeof(conn->last_error) - 1);
            conn->last_error[sizeof(conn->last_error) - 1] = '\0';
        }
    }
}

bool trino_get_last_error(argus_backend_conn_t raw_conn, char *buf, size_t buflen)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn || !conn->last_error[0] || buflen == 0) return false;
    strncpy(buf, conn->last_error, buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
}
