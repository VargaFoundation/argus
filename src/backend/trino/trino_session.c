#include "trino_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include "argus/obs_hooks.h"
#include "../browser.h"
#include "../curl_common.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "argus/compat.h"
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <bcrypt.h>
typedef SOCKET argus_sock_t;
#define ARGUS_CLOSESOCK closesocket
#define sleep_seconds(n) Sleep((DWORD)(n) * 1000)
#define sleep_millis(n) Sleep((DWORD)(n))
#else
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/select.h>
typedef int argus_sock_t;
#define ARGUS_CLOSESOCK close
#define sleep_seconds(n) sleep((unsigned)(n))
#define sleep_millis(n) usleep((useconds_t)(n) * 1000)
#endif

/* Forward declarations for the OAuth2 token-refresh path. */
static int trino_refresh_oauth_token(trino_conn_t *conn);
static int trino_fetch_device_token(trino_conn_t *conn, char **out_token);
static int trino_fetch_authcode_token(trino_conn_t *conn, char **out_token,
                                      char *why, size_t why_size);
static void trino_oidc_discover(trino_conn_t *conn, const char *issuer);
static void trino_build_default_headers(trino_conn_t *conn);

/* Rebuild the request headers if a response changed the session. Called
 * after every request to the coordinator, so the next one carries the
 * catalog, schema, role, session properties, prepared statements and
 * transaction the server last told us about. */
static void trino_sync_session(trino_conn_t *conn)
{
    if (conn->headers_dirty) {
        trino_build_default_headers(conn);
        conn->headers_dirty = false;
    }
}

/* ── Helper: Apply SSL and timeout settings to curl ─────────────── */

/*
 * `with_credentials` is false for URLs outside the connection's origin (the
 * host:port the user configured): the server may point at spooled-segment
 * stores or other hosts, and neither the Basic/Negotiate credentials nor
 * the bearer token (carried in default_headers, which the caller must then
 * leave out as well) belong there.
 */
static void trino_apply_curl_settings(trino_conn_t *conn, CURL *curl,
                                      bool with_credentials)
{
    argus_curl_apply_baseline(curl);

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

    /* Timeouts, plus the shared low-speed abort: a connection that stays
     * open and delivers nothing is not a slow query. */
    argus_curl_apply_timeouts(curl, (long)conn->connect_timeout_sec,
                              (long)conn->query_timeout_sec);

    /* Authentication. Bearer (JWT/OAuth2) is applied as a default header at
     * connect time; Basic and Negotiate are set on the easy handle here because
     * curl_easy_reset() wiped them at the start of each request. */
    if (!with_credentials) return;
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


/* ── CURL header callback: the session state the server sets ──── */

/* Trim CR/LF and surrounding spaces from a header value, in place. */
static char *trino_hdr_trim(char *v)
{
    while (*v == ' ' || *v == '\t') v++;
    size_t n = strlen(v);
    while (n && (v[n - 1] == '\r' || v[n - 1] == '\n' ||
                 v[n - 1] == ' '  || v[n - 1] == '\t'))
        v[--n] = '\0';
    return v;
}

/* "name=value" -> the two halves; returns NULL if there is no '='. */
static char *trino_hdr_split(char *kv)
{
    char *eq = strchr(kv, '=');
    if (!eq) return NULL;
    *eq = '\0';
    return eq + 1;
}

static bool trino_hdr_is(const char *line, const char *name, char **value_out,
                         char *scratch, size_t scratch_sz)
{
    size_t n = strlen(name);
    if (g_ascii_strncasecmp(line, name, n) != 0 || line[n] != ':') return false;
    g_strlcpy(scratch, line + n + 1, scratch_sz);
    *value_out = trino_hdr_trim(scratch);
    return true;
}

/*
 * Trino carries session changes in response headers: the client is expected
 * to remember them and send them back. `USE db`, `SET SESSION x=y`,
 * `SET ROLE r`, `PREPARE p FROM ...` and `START TRANSACTION` all work this
 * way, so a client that ignores these headers runs every statement in the
 * session it opened with.
 */
size_t trino_curl_header_cb(char *buffer, size_t size, size_t nitems,
                            void *userp)
{
    size_t total = size * nitems;
    trino_conn_t *conn = (trino_conn_t *)userp;
    if (!conn || total == 0 || total > 65536) return total;

    char line[8192];
    size_t n = total < sizeof(line) - 1 ? total : sizeof(line) - 1;
    memcpy(line, buffer, n);
    line[n] = '\0';

    char scratch[8192];
    char *v = NULL;

    if (trino_hdr_is(line, "X-Trino-Set-Catalog", &v, scratch, sizeof(scratch))) {
        if (*v) { free(conn->catalog); conn->catalog = strdup(v);
                  conn->headers_dirty = true; }
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Set-Schema", &v, scratch, sizeof(scratch))) {
        if (*v) { free(conn->schema); conn->schema = strdup(v);
                  conn->headers_dirty = true; }
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Set-Role", &v, scratch, sizeof(scratch))) {
        free(conn->session_role);
        conn->session_role = *v ? strdup(v) : NULL;
        conn->headers_dirty = true;
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Set-Session", &v, scratch, sizeof(scratch))) {
        char *val = trino_hdr_split(v);
        if (val && conn->session_props) {
            g_hash_table_insert(conn->session_props, g_strdup(v), g_strdup(val));
            conn->headers_dirty = true;
        }
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Clear-Session", &v, scratch, sizeof(scratch))) {
        if (conn->session_props && g_hash_table_remove(conn->session_props, v))
            conn->headers_dirty = true;
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Added-Prepare", &v, scratch, sizeof(scratch))) {
        char *sql = trino_hdr_split(v);
        if (sql && conn->prepared) {
            g_hash_table_insert(conn->prepared, g_strdup(v), g_strdup(sql));
            conn->headers_dirty = true;
        }
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Deallocated-Prepare", &v, scratch, sizeof(scratch))) {
        if (conn->prepared && g_hash_table_remove(conn->prepared, v))
            conn->headers_dirty = true;
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Started-Transaction-Id", &v,
                     scratch, sizeof(scratch))) {
        free(conn->txn_id);
        conn->txn_id = *v ? strdup(v) : NULL;
        conn->headers_dirty = true;
        return total;
    }
    if (trino_hdr_is(line, "X-Trino-Clear-Transaction-Id", &v,
                     scratch, sizeof(scratch))) {
        free(conn->txn_id);
        conn->txn_id = NULL;
        conn->headers_dirty = true;
        return total;
    }
    return total;
}

/* ── HTTP helper: POST ───────────────────────────────────────── */

int trino_http_post(trino_conn_t *conn, const char *url, const char *body,
                    trino_response_t *resp)
{
    CURL *curl = conn->curl;

    curl_easy_reset(curl);
    trino_apply_curl_settings(conn, curl, true);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, argus_http_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, trino_curl_header_cb);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, conn);

    resp->data = NULL;
    resp->size = 0;

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK)
        return -1;
    trino_sync_session(conn);

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    /* OAuth2 (M2M) access token may have expired; refresh once and retry. */
    if (http_code == 401 && conn->oauth_m2m &&
        trino_refresh_oauth_token(conn) == 0) {
        free(resp->data);
        resp->data = NULL;
        resp->size = 0;
        curl_easy_reset(curl);
        trino_apply_curl_settings(conn, curl, true);
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, argus_http_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, trino_curl_header_cb);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, conn);
        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
            return -1;
        trino_sync_session(conn);
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    }

    if (http_code >= 400)
        return -1;

    return 0;
}

/* ── HTTP helper: GET ────────────────────────────────────────── */

/*
 * One GET on the connection's handle. `headers` replaces the session's
 * default headers; `with_credentials` also controls Basic/Negotiate.
 */
static int trino_http_get_with(trino_conn_t *conn, const char *url,
                               struct curl_slist *headers, bool with_credentials,
                               trino_response_t *resp)
{
    CURL *curl = conn->curl;

    curl_easy_reset(curl);
    trino_apply_curl_settings(conn, curl, with_credentials);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, argus_http_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
    /* Only the coordinator speaks for our session; a spooled-segment host
     * must not be able to rewrite the catalog or the transaction id. */
    if (with_credentials) {
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, trino_curl_header_cb);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, conn);
    }

    resp->data = NULL;
    resp->size = 0;

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK)
        return -1;
    if (with_credentials) trino_sync_session(conn);

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    return http_code >= 400 ? (int)http_code : 0;
}

int trino_http_get(trino_conn_t *conn, const char *url,
                   trino_response_t *resp)
{
    /* Every URL the server hands back (nextUri, query info) is expected on
     * the origin the user connected to. Anything else is fetched without
     * the session's identity: the credentials were obtained for base_url. */
    bool same_origin = argus_url_same_origin(url, conn->base_url);
    if (!same_origin)
        ARGUS_LOG_WARN("Trino: %s is not on %s; sending the request without "
                       "credentials", url, conn->base_url);

    int rc = trino_http_get_with(conn, url,
                                 same_origin ? conn->default_headers : NULL,
                                 same_origin, resp);

    /*
     * A coordinator behind a load balancer answers 502/503/504 while it is
     * restarting or shedding load, and 429 when it is rate limiting. Polling
     * nextUri is the long tail of every query, so a single one of those used
     * to fail the whole fetch — the client protocol expects the poll to be
     * retried. Five attempts with a doubling delay, ~1.5s in total.
     */
    for (int attempt = 0; attempt < 4 && same_origin &&
                          (rc == 429 || rc == 502 || rc == 503 || rc == 504);
         attempt++) {
        ARGUS_LOG_WARN("Trino: %s answered %d; retrying in %d ms",
                       url, rc, 100 << attempt);
        sleep_millis(100 << attempt);
        free(resp->data);
        resp->data = NULL;
        resp->size = 0;
        rc = trino_http_get_with(conn, url, conn->default_headers, true, resp);
    }

    /* OAuth2 (M2M) access token may have expired; refresh once and retry. */
    if (rc == 401 && same_origin && conn->oauth_m2m &&
        trino_refresh_oauth_token(conn) == 0) {
        free(resp->data);
        rc = trino_http_get_with(conn, url, conn->default_headers, true, resp);
    }

    return rc == 0 ? 0 : -1;
}

/* Fetch a URL the server delegated to another host (spooled segments):
 * only the headers the server attached to it, none of the session's. */
int trino_http_get_plain(trino_conn_t *conn, const char *url,
                         struct curl_slist *headers, trino_response_t *resp)
{
    return trino_http_get_with(conn, url, headers, false, resp) == 0 ? 0 : -1;
}

/* ── HTTP helper: DELETE ─────────────────────────────────────── */

int trino_http_delete(trino_conn_t *conn, const char *url)
{
    CURL *curl = conn->curl;

    bool same_origin = argus_url_same_origin(url, conn->base_url);
    curl_easy_reset(curl);
    trino_apply_curl_settings(conn, curl, same_origin);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, same_origin ? conn->default_headers : NULL);

    CURLcode res = curl_easy_perform(curl);
    return (res == CURLE_OK) ? 0 : -1;
}

/* ── OAuth2 client-credentials: fetch an access token from the IdP ─── */

static int trino_fetch_oauth_token_uncached(trino_conn_t *conn,
                                            char **out_token,
                                            long *expires_in_out)
{
    *out_token = NULL;
    *expires_in_out = 0;
    if (!conn->oauth_token_url || !conn->oauth_client_id ||
        !conn->oauth_client_secret)
        return -1;

    CURL *c = curl_easy_init();
    if (!c) return -1;
    argus_curl_apply_baseline(c);

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
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, argus_http_write_cb);
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
                if (json_object_has_member(obj, "expires_in"))
                    *expires_in_out =
                        (long)json_object_get_int_member(obj, "expires_in");
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

/* Cache-aware wrapper (taps): reuse a still-fresh access token across
 * connections — the OAuth round-trip otherwise dominates connect latency for
 * BI tools that open many close-together connections. On a fresh fetch the
 * token is published with its absolute expiry. The open build is a no-op. */
static int trino_fetch_oauth_token(trino_conn_t *conn, char **out_token)
{
    *out_token = NULL;
    const char *scope = conn->oauth_scope ? conn->oauth_scope : "";
    char *cached = argus_obs_hook_token_get(conn->oauth_token_url,
                                            conn->oauth_client_id, scope,
                                            "m2m");
    if (cached) {
        ARGUS_LOG_DEBUG("Trino: OAuth2 access token served from cache");
        *out_token = cached;
        return 0;
    }
    long expires_in = 0;
    int rc = trino_fetch_oauth_token_uncached(conn, out_token, &expires_in);
    if (rc == 0 && expires_in > 0)
        argus_obs_hook_token_put(conn->oauth_token_url, conn->oauth_client_id,
                                 scope, "m2m", *out_token,
                                 (long long)(g_get_real_time() / 1000) +
                                     (long long)expires_in * 1000);
    return rc;
}

/* POST an x-www-form-urlencoded body to an IdP endpoint, return parsed JSON. */
static JsonParser *trino_oauth_form_post(trino_conn_t *conn, const char *url,
                                         const char *body, long *http_code)
{
    CURL *c = curl_easy_init();
    if (!c) return NULL;
    argus_curl_apply_baseline(c);
    struct curl_slist *hdrs = curl_slist_append(
        NULL, "Content-Type: application/x-www-form-urlencoded");
    trino_response_t resp = {0};
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, argus_http_write_cb);
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
        sleep_seconds(interval);
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

/* ── OIDC discovery (.well-known/openid-configuration) ──────────── */

static void trino_oidc_discover(trino_conn_t *conn, const char *issuer)
{
    if (!issuer || !*issuer) return;
    char url[1024];
    snprintf(url, sizeof(url), "%s%s.well-known/openid-configuration",
             issuer, issuer[strlen(issuer) - 1] == '/' ? "" : "/");

    CURL *c = curl_easy_init();
    if (!c) return;
    argus_curl_apply_baseline(c);
    trino_response_t resp = {0};
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, argus_http_write_cb);
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
    curl_easy_cleanup(c);

    if (res == CURLE_OK && code >= 200 && code < 300 && resp.data) {
        JsonParser *p = json_parser_new();
        if (json_parser_load_from_data(p, resp.data, (gssize)resp.size, NULL)) {
            JsonObject *o = json_node_get_object(json_parser_get_root(p));
            /* Only fill endpoints the user did not set explicitly. */
            if (!conn->oauth_auth_url && o && json_object_has_member(o, "authorization_endpoint"))
                conn->oauth_auth_url = strdup(json_object_get_string_member(o, "authorization_endpoint"));
            if (!conn->oauth_token_url && o && json_object_has_member(o, "token_endpoint"))
                conn->oauth_token_url = strdup(json_object_get_string_member(o, "token_endpoint"));
            if (!conn->oauth_device_url && o && json_object_has_member(o, "device_authorization_endpoint"))
                conn->oauth_device_url = strdup(json_object_get_string_member(o, "device_authorization_endpoint"));
            ARGUS_LOG_INFO("Trino: OIDC discovery from %s", url);
        }
        g_object_unref(p);
    } else {
        ARGUS_LOG_WARN("Trino: OIDC discovery failed (%s, http=%ld)", url, code);
    }
    free(resp.data);
}

/* ── OAuth2 authorization-code grant with PKCE + loopback (browser SSO) ── */

/* base64url(no padding) of raw bytes. */
static void trino_b64url(const unsigned char *data, size_t len, char *out, size_t outsz)
{
    gchar *b64 = g_base64_encode(data, len);
    size_t j = 0;
    for (size_t i = 0; b64[i] && j < outsz - 1; i++) {
        char ch = b64[i];
        if (ch == '+') ch = '-';
        else if (ch == '/') ch = '_';
        else if (ch == '=') continue;
        out[j++] = ch;
    }
    out[j] = '\0';
    g_free(b64);
}

static int trino_rand_bytes(unsigned char *buf, size_t n)
{
#ifdef _WIN32
    /* Windows has no /dev/urandom, so the PKCE verifier and the state were
     * left uninitialised there and the whole flow was unprotected. */
    return BCryptGenRandom(NULL, buf, (ULONG)n,
                           BCRYPT_USE_SYSTEM_PREFERRED_RNG) == 0 ? 0 : -1;
#else
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f) return -1;
    size_t got = fread(buf, 1, n, f);
    fclose(f);
    return got == n ? 0 : -1;
#endif
}

/* PKCE: verifier = base64url(32 random bytes); challenge = base64url(SHA256(verifier)). */
static int trino_pkce(char *verifier, size_t vsz, char *challenge, size_t csz)
{
    unsigned char rnd[32];
    if (trino_rand_bytes(rnd, sizeof(rnd)) != 0) return -1;
    trino_b64url(rnd, sizeof(rnd), verifier, vsz);

    GChecksum *ck = g_checksum_new(G_CHECKSUM_SHA256);
    g_checksum_update(ck, (const guchar *)verifier, (gssize)strlen(verifier));
    unsigned char digest[32];
    gsize dlen = sizeof(digest);
    g_checksum_get_digest(ck, digest, &dlen);
    g_checksum_free(ck);
    trino_b64url(digest, dlen, challenge, csz);
    return 0;
}

/* Open a loopback listener on 127.0.0.1:<ephemeral>; returns fd, sets *port. */
static argus_sock_t trino_loopback_open(int *port)
{
#ifdef _WIN32
    WSADATA wsa;
    static int wsa_started = 0;
    if (!wsa_started && WSAStartup(MAKEWORD(2, 2), &wsa) == 0) wsa_started = 1;
#endif
    argus_sock_t fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == (argus_sock_t)-1) return (argus_sock_t)-1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&one, sizeof(one));
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { ARGUS_CLOSESOCK(fd); return (argus_sock_t)-1; }
    socklen_t l = sizeof(a);
    getsockname(fd, (struct sockaddr *)&a, &l);
    *port = ntohs(a.sin_port);
    if (listen(fd, 1) < 0) { ARGUS_CLOSESOCK(fd); return (argus_sock_t)-1; }
    return fd;
}

/*
 * Percent-decode a query-string value in place ('+' is a space). Returns the
 * decoded length.
 */
static size_t trino_url_decode(char *v)
{
    char *o = v;
    for (const char *p = v; *p; p++) {
        if (*p == '+') { *o++ = ' '; continue; }
        if (*p == '%' && g_ascii_isxdigit(p[1]) && g_ascii_isxdigit(p[2])) {
            *o++ = (char)((g_ascii_xdigit_value(p[1]) << 4) |
                          g_ascii_xdigit_value(p[2]));
            p += 2;
            continue;
        }
        *o++ = *p;
    }
    *o = '\0';
    return (size_t)(o - v);
}

/*
 * Copy the value of query parameter `name` out of `query` (the part after
 * '?'), decoded. Matching is on a whole parameter, so "code" no longer
 * matches "error_code" or "postcode" the way strstr(buf, "code=") did —
 * an IdP that redirects with ?error=access_denied&error_code=... used to
 * be read as a successful sign-in carrying a nonsense code.
 */
static bool trino_query_param(const char *query, const char *name,
                              char *out, size_t out_sz)
{
    size_t nlen = strlen(name);
    const char *p = query;
    while (p && *p) {
        const char *amp = strchr(p, '&');
        size_t plen = amp ? (size_t)(amp - p) : strlen(p);
        if (plen > nlen && p[nlen] == '=' && strncmp(p, name, nlen) == 0) {
            size_t vlen = plen - nlen - 1;
            if (vlen >= out_sz) vlen = out_sz - 1;
            memcpy(out, p + nlen + 1, vlen);
            out[vlen] = '\0';
            trino_url_decode(out);
            return true;
        }
        p = amp ? amp + 1 : NULL;
    }
    return false;
}

/*
 * Wait (up to timeout_sec) for the browser redirect and read the code out of
 * it. `state` is what we sent to the IdP: the redirect must carry it back, or
 * the response is somebody else's (a cross-site request forgery on the
 * loopback listener) and is refused.
 */
static int trino_loopback_get_code(argus_sock_t fd, char *code, size_t clen,
                                   const char *state, int timeout_sec)
{
    fd_set rs;
    FD_ZERO(&rs);
    FD_SET(fd, &rs);
    struct timeval tv = { timeout_sec, 0 };
    if (select((int)fd + 1, &rs, NULL, NULL, &tv) <= 0) return -1;
    argus_sock_t c = accept(fd, NULL, NULL);
    if (c == (argus_sock_t)-1) return -1;

    char buf[8192];
    int n = (int)recv(c, buf, sizeof(buf) - 1, 0);
    int ret = -1;
    const char *why = "no authorization code in the redirect";
    if (n > 0) {
        buf[n] = '\0';

        /* "GET /?code=...&state=... HTTP/1.1" — take the request target. */
        char *sp = strchr(buf, ' ');
        char *target = sp ? sp + 1 : NULL;
        char *end = target ? strpbrk(target, " \r\n") : NULL;
        if (end) *end = '\0';
        char *query = target ? strchr(target, '?') : NULL;

        if (query) {
            query++;
            char err[256];
            char got_state[128];
            if (trino_query_param(query, "error", err, sizeof(err))) {
                why = "the identity provider refused the sign-in";
                ARGUS_LOG_ERROR("Trino OAuth2: authorization failed: %s", err);
            } else if (!trino_query_param(query, "state", got_state,
                                          sizeof(got_state))) {
                why = "the redirect carried no state parameter";
            } else if (strcmp(got_state, state) != 0) {
                why = "the redirect's state did not match the one sent";
                ARGUS_LOG_ERROR("Trino OAuth2: state mismatch; "
                                "ignoring the redirect");
            } else if (trino_query_param(query, "code", code, clen)) {
                ret = 0;
            }
        }

        char resp[512];
        snprintf(resp, sizeof(resp),
                 "HTTP/1.1 %s\r\nContent-Type: text/html\r\n"
                 "Connection: close\r\n\r\n"
                 "<html><body style='font-family:sans-serif'>"
                 "<h2>Argus — %s</h2>%s</body></html>",
                 ret == 0 ? "200 OK" : "400 Bad Request",
                 ret == 0 ? "sign-in complete" : "sign-in failed",
                 ret == 0 ? "You may close this window." : why);
        (void)!send(c, resp, (int)strlen(resp), 0);
    }
    ARGUS_CLOSESOCK(c);
    return ret;
}

/*
 * Authorization-code grant with PKCE: send the user's browser to the IdP,
 * collect the code on a loopback listener, exchange it at the token
 * endpoint. `why` receives a short reason on failure (may stay empty).
 */
static int trino_fetch_authcode_token(trino_conn_t *conn, char **out_token,
                                      char *why, size_t why_size)
{
    *out_token = NULL;
    if (why_size) why[0] = '\0';
    if (!conn->oauth_auth_url || !conn->oauth_token_url || !conn->oauth_client_id)
        return -1;

    /* The endpoint comes from the DSN (or OIDC discovery) and is handed to
     * the browser launcher: only a well-formed https:// URL goes out. */
    if (!argus_browser_url_ok(conn->oauth_auth_url)) {
        snprintf(why, why_size, "OAuth2AuthEndpoint must be an https:// URL "
                 "without spaces or quotes (got '%.80s')", conn->oauth_auth_url);
        ARGUS_LOG_ERROR("Trino auth-code: %s", why);
        return -1;
    }

    char verifier[128] = {0}, challenge[128] = {0};
    if (trino_pkce(verifier, sizeof(verifier), challenge, sizeof(challenge)) != 0)
        return -1;

    int port = 0;
    argus_sock_t srv = trino_loopback_open(&port);
    if (srv == (argus_sock_t)-1) return -1;

    /* Without an unguessable state there is nothing to check the redirect
     * against, so the flow stops rather than running unprotected. */
    unsigned char sr[12];
    char state[64] = {0};
    if (trino_rand_bytes(sr, sizeof(sr)) != 0) {
        if (why_size)
            g_strlcpy(why, "no source of randomness for the OAuth2 state",
                      why_size);
        ARGUS_CLOSESOCK(srv);
        return -1;
    }
    trino_b64url(sr, sizeof(sr), state, sizeof(state));

    CURL *e = curl_easy_init();
    char redirect[64];
    snprintf(redirect, sizeof(redirect), "http://127.0.0.1:%d/", port);
    char *ecid = curl_easy_escape(e, conn->oauth_client_id, 0);
    char *eredir = curl_easy_escape(e, redirect, 0);
    char *escope = curl_easy_escape(e, conn->oauth_scope ? conn->oauth_scope : "openid", 0);
    char *estate = curl_easy_escape(e, state, 0);
    char *url = g_strdup_printf(
             "%s%sresponse_type=code&client_id=%s&redirect_uri=%s&scope=%s"
             "&state=%s&code_challenge=%s&code_challenge_method=S256",
             conn->oauth_auth_url, strchr(conn->oauth_auth_url, '?') ? "&" : "?",
             ecid ? ecid : "", eredir ? eredir : "", escope ? escope : "",
             estate ? estate : "", challenge);
    curl_free(ecid); curl_free(eredir); curl_free(escope); curl_free(estate);

    fprintf(stderr, "\n[Argus][Trino] Opening your browser to sign in. If it does not "
                    "open, visit:\n  %s\n\n", url);
    ARGUS_LOG_WARN("Trino auth-code: open %s", url);
    char berr[256];
    if (argus_browser_open(url, berr, sizeof(berr)) != 0)
        ARGUS_LOG_WARN("Trino auth-code: could not start a browser (%s); "
                       "waiting for the URL above to be opened by hand", berr);
    g_free(url);

    char code[2048] = {0};
    int gc = trino_loopback_get_code(srv, code, sizeof(code), state, 300);
    ARGUS_CLOSESOCK(srv);
    if (gc != 0 || !code[0]) {
        if (why_size)
            g_strlcpy(why, "no valid authorization redirect was received",
                      why_size);
        curl_easy_cleanup(e);
        return -1;
    }

    char *ecode = curl_easy_escape(e, code, 0);
    char *eredir2 = curl_easy_escape(e, redirect, 0);
    char *ecid2 = curl_easy_escape(e, conn->oauth_client_id, 0);
    char body[4096];
    int off = snprintf(body, sizeof(body),
                       "grant_type=authorization_code&code=%s&redirect_uri=%s"
                       "&client_id=%s&code_verifier=%s",
                       ecode ? ecode : "", eredir2 ? eredir2 : "",
                       ecid2 ? ecid2 : "", verifier);
    if (conn->oauth_client_secret) {
        char *esec = curl_easy_escape(e, conn->oauth_client_secret, 0);
        snprintf(body + off, sizeof(body) - (size_t)off, "&client_secret=%s", esec ? esec : "");
        curl_free(esec);
    }
    curl_free(ecode); curl_free(eredir2); curl_free(ecid2);
    curl_easy_cleanup(e);

    long hc = 0;
    JsonParser *tp = trino_oauth_form_post(conn, conn->oauth_token_url, body, &hc);
    int ret = -1;
    if (tp && hc >= 200 && hc < 300) {
        JsonObject *o = json_node_get_object(json_parser_get_root(tp));
        if (o && json_object_has_member(o, "access_token")) {
            const char *t = json_object_get_string_member(o, "access_token");
            if (t && *t) { *out_token = strdup(t); ret = 0; }
        }
    }
    if (tp) g_object_unref(tp);
    return ret;
}

/* ── (Re)build the default request headers from connection state ── */

/*
 * A header value with a CR or LF in it splits the request and lets the next
 * line be read as a header of its own. The user, catalog, schema and source
 * all come from the DSN, which a shared .odc or .tds file can carry, so they
 * are filtered on the way out rather than trusted. Other control characters
 * go too; a header value is not the place for them.
 */
static const char *trino_hdr_safe(const char *v, char *out, size_t out_sz)
{
    size_t j = 0;
    for (size_t i = 0; v && v[i] && j + 1 < out_sz; i++) {
        unsigned char c = (unsigned char)v[i];
        if (c == '\r' || c == '\n' || c < 0x20 || c == 0x7f) continue;
        out[j++] = (char)c;
    }
    out[j] = '\0';
    return out;
}

static void trino_build_default_headers(trino_conn_t *conn)
{
    char safe[1024];

    if (conn->default_headers) {
        curl_slist_free_all(conn->default_headers);
        conn->default_headers = NULL;
    }

    char header_buf[2048];

    /* With a bearer token Trino derives the principal from the token; a
     * default X-Trino-User ("argus") then triggers an impersonation
     * conflict. Send the header only when it carries a real identity: an
     * explicit UID, or a non-token auth mode. */
    if (conn->user_explicit || conn->auth_mode != TRINO_AUTH_BEARER) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-User: %s",
                 trino_hdr_safe(conn->user, safe, sizeof(safe)));
        conn->default_headers =
            curl_slist_append(conn->default_headers, header_buf);
    }

    snprintf(header_buf, sizeof(header_buf), "X-Trino-Catalog: %s",
             trino_hdr_safe(conn->catalog, safe, sizeof(safe)));
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    snprintf(header_buf, sizeof(header_buf), "X-Trino-Schema: %s",
             trino_hdr_safe(conn->schema, safe, sizeof(safe)));
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    if (conn->app_name && conn->app_name[0]) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-Source: %s",
                 trino_hdr_safe(conn->app_name, safe, sizeof(safe)));
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }

    if (conn->protocol_version == 2) {
        conn->default_headers = curl_slist_append(
            conn->default_headers,
            "X-Trino-Client-Capabilities: CLIENT_OUTCOME_URI");
    }

    if (conn->auth_mode == TRINO_AUTH_BEARER && conn->password) {
        snprintf(header_buf, sizeof(header_buf), "Authorization: Bearer %s",
                 trino_hdr_safe(conn->password, safe, sizeof(safe)));
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }

    /*
     * Trino renders TIMESTAMP WITH TIME ZONE and now() in the session's zone,
     * and defaults to the coordinator's when the client does not say. A BI
     * tool showing timestamps an hour off from the same query in the Trino UI
     * is this header missing.
     */
    if (conn->time_zone && conn->time_zone[0]) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-Time-Zone: %s",
                 trino_hdr_safe(conn->time_zone, safe, sizeof(safe)));
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }

    if (conn->session_role && conn->session_role[0]) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-Role: %s",
                 trino_hdr_safe(conn->session_role, safe, sizeof(safe)));
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }

    if (conn->txn_id && conn->txn_id[0]) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-Transaction-Id: %s",
                 trino_hdr_safe(conn->txn_id, safe, sizeof(safe)));
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
    }

    /*
     * Session properties and prepared statements go back one header each
     * rather than comma-joined: a prepared statement is a whole SQL text,
     * and a fixed buffer is the wrong shape for it.
     */
    if (conn->session_props) {
        GHashTableIter it;
        gpointer k, v;
        g_hash_table_iter_init(&it, conn->session_props);
        while (g_hash_table_iter_next(&it, &k, &v)) {
            char *h = g_strdup_printf("X-Trino-Session: %s=%s",
                                      (const char *)k, (const char *)v);
            conn->default_headers = curl_slist_append(conn->default_headers, h);
            g_free(h);
        }
    }
    if (conn->prepared) {
        GHashTableIter it;
        gpointer k, v;
        g_hash_table_iter_init(&it, conn->prepared);
        while (g_hash_table_iter_next(&it, &k, &v)) {
            char *h = g_strdup_printf("X-Trino-Prepared-Statement: %s=%s",
                                      (const char *)k, (const char *)v);
            conn->default_headers = curl_slist_append(conn->default_headers, h);
            g_free(h);
        }
    }
}

/* ── Re-fetch an OAuth2 (M2M) access token and refresh the header ── */

static int trino_refresh_oauth_token(trino_conn_t *conn)
{
    if (!conn->oauth_m2m) return -1;

    /* The old token just got a 401: bypass the cache and publish the fresh
     * one, so every waiting connection converges on it. */
    char *tok = NULL;
    long expires_in = 0;
    if (trino_fetch_oauth_token_uncached(conn, &tok, &expires_in) != 0 || !tok)
        return -1;
    if (expires_in > 0)
        argus_obs_hook_token_put(conn->oauth_token_url, conn->oauth_client_id,
                                 conn->oauth_scope ? conn->oauth_scope : "",
                                 "m2m", tok,
                                 (long long)(g_get_real_time() / 1000) +
                                     (long long)expires_in * 1000);

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

    conn->user_explicit = (username && *username);
    conn->user = strdup(conn->user_explicit ? username : "argus");
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
         strcasecmp(auth_mechanism, "DEVICE") == 0 ||
         strcasecmp(auth_mechanism, "AUTH_CODE") == 0 ||
         strcasecmp(auth_mechanism, "AUTHCODE") == 0 ||
         strcasecmp(auth_mechanism, "BROWSER") == 0 ||
         strcasecmp(auth_mechanism, "SSO") == 0)) {
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

    /* OAuth2 family: copy endpoints/credentials, optionally discover them from an
     * OIDC issuer, then run the grant selected by AuthMech — authorization-code
     * with PKCE + browser/loopback (AUTH_CODE/BROWSER/SSO), device-code
     * (DEVICE_CODE), or client-credentials M2M (default when a client secret is
     * present). */
    if (conn->auth_mode == TRINO_AUTH_BEARER && dbc->oauth_client_id &&
        (dbc->oauth_token_url || dbc->oauth_issuer)) {
        conn->oauth_client_id = strdup(dbc->oauth_client_id);
        if (dbc->oauth_client_secret) conn->oauth_client_secret = strdup(dbc->oauth_client_secret);
        if (dbc->oauth_scope)         conn->oauth_scope = strdup(dbc->oauth_scope);
        if (dbc->oauth_token_url)     conn->oauth_token_url = strdup(dbc->oauth_token_url);
        if (dbc->oauth_auth_url)      conn->oauth_auth_url = strdup(dbc->oauth_auth_url);
        if (dbc->oauth_device_url)    conn->oauth_device_url = strdup(dbc->oauth_device_url);
        if (dbc->oauth_issuer)        trino_oidc_discover(conn, dbc->oauth_issuer);

        bool is_authcode = auth_mechanism &&
            (strcasecmp(auth_mechanism, "AUTH_CODE") == 0 ||
             strcasecmp(auth_mechanism, "AUTHCODE") == 0 ||
             strcasecmp(auth_mechanism, "BROWSER") == 0 ||
             strcasecmp(auth_mechanism, "SSO") == 0);
        bool is_device = auth_mechanism &&
            (strcasecmp(auth_mechanism, "DEVICE_CODE") == 0 ||
             strcasecmp(auth_mechanism, "DEVICE") == 0);

        char *tok = NULL;
        const char *how = NULL;
        char why[256] = {0};
        if (is_authcode && conn->oauth_auth_url && conn->oauth_token_url) {
            how = "authorization-code (browser SSO)";
            trino_fetch_authcode_token(conn, &tok, why, sizeof(why));
        } else if (is_device && conn->oauth_device_url && conn->oauth_token_url) {
            how = "device-code";
            trino_fetch_device_token(conn, &tok);
        } else if (conn->oauth_token_url && conn->oauth_client_secret) {
            how = "client-credentials";
            conn->oauth_m2m = true;   /* enables transparent re-fetch on 401 */
            trino_fetch_oauth_token(conn, &tok);
        }

        if (tok) {
            free(conn->password);
            conn->password = tok;
            ARGUS_LOG_INFO("Trino: obtained OAuth2 access token via %s", how);
        } else if (how) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Trino] OAuth2 %s authorization failed%s%s",
                     how, why[0] ? ": " : "", why);
            argus_set_error(&dbc->diag, "08001", msg, 0);
            curl_easy_cleanup(conn->curl);
            free(conn->base_url); free(conn->user); free(conn->password);
            free(conn->catalog); free(conn->schema);
            free(conn->oauth_token_url); free(conn->oauth_auth_url);
            free(conn->oauth_device_url); free(conn->oauth_client_id);
            free(conn->oauth_client_secret); free(conn->oauth_scope);
            free(conn);
            return -1;
        }
    }

    /* Retain the application name so headers can be rebuilt on token refresh. */
    if (dbc->app_name && dbc->app_name[0])
        conn->app_name = strdup(dbc->app_name);

    conn->session_props = g_hash_table_new_full(g_str_hash, g_str_equal,
                                               g_free, g_free);
    conn->prepared      = g_hash_table_new_full(g_str_hash, g_str_equal,
                                               g_free, g_free);

    /*
     * The zone the application's machine is in, so the server renders
     * timestamps the way the user's other tools do. g_time_zone_get_identifier
     * gives the IANA name Trino expects ("Europe/Paris"); a machine that only
     * knows an offset yields something like "+02:00", which Trino also
     * accepts. "local" means glib could not name the zone — send nothing then
     * and let the coordinator's default stand, as before.
     */
    GTimeZone *tz = g_time_zone_new_local();
    if (tz) {
        const char *id = g_time_zone_get_identifier(tz);
        if (id && *id && strcmp(id, "local") != 0)
            conn->time_zone = strdup(id);
        g_time_zone_unref(tz);
    }

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
    free(conn->time_zone);
    free(conn->session_role);
    free(conn->txn_id);
    if (conn->session_props) g_hash_table_destroy(conn->session_props);
    if (conn->prepared)      g_hash_table_destroy(conn->prepared);

    /* Free OAuth2 (M2M) refresh params */
    free(conn->oauth_token_url);
    free(conn->oauth_client_id);
    free(conn->oauth_client_secret);
    free(conn->oauth_scope);
    free(conn->oauth_device_url);
    free(conn->oauth_auth_url);

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

/* ── Server version ──────────────────────────────────────────────
 * Backs SQLGetInfo(SQL_DBMS_VER). /v1/info answers
 * {"nodeVersion":{"version":"467"}, ...} without authentication on an open
 * cluster and with the session's headers otherwise, which is why it goes
 * through trino_http_get rather than a bare curl call.
 *
 * Probed lazily and cached: BI tools ask once per connection at most, and
 * fetching at connect time would add a round trip to every pooled connection. */
bool trino_get_server_version(argus_backend_conn_t raw_conn, char *buf, size_t buflen)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn || buflen == 0) return false;

    if (!conn->version_probed) {
        conn->version_probed = true;   /* one attempt per connection, pass or fail */

        char url[512];
        snprintf(url, sizeof(url), "%s/v1/info", conn->base_url);

        trino_response_t resp = {0};
        if (trino_http_get(conn, url, &resp) == 0 && resp.data) {
            JsonParser *parser = json_parser_new();
            if (json_parser_load_from_data(parser, resp.data, (gssize)resp.size, NULL)) {
                JsonNode *root = json_parser_get_root(parser);
                if (root && JSON_NODE_HOLDS_OBJECT(root)) {
                    JsonObject *obj = json_node_get_object(root);
                    if (json_object_has_member(obj, "nodeVersion")) {
                        JsonObject *nv = json_object_get_object_member(obj, "nodeVersion");
                        if (nv && json_object_has_member(nv, "version")) {
                            const char *v = json_object_get_string_member(nv, "version");
                            if (v && *v) {
                                strncpy(conn->server_version, v,
                                        sizeof(conn->server_version) - 1);
                                conn->server_version[sizeof(conn->server_version) - 1] = '\0';
                            }
                        }
                    }
                }
            }
            g_object_unref(parser);
        } else {
            ARGUS_LOG_DEBUG("Trino /v1/info unavailable; SQL_DBMS_VER stays unknown");
        }
        free(resp.data);
    }

    if (!conn->server_version[0]) return false;

    strncpy(buf, conn->server_version, buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
}
