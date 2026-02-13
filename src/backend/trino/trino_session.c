#include "trino_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

/* ── Connect to Trino ────────────────────────────────────────── */

int trino_connect(argus_dbc_t *dbc,
                  const char *host, int port,
                  const char *username, const char *password,
                  const char *database, const char *auth_mechanism,
                  argus_backend_conn_t *out_conn)
{
    (void)password;
    (void)auth_mechanism;

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

    /* Build base URL (use https:// if SSL enabled) */
    char url_buf[512];
    const char *scheme = conn->ssl_enabled ? "https" : "http";
    snprintf(url_buf, sizeof(url_buf), "%s://%s:%d", scheme, host, port);
    conn->base_url = strdup(url_buf);

    ARGUS_LOG_DEBUG("Trino base URL: %s (SSL=%d)", conn->base_url, conn->ssl_enabled);

    conn->user = strdup(username && *username ? username : "argus");
    conn->catalog = strdup(database && *database ? database : "hive");
    conn->schema = strdup("default");

    /* Initialize CURL */
    conn->curl = curl_easy_init();
    if (!conn->curl) {
        argus_set_error(&dbc->diag, "08001",
                        "[Argus][Trino] Failed to initialize HTTP client", 0);
        free(conn->base_url);
        free(conn->user);
        free(conn->catalog);
        free(conn->schema);
        free(conn);
        return -1;
    }

    /* Build default headers */
    char header_buf[256];

    snprintf(header_buf, sizeof(header_buf), "X-Trino-User: %s", conn->user);
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    snprintf(header_buf, sizeof(header_buf), "X-Trino-Catalog: %s", conn->catalog);
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    snprintf(header_buf, sizeof(header_buf), "X-Trino-Schema: %s", conn->schema);
    conn->default_headers = curl_slist_append(conn->default_headers, header_buf);

    /* Add application name if specified */
    if (dbc->app_name && dbc->app_name[0]) {
        snprintf(header_buf, sizeof(header_buf), "X-Trino-Source: %s", dbc->app_name);
        conn->default_headers = curl_slist_append(conn->default_headers, header_buf);
        ARGUS_LOG_DEBUG("Trino application name: %s", dbc->app_name);
    }

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
    free(conn->catalog);
    free(conn->schema);

    /* Free SSL/TLS fields */
    free(conn->ssl_cert_file);
    free(conn->ssl_key_file);
    free(conn->ssl_ca_file);

    free(conn);
}
