#include "phoenix_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Helper: Apply SSL and timeout settings to curl ─────────────── */

static void phoenix_apply_curl_settings(phoenix_conn_t *conn, CURL *curl)
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
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                         (long)conn->connect_timeout_sec);
    }
    if (conn->query_timeout_sec > 0) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                         (long)conn->query_timeout_sec);
    }
}

/* ── CURL write callback ─────────────────────────────────────── */

size_t phoenix_curl_write_cb(void *contents, size_t size, size_t nmemb,
                              void *userp)
{
    size_t total = size * nmemb;
    phoenix_response_t *resp = (phoenix_response_t *)userp;

    char *ptr = realloc(resp->data, resp->size + total + 1);
    if (!ptr) return 0;

    resp->data = ptr;
    memcpy(resp->data + resp->size, contents, total);
    resp->size += total;
    resp->data[resp->size] = '\0';

    return total;
}

/* ── HTTP helper: POST ───────────────────────────────────────── */

int phoenix_http_post(phoenix_conn_t *conn, const char *url,
                      const char *body, phoenix_response_t *resp)
{
    CURL *curl = conn->curl;

    curl_easy_reset(curl);
    phoenix_apply_curl_settings(conn, curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, conn->default_headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, phoenix_curl_write_cb);
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

/* ── Avatica RPC helper ──────────────────────────────────────── */

int phoenix_avatica_request(phoenix_conn_t *conn, const char *request_type,
                            JsonBuilder *params, JsonParser **out_parser)
{
    /* Build the Avatica JSON request envelope */
    JsonBuilder *builder = json_builder_new();
    json_builder_begin_object(builder);
    json_builder_set_member_name(builder, "request");
    json_builder_add_string_value(builder, request_type);

    /* Merge in params if provided */
    if (params) {
        JsonNode *params_root = json_builder_get_root(params);
        if (params_root) {
            JsonObject *params_obj = json_node_get_object(params_root);
            if (params_obj) {
                GList *members = json_object_get_members(params_obj);
                for (GList *l = members; l != NULL; l = l->next) {
                    const char *name = (const char *)l->data;
                    JsonNode *val = json_object_dup_member(params_obj, name);
                    json_builder_set_member_name(builder, name);
                    json_builder_add_value(builder, val);
                }
                g_list_free(members);
            }
            json_node_unref(params_root);
        }
    }

    json_builder_end_object(builder);

    /* Serialize to JSON string */
    JsonGenerator *gen = json_generator_new();
    JsonNode *root = json_builder_get_root(builder);
    json_generator_set_root(gen, root);
    gchar *body = json_generator_to_data(gen, NULL);
    json_node_unref(root);
    g_object_unref(gen);
    g_object_unref(builder);

    ARGUS_LOG_TRACE("Avatica request [%s]: %s", request_type, body);

    /* Send HTTP POST */
    phoenix_response_t resp = {0};
    int rc = phoenix_http_post(conn, conn->base_url, body, &resp);
    g_free(body);

    if (rc != 0) {
        free(resp.data);
        return -1;
    }

    if (!resp.data) return -1;

    ARGUS_LOG_TRACE("Avatica response: %s", resp.data);

    /* Parse response JSON */
    JsonParser *parser = json_parser_new();
    if (!json_parser_load_from_data(parser, resp.data, -1, NULL)) {
        g_object_unref(parser);
        free(resp.data);
        return -1;
    }

    free(resp.data);

    /* Check for error response */
    JsonNode *resp_root = json_parser_get_root(parser);
    JsonObject *resp_obj = json_node_get_object(resp_root);
    if (json_object_has_member(resp_obj, "errorMessage")) {
        const char *err = json_object_get_string_member(resp_obj,
                                                         "errorMessage");
        ARGUS_LOG_ERROR("Avatica error: %s", err ? err : "unknown");
        g_object_unref(parser);
        return -1;
    }

    *out_parser = parser;
    return 0;
}

/* ── Connect to Phoenix Query Server ─────────────────────────── */

int phoenix_connect(argus_dbc_t *dbc,
                    const char *host, int port,
                    const char *username, const char *password,
                    const char *database, const char *auth_mechanism,
                    argus_backend_conn_t *out_conn)
{
    (void)password;
    (void)auth_mechanism;

    phoenix_conn_t *conn = calloc(1, sizeof(phoenix_conn_t));
    if (!conn) {
        argus_set_error(&dbc->diag, "HY001",
                        "[Argus][Phoenix] Memory allocation failed", 0);
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

    /* Build base URL (Avatica endpoint) */
    char url_buf[512];
    const char *scheme = conn->ssl_enabled ? "https" : "http";
    snprintf(url_buf, sizeof(url_buf), "%s://%s:%d", scheme, host, port);
    conn->base_url = strdup(url_buf);

    ARGUS_LOG_DEBUG("Phoenix base URL: %s (SSL=%d)",
                    conn->base_url, conn->ssl_enabled);

    conn->user = strdup(username && *username ? username : "argus");
    conn->database = strdup(database && *database ? database : "");
    conn->next_statement_id = 1;

    /* Initialize CURL */
    conn->curl = curl_easy_init();
    if (!conn->curl) {
        argus_set_error(&dbc->diag, "08001",
                        "[Argus][Phoenix] Failed to initialize HTTP client", 0);
        free(conn->base_url);
        free(conn->user);
        free(conn->database);
        free(conn);
        return -1;
    }

    /* Build default headers for Avatica JSON protocol */
    conn->default_headers = curl_slist_append(conn->default_headers,
                                               "Content-Type: application/json");
    conn->default_headers = curl_slist_append(conn->default_headers,
                                               "Accept: application/json");

    /* Open Avatica connection */
    JsonBuilder *params = json_builder_new();
    json_builder_begin_object(params);
    json_builder_set_member_name(params, "connectionId");
    /* Generate a simple connection ID based on pointer */
    char conn_id[64];
    snprintf(conn_id, sizeof(conn_id), "argus-%p-%d",
             (void *)conn, (int)g_get_monotonic_time());
    json_builder_add_string_value(params, conn_id);
    json_builder_set_member_name(params, "info");
    json_builder_begin_object(params);
    if (conn->database[0]) {
        json_builder_set_member_name(params, "schema");
        json_builder_add_string_value(params, conn->database);
    }
    json_builder_set_member_name(params, "user");
    json_builder_add_string_value(params, conn->user);
    json_builder_end_object(params);
    json_builder_end_object(params);

    JsonParser *parser = NULL;
    int rc = phoenix_avatica_request(conn, "openConnection", params, &parser);
    g_object_unref(params);

    if (rc != 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "[Argus][Phoenix] Failed to connect to %s:%d", host, port);
        argus_set_error(&dbc->diag, "08001", msg, 0);
        if (parser) g_object_unref(parser);
        curl_slist_free_all(conn->default_headers);
        curl_easy_cleanup(conn->curl);
        free(conn->base_url);
        free(conn->user);
        free(conn->database);
        free(conn);
        return -1;
    }

    conn->connection_id = strdup(conn_id);
    if (parser) g_object_unref(parser);

    ARGUS_LOG_INFO("Phoenix connection opened: %s", conn->connection_id);

    *out_conn = conn;
    return 0;
}

/* ── Disconnect from Phoenix Query Server ───────────────────── */

void phoenix_disconnect(argus_backend_conn_t raw_conn)
{
    phoenix_conn_t *conn = (phoenix_conn_t *)raw_conn;
    if (!conn) return;

    /* Send closeConnection to PQS */
    if (conn->connection_id) {
        JsonBuilder *params = json_builder_new();
        json_builder_begin_object(params);
        json_builder_set_member_name(params, "connectionId");
        json_builder_add_string_value(params, conn->connection_id);
        json_builder_end_object(params);

        JsonParser *parser = NULL;
        phoenix_avatica_request(conn, "closeConnection", params, &parser);
        g_object_unref(params);
        if (parser) g_object_unref(parser);

        ARGUS_LOG_INFO("Phoenix connection closed: %s", conn->connection_id);
    }

    if (conn->default_headers)
        curl_slist_free_all(conn->default_headers);
    if (conn->curl)
        curl_easy_cleanup(conn->curl);

    free(conn->base_url);
    free(conn->connection_id);
    free(conn->user);
    free(conn->database);
    free(conn->ssl_cert_file);
    free(conn->ssl_key_file);
    free(conn->ssl_ca_file);

    free(conn);
}
