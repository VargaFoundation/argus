#include "hive_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include "argus/compat.h"
#include "../thrift_sasl.h"
#ifdef ARGUS_HAS_CURL
#include "thrift_http_transport.h"
#endif
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <gio/gio.h>

/* ── Connect to HiveServer2 via Thrift ───────────────────────── */

static bool use_http_transport(const argus_dbc_t *dbc)
{
    if (dbc->http_path && dbc->http_path[0])
        return true;
    return false;
}

static bool use_sasl(const char *auth_mechanism)
{
    if (!auth_mechanism) return false;
    return strcasecmp(auth_mechanism, "NOSASL") != 0;
}

static bool use_gssapi(const char *auth_mechanism)
{
    if (!auth_mechanism) return false;
    return strcasecmp(auth_mechanism, "KERBEROS") == 0 ||
           strcasecmp(auth_mechanism, "GSSAPI") == 0;
}

/* Bearer token over HTTP (the password carries a JWT / Databricks PAT). */
static bool use_bearer(const char *auth_mechanism)
{
    if (!auth_mechanism) return false;
    return strcasecmp(auth_mechanism, "JWT") == 0 ||
           strcasecmp(auth_mechanism, "BEARER") == 0 ||
           strcasecmp(auth_mechanism, "TOKEN") == 0 ||
           strcasecmp(auth_mechanism, "DATABRICKS") == 0;
}

int hive_connect(argus_dbc_t *dbc,
                 const char *host, int port,
                 const char *username, const char *password,
                 const char *database, const char *auth_mechanism,
                 argus_backend_conn_t *out_conn)
{
    GError *error = NULL;
    bool sasl = use_sasl(auth_mechanism);
    bool gssapi = use_gssapi(auth_mechanism);
    bool bearer = use_bearer(auth_mechanism);

    hive_conn_t *conn = calloc(1, sizeof(hive_conn_t));
    if (!conn) {
        argus_set_error(&dbc->diag, "HY001",
                        "[Argus][Hive] Memory allocation failed", 0);
        return -1;
    }

    /* ── HTTP transport mode ────────────────────────────────────── */
#ifdef ARGUS_HAS_CURL
    if (use_http_transport(dbc)) {
        const char *scheme = dbc->ssl_enabled ? "https" : "http";
        const char *path = dbc->http_path;
        char url_buf[1024];
        snprintf(url_buf, sizeof(url_buf), "%s://%s:%d/%s",
                 scheme, host, port, path);

        ARGUS_LOG_INFO("Hive: Using HTTP transport to %s", url_buf);

        conn->transport = (ThriftTransport *)g_object_new(
            THRIFT_TYPE_HTTP_TRANSPORT,
            "url",             url_buf,
            "use-spnego",      gssapi,
            "ssl-verify",      dbc->ssl_verify,
            "ssl-ca-file",     dbc->ssl_ca_file,
            "ssl-cert-file",   dbc->ssl_cert_file,
            "ssl-key-file",    dbc->ssl_key_file,
            "connect-timeout", dbc->connect_timeout_sec > 0 ? dbc->connect_timeout_sec : 30,
            "request-timeout", dbc->socket_timeout_sec > 0 ? dbc->socket_timeout_sec : 300,
            "username",        bearer ? "" : (username ? username : ""),
            "password",        bearer ? "" : (password ? password : ""),
            "bearer-token",    bearer ? (password ? password : "") : NULL,
            NULL);

        if (!thrift_transport_open(conn->transport, &error)) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Hive] HTTP transport open failed: %s",
                     error ? error->message : "unknown error");
            argus_set_error(&dbc->diag, "08001", msg, 0);
            if (error) g_error_free(error);
            goto fail;
        }

        conn->socket = NULL;
        conn->http_mode = true;
        goto setup_protocol;
    }
#else
    if (use_http_transport(dbc)) {
        argus_set_error(&dbc->diag, "HY000",
                        "[Argus][Hive] HTTP transport not available: "
                        "driver was built without libcurl", 0);
        goto fail;
    }
#endif

    /* ── Binary Thrift transport (TCP socket) ────────────────── */

    /* GIO transport: portable TCP/TLS with construction-time timeout */
    if (dbc->ssl_enabled)
        ARGUS_LOG_DEBUG("Hive: TLS socket to %s:%d (verify=%d)", host, port,
                        dbc->ssl_verify ? 1 : 0);
    conn->socket = argus_gio_transport_new(
        host, port, dbc->ssl_enabled, dbc->ssl_verify, dbc->ssl_ca_file,
        dbc->socket_timeout_sec > 0 ? (guint)dbc->socket_timeout_sec : 0);

    /*
     * For NOSASL: socket → buffered_transport → binary_protocol
     * For SASL:   socket → [SASL handshake] → framed_transport → binary_protocol
     */
    if (sasl) {
        /* Open raw socket for SASL handshake */
        ThriftTransport *raw = (ThriftTransport *)conn->socket;
        if (!thrift_transport_open(raw, &error)) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Hive] Failed to connect to %s:%d: %s",
                     host, port, error ? error->message : "unknown error");
            argus_set_error(&dbc->diag, "08001", msg, 0);
            if (error) g_error_free(error);
            goto fail;
        }


        /* Perform SASL handshake (GSSAPI or PLAIN) */
        char sasl_err[512];
        int sasl_rc;
        if (gssapi) {
            ARGUS_LOG_DEBUG("Hive: Performing SASL GSSAPI handshake to %s:%d",
                            host, port);
            sasl_rc = argus_thrift_sasl_handshake_gssapi(
                raw, "hive", host, sasl_err, sizeof(sasl_err));
        } else {
            ARGUS_LOG_DEBUG("Hive: Performing SASL PLAIN handshake to %s:%d",
                            host, port);
            sasl_rc = argus_thrift_sasl_handshake_plain(
                raw, username ? username : "", password ? password : "",
                sasl_err, sizeof(sasl_err));
        }
        if (sasl_rc != 0) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Hive] SASL handshake failed on %s:%d: %s",
                     host, port, sasl_err);
            argus_set_error(&dbc->diag, "08001", msg, 0);
            thrift_transport_close(raw, NULL);
            goto fail;
        }
        ARGUS_LOG_DEBUG("Hive: SASL handshake completed successfully");

        /* After SASL, use framed transport */
        conn->transport = (ThriftTransport *)g_object_new(
            THRIFT_TYPE_FRAMED_TRANSPORT,
            "transport", conn->socket,
            NULL);
    } else {
        /* NOSASL: buffered transport, opened normally */
        conn->transport = (ThriftTransport *)g_object_new(
            THRIFT_TYPE_BUFFERED_TRANSPORT,
            "transport", conn->socket,
            NULL);

        if (!thrift_transport_open(conn->transport, &error)) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Hive] Failed to connect to %s:%d: %s",
                     host, port, error ? error->message : "unknown error");
            argus_set_error(&dbc->diag, "08001", msg, 0);
            if (error) g_error_free(error);
            goto fail;
        }

    }

setup_protocol:
    conn->protocol = (ThriftProtocol *)g_object_new(
        THRIFT_TYPE_BINARY_PROTOCOL,
        "transport", conn->transport,
        NULL);

    /* Create the TCLIService client */
    conn->client = (TCLIServiceIf *)g_object_new(
        TYPE_T_C_L_I_SERVICE_CLIENT,
        "input_protocol", conn->protocol,
        "output_protocol", conn->protocol,
        NULL);

    /* Open a Hive session */
    TOpenSessionReq *open_req = g_object_new(TYPE_T_OPEN_SESSION_REQ, NULL);
    TOpenSessionResp *open_resp = g_object_new(TYPE_T_OPEN_SESSION_RESP, NULL);

    /* Set client protocol version */
    g_object_set(open_req,
                 "client_protocol", T_PROTOCOL_VERSION_HIVE_CLI_SERVICE_PROTOCOL_V10,
                 NULL);

    /* Set username if provided */
    if (username && *username) {
        g_object_set(open_req,
                     "username", username,
                     "password", password ? password : "",
                     NULL);
    }

    /* Set initial database and app name via configuration */
    if ((database && *database) || (dbc->app_name && dbc->app_name[0])) {
        GHashTable *config = g_hash_table_new_full(
            g_str_hash, g_str_equal, g_free, g_free);

        if (database && *database) {
            g_hash_table_insert(config,
                                g_strdup("use:database"),
                                g_strdup(database));
        }

        if (dbc->app_name && dbc->app_name[0]) {
            g_hash_table_insert(config,
                                g_strdup("hive.query.source"),
                                g_strdup(dbc->app_name));
            ARGUS_LOG_DEBUG("Hive: Set application name to %s", dbc->app_name);
        }

        g_object_set(open_req, "configuration", config, NULL);
        /* config ownership transferred to open_req */
    }

    gboolean ok = t_c_l_i_service_client_open_session(
        conn->client, &open_resp, open_req, &error);

    if (!ok || !open_resp) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "[Argus][Hive] OpenSession failed: %s",
                 error ? error->message : "unknown error");
        argus_set_error(&dbc->diag, "08001", msg, 0);
        if (error) g_error_free(error);
        g_object_unref(open_req);
        if (open_resp) g_object_unref(open_resp);
        goto fail_transport;
    }

    /* Check status */
    TStatus *status = NULL;
    g_object_get(open_resp, "status", &status, NULL);
    if (status) {
        TStatusCode status_code;
        g_object_get(status, "statusCode", &status_code, NULL);
        if (status_code == T_STATUS_CODE_ERROR_STATUS) {
            gchar *err_msg = NULL;
            g_object_get(status, "errorMessage", &err_msg, NULL);
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Hive] OpenSession error: %s",
                     err_msg ? err_msg : "unknown");
            argus_set_error(&dbc->diag, "08001", msg, 0);
            g_free(err_msg);
            g_object_unref(status);
            g_object_unref(open_req);
            g_object_unref(open_resp);
            goto fail_transport;
        }
        g_object_unref(status);
    }

    /* Extract session handle */
    g_object_get(open_resp, "sessionHandle", &conn->session_handle, NULL);

    g_object_unref(open_req);
    g_object_unref(open_resp);

    if (database)
        conn->database = strdup(database);

    *out_conn = conn;
    return 0;

fail_transport:
    thrift_transport_close(conn->transport, NULL);
fail:
    if (conn->client)    g_object_unref(conn->client);
    if (conn->protocol)  g_object_unref(conn->protocol);
    if (conn->transport) g_object_unref(conn->transport);
    if (conn->socket)    g_object_unref(conn->socket);
    free(conn);
    return -1;
}

/* ── Liveness check ──────────────────────────────────────────── */

bool hive_is_alive(argus_backend_conn_t raw_conn)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
    if (!conn || !conn->transport) return false;

    return thrift_transport_is_open(conn->transport);
}

/* ── Disconnect from HiveServer2 ─────────────────────────────── */

void hive_disconnect(argus_backend_conn_t raw_conn)
{
    hive_conn_t *conn = (hive_conn_t *)raw_conn;
    if (!conn) return;

    GError *error = NULL;

    /* Close session */
    if (conn->session_handle) {
        TCloseSessionReq *close_req = g_object_new(
            TYPE_T_CLOSE_SESSION_REQ, NULL);
        g_object_set(close_req,
                     "sessionHandle", conn->session_handle,
                     NULL);

        TCloseSessionResp *close_resp = g_object_new(
            TYPE_T_CLOSE_SESSION_RESP, NULL);
        t_c_l_i_service_client_close_session(
            conn->client, &close_resp, close_req, &error);

        if (error) g_error_free(error);
        g_object_unref(close_req);
        if (close_resp) g_object_unref(close_resp);
        g_object_unref(conn->session_handle);
    }

    /* Close transport */
    thrift_transport_close(conn->transport, NULL);

    g_object_unref(conn->client);
    g_object_unref(conn->protocol);
    g_object_unref(conn->transport);
    if (conn->socket) g_object_unref(conn->socket);
    free(conn->database);
    free(conn);
}
