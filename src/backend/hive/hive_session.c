#include "hive_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <gio/gio.h>

/* ── Connect to HiveServer2 via Thrift ───────────────────────── */

int hive_connect(argus_dbc_t *dbc,
                 const char *host, int port,
                 const char *username, const char *password,
                 const char *database, const char *auth_mechanism,
                 argus_backend_conn_t *out_conn)
{
    (void)auth_mechanism;
    GError *error = NULL;

    hive_conn_t *conn = calloc(1, sizeof(hive_conn_t));
    if (!conn) {
        argus_set_error(&dbc->diag, "HY001",
                        "[Argus][Hive] Memory allocation failed", 0);
        return -1;
    }

    /* Create Thrift transport stack (with SSL support if available) */
#ifdef ARGUS_HAS_THRIFT_SSL
    if (dbc->ssl_enabled) {
        ARGUS_LOG_DEBUG("Hive: Creating SSL socket to %s:%d", host, port);
        conn->socket = (ThriftSocket *)g_object_new(
            THRIFT_TYPE_SSL_SOCKET,
            "hostname", host,
            "port", port,
            NULL);

        /* Configure SSL certificate if provided */
        if (dbc->ssl_ca_file) {
            ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET(conn->socket);
            thrift_ssl_socket_set_ca_certificate(ssl_socket, dbc->ssl_ca_file);
            ARGUS_LOG_DEBUG("Hive: SSL CA cert: %s", dbc->ssl_ca_file);
        }
    } else
#endif
    {
        if (dbc->ssl_enabled) {
            ARGUS_LOG_WARN("Hive: SSL requested but not available (OpenSSL not installed)");
        }
        conn->socket = (ThriftSocket *)g_object_new(
            THRIFT_TYPE_SOCKET,
            "hostname", host,
            "port", port,
            NULL);
    }

    conn->transport = (ThriftTransport *)g_object_new(
        THRIFT_TYPE_BUFFERED_TRANSPORT,
        "transport", conn->socket,
        NULL);

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

    /* Open the transport */
    if (!thrift_transport_open(conn->transport, &error)) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "[Argus][Hive] Failed to connect to %s:%d: %s",
                 host, port, error ? error->message : "unknown error");
        argus_set_error(&dbc->diag, "08001", msg, 0);
        if (error) g_error_free(error);
        goto fail;
    }

    /* Set socket timeout if specified */
    if (dbc->socket_timeout_sec > 0) {
        GSocket *gsocket = NULL;
        g_object_get(conn->socket, "socket", &gsocket, NULL);
        if (gsocket) {
            g_socket_set_timeout(gsocket, (guint)dbc->socket_timeout_sec);
            ARGUS_LOG_DEBUG("Hive: Set socket timeout to %d seconds", dbc->socket_timeout_sec);
            g_object_unref(gsocket);
        }
    }

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
    g_object_unref(conn->socket);
    free(conn->database);
    free(conn);
}
