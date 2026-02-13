#include "impala_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <gio/gio.h>

/* ── Connect to Impala via Thrift (TCLIService) ──────────────── */

int impala_connect(argus_dbc_t *dbc,
                   const char *host, int port,
                   const char *username, const char *password,
                   const char *database, const char *auth_mechanism,
                   argus_backend_conn_t *out_conn)
{
    (void)auth_mechanism;
    GError *error = NULL;

    impala_conn_t *conn = calloc(1, sizeof(impala_conn_t));
    if (!conn) {
        argus_set_error(&dbc->diag, "HY001",
                        "[Argus][Impala] Memory allocation failed", 0);
        return -1;
    }

    /* Create Thrift transport stack (with SSL support if available) */
#ifdef ARGUS_HAS_THRIFT_SSL
    if (dbc->ssl_enabled) {
        ARGUS_LOG_DEBUG("Impala: Creating SSL socket to %s:%d", host, port);
        /* Note: Thrift C GLib SSL socket configuration is done via system SSL settings */
        conn->socket = (ThriftSocket *)g_object_new(
            THRIFT_TYPE_SSL_SOCKET,
            "hostname", host,
            "port", port,
            NULL);
        if (dbc->ssl_ca_file) {
            ARGUS_LOG_DEBUG("Impala: SSL CA cert specified: %s (configured via system)",
                           dbc->ssl_ca_file);
        }
    } else
#endif
    {
        if (dbc->ssl_enabled) {
            ARGUS_LOG_WARN("Impala: SSL requested but not available (OpenSSL not installed)");
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
                 "[Argus][Impala] Failed to connect to %s:%d: %s",
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
            ARGUS_LOG_DEBUG("Impala: Set socket timeout to %d seconds", dbc->socket_timeout_sec);
            g_object_unref(gsocket);
        }
    }

    /* Open an Impala session with protocol V6 */
    TOpenSessionReq *open_req = g_object_new(TYPE_T_OPEN_SESSION_REQ, NULL);
    TOpenSessionResp *open_resp = g_object_new(TYPE_T_OPEN_SESSION_RESP, NULL);

    /* Impala uses protocol V6 (not V10 like Hive) */
    g_object_set(open_req,
                 "client_protocol", T_PROTOCOL_VERSION_HIVE_CLI_SERVICE_PROTOCOL_V6,
                 NULL);

    /* Set username if provided */
    if (username && *username) {
        g_object_set(open_req,
                     "username", username,
                     "password", password ? password : "",
                     NULL);
    }

    /* Impala does NOT support use:database in OpenSession config.
     * We issue a USE <db> statement after connecting. */

    gboolean ok = t_c_l_i_service_client_open_session(
        conn->client, &open_resp, open_req, &error);

    if (!ok || !open_resp) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "[Argus][Impala] OpenSession failed: %s",
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
                     "[Argus][Impala] OpenSession error: %s",
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

    /* Switch to the requested database via USE statement */
    if (database && *database && strcmp(database, "default") != 0) {
        char use_stmt[256];
        snprintf(use_stmt, sizeof(use_stmt), "USE %s", database);

        TExecuteStatementReq *use_req = g_object_new(
            TYPE_T_EXECUTE_STATEMENT_REQ, NULL);
        g_object_set(use_req,
                     "sessionHandle", conn->session_handle,
                     "statement", use_stmt,
                     "runAsync", FALSE,
                     NULL);

        TExecuteStatementResp *use_resp = g_object_new(
            TYPE_T_EXECUTE_STATEMENT_RESP, NULL);

        GError *use_error = NULL;
        gboolean use_ok = t_c_l_i_service_client_execute_statement(
            conn->client, &use_resp, use_req, &use_error);

        if (!use_ok) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                     "[Argus][Impala] USE %s failed: %s",
                     database,
                     use_error ? use_error->message : "unknown error");
            argus_set_error(&dbc->diag, "08001", msg, 0);
            if (use_error) g_error_free(use_error);
            g_object_unref(use_req);
            if (use_resp) g_object_unref(use_resp);
            goto fail_session;
        }

        /* Close the USE operation handle */
        if (use_resp) {
            TOperationHandle *use_op = NULL;
            g_object_get(use_resp, "operationHandle", &use_op, NULL);
            if (use_op) {
                TCloseOperationReq *close_req = g_object_new(
                    TYPE_T_CLOSE_OPERATION_REQ, NULL);
                g_object_set(close_req, "operationHandle", use_op, NULL);
                TCloseOperationResp *close_resp = g_object_new(
                    TYPE_T_CLOSE_OPERATION_RESP, NULL);
                t_c_l_i_service_client_close_operation(
                    conn->client, &close_resp, close_req, NULL);
                g_object_unref(close_req);
                if (close_resp) g_object_unref(close_resp);
                g_object_unref(use_op);
            }
        }

        if (use_error) g_error_free(use_error);
        g_object_unref(use_req);
        if (use_resp) g_object_unref(use_resp);

        conn->database = strdup(database);
    } else if (database) {
        conn->database = strdup(database);
    }

    *out_conn = conn;
    return 0;

fail_session:
    {
        TCloseSessionReq *close_req = g_object_new(
            TYPE_T_CLOSE_SESSION_REQ, NULL);
        g_object_set(close_req,
                     "sessionHandle", conn->session_handle,
                     NULL);
        TCloseSessionResp *close_resp = g_object_new(
            TYPE_T_CLOSE_SESSION_RESP, NULL);
        t_c_l_i_service_client_close_session(
            conn->client, &close_resp, close_req, NULL);
        g_object_unref(close_req);
        if (close_resp) g_object_unref(close_resp);
        g_object_unref(conn->session_handle);
    }
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

/* ── Disconnect from Impala ──────────────────────────────────── */

void impala_disconnect(argus_backend_conn_t raw_conn)
{
    impala_conn_t *conn = (impala_conn_t *)raw_conn;
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
