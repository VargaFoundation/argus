/*
 * Kudu session management (connect/disconnect).
 * Uses the Kudu C++ client library with extern "C" wrappers.
 */
#include <kudu/client/client.h>
#include <string>
#include <memory>

extern "C" {
#include "kudu_internal.h"
#include "argus/handle.h"
#include "argus/error.h"
#include "argus/log.h"
}

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTable;
using kudu::Status;

/* ── C++ client creation/destruction ─────────────────────────── */

extern "C"
int kudu_cpp_client_create(const char *master_addresses,
                           int timeout_sec,
                           void **out_client)
{
    KuduClientBuilder builder;
    builder.add_master_server_addr(master_addresses);
    if (timeout_sec > 0) {
        builder.default_admin_operation_timeout(
            kudu::MonoDelta::FromSeconds(timeout_sec));
        builder.default_rpc_timeout(
            kudu::MonoDelta::FromSeconds(timeout_sec));
    }

    std::shared_ptr<KuduClient> *client_ptr =
        new std::shared_ptr<KuduClient>();
    Status s = builder.Build(client_ptr);
    if (!s.ok()) {
        delete client_ptr;
        return -1;
    }

    *out_client = client_ptr;
    return 0;
}

extern "C"
void kudu_cpp_client_destroy(void *client)
{
    if (!client) return;
    auto *client_ptr = static_cast<std::shared_ptr<KuduClient> *>(client);
    delete client_ptr;
}

/* ── Connect to Kudu ─────────────────────────────────────────── */

extern "C"
int kudu_connect(argus_dbc_t *dbc,
                 const char *host, int port,
                 const char *username, const char *password,
                 const char *database, const char *auth_mechanism,
                 argus_backend_conn_t *out_conn)
{
    (void)username;
    (void)password;
    (void)auth_mechanism;

    kudu_conn_t *conn =
        static_cast<kudu_conn_t *>(calloc(1, sizeof(kudu_conn_t)));
    if (!conn) {
        argus_set_error(&dbc->diag, "HY001",
                        "[Argus][Kudu] Memory allocation failed", 0);
        return -1;
    }

    /* Build master address (host:port) */
    char addr_buf[512];
    snprintf(addr_buf, sizeof(addr_buf), "%s:%d", host, port);
    conn->master_addresses = strdup(addr_buf);

    conn->database = strdup(database && *database ? database : "default");
    conn->connect_timeout_sec = dbc->connect_timeout_sec;
    conn->query_timeout_sec = dbc->query_timeout_sec;

    ARGUS_LOG_DEBUG("Kudu master addresses: %s", conn->master_addresses);

    /* Create Kudu client */
    int rc = kudu_cpp_client_create(conn->master_addresses,
                                    conn->connect_timeout_sec,
                                    &conn->client);
    if (rc != 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "[Argus][Kudu] Failed to connect to %s:%d", host, port);
        argus_set_error(&dbc->diag, "08001", msg, 0);
        free(conn->master_addresses);
        free(conn->database);
        free(conn);
        return -1;
    }

    ARGUS_LOG_INFO("Kudu client connected to %s", conn->master_addresses);

    *out_conn = conn;
    return 0;
}

/* ── Disconnect from Kudu ────────────────────────────────────── */

extern "C"
void kudu_disconnect(argus_backend_conn_t raw_conn)
{
    kudu_conn_t *conn = static_cast<kudu_conn_t *>(raw_conn);
    if (!conn) return;

    kudu_cpp_client_destroy(conn->client);

    ARGUS_LOG_INFO("Kudu client disconnected from %s",
                   conn->master_addresses);

    free(conn->master_addresses);
    free(conn->database);
    free(conn);
}
