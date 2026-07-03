#include "thrift_gio_transport.h"
#include "argus/log.h"

#include <string.h>

G_DEFINE_TYPE(ArgusGioTransport, argus_gio_transport, THRIFT_TYPE_TRANSPORT)

enum {
    PROP_0,
    PROP_HOSTNAME,
    PROP_PORT,
    PROP_USE_TLS,
    PROP_TLS_VERIFY,
    PROP_CA_FILE,
    PROP_TIMEOUT_SEC,
};

/* ── vtable ──────────────────────────────────────────────────── */

static gboolean gio_is_open(ThriftTransport *transport)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(transport);
    return t->stream != NULL;
}

static gboolean gio_peek(ThriftTransport *transport, GError **error)
{
    (void)error;
    return gio_is_open(transport);
}

/* Accept any certificate when verification is disabled. */
static gboolean gio_accept_any_cert(GTlsConnection *conn,
                                    GTlsCertificate *peer_cert,
                                    GTlsCertificateFlags errors,
                                    gpointer user_data)
{
    (void)conn; (void)peer_cert; (void)errors; (void)user_data;
    return TRUE;
}

#ifdef _WIN32
/* GIO finds its TLS backend (glib-networking) through a compile-time
 * module path that does not exist on end-user machines. The installer
 * ships the module in a gio-modules\ directory next to the driver DLL;
 * load it from there, once, before the first TLS connection. */
static gsize gio_tls_modules_once = 0;

static void gio_load_bundled_tls_modules(void)
{
    if (!g_once_init_enter(&gio_tls_modules_once))
        return;

    HMODULE self = NULL;
    char path[MAX_PATH];
    if (GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                           GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                           (LPCSTR)&gio_load_bundled_tls_modules, &self) &&
        GetModuleFileNameA(self, path, sizeof(path)) > 0) {
        char *slash = strrchr(path, '\\');
        if (slash) {
            *slash = '\0';
            gchar *dir = g_build_filename(path, "gio-modules", NULL);
            if (g_file_test(dir, G_FILE_TEST_IS_DIR)) {
                g_io_modules_scan_all_in_directory(dir);
                ARGUS_LOG_DEBUG("GIO transport: loaded TLS modules from %s",
                                dir);
            }
            g_free(dir);
        }
    }

    g_once_init_leave(&gio_tls_modules_once, 1);
}
#endif /* _WIN32 */

static gboolean gio_open(ThriftTransport *transport, GError **error)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(transport);
    if (t->stream) return TRUE;

    t->client = g_socket_client_new();
    if (t->timeout_sec > 0)
        g_socket_client_set_timeout(t->client, t->timeout_sec);

    t->socket_conn = g_socket_client_connect_to_host(
        t->client, t->hostname, (guint16)t->port, NULL, error);
    if (!t->socket_conn) {
        g_clear_object(&t->client);
        return FALSE;
    }

    if (t->use_tls) {
#ifdef _WIN32
        gio_load_bundled_tls_modules();
#endif
        GSocketConnectable *identity =
            G_SOCKET_CONNECTABLE(g_network_address_new(t->hostname,
                                                       (guint16)t->port));
        GIOStream *tls = g_tls_client_connection_new(
            G_IO_STREAM(t->socket_conn), identity, error);
        g_object_unref(identity);
        if (!tls) {
            g_clear_object(&t->socket_conn);
            g_clear_object(&t->client);
            return FALSE;
        }

        if (t->ca_file && *t->ca_file) {
            GError *dberr = NULL;
            GTlsDatabase *db = g_tls_file_database_new(t->ca_file, &dberr);
            if (db) {
                g_tls_connection_set_database(G_TLS_CONNECTION(tls), db);
                g_object_unref(db);
            } else {
                ARGUS_LOG_WARN("GIO transport: cannot load CA file %s: %s",
                               t->ca_file,
                               dberr ? dberr->message : "unknown error");
                if (dberr) g_error_free(dberr);
            }
        }
        if (!t->tls_verify)
            g_signal_connect(tls, "accept-certificate",
                             G_CALLBACK(gio_accept_any_cert), NULL);

        if (!g_tls_connection_handshake(G_TLS_CONNECTION(tls), NULL, error)) {
            g_object_unref(tls);
            g_clear_object(&t->socket_conn);
            g_clear_object(&t->client);
            return FALSE;
        }
        t->stream = tls;
    } else {
        t->stream = G_IO_STREAM(g_object_ref(t->socket_conn));
    }

    t->in = g_io_stream_get_input_stream(t->stream);
    t->out = g_io_stream_get_output_stream(t->stream);
    return TRUE;
}

static gboolean gio_close(ThriftTransport *transport, GError **error)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(transport);
    if (t->stream) {
        g_io_stream_close(t->stream, NULL, error ? error : NULL);
        g_clear_object(&t->stream);
    }
    g_clear_object(&t->socket_conn);
    g_clear_object(&t->client);
    t->in = NULL;
    t->out = NULL;
    return TRUE;
}

static gint32 gio_read(ThriftTransport *transport, gpointer buf,
                       guint32 len, GError **error)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(transport);
    if (!t->in) {
        g_set_error(error, g_quark_from_static_string("argus-gio"),
                    1, "transport not open");
        return -1;
    }
    gssize n = g_input_stream_read(t->in, buf, len, NULL, error);
    if (n < 0) return -1;
    if (n == 0 && len > 0) {
        /* EOF: the peer closed the connection. Report an error rather
         * than 0, or thrift's read_all loop would spin forever. */
        g_set_error(error, g_quark_from_static_string("argus-gio"),
                    2, "connection closed by peer");
        return -1;
    }
    return (gint32)n;
}

static gboolean gio_write(ThriftTransport *transport, const gpointer buf,
                          const guint32 len, GError **error)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(transport);
    if (!t->out) {
        g_set_error(error, g_quark_from_static_string("argus-gio"),
                    1, "transport not open");
        return FALSE;
    }
    return g_output_stream_write_all(t->out, buf, len, NULL, NULL, error);
}

static gboolean gio_flush(ThriftTransport *transport, GError **error)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(transport);
    if (!t->out) return TRUE;
    return g_output_stream_flush(t->out, NULL, error);
}

static gboolean gio_read_end(ThriftTransport *transport, GError **error)
{
    (void)transport; (void)error;
    return TRUE;
}

static gboolean gio_write_end(ThriftTransport *transport, GError **error)
{
    (void)transport; (void)error;
    return TRUE;
}

/* ── GObject plumbing ────────────────────────────────────────── */

static void argus_gio_transport_set_property(GObject *object, guint prop_id,
                                             const GValue *value,
                                             GParamSpec *pspec)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(object);
    switch (prop_id) {
    case PROP_HOSTNAME:
        g_free(t->hostname);
        t->hostname = g_value_dup_string(value);
        break;
    case PROP_PORT:        t->port = g_value_get_uint(value); break;
    case PROP_USE_TLS:     t->use_tls = g_value_get_boolean(value); break;
    case PROP_TLS_VERIFY:  t->tls_verify = g_value_get_boolean(value); break;
    case PROP_CA_FILE:
        g_free(t->ca_file);
        t->ca_file = g_value_dup_string(value);
        break;
    case PROP_TIMEOUT_SEC: t->timeout_sec = g_value_get_uint(value); break;
    default:
        G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    }
}

static void argus_gio_transport_get_property(GObject *object, guint prop_id,
                                             GValue *value, GParamSpec *pspec)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(object);
    switch (prop_id) {
    case PROP_HOSTNAME:    g_value_set_string(value, t->hostname); break;
    case PROP_PORT:        g_value_set_uint(value, t->port); break;
    case PROP_USE_TLS:     g_value_set_boolean(value, t->use_tls); break;
    case PROP_TLS_VERIFY:  g_value_set_boolean(value, t->tls_verify); break;
    case PROP_CA_FILE:     g_value_set_string(value, t->ca_file); break;
    case PROP_TIMEOUT_SEC: g_value_set_uint(value, t->timeout_sec); break;
    default:
        G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    }
}

static void argus_gio_transport_finalize(GObject *object)
{
    ArgusGioTransport *t = ARGUS_GIO_TRANSPORT(object);
    gio_close((ThriftTransport *)t, NULL);
    g_free(t->hostname);
    g_free(t->ca_file);
    G_OBJECT_CLASS(argus_gio_transport_parent_class)->finalize(object);
}

static void argus_gio_transport_init(ArgusGioTransport *t)
{
    t->tls_verify = TRUE;
}

static void argus_gio_transport_class_init(ArgusGioTransportClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS(klass);
    ThriftTransportClass *ttc = THRIFT_TRANSPORT_CLASS(klass);

    gobject_class->set_property = argus_gio_transport_set_property;
    gobject_class->get_property = argus_gio_transport_get_property;
    gobject_class->finalize = argus_gio_transport_finalize;

    ttc->is_open = gio_is_open;
    ttc->peek = gio_peek;
    ttc->open = gio_open;
    ttc->close = gio_close;
    ttc->read = gio_read;
    ttc->read_end = gio_read_end;
    ttc->write = gio_write;
    ttc->write_end = gio_write_end;
    ttc->flush = gio_flush;

    g_object_class_install_property(gobject_class, PROP_HOSTNAME,
        g_param_spec_string("hostname", "hostname", "Remote host",
                            "localhost",
                            G_PARAM_READWRITE | G_PARAM_CONSTRUCT));
    g_object_class_install_property(gobject_class, PROP_PORT,
        g_param_spec_uint("port", "port", "Remote port", 0, 65535, 9090,
                          G_PARAM_READWRITE | G_PARAM_CONSTRUCT));
    g_object_class_install_property(gobject_class, PROP_USE_TLS,
        g_param_spec_boolean("tls", "tls", "Wrap the connection in TLS",
                             FALSE,
                             G_PARAM_READWRITE | G_PARAM_CONSTRUCT));
    g_object_class_install_property(gobject_class, PROP_TLS_VERIFY,
        g_param_spec_boolean("tls-verify", "tls-verify",
                             "Verify the server certificate", TRUE,
                             G_PARAM_READWRITE | G_PARAM_CONSTRUCT));
    g_object_class_install_property(gobject_class, PROP_CA_FILE,
        g_param_spec_string("ca-file", "ca-file",
                            "PEM CA bundle for TLS verification", NULL,
                            G_PARAM_READWRITE | G_PARAM_CONSTRUCT));
    g_object_class_install_property(gobject_class, PROP_TIMEOUT_SEC,
        g_param_spec_uint("timeout", "timeout",
                          "Socket timeout in seconds (0 = none)",
                          0, 3600, 0,
                          G_PARAM_READWRITE | G_PARAM_CONSTRUCT));
}

ThriftTransport *argus_gio_transport_new(const char *hostname, int port,
                                         gboolean use_tls,
                                         gboolean tls_verify,
                                         const char *ca_file,
                                         guint timeout_sec)
{
    return (ThriftTransport *)g_object_new(ARGUS_TYPE_GIO_TRANSPORT,
                                           "hostname", hostname,
                                           "port", port,
                                           "tls", use_tls,
                                           "tls-verify", tls_verify,
                                           "ca-file", ca_file,
                                           "timeout", timeout_sec,
                                           NULL);
}
