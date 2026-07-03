#ifndef ARGUS_THRIFT_GIO_TRANSPORT_H
#define ARGUS_THRIFT_GIO_TRANSPORT_H

#include <gio/gio.h>
#include <thrift/c_glib/transport/thrift_transport.h>

/*
 * GIO-based Thrift transport.
 *
 * thrift_c_glib's own ThriftSocket is implemented on raw POSIX sockets
 * and does not build on Windows. This transport speaks through
 * GSocketClient / GSocketConnection instead, which GLib implements on
 * every platform, and adds what ThriftSocket lacks: socket timeouts and
 * TLS (GTlsClientConnection, with CA file and verification toggles).
 * Hive and Impala use it on all platforms so the exact code exercised
 * by the Linux integration tests is what ships on Windows.
 *
 * TLS needs the glib-networking GIO module at runtime.
 */

G_BEGIN_DECLS

#define ARGUS_TYPE_GIO_TRANSPORT (argus_gio_transport_get_type())
#define ARGUS_GIO_TRANSPORT(obj) \
    (G_TYPE_CHECK_INSTANCE_CAST((obj), ARGUS_TYPE_GIO_TRANSPORT, \
                                ArgusGioTransport))

typedef struct _ArgusGioTransport
{
    ThriftTransport parent;

    /* construct properties */
    gchar    *hostname;
    guint     port;
    gboolean  use_tls;
    gboolean  tls_verify;
    gchar    *ca_file;
    guint     timeout_sec;

    /* live state */
    GSocketClient     *client;
    GSocketConnection *socket_conn;
    GIOStream         *stream;      /* socket_conn or its TLS wrapper */
    GInputStream      *in;
    GOutputStream     *out;
} ArgusGioTransport;

typedef struct _ArgusGioTransportClass
{
    ThriftTransportClass parent;
} ArgusGioTransportClass;

GType argus_gio_transport_get_type(void);

/* Convenience constructor. ca_file may be NULL (system trust store). */
ThriftTransport *argus_gio_transport_new(const char *hostname, int port,
                                         gboolean use_tls,
                                         gboolean tls_verify,
                                         const char *ca_file,
                                         guint timeout_sec);

G_END_DECLS

#endif /* ARGUS_THRIFT_GIO_TRANSPORT_H */
