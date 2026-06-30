#ifndef ARGUS_THRIFT_HTTP_TRANSPORT_H
#define ARGUS_THRIFT_HTTP_TRANSPORT_H

#include <glib-object.h>
#include <thrift/c_glib/transport/thrift_transport.h>
#include <curl/curl.h>

G_BEGIN_DECLS

#define THRIFT_TYPE_HTTP_TRANSPORT (thrift_http_transport_get_type())
#define THRIFT_HTTP_TRANSPORT(obj) \
    (G_TYPE_CHECK_INSTANCE_CAST((obj), THRIFT_TYPE_HTTP_TRANSPORT, ThriftHttpTransport))

typedef struct _ThriftHttpTransport ThriftHttpTransport;
typedef struct _ThriftHttpTransportClass ThriftHttpTransportClass;

GType thrift_http_transport_get_type(void);

struct _ThriftHttpTransportClass
{
    ThriftTransportClass parent_class;
};

struct _ThriftHttpTransport
{
    ThriftTransport parent;

    /* Configuration (set via g_object_new properties) */
    char       *url;              /* full URL: https://host:port/cliservice */
    gboolean    use_spnego;       /* SPNEGO (Kerberos) auth */
    gboolean    ssl_verify;
    char       *ssl_ca_file;
    char       *ssl_cert_file;
    char       *ssl_key_file;
    int         connect_timeout;  /* seconds */
    int         request_timeout;  /* seconds */
    char       *username;         /* for basic auth */
    char       *password;         /* for basic auth */
    char       *bearer_token;     /* Authorization: Bearer <token> (JWT/PAT) */

    /* Runtime state */
    CURL       *curl;
    GByteArray *write_buf;
    GByteArray *read_buf;
    gsize       read_pos;
    gboolean    is_connected;
    struct curl_slist *headers;
};

G_END_DECLS

#endif /* ARGUS_THRIFT_HTTP_TRANSPORT_H */
