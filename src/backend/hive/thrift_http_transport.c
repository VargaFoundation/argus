#include "thrift_http_transport.h"
#include <string.h>
#include <stdio.h>

/*
 * ThriftHttpTransport — Thrift binary protocol over HTTP POST.
 *
 * HiveServer2 in HTTP transport mode accepts Thrift binary messages as
 * HTTP POST body to /cliservice.  This GObject subclass of ThriftTransport
 * buffers write() calls into a request buffer, then on flush() performs an
 * HTTP POST via libcurl and stores the response body for subsequent read()
 * calls.
 */

G_DEFINE_TYPE(ThriftHttpTransport, thrift_http_transport, THRIFT_TYPE_TRANSPORT)

/* ── Property IDs ─────────────────────────────────────────────── */

enum {
    PROP_0,
    PROP_URL,
    PROP_USE_SPNEGO,
    PROP_SSL_VERIFY,
    PROP_SSL_CA_FILE,
    PROP_SSL_CERT_FILE,
    PROP_SSL_KEY_FILE,
    PROP_CONNECT_TIMEOUT,
    PROP_REQUEST_TIMEOUT,
    PROP_USERNAME,
    PROP_PASSWORD,
    PROP_BEARER_TOKEN,
};

/* ── curl write callback ─────────────────────────────────────── */

static size_t curl_write_cb(void *data, size_t size, size_t nmemb, void *userp)
{
    GByteArray *buf = (GByteArray *)userp;
    gsize total = size * nmemb;
    g_byte_array_append(buf, data, (guint)total);
    return total;
}

/* ── ThriftTransport virtual methods ─────────────────────────── */

static gboolean
thrift_http_transport_is_open_impl(ThriftTransport *transport)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(transport);
    return self->is_connected;
}

static gboolean
thrift_http_transport_open_impl(ThriftTransport *transport, GError **error)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(transport);

    if (self->is_connected) return TRUE;

    self->curl = curl_easy_init();
    if (!self->curl) {
        g_set_error(error, THRIFT_TRANSPORT_ERROR,
                    THRIFT_TRANSPORT_ERROR_CONNECT,
                    "Failed to initialize libcurl");
        return FALSE;
    }

    self->write_buf = g_byte_array_new();
    self->read_buf  = g_byte_array_new();
    self->read_pos  = 0;

    self->headers = NULL;
    self->headers = curl_slist_append(self->headers,
                                       "Content-Type: application/x-thrift");
    self->headers = curl_slist_append(self->headers,
                                       "Accept: application/x-thrift");

    /* Configure persistent curl settings */
    curl_easy_setopt(self->curl, CURLOPT_URL, self->url);
    curl_easy_setopt(self->curl, CURLOPT_POST, 1L);
    curl_easy_setopt(self->curl, CURLOPT_HTTPHEADER, self->headers);

    /* SSL */
    curl_easy_setopt(self->curl, CURLOPT_SSL_VERIFYPEER,
                     self->ssl_verify ? 1L : 0L);
    curl_easy_setopt(self->curl, CURLOPT_SSL_VERIFYHOST,
                     self->ssl_verify ? 2L : 0L);
    if (self->ssl_ca_file)
        curl_easy_setopt(self->curl, CURLOPT_CAINFO, self->ssl_ca_file);
    if (self->ssl_cert_file)
        curl_easy_setopt(self->curl, CURLOPT_SSLCERT, self->ssl_cert_file);
    if (self->ssl_key_file)
        curl_easy_setopt(self->curl, CURLOPT_SSLKEY, self->ssl_key_file);

    /* Authentication */
    if (self->use_spnego) {
        curl_easy_setopt(self->curl, CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE);
        curl_easy_setopt(self->curl, CURLOPT_USERPWD, ":");
    } else if (self->username && self->username[0]) {
        char userpwd[512];
        snprintf(userpwd, sizeof(userpwd), "%s:%s",
                 self->username, self->password ? self->password : "");
        curl_easy_setopt(self->curl, CURLOPT_USERPWD, userpwd);
    }
    if (self->bearer_token && self->bearer_token[0]) {
        char hdr[8192];
        snprintf(hdr, sizeof(hdr), "Authorization: Bearer %s", self->bearer_token);
        self->headers = curl_slist_append(self->headers, hdr);
        curl_easy_setopt(self->curl, CURLOPT_HTTPHEADER, self->headers);
    }

    /* Cookie jar for session persistence */
    curl_easy_setopt(self->curl, CURLOPT_COOKIEFILE, "");
    curl_easy_setopt(self->curl, CURLOPT_COOKIEJAR, "");

    /* Timeouts */
    if (self->connect_timeout > 0)
        curl_easy_setopt(self->curl, CURLOPT_CONNECTTIMEOUT,
                         (long)self->connect_timeout);

    /* Debug: enable verbose output to stderr */
    if (getenv("ARGUS_CURL_VERBOSE"))
        curl_easy_setopt(self->curl, CURLOPT_VERBOSE, 1L);

    self->is_connected = TRUE;
    return TRUE;
}

static gboolean
thrift_http_transport_close_impl(ThriftTransport *transport, GError **error)
{
    (void)error;
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(transport);

    if (self->curl) {
        curl_easy_cleanup(self->curl);
        self->curl = NULL;
    }
    if (self->headers) {
        curl_slist_free_all(self->headers);
        self->headers = NULL;
    }
    if (self->write_buf) {
        g_byte_array_free(self->write_buf, TRUE);
        self->write_buf = NULL;
    }
    if (self->read_buf) {
        g_byte_array_free(self->read_buf, TRUE);
        self->read_buf = NULL;
    }
    self->is_connected = FALSE;
    return TRUE;
}

static gint32
thrift_http_transport_read_impl(ThriftTransport *transport,
                                gpointer buf, guint32 len, GError **error)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(transport);

    if (!self->read_buf || self->read_pos + len > self->read_buf->len) {
        g_set_error(error, THRIFT_TRANSPORT_ERROR,
                    THRIFT_TRANSPORT_ERROR_RECEIVE,
                    "failed to read %u bytes - %zu available",
                    len,
                    self->read_buf ? self->read_buf->len - self->read_pos : 0);
        return -1;
    }

    memcpy(buf, self->read_buf->data + self->read_pos, len);
    self->read_pos += len;
    return (gint32)len;
}

static gint32
thrift_http_transport_read_all_impl(ThriftTransport *transport,
                                     gpointer buf, guint32 len,
                                     GError **error)
{
    return thrift_http_transport_read_impl(transport, buf, len, error);
}

static gboolean
thrift_http_transport_write_impl(ThriftTransport *transport,
                                 const gpointer buf, guint32 len,
                                 GError **error)
{
    (void)error;
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(transport);

    if (!self->write_buf)
        self->write_buf = g_byte_array_new();

    g_byte_array_append(self->write_buf, buf, len);
    if (getenv("ARGUS_CURL_VERBOSE") && len > 0)
        fprintf(stderr, "[HTTP] write: %u bytes (total=%u)\n",
                len, self->write_buf->len);
    return TRUE;
}

static gboolean
thrift_http_transport_flush_impl(ThriftTransport *transport, GError **error)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(transport);

    if (getenv("ARGUS_CURL_VERBOSE"))
        fprintf(stderr, "[HTTP] flush called (write_buf=%u bytes)\n",
                self->write_buf ? self->write_buf->len : 0);

    if (!self->curl || !self->write_buf || self->write_buf->len == 0)
        return TRUE;

    /* Per-request settings (persistent settings configured in open()) */
    curl_easy_setopt(self->curl, CURLOPT_POSTFIELDS, self->write_buf->data);
    curl_easy_setopt(self->curl, CURLOPT_POSTFIELDSIZE,
                     (long)self->write_buf->len);

    if (self->request_timeout > 0)
        curl_easy_setopt(self->curl, CURLOPT_TIMEOUT,
                         (long)self->request_timeout);

    /* Response buffer */
    if (self->read_buf)
        g_byte_array_set_size(self->read_buf, 0);
    else
        self->read_buf = g_byte_array_new();
    self->read_pos = 0;

    curl_easy_setopt(self->curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(self->curl, CURLOPT_WRITEDATA, self->read_buf);

    /* Perform the request */
    CURLcode rc = curl_easy_perform(self->curl);

    if (getenv("ARGUS_CURL_VERBOSE")) {
        long http_code_dbg = 0;
        curl_easy_getinfo(self->curl, CURLINFO_RESPONSE_CODE, &http_code_dbg);
        fprintf(stderr, "[HTTP] flush: sent %u bytes, got %u bytes back (HTTP %ld, curl=%d)\n",
                self->write_buf->len, self->read_buf->len, http_code_dbg, rc);
        if (self->read_buf->len >= 4) {
            fprintf(stderr, "[HTTP] response first bytes: %02x %02x %02x %02x\n",
                    self->read_buf->data[0], self->read_buf->data[1],
                    self->read_buf->data[2], self->read_buf->data[3]);
        }
    }

    /* Clear write buffer regardless of result */
    g_byte_array_set_size(self->write_buf, 0);

    if (rc != CURLE_OK) {
        g_set_error(error, THRIFT_TRANSPORT_ERROR,
                    THRIFT_TRANSPORT_ERROR_SEND,
                    "HTTP POST failed: %s", curl_easy_strerror(rc));
        return FALSE;
    }

    /* Check HTTP status */
    long http_code = 0;
    curl_easy_getinfo(self->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code != 200) {
        g_set_error(error, THRIFT_TRANSPORT_ERROR,
                    THRIFT_TRANSPORT_ERROR_SEND,
                    "HTTP %ld from %s", http_code, self->url);
        return FALSE;
    }

    return TRUE;
}

/* ── No-op for read_end / write_end ──────────────────────────── */

static gboolean
thrift_http_transport_noop(ThriftTransport *transport, GError **error)
{
    (void)transport;
    (void)error;
    return TRUE;
}

/* ── GObject boilerplate ─────────────────────────────────────── */

static void
thrift_http_transport_finalize(GObject *object)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(object);

    thrift_http_transport_close_impl(THRIFT_TRANSPORT(self), NULL);
    g_free(self->url);
    g_free(self->ssl_ca_file);
    g_free(self->ssl_cert_file);
    g_free(self->ssl_key_file);
    g_free(self->username);
    g_free(self->password);
    g_free(self->bearer_token);

    G_OBJECT_CLASS(thrift_http_transport_parent_class)->finalize(object);
}

static void
thrift_http_transport_set_property(GObject *object, guint prop_id,
                                    const GValue *value, GParamSpec *pspec)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(object);
    switch (prop_id) {
    case PROP_URL:
        g_free(self->url);
        self->url = g_value_dup_string(value);
        break;
    case PROP_USE_SPNEGO:
        self->use_spnego = g_value_get_boolean(value);
        break;
    case PROP_SSL_VERIFY:
        self->ssl_verify = g_value_get_boolean(value);
        break;
    case PROP_SSL_CA_FILE:
        g_free(self->ssl_ca_file);
        self->ssl_ca_file = g_value_dup_string(value);
        break;
    case PROP_SSL_CERT_FILE:
        g_free(self->ssl_cert_file);
        self->ssl_cert_file = g_value_dup_string(value);
        break;
    case PROP_SSL_KEY_FILE:
        g_free(self->ssl_key_file);
        self->ssl_key_file = g_value_dup_string(value);
        break;
    case PROP_CONNECT_TIMEOUT:
        self->connect_timeout = g_value_get_int(value);
        break;
    case PROP_REQUEST_TIMEOUT:
        self->request_timeout = g_value_get_int(value);
        break;
    case PROP_USERNAME:
        g_free(self->username);
        self->username = g_value_dup_string(value);
        break;
    case PROP_PASSWORD:
        g_free(self->password);
        self->password = g_value_dup_string(value);
        break;
    case PROP_BEARER_TOKEN:
        g_free(self->bearer_token);
        self->bearer_token = g_value_dup_string(value);
        break;
    default:
        G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    }
}

static void
thrift_http_transport_get_property(GObject *object, guint prop_id,
                                    GValue *value, GParamSpec *pspec)
{
    ThriftHttpTransport *self = THRIFT_HTTP_TRANSPORT(object);
    switch (prop_id) {
    case PROP_URL:
        g_value_set_string(value, self->url);
        break;
    case PROP_USE_SPNEGO:
        g_value_set_boolean(value, self->use_spnego);
        break;
    case PROP_SSL_VERIFY:
        g_value_set_boolean(value, self->ssl_verify);
        break;
    case PROP_SSL_CA_FILE:
        g_value_set_string(value, self->ssl_ca_file);
        break;
    case PROP_SSL_CERT_FILE:
        g_value_set_string(value, self->ssl_cert_file);
        break;
    case PROP_SSL_KEY_FILE:
        g_value_set_string(value, self->ssl_key_file);
        break;
    case PROP_CONNECT_TIMEOUT:
        g_value_set_int(value, self->connect_timeout);
        break;
    case PROP_REQUEST_TIMEOUT:
        g_value_set_int(value, self->request_timeout);
        break;
    case PROP_USERNAME:
        g_value_set_string(value, self->username);
        break;
    case PROP_PASSWORD:
        g_value_set_string(value, self->password);
        break;
    case PROP_BEARER_TOKEN:
        g_value_set_string(value, self->bearer_token);
        break;
    default:
        G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    }
}

static void
thrift_http_transport_init(ThriftHttpTransport *self)
{
    self->curl = NULL;
    self->write_buf = NULL;
    self->read_buf = NULL;
    self->read_pos = 0;
    self->is_connected = FALSE;
    self->headers = NULL;
    self->ssl_verify = FALSE;
    self->connect_timeout = 30;
    self->request_timeout = 300;
}

static void
thrift_http_transport_class_init(ThriftHttpTransportClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS(klass);
    ThriftTransportClass *transport_class = THRIFT_TRANSPORT_CLASS(klass);

    gobject_class->finalize = thrift_http_transport_finalize;
    gobject_class->set_property = thrift_http_transport_set_property;
    gobject_class->get_property = thrift_http_transport_get_property;

    transport_class->is_open  = thrift_http_transport_is_open_impl;
    transport_class->open     = thrift_http_transport_open_impl;
    transport_class->close    = thrift_http_transport_close_impl;
    transport_class->read     = thrift_http_transport_read_impl;
    transport_class->read_all = thrift_http_transport_read_all_impl;
    transport_class->read_end = thrift_http_transport_noop;
    transport_class->write_end = thrift_http_transport_noop;
    transport_class->write    = thrift_http_transport_write_impl;
    transport_class->flush    = thrift_http_transport_flush_impl;

    g_object_class_install_property(gobject_class, PROP_URL,
        g_param_spec_string("url", NULL, NULL, NULL, G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_USE_SPNEGO,
        g_param_spec_boolean("use-spnego", NULL, NULL, FALSE,
                             G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_SSL_VERIFY,
        g_param_spec_boolean("ssl-verify", NULL, NULL, FALSE,
                             G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_SSL_CA_FILE,
        g_param_spec_string("ssl-ca-file", NULL, NULL, NULL,
                            G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_SSL_CERT_FILE,
        g_param_spec_string("ssl-cert-file", NULL, NULL, NULL,
                            G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_SSL_KEY_FILE,
        g_param_spec_string("ssl-key-file", NULL, NULL, NULL,
                            G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_CONNECT_TIMEOUT,
        g_param_spec_int("connect-timeout", NULL, NULL, 0, 3600, 30,
                         G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_REQUEST_TIMEOUT,
        g_param_spec_int("request-timeout", NULL, NULL, 0, 86400, 300,
                         G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_USERNAME,
        g_param_spec_string("username", NULL, NULL, NULL, G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_PASSWORD,
        g_param_spec_string("password", NULL, NULL, NULL, G_PARAM_READWRITE));
    g_object_class_install_property(gobject_class, PROP_BEARER_TOKEN,
        g_param_spec_string("bearer-token", NULL, NULL, NULL, G_PARAM_READWRITE));
}
