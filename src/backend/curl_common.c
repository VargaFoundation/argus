#include "curl_common.h"

#include <glib.h>
#include <string.h>

void argus_curl_apply_baseline(CURL *curl)
{
    if (!curl) return;
#if CURL_AT_LEAST_VERSION(7, 85, 0)
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
#else
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS, (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
    curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS, (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
#endif
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, (long)CURL_SSLVERSION_TLSv1_2);
    /* Without this, curl's DNS/connect timeouts use SIGALRM, which is not
     * safe in the multithreaded applications that load an ODBC driver. */
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
}

/* Scheme, host and port of an http(s) URL. Returns false when it has none. */
typedef struct {
    const char *scheme; size_t scheme_len;
    const char *host;   size_t host_len;
    long        port;
} url_origin_t;

static bool parse_origin(const char *url, url_origin_t *o)
{
    if (!url) return false;
    const char *p = strstr(url, "://");
    if (!p || p == url) return false;
    o->scheme = url;
    o->scheme_len = (size_t)(p - url);

    const char *auth = p + 3;
    size_t auth_len = strcspn(auth, "/?#");
    const char *at = memchr(auth, '@', auth_len);
    if (at) { auth_len -= (size_t)(at + 1 - auth); auth = at + 1; }
    if (auth_len == 0) return false;

    const char *host = auth, *host_end;
    if (*auth == '[') {
        const char *close = memchr(auth, ']', auth_len);
        if (!close) return false;
        host_end = close + 1;
    } else {
        const char *colon = memchr(auth, ':', auth_len);
        host_end = colon ? colon : auth + auth_len;
    }
    o->host = host;
    o->host_len = (size_t)(host_end - host);

    o->port = -1;
    if (host_end < auth + auth_len) {
        if (*host_end != ':') return false;
        const char *q = host_end + 1;
        if (q == auth + auth_len) return false;
        long port = 0;
        for (; q < auth + auth_len; q++) {
            if (!g_ascii_isdigit(*q)) return false;
            port = port * 10 + (*q - '0');
            if (port > 65535) return false;
        }
        o->port = port;
    }
    if (o->port < 0) {
        if (o->scheme_len == 5 && g_ascii_strncasecmp(o->scheme, "https", 5) == 0) o->port = 443;
        else if (o->scheme_len == 4 && g_ascii_strncasecmp(o->scheme, "http", 4) == 0) o->port = 80;
    }
    return true;
}

bool argus_url_same_origin(const char *url, const char *origin)
{
    url_origin_t a, b;
    if (!parse_origin(url, &a) || !parse_origin(origin, &b)) return false;
    return a.scheme_len == b.scheme_len &&
           g_ascii_strncasecmp(a.scheme, b.scheme, a.scheme_len) == 0 &&
           a.host_len == b.host_len &&
           g_ascii_strncasecmp(a.host, b.host, a.host_len) == 0 &&
           a.port == b.port;
}
