#ifndef ARGUS_CURL_COMMON_H
#define ARGUS_CURL_COMMON_H

/*
 * curl_common.h - Settings shared by every libcurl easy handle in the driver.
 *
 * Each HTTP backend keeps its own client code; what they all need is the
 * same floor: URLs that the server hands back (nextUri, spooled segments,
 * OIDC discovery, redirects) must not turn a query into a request on some
 * other protocol, and the handle must behave inside a multithreaded host.
 */

#include <curl/curl.h>
#include <stdbool.h>
#include <stddef.h>

/*
 * The default ceiling on one response body. Every backend accumulated the
 * body with an unbounded realloc, so a server (or anything that could answer
 * in its place) could grow the client's heap until the host application
 * died. No legitimate page from any of these APIs comes near this.
 */
#define ARGUS_HTTP_MAX_BODY ((size_t)256 * 1024 * 1024)

/*
 * One response body being accumulated. The five HTTP backends each had their
 * own copy of this struct and of the callback that fills it; they share both
 * now, and with them the ceiling.
 */
typedef struct argus_http_buf {
    char   *data;       /* NUL-terminated body, or NULL; free() when done */
    size_t  size;       /* bytes in `data`, excluding the NUL */
    size_t  limit;      /* ceiling; 0 means ARGUS_HTTP_MAX_BODY */
    long    http_code;  /* the caller fills this from CURLINFO_RESPONSE_CODE */
    bool    truncated;  /* the ceiling was reached and the transfer aborted */
} argus_http_buf_t;

/*
 * CURLOPT_WRITEFUNCTION over an argus_http_buf_t: appends, keeps the body
 * NUL-terminated, and stops the transfer once the ceiling is passed (curl
 * then fails the request with CURLE_WRITE_ERROR, so a truncated body is
 * never mistaken for a complete one).
 */
size_t argus_http_write_cb(void *contents, size_t size, size_t nmemb,
                           void *userp);

/* Release what argus_http_write_cb allocated and reset the buffer. */
void argus_http_buf_free(argus_http_buf_t *buf);

/*
 * Apply the baseline to `curl`: http and https only (redirects included),
 * TLS 1.2 or newer, no signals. Call right after curl_easy_init() or
 * curl_easy_reset(), before request-specific options.
 */
void argus_curl_apply_baseline(CURL *curl);

/*
 * Connect and overall timeouts, in seconds; 0 leaves curl's default. Also
 * sets a low-speed abort, so a connection that is technically alive but
 * delivering nothing does not hang the application forever — which is what
 * SQL_ATTR_LOGIN_TIMEOUT and SQL_ATTR_QUERY_TIMEOUT are asking for.
 */
void argus_curl_apply_timeouts(CURL *curl, long connect_sec, long total_sec);

/*
 * True when `url` and `origin` share scheme, host and port (default ports
 * normalised, host compared case-insensitively). Credentials obtained for
 * `origin` are only ever sent to URLs that satisfy this.
 */
bool argus_url_same_origin(const char *url, const char *origin);

#endif /* ARGUS_CURL_COMMON_H */
