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

/*
 * Apply the baseline to `curl`: http and https only (redirects included),
 * TLS 1.2 or newer, no signals. Call right after curl_easy_init() or
 * curl_easy_reset(), before request-specific options.
 */
void argus_curl_apply_baseline(CURL *curl);

/*
 * True when `url` and `origin` share scheme, host and port (default ports
 * normalised, host compared case-insensitively). Credentials obtained for
 * `origin` are only ever sent to URLs that satisfy this.
 */
bool argus_url_same_origin(const char *url, const char *origin);

#endif /* ARGUS_CURL_COMMON_H */
