#ifndef ARGUS_HTTP_CLIENT_H
#define ARGUS_HTTP_CLIENT_H

/*
 * http_client.h - Minimal shared HTTPS client.
 *
 * Factored out of the per-backend curl helpers (cf. trino_session.c) for
 * fire-and-forget JSON POSTs that do not need a persistent session, custom
 * auth, or response bodies — currently the telemetry sender. TLS peer/host
 * verification is always on and the system trust store is used; there is no
 * option to disable verification here.
 */

#include <stddef.h>

/*
 * Consulted by argus_http_post_json() while the transfer runs -- libcurl
 * calls it frequently, and at least about once a second when nothing moves
 * (connecting, waiting for the response). A non-zero return aborts the
 * transfer, which is how a caller that is shutting down gets out of a POST
 * long before `timeout_sec` would.
 */
typedef int (*argus_http_abort_fn)(void *ctx);

/*
 * POST `body` as application/json to `url` over HTTPS.
 * `timeout_sec` bounds the whole transfer (connect + transfer).
 * `should_abort` (may be NULL) is polled during the transfer; see above.
 * Returns 0 on a 2xx response, -1 otherwise. The response body is discarded.
 * Thread-safe provided curl_global_init() has run (done at library load).
 */
int argus_http_post_json(const char *url, const char *body, long timeout_sec,
                         argus_http_abort_fn should_abort, void *ctx);

#endif /* ARGUS_HTTP_CLIENT_H */
