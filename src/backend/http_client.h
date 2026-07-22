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
 * POST `body` as application/json to `url` over HTTPS.
 * `timeout_sec` bounds the whole transfer (connect + transfer).
 * Returns 0 on a 2xx response, -1 otherwise. The response body is discarded.
 * Thread-safe provided curl_global_init() has run (done at library load).
 */
int argus_http_post_json(const char *url, const char *body, long timeout_sec);

#endif /* ARGUS_HTTP_CLIENT_H */
