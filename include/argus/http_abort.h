/* SPDX-License-Identifier: Apache-2.0 */
/*
 * argus/http_abort.h — a request to abandon the call in flight on a connection.
 *
 * SQLCancel from another thread cannot take the statement lock the running
 * call holds, so it raises a flag; the running call used to notice it only
 * at its next checkpoint, which for the HTTP backends is after
 * curl_easy_perform returns -- the end of a query that may take minutes.
 * The README said as much: "it takes effect when the backend call in
 * progress returns".
 *
 * This flag is what libcurl's progress callback polls, about once a second,
 * while a transfer waits. A backend keeps one in its connection state,
 * hands it to argus_curl_apply_abort() on every request, and exposes it to
 * the ODBC layer through argus_backend_t.abort_flag; SQLCancel raises it and
 * the transfer comes back CURLE_ABORTED_BY_CALLBACK, which the call reports
 * as the cancel it is (HY008) at the checkpoint it then reaches at once.
 *
 * One flag per connection, because one request is in flight per connection:
 * the backends share one easy handle per connection and libcurl allows one
 * transfer on it at a time. It is raised only for a running call, and
 * cleared by that call's checkpoint, by the next execute, and by the reset
 * every catalog function starts with -- so a raise that outlived its call
 * cannot abandon the next one. Atomics only: it is written from the
 * cancelling thread and read from the transfer's.
 */
#ifndef ARGUS_HTTP_ABORT_H
#define ARGUS_HTTP_ABORT_H

#include <glib.h>
#include <stdbool.h>

typedef struct argus_http_abort {
    gint requested;
} argus_http_abort_t;

static inline void argus_http_abort_request(argus_http_abort_t *a)
{
    if (a) g_atomic_int_set(&a->requested, 1);
}

static inline void argus_http_abort_clear(argus_http_abort_t *a)
{
    if (a) g_atomic_int_set(&a->requested, 0);
}

static inline bool argus_http_abort_pending(const argus_http_abort_t *a)
{
    return a && g_atomic_int_get(&a->requested) != 0;
}

#endif /* ARGUS_HTTP_ABORT_H */
