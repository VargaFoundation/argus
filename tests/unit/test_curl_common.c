/* SPDX-License-Identifier: Apache-2.0 */
/*
 * The libcurl baseline every HTTP client in the driver applies: only http
 * and https may be spoken, and credentials belong to one origin.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <curl/curl.h>
#include <string.h>
#include <glib.h>

#include "curl_common.h"

#ifndef _WIN32
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

/* A server that accepts and then says nothing, the way a coordinator does
 * while a long query runs: the transfer waits for a first byte that never
 * comes. Closed when the test is done with it. */
typedef struct {
    int      listen_fd;
    int      port;
    GThread *thread;
    gint     stop;
} silent_server_t;

static gpointer silent_server_run(gpointer data)
{
    silent_server_t *srv = data;
    while (!g_atomic_int_get(&srv->stop)) {
        struct timeval tv = { 0, 200000 };
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(srv->listen_fd, &rfds);
        if (select(srv->listen_fd + 1, &rfds, NULL, NULL, &tv) <= 0) continue;
        int c = accept(srv->listen_fd, NULL, NULL);
        if (c < 0) continue;
        /* Read the request and keep the socket open, answering nothing. */
        char buf[1024];
        while (!g_atomic_int_get(&srv->stop)) {
            struct timeval tv2 = { 0, 200000 };
            fd_set r2;
            FD_ZERO(&r2);
            FD_SET(c, &r2);
            int n = select(c + 1, &r2, NULL, NULL, &tv2);
            if (n > 0 && recv(c, buf, sizeof(buf), 0) <= 0) break;
        }
        close(c);
    }
    return NULL;
}

static int silent_server_start(silent_server_t *srv)
{
    memset(srv, 0, sizeof(*srv));
    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) return -1;
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    if (bind(srv->listen_fd, (struct sockaddr *)&a, sizeof(a)) < 0) return -1;
    if (listen(srv->listen_fd, 4) < 0) return -1;
    socklen_t len = sizeof(a);
    if (getsockname(srv->listen_fd, (struct sockaddr *)&a, &len) < 0) return -1;
    srv->port = ntohs(a.sin_port);
    srv->thread = g_thread_new("silent", silent_server_run, srv);
    return 0;
}

static void silent_server_stop(silent_server_t *srv)
{
    g_atomic_int_set(&srv->stop, 1);
    if (srv->thread) g_thread_join(srv->thread);
    if (srv->listen_fd >= 0) close(srv->listen_fd);
}

static size_t sink(void *p, size_t sz, size_t n, void *u)
{
    (void)p; (void)u;
    return sz * n;
}

typedef struct {
    argus_http_abort_t *flag;
    int                 after_ms;
} raise_later_t;

static gpointer raise_later(gpointer data)
{
    raise_later_t *r = data;
    g_usleep((gulong)r->after_ms * 1000);
    argus_http_abort_request(r->flag);
    return NULL;
}

/*
 * The whole point of the flag: a transfer waiting on a server that has not
 * answered ends within about a second of the flag being raised from another
 * thread, as CURLE_ABORTED_BY_CALLBACK -- not when the server finally
 * speaks, and not at the transfer's own timeout. Without the flag the same
 * transfer waits for the timeout, which is the behaviour SQLCancel had on
 * every HTTP backend.
 */
static void test_abort_ends_a_transfer_that_is_waiting(void **state)
{
    (void)state;
    silent_server_t srv;
    assert_int_equal(silent_server_start(&srv), 0);

    char url[64];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d/", srv.port);

    argus_http_abort_t flag = {0};
    CURL *curl = curl_easy_init();
    assert_non_null(curl);
    argus_curl_apply_baseline(curl);
    argus_curl_apply_abort(curl, &flag);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, sink);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    raise_later_t r = { &flag, 300 };
    GThread *t = g_thread_new("cancel", raise_later, &r);
    gint64 t0 = g_get_monotonic_time();
    CURLcode cc = curl_easy_perform(curl);
    gint64 elapsed_ms = (g_get_monotonic_time() - t0) / 1000;
    g_thread_join(t);

    print_message("abort raised at 300 ms, transfer ended at %ld ms\n",
                  (long)elapsed_ms);
    assert_int_equal(cc, CURLE_ABORTED_BY_CALLBACK);
    assert_true(elapsed_ms >= 300);        /* not before it was raised */
    assert_true(elapsed_ms < 5000);        /* and nowhere near the timeout */
    assert_true(argus_http_abort_pending(&flag));

    /* Lowered, the same handle transfers again; here it waits out a short
     * timeout instead, which shows the callback aborts nothing on its own. */
    argus_http_abort_clear(&flag);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 1L);
    cc = curl_easy_perform(curl);
    assert_int_equal(cc, CURLE_OPERATION_TIMEDOUT);

    curl_easy_cleanup(curl);
    silent_server_stop(&srv);
}
#endif /* !_WIN32 */

/* NULL is safe on every side of the flag. */
static void test_abort_flag_null_is_inert(void **state)
{
    (void)state;
    argus_http_abort_request(NULL);
    argus_http_abort_clear(NULL);
    assert_false(argus_http_abort_pending(NULL));
    argus_curl_apply_abort(NULL, NULL);
    argus_http_abort_t f = {0};
    assert_false(argus_http_abort_pending(&f));
    argus_http_abort_request(&f);
    assert_true(argus_http_abort_pending(&f));
    argus_http_abort_clear(&f);
    assert_false(argus_http_abort_pending(&f));
}

static void test_same_origin_matches_scheme_host_port(void **state)
{
    (void)state;
    assert_true(argus_url_same_origin("https://trino.example.com:8443/v1/statement/abc/1",
                                      "https://trino.example.com:8443"));
    assert_true(argus_url_same_origin("https://TRINO.example.com:8443/x", "https://trino.EXAMPLE.com:8443/"));
    assert_true(argus_url_same_origin("https://h/x", "https://h:443/y?q#f"));
    assert_true(argus_url_same_origin("http://h:80/x", "http://h"));
    assert_true(argus_url_same_origin("https://[::1]:8443/x", "https://[::1]:8443"));
    assert_true(argus_url_same_origin("https://user:pw@h:8443/x", "https://h:8443"));
    assert_true(argus_url_same_origin("HTTPS://h:8443/x", "https://h:8443"));
}

static void test_same_origin_rejects_other_hosts(void **state)
{
    (void)state;
    assert_false(argus_url_same_origin("https://s3.amazonaws.com/bucket/seg", "https://trino.example.com:8443"));
    assert_false(argus_url_same_origin("https://trino.example.com:8444/x", "https://trino.example.com:8443"));
    assert_false(argus_url_same_origin("http://trino.example.com:8443/x", "https://trino.example.com:8443"));
    assert_false(argus_url_same_origin("https://trino.example.com/x", "https://trino.example.com:8443"));
    assert_false(argus_url_same_origin("https://trino.example.com.evil/x", "https://trino.example.com"));
    assert_false(argus_url_same_origin("https://h@evil/x", "https://h"));
    assert_false(argus_url_same_origin("https://[::1]:8443/x", "https://[::2]:8443"));
    assert_false(argus_url_same_origin("/v1/statement", "https://h"));
    assert_false(argus_url_same_origin("https://h:port/x", "https://h"));
    assert_false(argus_url_same_origin("https://h:/x", "https://h"));
    assert_false(argus_url_same_origin(NULL, "https://h"));
    assert_false(argus_url_same_origin("https://h", NULL));
    assert_false(argus_url_same_origin("", ""));
}

static size_t discard(void *p, size_t s, size_t n, void *u) { (void)p; (void)u; return s * n; }

static void test_baseline_refuses_non_http_schemes(void **state)
{
    (void)state;
    CURL *c = curl_easy_init();
    assert_non_null(c);
    argus_curl_apply_baseline(c);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, discard);

    /* A URL a server could hand back in nextUri or a segment descriptor. */
    curl_easy_setopt(c, CURLOPT_URL, "file:///etc/hostname");
    assert_int_equal(curl_easy_perform(c), CURLE_UNSUPPORTED_PROTOCOL);

    curl_easy_setopt(c, CURLOPT_URL, "ftp://127.0.0.1:1/x");
    assert_int_equal(curl_easy_perform(c), CURLE_UNSUPPORTED_PROTOCOL);

    curl_easy_setopt(c, CURLOPT_URL, "gopher://127.0.0.1:1/x");
    assert_int_equal(curl_easy_perform(c), CURLE_UNSUPPORTED_PROTOCOL);

    curl_easy_cleanup(c);
}


/*
 * Every HTTP backend used to accumulate a response body with an unbounded
 * realloc, so a server that never stopped sending grew the host
 * application's heap until it died. The shared buffer has a ceiling, and
 * hitting it aborts the transfer rather than handing back a short body that
 * would read as a complete one.
 */
static void test_response_body_has_a_ceiling(void **state)
{
    (void)state;
    char chunk[64];
    memset(chunk, 'x', sizeof(chunk));

    argus_http_buf_t buf = {0};
    buf.limit = 100;

    /* Under the ceiling: accepted, and the body stays NUL-terminated. */
    assert_int_equal(argus_http_write_cb(chunk, 1, 64, &buf), 64);
    assert_int_equal((int)buf.size, 64);
    assert_int_equal(buf.data[64], '\0');
    assert_false(buf.truncated);

    /* Over it: a short write, which is how curl is told to give up. */
    assert_int_equal(argus_http_write_cb(chunk, 1, 64, &buf), 0);
    assert_true(buf.truncated);
    assert_int_equal((int)buf.size, 64);   /* nothing was appended */

    argus_http_buf_free(&buf);
    assert_null(buf.data);
    assert_int_equal((int)buf.size, 0);
}

/* A body that fits is assembled across as many writes as curl makes. */
static void test_response_body_is_assembled(void **state)
{
    (void)state;
    argus_http_buf_t buf = {0};
    assert_int_equal(argus_http_write_cb("he", 1, 2, &buf), 2);
    assert_int_equal(argus_http_write_cb("llo", 1, 3, &buf), 3);
    assert_string_equal(buf.data, "hello");
    assert_int_equal((int)buf.size, 5);
    assert_false(buf.truncated);
    argus_http_buf_free(&buf);
}


/*
 * Which statuses are worth asking again about, and how long to wait. A
 * policy rather than a loop: only a request that can be repeated safely may
 * use it, which is why it lives here and not inside a client.
 */
static void test_retry_policy(void **state)
{
    (void)state;

    /* Not transient: no retry, whatever the attempt. */
    assert_int_equal(argus_http_retry_delay_ms(200, 0, 0), 0);
    assert_int_equal(argus_http_retry_delay_ms(400, 0, 0), 0);
    assert_int_equal(argus_http_retry_delay_ms(401, 3, 0), 0);
    assert_int_equal(argus_http_retry_delay_ms(404, 0, 0), 0);
    assert_int_equal(argus_http_retry_delay_ms(500, 0, 0), 0);

    /* Transient: a doubling backoff from 100 ms. */
    assert_int_equal(argus_http_retry_delay_ms(429, 0, 0), 100);
    assert_int_equal(argus_http_retry_delay_ms(502, 1, 0), 200);
    assert_int_equal(argus_http_retry_delay_ms(503, 2, 0), 400);
    assert_int_equal(argus_http_retry_delay_ms(504, 3, 0), 800);

    /* The server's own Retry-After wins over the backoff. */
    assert_int_equal(argus_http_retry_delay_ms(503, 0, 2), 2000);
    assert_int_equal(argus_http_retry_delay_ms(429, 5, 1), 1000);

    /* ...but cannot park the calling thread: a hostile or mistaken
     * Retry-After is clamped to 30 s. */
    assert_int_equal(argus_http_retry_delay_ms(503, 0, 86400), 30000);
    /* The backoff has its own ceiling, reached at the eighth attempt. */
    assert_int_equal(argus_http_retry_delay_ms(503, 8, 0), 25600);
    assert_int_equal(argus_http_retry_delay_ms(503, 99, 0), 25600);

    /* A negative or zero Retry-After is treated as absent. */
    assert_int_equal(argus_http_retry_delay_ms(503, 0, 0), 100);
    assert_int_equal(argus_http_retry_delay_ms(503, 0, -5), 100);
}

int main(void)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_same_origin_matches_scheme_host_port),
        cmocka_unit_test(test_same_origin_rejects_other_hosts),
        cmocka_unit_test(test_baseline_refuses_non_http_schemes),
        cmocka_unit_test(test_response_body_has_a_ceiling),
        cmocka_unit_test(test_response_body_is_assembled),
        cmocka_unit_test(test_retry_policy),
        cmocka_unit_test(test_abort_flag_null_is_inert),
#ifndef _WIN32
        cmocka_unit_test(test_abort_ends_a_transfer_that_is_waiting),
#endif
    };
    int rc = cmocka_run_group_tests(tests, NULL, NULL);
    curl_global_cleanup();
    return rc;
}
