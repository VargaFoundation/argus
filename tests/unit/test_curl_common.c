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

#include "curl_common.h"

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
    };
    int rc = cmocka_run_group_tests(tests, NULL, NULL);
    curl_global_cleanup();
    return rc;
}
