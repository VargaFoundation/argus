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

int main(void)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_same_origin_matches_scheme_host_port),
        cmocka_unit_test(test_same_origin_rejects_other_hosts),
        cmocka_unit_test(test_baseline_refuses_non_http_schemes),
    };
    int rc = cmocka_run_group_tests(tests, NULL, NULL);
    curl_global_cleanup();
    return rc;
}
