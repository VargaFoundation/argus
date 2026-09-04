/*
 * Which headers Trino requests carry, checked against a local listener:
 * the session's identity (bearer token, Basic credentials, X-Trino-*) goes
 * to the origin the user connected to and nowhere else — in particular not
 * to the store holding spooled segments.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "trino_internal.h"
#include "locale_helper.h"

int trino_fetch_results(argus_backend_conn_t conn, argus_backend_op_t op,
                        int max_rows, argus_row_cache_t *cache,
                        argus_column_desc_t *columns, int *num_cols);

#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ── A one-request-at-a-time HTTP listener on 127.0.0.1 ──────── */

typedef struct {
    int      fd;
    int      port;
    GThread *thread;
    GMutex   lock;
    GPtrArray *requests;   /* char*: raw request head of each connection */
    int      to_serve;
    const char *body;      /* JSON body of every reply; "[]" by default */
    const char *extra;     /* extra response headers, each CRLF-terminated */
} listener_t;

static gpointer serve(gpointer data)
{
    listener_t *l = data;
    for (int i = 0; i < l->to_serve; i++) {
        int c = accept(l->fd, NULL, NULL);
        if (c < 0) break;
        GString *req = g_string_new(NULL);
        char buf[1024];
        for (;;) {
            ssize_t n = read(c, buf, sizeof(buf));
            if (n <= 0) break;
            g_string_append_len(req, buf, n);
            if (strstr(req->str, "\r\n\r\n")) break;
        }
        char *reply = g_strdup_printf(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            "%sContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
            l->extra ? l->extra : "", strlen(l->body), l->body);
        ssize_t w = write(c, reply, strlen(reply));
        (void)w;
        g_free(reply);
        close(c);
        g_mutex_lock(&l->lock);
        g_ptr_array_add(l->requests, g_string_free(req, FALSE));
        g_mutex_unlock(&l->lock);
    }
    return NULL;
}

static listener_t *listener_start_with_body(int to_serve, const char *body)
{
    listener_t *l = g_new0(listener_t, 1);
    g_mutex_init(&l->lock);
    l->requests = g_ptr_array_new_with_free_func(g_free);
    l->to_serve = to_serve;
    l->body = body;
    l->fd = socket(AF_INET, SOCK_STREAM, 0);
    assert_true(l->fd >= 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    assert_int_equal(bind(l->fd, (struct sockaddr *)&a, sizeof(a)), 0);
    socklen_t alen = sizeof(a);
    assert_int_equal(getsockname(l->fd, (struct sockaddr *)&a, &alen), 0);
    l->port = ntohs(a.sin_port);
    assert_int_equal(listen(l->fd, 4), 0);
    l->thread = g_thread_new("listener", serve, l);
    return l;
}

static listener_t *listener_start(int to_serve)
{
    return listener_start_with_body(to_serve, "[]");
}

static const char *listener_request(listener_t *l, guint i)
{
    g_mutex_lock(&l->lock);
    const char *r = i < l->requests->len ? g_ptr_array_index(l->requests, i) : NULL;
    g_mutex_unlock(&l->lock);
    return r;
}

/* Wait for the listener to have served its quota; requests stay readable. */
static void listener_join(listener_t *l)
{
    if (l->thread) {
        g_thread_join(l->thread);
        l->thread = NULL;
    }
}

static void listener_free(listener_t *l)
{
    listener_join(l);
    close(l->fd);
    g_ptr_array_free(l->requests, TRUE);
    g_mutex_clear(&l->lock);
    g_free(l);
}

static char *origin_url(listener_t *l)
{
    return g_strdup_printf("http://127.0.0.1:%d", l->port);
}

/* Case-insensitive "does this request carry a header starting with `name`". */
static bool has_header(const char *request, const char *name)
{
    char *lr = g_ascii_strdown(request, -1);
    char *ln = g_ascii_strdown(name, -1);
    char *needle = g_strdup_printf("\r\n%s", ln);
    bool found = strstr(lr, needle) != NULL;
    g_free(needle); g_free(ln); g_free(lr);
    return found;
}

/* ── Fixture: a connected-looking Trino session with a bearer token ── */

static trino_conn_t *conn_new(listener_t *origin, trino_auth_mode_t mode)
{
    trino_conn_t *conn = g_new0(trino_conn_t, 1);
    conn->curl = curl_easy_init();
    conn->base_url = origin_url(origin);
    conn->user = g_strdup("alice");
    conn->user_explicit = true;
    conn->auth_mode = mode;
    conn->password = g_strdup(mode == TRINO_AUTH_BEARER ? "secret-token" : "hunter2");
    conn->catalog = g_strdup("hive");
    conn->schema = g_strdup("default");
    conn->session_props = g_hash_table_new_full(g_str_hash, g_str_equal,
                                               g_free, g_free);
    conn->prepared      = g_hash_table_new_full(g_str_hash, g_str_equal,
                                               g_free, g_free);
    conn->default_headers = curl_slist_append(NULL, "X-Trino-User: alice");
    conn->default_headers = curl_slist_append(conn->default_headers, "X-Trino-Catalog: hive");
    if (mode == TRINO_AUTH_BEARER)
        conn->default_headers = curl_slist_append(conn->default_headers,
                                                  "Authorization: Bearer secret-token");
    return conn;
}

static void conn_free(trino_conn_t *conn)
{
    curl_slist_free_all(conn->default_headers);
    curl_easy_cleanup(conn->curl);
    g_free(conn->base_url); g_free(conn->user); g_free(conn->password);
    g_free(conn->catalog); g_free(conn->schema);
    g_free(conn->time_zone); g_free(conn->session_role); g_free(conn->txn_id);
    if (conn->session_props) g_hash_table_destroy(conn->session_props);
    if (conn->prepared) g_hash_table_destroy(conn->prepared);
    g_free(conn);
}

/* ── Tests ───────────────────────────────────────────────────── */

static void test_origin_requests_carry_the_session(void **state)
{
    (void)state;
    listener_t *origin = listener_start(1);
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);

    char *url = g_strdup_printf("%s/v1/statement/20240101_000000_00001_abcde/1", conn->base_url);
    trino_response_t resp = {0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    assert_string_equal(resp.data, "[]");
    free(resp.data);
    g_free(url);

    listener_join(origin);
    const char *req = listener_request(origin, 0);
    assert_non_null(req);
    assert_true(has_header(req, "Authorization: Bearer secret-token"));
    assert_true(has_header(req, "X-Trino-User: alice"));
    assert_true(has_header(req, "X-Trino-Catalog: hive"));
    conn_free(conn);
    listener_free(origin);
}

static void test_spooled_segment_gets_only_its_own_headers(void **state)
{
    (void)state;
    listener_t *origin = listener_start(0);
    listener_t *store = listener_start(1);
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);

    /* What the coordinator would put in the segment descriptor. */
    JsonParser *p = json_parser_new();
    assert_true(json_parser_load_from_data(p,
        "{\"x-amz-security-token\": [\"tok-1\"], \"x-amz-date\": [\"20240101T000000Z\"]}",
        -1, NULL));
    JsonObject *seg_headers = json_node_get_object(json_parser_get_root(p));

    char *uri = g_strdup_printf("http://127.0.0.1:%d/bucket/query/segment-0", store->port);
    trino_response_t resp = {0};
    assert_int_equal(trino_fetch_segment(conn, uri, seg_headers, &resp), 0);
    assert_string_equal(resp.data, "[]");
    free(resp.data);
    g_free(uri);
    g_object_unref(p);

    listener_join(store);
    const char *req = listener_request(store, 0);
    assert_non_null(req);
    assert_false(has_header(req, "Authorization"));
    assert_false(has_header(req, "X-Trino-"));
    assert_true(has_header(req, "x-amz-security-token: tok-1"));
    assert_true(has_header(req, "x-amz-date: 20240101T000000Z"));

    conn_free(conn);
    listener_free(store);
    listener_free(origin);
}

static void test_next_uri_on_another_host_gets_no_credentials(void **state)
{
    (void)state;
    listener_t *origin = listener_start(0);
    listener_t *other = listener_start(1);
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);

    char *url = g_strdup_printf("http://127.0.0.1:%d/v1/statement/x/1", other->port);
    trino_response_t resp = {0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    g_free(url);

    listener_join(other);
    const char *req = listener_request(other, 0);
    assert_non_null(req);
    assert_false(has_header(req, "Authorization"));
    assert_false(has_header(req, "X-Trino-"));

    conn_free(conn);
    listener_free(other);
    listener_free(origin);
}

static void test_basic_credentials_stay_on_the_origin(void **state)
{
    (void)state;
    listener_t *origin = listener_start(1);
    listener_t *other = listener_start(1);
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BASIC);

    char *url = g_strdup_printf("%s/v1/info", conn->base_url);
    trino_response_t resp = {0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    g_free(url);

    url = g_strdup_printf("http://127.0.0.1:%d/v1/info", other->port);
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    g_free(url);

    listener_join(origin);
    listener_join(other);
    assert_true(has_header(listener_request(origin, 0), "Authorization: Basic "));
    assert_false(has_header(listener_request(other, 0), "Authorization"));
    conn_free(conn);
    listener_free(origin);
    listener_free(other);
}

/* Doubles in a result page are parsed by the driver's own JSON scanner
 * (and by json-glib when it is disabled). Both must read "1.5" as 1.5 under
 * a comma-decimal locale, where strtod() would stop at the '.'. */
static void fetch_one_double_page(bool fast_scanner, double expected)
{
    static const char page[] =
        "{\"id\":\"20240101_000000_00001_abcde\","
        "\"columns\":[{\"name\":\"x\",\"type\":\"double\","
        "\"typeSignature\":{\"rawType\":\"double\",\"arguments\":[]}}],"
        "\"data\":[[1.5],[-2.25e-3]],"
        "\"stats\":{\"state\":\"FINISHED\"}}";
    listener_t *origin = listener_start_with_body(1, page);
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);

    trino_operation_t *op = trino_operation_new();
    op->next_uri = g_strdup_printf("%s/v1/statement/20240101_000000_00001_abcde/1",
                                   conn->base_url);
    op->metadata_fetched = true;
    op->columns = calloc(1, sizeof(argus_column_desc_t));
    op->num_cols = 1;

    if (fast_scanner) g_unsetenv("ARGUS_TRINO_NOFASTJSON");
    else g_setenv("ARGUS_TRINO_NOFASTJSON", "1", TRUE);

    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));
    int rc = trino_fetch_results((argus_backend_conn_t)conn,
                                 (argus_backend_op_t)op, 100, &cache,
                                 NULL, NULL);
    g_unsetenv("ARGUS_TRINO_NOFASTJSON");
    listener_join(origin);

    assert_int_equal(rc, 0);
    assert_int_equal(cache.num_rows, 2);
    assert_int_equal(cache.rows[0].cells[0].native_kind, ARGUS_NATIVE_F64);
    assert_true(cache.rows[0].cells[0].native.f64 == expected);
    assert_true(cache.rows[1].cells[0].native.f64 == -2.25e-3);

    argus_row_cache_clear(&cache);
    trino_operation_free(op);
    conn_free(conn);
    listener_free(origin);
}

static void test_result_doubles_ignore_locale(void **state)
{
    (void)state;
    const char *locale = argus_test_use_comma_locale();
    if (!locale && argus_test_locale_required()) fail();
    if (!locale) skip();

    fetch_one_double_page(true, 1.5);
    fetch_one_double_page(false, 1.5);
    argus_test_restore_c_locale();
}

#else /* _WIN32 */

/* The fake Trino above is a POSIX socket listener; an empty test table is
 * a compile error, so say explicitly that nothing runs here. */
static void test_loopback_listener_is_posix_only(void **state)
{
    (void)state;
    skip();
}

#endif /* _WIN32 */


/*
 * Trino carries session changes in response headers and expects them back on
 * the next request. Without this, USE / SET SESSION / SET ROLE / PREPARE /
 * START TRANSACTION report success and then have no effect on the statement
 * that follows.
 */
static void test_session_headers_are_echoed_back(void **state)
{
    (void)state;
    listener_t *origin = listener_start(2);
    origin->extra =
        "X-Trino-Set-Catalog: iceberg\r\n"
        "X-Trino-Set-Schema: analytics\r\n"
        "X-Trino-Set-Session: query_max_run_time=10m\r\n"
        "X-Trino-Set-Role: ALL\r\n"
        "X-Trino-Added-Prepare: p1=SELECT 1\r\n"
        "X-Trino-Started-Transaction-Id: tx-42\r\n";
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);

    char *url = g_strdup_printf("%s/v1/statement/q/1", conn->base_url);
    trino_response_t resp = {0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);

    /* The connection took the change... */
    assert_string_equal(conn->catalog, "iceberg");
    assert_string_equal(conn->schema, "analytics");
    assert_string_equal(conn->session_role, "ALL");
    assert_string_equal(conn->txn_id, "tx-42");

    /* ...and the next request carries it. */
    origin->extra = NULL;
    resp = (trino_response_t){0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    g_free(url);

    listener_join(origin);
    const char *req = listener_request(origin, 1);
    assert_non_null(req);
    assert_true(has_header(req, "X-Trino-Catalog: iceberg"));
    assert_true(has_header(req, "X-Trino-Schema: analytics"));
    assert_true(has_header(req, "X-Trino-Session: query_max_run_time=10m"));
    assert_true(has_header(req, "X-Trino-Role: ALL"));
    assert_true(has_header(req, "X-Trino-Prepared-Statement: p1=SELECT 1"));
    assert_true(has_header(req, "X-Trino-Transaction-Id: tx-42"));

    conn_free(conn);
    listener_free(origin);
}

/* A cleared session property and a closed transaction stop being sent. */
static void test_cleared_session_state_stops_being_sent(void **state)
{
    (void)state;
    listener_t *origin = listener_start(3);
    origin->extra =
        "X-Trino-Set-Session: a=1\r\n"
        "X-Trino-Started-Transaction-Id: tx-1\r\n";
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);

    char *url = g_strdup_printf("%s/v1/statement/q/1", conn->base_url);
    trino_response_t resp = {0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);

    origin->extra =
        "X-Trino-Clear-Session: a\r\n"
        "X-Trino-Clear-Transaction-Id: true\r\n";
    resp = (trino_response_t){0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    assert_null(conn->txn_id);

    origin->extra = NULL;
    resp = (trino_response_t){0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    g_free(url);

    listener_join(origin);
    const char *req = listener_request(origin, 2);
    assert_non_null(req);
    assert_false(has_header(req, "X-Trino-Session: a=1"));
    assert_false(has_header(req, "X-Trino-Transaction-Id:"));

    conn_free(conn);
    listener_free(origin);
}

/*
 * A DSN value carrying a CR/LF must not be able to add a header of its own.
 */
static void test_dsn_values_cannot_inject_headers(void **state)
{
    (void)state;
    listener_t *origin = listener_start(1);
    trino_conn_t *conn = conn_new(origin, TRINO_AUTH_BEARER);
    g_free(conn->catalog);
    conn->catalog = g_strdup("hive\r\nX-Trino-Role: admin");
    conn->headers_dirty = true;

    char *url = g_strdup_printf("%s/v1/statement/q/1", conn->base_url);
    trino_response_t resp = {0};
    assert_int_equal(trino_http_get(conn, url, &resp), 0);
    free(resp.data);
    g_free(url);

    listener_join(origin);
    const char *req = listener_request(origin, 0);
    assert_non_null(req);
    assert_false(has_header(req, "X-Trino-Role: admin"));

    conn_free(conn);
    listener_free(origin);
}

int main(void)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    const struct CMUnitTest tests[] = {
#ifndef _WIN32
        cmocka_unit_test(test_origin_requests_carry_the_session),
        cmocka_unit_test(test_spooled_segment_gets_only_its_own_headers),
        cmocka_unit_test(test_next_uri_on_another_host_gets_no_credentials),
        cmocka_unit_test(test_basic_credentials_stay_on_the_origin),
        cmocka_unit_test(test_result_doubles_ignore_locale),
        cmocka_unit_test(test_session_headers_are_echoed_back),
        cmocka_unit_test(test_cleared_session_state_stops_being_sent),
        cmocka_unit_test(test_dsn_values_cannot_inject_headers),
#else
        cmocka_unit_test(test_loopback_listener_is_posix_only),
#endif
    };
    int rc = cmocka_run_group_tests(tests, NULL, NULL);
    curl_global_cleanup();
    return rc;
}
