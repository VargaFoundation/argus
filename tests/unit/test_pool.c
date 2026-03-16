/*
 * Unit tests for connection pool (pool.c)
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/* ── Test: acquire from empty pool returns NULL ──────────────── */

static void test_pool_acquire_empty(void **state)
{
    (void)state;

    const argus_backend_t *backend = NULL;
    argus_backend_conn_t conn = argus_pool_acquire(
        "localhost", 10000, "hive", "user1", &backend);

    assert_null(conn);
    assert_null(backend);
}

/* ── Test: release then acquire returns same connection ──────── */

static void test_pool_release_acquire(void **state)
{
    (void)state;

    /* Create a fake backend for the pool entry */
    static const argus_backend_t fake_backend = {
        .name = "hive",
    };

    /* Use a non-NULL sentinel as fake connection */
    argus_backend_conn_t fake_conn = (argus_backend_conn_t)(uintptr_t)0xDEAD;

    argus_pool_release("myhost", 10000, "hive", "user1",
                       &fake_backend, fake_conn);

    const argus_backend_t *out_backend = NULL;
    argus_backend_conn_t got = argus_pool_acquire(
        "myhost", 10000, "hive", "user1", &out_backend);

    assert_ptr_equal(got, fake_conn);
    assert_ptr_equal(out_backend, &fake_backend);

    /* Release it back so cleanup can free it (it's not a real conn) */
    /* Don't cleanup because disconnect would crash on fake conn */
}

/* ── Test: different key does not match ──────────────────────── */

static void test_pool_different_key(void **state)
{
    (void)state;

    static const argus_backend_t fake_backend = { .name = "hive" };
    argus_backend_conn_t fake_conn = (argus_backend_conn_t)(uintptr_t)0xBEEF;

    argus_pool_release("hostA", 10000, "hive", "user1",
                       &fake_backend, fake_conn);

    /* Different host */
    const argus_backend_t *out_backend = NULL;
    argus_backend_conn_t got = argus_pool_acquire(
        "hostB", 10000, "hive", "user1", &out_backend);
    assert_null(got);

    /* Different port */
    got = argus_pool_acquire("hostA", 9999, "hive", "user1", &out_backend);
    assert_null(got);

    /* Different backend */
    got = argus_pool_acquire("hostA", 10000, "trino", "user1", &out_backend);
    assert_null(got);

    /* Different user */
    got = argus_pool_acquire("hostA", 10000, "hive", "user2", &out_backend);
    assert_null(got);

    /* Correct key should work */
    got = argus_pool_acquire("hostA", 10000, "hive", "user1", &out_backend);
    assert_ptr_equal(got, fake_conn);
}

/* ── Test: acquired connection is not re-acquired ────────────── */

static void test_pool_in_use(void **state)
{
    (void)state;

    static const argus_backend_t fake_backend = { .name = "hive" };
    argus_backend_conn_t fake_conn = (argus_backend_conn_t)(uintptr_t)0xCAFE;

    argus_pool_release("poolhost", 5000, "hive", "alice",
                       &fake_backend, fake_conn);

    /* First acquire should succeed */
    const argus_backend_t *out = NULL;
    argus_backend_conn_t got = argus_pool_acquire(
        "poolhost", 5000, "hive", "alice", &out);
    assert_ptr_equal(got, fake_conn);

    /* Second acquire should fail (connection is in-use) */
    got = argus_pool_acquire("poolhost", 5000, "hive", "alice", &out);
    assert_null(got);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_pool_acquire_empty),
        cmocka_unit_test(test_pool_release_acquire),
        cmocka_unit_test(test_pool_different_key),
        cmocka_unit_test(test_pool_in_use),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
