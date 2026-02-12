#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include "argus/types.h"

/* ── Test: Parse simple connection string ────────────────────── */

static void test_parse_simple(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    int rc = argus_conn_params_parse(&params,
        "HOST=localhost;PORT=10000;UID=hive;PWD=secret");
    assert_int_equal(rc, 0);
    assert_int_equal(params.count, 4);

    assert_string_equal(argus_conn_params_get(&params, "HOST"), "localhost");
    assert_string_equal(argus_conn_params_get(&params, "PORT"), "10000");
    assert_string_equal(argus_conn_params_get(&params, "UID"), "hive");
    assert_string_equal(argus_conn_params_get(&params, "PWD"), "secret");

    argus_conn_params_free(&params);
}

/* ── Test: Case insensitive key lookup ───────────────────────── */

static void test_case_insensitive(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    argus_conn_params_parse(&params, "Host=myserver;port=5433");

    assert_string_equal(argus_conn_params_get(&params, "host"), "myserver");
    assert_string_equal(argus_conn_params_get(&params, "HOST"), "myserver");
    assert_string_equal(argus_conn_params_get(&params, "Port"), "5433");

    argus_conn_params_free(&params);
}

/* ── Test: Brace-enclosed values ─────────────────────────────── */

static void test_brace_values(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    argus_conn_params_parse(&params,
        "HOST=myhost;PWD={pass;with;semicolons};DATABASE=mydb");

    assert_string_equal(argus_conn_params_get(&params, "HOST"), "myhost");
    assert_string_equal(argus_conn_params_get(&params, "PWD"),
                        "pass;with;semicolons");
    assert_string_equal(argus_conn_params_get(&params, "DATABASE"), "mydb");

    argus_conn_params_free(&params);
}

/* ── Test: Empty connection string ───────────────────────────── */

static void test_empty_string(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    int rc = argus_conn_params_parse(&params, "");
    assert_int_equal(rc, 0);
    assert_int_equal(params.count, 0);

    assert_null(argus_conn_params_get(&params, "HOST"));

    argus_conn_params_free(&params);
}

/* ── Test: NULL connection string ────────────────────────────── */

static void test_null_string(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    int rc = argus_conn_params_parse(&params, NULL);
    assert_int_equal(rc, -1);

    argus_conn_params_free(&params);
}

/* ── Test: Whitespace handling ───────────────────────────────── */

static void test_whitespace(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    argus_conn_params_parse(&params,
        "  HOST = myhost ; PORT = 10000 ; UID = hive  ");

    assert_string_equal(argus_conn_params_get(&params, "HOST"), "myhost");
    assert_string_equal(argus_conn_params_get(&params, "PORT"), "10000");
    assert_string_equal(argus_conn_params_get(&params, "UID"), "hive");

    argus_conn_params_free(&params);
}

/* ── Test: Missing key returns NULL ──────────────────────────── */

static void test_missing_key(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    argus_conn_params_parse(&params, "HOST=localhost");

    assert_null(argus_conn_params_get(&params, "PORT"));
    assert_null(argus_conn_params_get(&params, "NONEXISTENT"));

    argus_conn_params_free(&params);
}

/* ── Test: Trailing semicolons ───────────────────────────────── */

static void test_trailing_semicolons(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    argus_conn_params_parse(&params, "HOST=localhost;;;PORT=10000;");

    assert_int_equal(params.count, 2);
    assert_string_equal(argus_conn_params_get(&params, "HOST"), "localhost");
    assert_string_equal(argus_conn_params_get(&params, "PORT"), "10000");

    argus_conn_params_free(&params);
}

/* ── Test: Full connection string ────────────────────────────── */

static void test_full_conn_string(void **state)
{
    (void)state;

    argus_conn_params_t params;
    argus_conn_params_init(&params);

    argus_conn_params_parse(&params,
        "DRIVER=Argus;HOST=hive.example.com;PORT=10000;"
        "UID=admin;PWD={p@ss!word};DATABASE=analytics;"
        "AuthMech=PLAIN;Backend=hive");

    assert_string_equal(argus_conn_params_get(&params, "DRIVER"), "Argus");
    assert_string_equal(argus_conn_params_get(&params, "HOST"), "hive.example.com");
    assert_string_equal(argus_conn_params_get(&params, "PORT"), "10000");
    assert_string_equal(argus_conn_params_get(&params, "UID"), "admin");
    assert_string_equal(argus_conn_params_get(&params, "PWD"), "p@ss!word");
    assert_string_equal(argus_conn_params_get(&params, "DATABASE"), "analytics");
    assert_string_equal(argus_conn_params_get(&params, "AUTHMECH"), "PLAIN");
    assert_string_equal(argus_conn_params_get(&params, "BACKEND"), "hive");

    argus_conn_params_free(&params);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_parse_simple),
        cmocka_unit_test(test_case_insensitive),
        cmocka_unit_test(test_brace_values),
        cmocka_unit_test(test_empty_string),
        cmocka_unit_test(test_null_string),
        cmocka_unit_test(test_whitespace),
        cmocka_unit_test(test_missing_key),
        cmocka_unit_test(test_trailing_semicolons),
        cmocka_unit_test(test_full_conn_string),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
