#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
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

/* ── Redaction: the one secret-key list used by every copy that leaves
 *    the driver (OutConnectionString, observability taps) ───────── */

static void assert_redacted(const char *in, const char *expected)
{
    char *out = argus_connstr_redact(in);
    assert_non_null(out);
    assert_string_equal(out, expected);
    free(out);
}

static void test_redact_masks_password(void **state)
{
    (void)state;
    assert_redacted("HOST=h;PWD=secret;UID=u", "HOST=h;PWD=***;UID=u");
    assert_redacted("Password=secret;host=h", "Password=***;host=h");
    /* Spelling and order of the keys are preserved: the string is replayed
     * by the caller to reconnect. */
    assert_redacted("Host=h;Pwd=x", "Host=h;Pwd=***");
}

static void test_redact_tolerates_spaces_and_braces(void **state)
{
    (void)state;
    /* " PWD = secret " would slip past a "PWD=" prefix match. */
    assert_redacted("HOST=h; PWD = secret ;UID=u", "HOST=h;PWD=***;UID=u");
    assert_redacted("HOST=h;\tPassword\t=\tsecret", "HOST=h;Password=***");
    /* Braced values may contain ';' — the whole value is one secret. */
    assert_redacted("PWD={a;b};HOST=h", "PWD=***;HOST=h");
    assert_redacted("PWD={p@ss;word}", "PWD=***");
    /* Braces around a non-secret value survive verbatim. */
    assert_redacted("HOST=h;SearchPath={a;b}", "HOST=h;SearchPath={a;b}");
}

static void test_redact_masks_every_credential_key(void **state)
{
    (void)state;
    static const char *const secret_keys[] = {
        "PWD", "PASSWORD", "ClientSecret", "OAuth2ClientSecret",
        "AccessToken", "BQAccessToken", "RefreshToken", "IdToken",
        "BearerToken", "Token", "ApiKey", "SSLKeyPassword", "Passphrase",
        "LicenseToken", "AuditKey", "OtlpAuthHeader", "VaultToken",
        "MyCustomSecret", NULL
    };
    for (int i = 0; secret_keys[i]; i++) {
        assert_true(argus_connstr_key_is_secret(secret_keys[i]));
        char in[128], expected[128];
        snprintf(in, sizeof(in), "HOST=h;%s=hunter2;UID=u", secret_keys[i]);
        snprintf(expected, sizeof(expected), "HOST=h;%s=***;UID=u",
                 secret_keys[i]);
        assert_redacted(in, expected);
    }
}

static void test_redact_keeps_endpoints_and_paths(void **state)
{
    (void)state;
    /* Endpoints and key *files* are needed to reconnect and carry no
     * credential — masking them would break the persisted string. */
    static const char *const public_keys[] = {
        "HOST", "UID", "USER", "DATABASE", "TokenURL", "TokenURI",
        "OAuth2TokenEndpoint", "BQTokenEndpoint", "OAuth2AuthEndpoint",
        "SSLKeyFile", "SSLCertFile", "KeyFilePath", "BQKeyFile",
        "OAuth2ClientId", "ClientId", "AuthMech", "SSLCAFile", NULL
    };
    for (int i = 0; public_keys[i]; i++)
        assert_false(argus_connstr_key_is_secret(public_keys[i]));

    assert_redacted(
        "OAuth2TokenEndpoint=https://idp/token;SSLKeyFile=/p/k.pem;"
        "KeyFilePath=/p;ClientSecret=s",
        "OAuth2TokenEndpoint=https://idp/token;SSLKeyFile=/p/k.pem;"
        "KeyFilePath=/p;ClientSecret=***");
}

static void test_redact_edge_cases(void **state)
{
    (void)state;
    assert_null(argus_connstr_redact(NULL));
    assert_redacted("", "");
    assert_redacted(";;;", "");
    /* Fragments without '=' carry no value and are kept as is. */
    assert_redacted("HOST=h;garbage", "HOST=h;garbage");
    assert_redacted("garbage;PWD=x", "garbage;PWD=***");
    /* Empty values stay empty (never turned into "***"). */
    assert_redacted("PWD=;HOST=h", "PWD=***;HOST=h");
    assert_redacted("HOST=;UID=u", "HOST=;UID=u");
    /* Trailing separators and whitespace are normalised away. */
    assert_redacted("HOST=h;PWD=x;;", "HOST=h;PWD=***");
    assert_false(argus_connstr_key_is_secret(NULL));
    assert_false(argus_connstr_key_is_secret(""));
}

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
        cmocka_unit_test(test_redact_masks_password),
        cmocka_unit_test(test_redact_tolerates_spaces_and_braces),
        cmocka_unit_test(test_redact_masks_every_credential_key),
        cmocka_unit_test(test_redact_keeps_endpoints_and_paths),
        cmocka_unit_test(test_redact_edge_cases),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
