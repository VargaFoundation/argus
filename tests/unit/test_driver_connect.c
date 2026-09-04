/* SPDX-License-Identifier: Apache-2.0 */
/*
 * SQLDriverConnect end to end against a registered fake backend: what the
 * driver hands back in OutConnectionString and to the observability taps
 * is the redacted copy of the connection string — every credential is
 * "***", whatever the key is called and however the pair is spelled.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include "argus/handle.h"
#include "argus/backend.h"

static int fake_connect(argus_dbc_t *dbc, const char *host, int port,
                        const char *username, const char *password,
                        const char *database, const char *auth_mechanism,
                        argus_backend_conn_t *out_conn)
{
    (void)dbc; (void)host; (void)port; (void)username; (void)password;
    (void)database; (void)auth_mechanism;
    *out_conn = (argus_backend_conn_t)(uintptr_t)0xFA4E;
    return 0;
}

static void fake_disconnect(argus_backend_conn_t conn) { (void)conn; }
static bool fake_is_alive(argus_backend_conn_t conn) { (void)conn; return true; }

static const argus_backend_t fake_backend = {
    .name       = "fake",
    .connect    = fake_connect,
    .disconnect = fake_disconnect,
    .is_alive   = fake_is_alive,
};

static int group_setup(void **state)
{
    (void)state;
    argus_backend_register(&fake_backend);
    return 0;
}

static SQLHDBC alloc_dbc(SQLHENV *env_out)
{
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    *env_out = env;
    return dbc;
}

static void free_dbc(SQLHENV env, SQLHDBC dbc)
{
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_out_connection_string_is_redacted(void **state)
{
    (void)state;
    SQLHENV env;
    SQLHDBC dbc = alloc_dbc(&env);

    const char *in =
        "BACKEND=fake;HOST=h; PWD = s3cret ;ClientSecret={cs;1};"
        "AccessToken=at-9f8e;OAuth2TokenEndpoint=https://idp/token;"
        "SSLKeyFile=/p/k.pem;UID=u";
    SQLCHAR out[512];
    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)in, SQL_NTS,
                                     out, sizeof(out), &out_len,
                                     SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    const char *expected =
        "BACKEND=fake;HOST=h;PWD=***;ClientSecret=***;AccessToken=***;"
        "OAuth2TokenEndpoint=https://idp/token;SSLKeyFile=/p/k.pem;UID=u";
    assert_string_equal((const char *)out, expected);
    assert_int_equal(out_len, (SQLSMALLINT)strlen(expected));
    assert_null(strstr((const char *)out, "s3cret"));
    assert_null(strstr((const char *)out, "cs;1"));
    assert_null(strstr((const char *)out, "9f8e"));

    /* The taps get the same copy. */
    argus_dbc_t *h = (argus_dbc_t *)dbc;
    assert_non_null(h->obs_connstr);
    assert_string_equal(h->obs_connstr, expected);

    /* The driver itself still saw the real password. */
    assert_string_equal(h->password, "s3cret");

    free_dbc(env, dbc);
}

static void test_length_only_reports_redacted_length(void **state)
{
    (void)state;
    SQLHENV env;
    SQLHDBC dbc = alloc_dbc(&env);

    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL,
                                     (SQLCHAR *)"BACKEND=fake;HOST=h;PWD=verylongpassword",
                                     SQL_NTS, NULL, 0, &out_len,
                                     SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(out_len, (SQLSMALLINT)strlen("BACKEND=fake;HOST=h;PWD=***"));

    free_dbc(env, dbc);
}

static void test_truncated_buffer_never_leaks_more(void **state)
{
    (void)state;
    SQLHENV env;
    SQLHDBC dbc = alloc_dbc(&env);

    SQLCHAR out[16];
    memset(out, 'X', sizeof(out));
    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL,
                                     (SQLCHAR *)"BACKEND=fake;PWD=secret;HOST=h",
                                     SQL_NTS, out, sizeof(out), &out_len,
                                     SQL_DRIVER_NOPROMPT);
    assert_true(SQL_SUCCEEDED(ret));
    assert_int_equal(out[sizeof(out) - 1], 0);
    assert_string_equal((const char *)out, "BACKEND=fake;PW");
    assert_int_equal(out_len, (SQLSMALLINT)strlen("BACKEND=fake;PWD=***;HOST=h"));

    free_dbc(env, dbc);
}

static void test_failed_connect_still_redacts(void **state)
{
    (void)state;
    SQLHENV env;
    SQLHDBC dbc = alloc_dbc(&env);

    SQLCHAR out[256];
    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL,
                                     (SQLCHAR *)"BACKEND=nonexistent;HOST=h;Password=p",
                                     SQL_NTS, out, sizeof(out), &out_len,
                                     SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_ERROR);
    assert_string_equal((const char *)out, "BACKEND=nonexistent;HOST=h;Password=***");

    free_dbc(env, dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_out_connection_string_is_redacted),
        cmocka_unit_test(test_length_only_reports_redacted_length),
        cmocka_unit_test(test_truncated_buffer_never_leaks_more),
        cmocka_unit_test(test_failed_connect_still_redacts),
    };
    return cmocka_run_group_tests(tests, group_setup, NULL);
}
