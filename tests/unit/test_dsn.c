/*
 * Unit tests for DSN resolution (src/odbc/dsn.c).
 *
 * Points ODBCINI at a generated ini file and checks every keyword family
 * apply_dsn_param handles lands on the right DBC field. Uses a DSN name
 * unlikely to exist in ~/.odbc.ini (checked first by argus_resolve_dsn).
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include "argus/handle.h"

extern bool argus_resolve_dsn(argus_dbc_t *dbc, const char *dsn_name);

#define TEST_DSN "ArgusUnitTestDsn_e51c1"

static const char *INI_CONTENT =
    "[" TEST_DSN "]\n"
    "Host = h1.example,h2.example\n"
    "Port = 9999\n"
    "UID = alice\n"
    "PWD = s3cr3t\n"
    "Database = sales\n"
    "Backend = trino\n"
    "AuthMech = LDAP\n"
    "SSL = true\n"
    "SSLVerify = false\n"
    "SSLCAFile = /tmp/ca.pem\n"
    "KrbServiceName = svc\n"
    "KrbHostFqdn = kdc.example\n"
    "KrbRealm = EX.COM\n"
    "RetryCount = 2\n"
    "RetryDelay = 1\n"
    "ConnectTimeout = 7\n"
    "QueryTimeout = 33\n"
    "FetchBufferSize = 1234\n"
    "MaxScrollRows = 777\n"
    "LogLevel = 4\n"
    "Project = my-gcp\n"
    "AccessToken = tok123\n";

static char *g_dir;
static char *g_ini;

static int setup(void **state)
{
    (void)state;
    g_dir = g_dir_make_tmp("argusdsn-XXXXXX", NULL);
    assert_non_null(g_dir);
    g_ini = g_build_filename(g_dir, "odbc.ini", NULL);
    assert_true(g_file_set_contents(g_ini, INI_CONTENT, -1, NULL));
    g_setenv("ODBCINI", g_ini, TRUE);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    g_unsetenv("ODBCINI");
    g_remove(g_ini);
    g_rmdir(g_dir);
    g_free(g_ini);
    g_free(g_dir);
    return 0;
}

static void test_resolve_known_dsn(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    argus_dbc_t *d = (argus_dbc_t *)dbc;

    assert_true(argus_resolve_dsn(d, TEST_DSN));

    assert_string_equal(d->host, "h1.example,h2.example");
    assert_int_equal(d->port, 9999);
    assert_string_equal(d->username, "alice");
    assert_string_equal(d->password, "s3cr3t");
    assert_string_equal(d->database, "sales");
    assert_string_equal(d->backend_name, "trino");
    assert_string_equal(d->auth_mechanism, "LDAP");
    assert_true(d->ssl_enabled);
    assert_false(d->ssl_verify);
    assert_string_equal(d->ssl_ca_file, "/tmp/ca.pem");
    assert_string_equal(d->krb_service_name, "svc");
    assert_string_equal(d->krb_host_fqdn, "kdc.example");
    assert_string_equal(d->krb_realm, "EX.COM");
    assert_int_equal(d->retry_count, 2);
    assert_int_equal(d->retry_delay_sec, 1);
    assert_int_equal(d->connect_timeout_sec, 7);
    assert_int_equal(d->query_timeout_sec, 33);
    assert_int_equal(d->fetch_buffer_size, 1234);
    assert_int_equal(d->max_scroll_rows, 777);
    assert_int_equal(d->log_level, 4);
    assert_string_equal(d->bq_project, "my-gcp");
    assert_string_equal(d->bq_access_token, "tok123");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_resolve_unknown_dsn(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    argus_dbc_t *d = (argus_dbc_t *)dbc;

    assert_false(argus_resolve_dsn(d, "ArgusUnitTestDsn_definitely_absent"));
    assert_null(d->host);   /* nothing applied */

    assert_false(argus_resolve_dsn(d, ""));
    assert_false(argus_resolve_dsn(d, NULL));

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_resolve_known_dsn),
        cmocka_unit_test(test_resolve_unknown_dsn),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
