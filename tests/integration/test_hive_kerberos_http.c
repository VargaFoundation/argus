/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Integration test: connect to a Kerberized HiveServer2 over the HTTP
 * transport, exercising SPNEGO end to end.
 *
 * This is the other half of test_hive_kerberos.c. That one drives the binary
 * Thrift SASL/GSSAPI handshake the driver implements itself; this one drives
 * Thrift-over-HTTP, where the Negotiate exchange is delegated to libcurl
 * (CURLAUTH_NEGOTIATE against the ambient credential cache). Setting HttpPath
 * is what selects the HTTP transport; AuthMech=KERBEROS is what turns on
 * use-spnego.
 *
 * Requires:
 *   - the KDC + HTTP-mode HiveServer2 (tests/integration/kerberos/, --profile http)
 *   - a valid TGT in the ticket cache (kinit) before running
 *
 * Note on the SPN: unlike the binary path there is no KrbHostFQDN override
 * here, because libcurl derives the service principal from the URL host. The
 * host you dial IS the SPN host, so HIVE_HOST must be a name the KDC has an
 * HTTP/ principal for -- the stack registers both hive.example.com and
 * 127.0.0.1 for exactly this reason.
 *
 * Override with HIVE_HOST / HIVE_PORT / HIVE_HTTP_PATH.
 */

static const char *hive_host(void)
{
    const char *h = getenv("HIVE_HOST");
    return h ? h : "hive.example.com";
}

static int hive_port(void)
{
    const char *p = getenv("HIVE_PORT");
    return p ? atoi(p) : 10001;
}

static const char *hive_http_path(void)
{
    const char *p = getenv("HIVE_HTTP_PATH");
    return p ? p : "cliservice";
}

/* ── Test: SPNEGO over HTTP connect + SELECT 1 ────────────────── */

static void test_spnego_http_connect_query(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    char conn_str[600];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;Backend=hive;AuthMech=KERBEROS;"
             "HttpPath=%s;Database=default",
             hive_host(), hive_port(), hive_http_path());

    SQLCHAR out[1024];
    SQLSMALLINT out_len;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                     out, sizeof(out), &out_len,
                                     SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6], msg[512];
        SQLINTEGER native;
        SQLSMALLINT len;
        SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, st, &native, msg, sizeof(msg),
                      &len);
        fail_msg("SPNEGO/HTTP connect failed: [%s] %s", st, msg);
    }

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLINTEGER val = 0;
    SQLGetData(stmt, 1, SQL_C_SLONG, &val, sizeof(val), NULL);
    assert_int_equal(val, 1);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_spnego_http_connect_query),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
