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
 * Integration test: connect to a Kerberized HiveServer2 with AuthMech=KERBEROS
 * and run a query, exercising the GSSAPI SASL handshake end to end.
 *
 * Requires:
 *   - a KDC + Kerberized HiveServer2 (tests/integration/kerberos/)
 *   - a valid TGT in the ticket cache (kinit) before running
 *   - HIVE_HOST set to the FQDN matching the service keytab principal
 *     (hive/<HIVE_HOST>@REALM), e.g. hive.example.com — connecting via
 *     localhost/127.0.0.1 will not match the SPN.
 *
 * Override with HIVE_HOST / HIVE_PORT (default hive.example.com:10000).
 */

static const char *hive_host(void)
{
    const char *h = getenv("HIVE_HOST");
    return h ? h : "hive.example.com";
}

static int hive_port(void)
{
    const char *p = getenv("HIVE_PORT");
    return p ? atoi(p) : 10000;
}

/* ── Test: Kerberos connect + SELECT 1 ───────────────────────── */

static void test_kerberos_connect_query(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    /* KRB_HOST_FQDN overrides the SPN host (KrbHostFQDN), so the TCP host can
     * be 127.0.0.1 while the service principal stays hive/<fqdn>@REALM. */
    const char *spn_fqdn = getenv("KRB_HOST_FQDN");
    char conn_str[600];
    if (spn_fqdn && *spn_fqdn)
        snprintf(conn_str, sizeof(conn_str),
                 "HOST=%s;PORT=%d;Backend=hive;AuthMech=KERBEROS;"
                 "KrbHostFQDN=%s;Database=default",
                 hive_host(), hive_port(), spn_fqdn);
    else
        snprintf(conn_str, sizeof(conn_str),
                 "HOST=%s;PORT=%d;Backend=hive;AuthMech=KERBEROS;Database=default",
                 hive_host(), hive_port());

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
        fail_msg("Kerberos connect failed: [%s] %s", st, msg);
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
        cmocka_unit_test(test_kerberos_connect_query),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
