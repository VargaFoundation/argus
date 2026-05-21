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
 * Integration tests: Connect to a real Flink SQL Gateway exposing its
 * HiveServer2 endpoint (FLIP-223). The endpoint speaks the HiveServer2 Thrift
 * protocol, so it is reached through the Hive backend (BACKEND=hive).
 *
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d flink-sql-gateway
 * Set FLINK_HOST and FLINK_PORT to override defaults (localhost:10011).
 */

static const char *get_flink_host(void) {
    const char *h = getenv("FLINK_HOST");
    return h ? h : "localhost";
}

static int get_flink_port(void) {
    const char *p = getenv("FLINK_PORT");
    return p ? atoi(p) : 10011;
}

/* ── Test: Connect and disconnect via SQLDriverConnect ───────── */

static void test_driver_connect(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                        (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "BACKEND=hive;HOST=%s;PORT=%d;UID=flink;AuthMech=NOSASL;Database=default",
             get_flink_host(), get_flink_port());

    SQLCHAR out_conn[1024];
    SQLSMALLINT out_len;

    ret = SQLDriverConnect(dbc, NULL,
                            (SQLCHAR *)conn_str, SQL_NTS,
                            out_conn, sizeof(out_conn), &out_len,
                            SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLDisconnect(dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    assert_int_equal(ret, SQL_SUCCESS);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_connect),
    };
    return cmocka_run_group_tests_name("flink_connect", tests, NULL, NULL);
}
