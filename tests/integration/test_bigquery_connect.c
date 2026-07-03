#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/*
 * Integration tests: connection to a BigQuery API endpoint via the
 * BigQuery backend. Runs against the goccy/bigquery-emulator with the
 * BQEndpoint override — the same mechanism sovereign-cloud deployments
 * (S3NS) use to redirect the Google endpoints.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d bigquery
 *
 * Override with BQ_ENDPOINT / BQ_PROJECT (default http://localhost:9050, test).
 */

static const char *bq_endpoint(void)
{
    const char *e = getenv("BQ_ENDPOINT");
    return e ? e : "http://localhost:9050";
}

static const char *bq_project(void)
{
    const char *p = getenv("BQ_PROJECT");
    return p ? p : "test";
}

/* ── Test: connect + disconnect via the endpoint override ────── */

static void test_connect_disconnect(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "Backend=bigquery;Project=%s;BQEndpoint=%s",
             bq_project(), bq_endpoint());

    SQLCHAR out_conn[1024];
    SQLSMALLINT out_len;
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                     out_conn, sizeof(out_conn), &out_len,
                                     SQL_DRIVER_NOPROMPT);
    assert_int_equal(ret, SQL_SUCCESS);

    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: a missing project must fail with a diagnostic ─────── */

static void test_missing_project_fails(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str), "Backend=bigquery;BQEndpoint=%s",
             bq_endpoint());

    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_true(ret == SQL_ERROR);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: a wrong endpoint must fail, not hang ──────────────── */

static void test_bad_endpoint_fails(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "Backend=bigquery;Project=%s;"
             "BQEndpoint=http://localhost:1;ConnectTimeout=3",
             bq_project());

    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_true(ret == SQL_ERROR);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_connect_disconnect),
        cmocka_unit_test(test_missing_project_fails),
        cmocka_unit_test(test_bad_endpoint_fails),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
