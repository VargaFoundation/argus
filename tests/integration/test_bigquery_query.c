#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

/*
 * Integration tests: queries + catalog ops against the BigQuery emulator
 * (dataset "testds", table "samples" seeded from bigquery-seed.yaml).
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

static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;

static int setup(void **state)
{
    (void)state;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "Backend=bigquery;Project=%s;Database=testds;BQEndpoint=%s",
             bq_project(), bq_endpoint());

    return SQLDriverConnect(g_dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                            NULL, 0, NULL, SQL_DRIVER_NOPROMPT) == SQL_SUCCESS
               ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

/* ── SELECT with typed columns, ORDER BY and WHERE ───────────── */

static void test_select_rows(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt),
                     SQL_SUCCESS);

    SQLRETURN ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT id, name, score, active FROM testds.samples "
                   "ORDER BY id", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ncols, 4);

    /* row 1: 1, alice, 9.5, true */
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLBIGINT id = 0;
    char name[64] = {0};
    double score = 0;
    unsigned char active = 0;
    SQLGetData(stmt, 1, SQL_C_SBIGINT, &id, sizeof(id), NULL);
    SQLGetData(stmt, 2, SQL_C_CHAR, name, sizeof(name), NULL);
    SQLGetData(stmt, 3, SQL_C_DOUBLE, &score, sizeof(score), NULL);
    SQLGetData(stmt, 4, SQL_C_BIT, &active, sizeof(active), NULL);
    assert_true(id == 1);
    assert_string_equal(name, "alice");
    assert_true(score > 9.4 && score < 9.6);
    assert_int_equal(active, 1);

    /* rows 2 and 3 exist, then no more data */
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Aggregate over the seed data ────────────────────────────── */

static void test_aggregate(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT COUNT(*) FROM testds.samples WHERE active",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLBIGINT n = 0;
    SQLGetData(stmt, 1, SQL_C_SBIGINT, &n, sizeof(n), NULL);
    assert_true(n == 2);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Server-side errors surface as diagnostics ───────────────── */

static void test_query_error(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT * FROM testds.does_not_exist", SQL_NTS);
    assert_int_equal(ret, SQL_ERROR);

    SQLCHAR sqlstate[6] = {0}, msg[512] = {0};
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, &native,
                  msg, sizeof(msg), &len);
    assert_true(len > 0);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Catalog: SQLTables / SQLColumns over the REST metadata ──── */

static void test_sqltables(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLTables(stmt, NULL, 0, (SQLCHAR *)"testds", SQL_NTS,
                              (SQLCHAR *)"%", SQL_NTS, NULL, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    bool found = false;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        char table[128] = {0};
        SQLGetData(stmt, 3, SQL_C_CHAR, table, sizeof(table), NULL);
        if (strcmp(table, "samples") == 0) found = true;
    }
    assert_true(found);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

static void test_sqlcolumns(void **state)
{
    (void)state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLColumns(stmt, NULL, 0, (SQLCHAR *)"testds", SQL_NTS,
                               (SQLCHAR *)"samples", SQL_NTS, NULL, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    int ncols_seen = 0;
    bool saw_id = false;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        char col[128] = {0};
        SQLGetData(stmt, 4, SQL_C_CHAR, col, sizeof(col), NULL);
        if (strcmp(col, "id") == 0) saw_id = true;
        ncols_seen++;
    }
    assert_int_equal(ncols_seen, 4);
    assert_true(saw_id);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_rows),
        cmocka_unit_test(test_aggregate),
        cmocka_unit_test(test_query_error),
        cmocka_unit_test(test_sqltables),
        cmocka_unit_test(test_sqlcolumns),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
