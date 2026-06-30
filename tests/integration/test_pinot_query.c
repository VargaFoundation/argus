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
 * Integration tests: queries + catalog ops against a real Apache Pinot
 * QuickStart cluster (sample table "baseballStats") via the Pinot backend.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d pinot
 *
 * Override with PINOT_HOST / PINOT_PORT (broker, default localhost:8000).
 */

static const char *pinot_host(void)
{
    const char *h = getenv("PINOT_HOST");
    return h ? h : "localhost";
}

static int pinot_port(void)
{
    const char *p = getenv("PINOT_PORT");
    return p ? atoi(p) : 8000;
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
    snprintf(conn_str, sizeof(conn_str), "HOST=%s;PORT=%d;Backend=pinot",
             pinot_host(), pinot_port());

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

/* ── SELECT: string + numeric columns, with a filter ─────────── */

static void test_select_baseball(void **state)
{
    (void)state;
    SQLHSTMT s;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(s, (SQLCHAR *)
        "SELECT playerName, homeRuns, league FROM baseballStats "
        "WHERE homeRuns > 40 LIMIT 5", SQL_NTS), SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 3);

    char name[128], hr[32], league[32];
    SQLLEN ind;
    int rows = 0;
    while (SQLFetch(s) == SQL_SUCCESS) {
        assert_int_equal(SQLGetData(s, 1, SQL_C_CHAR, name, sizeof(name), &ind),
                         SQL_SUCCESS);
        assert_true(ind != SQL_NULL_DATA && name[0] != '\0');
        /* homeRuns is an INT column -> readable both as number and as text. */
        assert_int_equal(SQLGetData(s, 2, SQL_C_CHAR, hr, sizeof(hr), &ind),
                         SQL_SUCCESS);
        assert_true(atoi(hr) > 40);
        SQLGetData(s, 3, SQL_C_CHAR, league, sizeof(league), &ind);
        rows++;
    }
    assert_true(rows > 0);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── homeRuns reads natively as a bigint (typed-cell path) ────── */

static void test_select_numeric_native(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);
    assert_int_equal(SQLExecDirect(s, (SQLCHAR *)
        "SELECT homeRuns FROM baseballStats WHERE homeRuns > 40 LIMIT 3", SQL_NTS),
        SQL_SUCCESS);

    int rows = 0;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLBIGINT v = 0; SQLLEN ind;
        assert_int_equal(SQLGetData(s, 1, SQL_C_SBIGINT, &v, 0, &ind), SQL_SUCCESS);
        assert_true(v > 40);
        rows++;
    }
    assert_true(rows > 0);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLColumns returns the table's columns ──────────────────── */

static void test_columns(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);
    assert_int_equal(SQLColumns(s, NULL, 0, NULL, 0,
                                (SQLCHAR *)"baseballStats", SQL_NTS, NULL, 0),
                     SQL_SUCCESS);

    int rows = 0, found_homeruns = 0;
    char col[128];
    SQLLEN ind;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLGetData(s, 4, SQL_C_CHAR, col, sizeof(col), &ind);   /* COLUMN_NAME */
        if (strcmp(col, "homeRuns") == 0) found_homeruns = 1;
        rows++;
    }
    assert_true(rows > 0);
    assert_int_equal(found_homeruns, 1);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLTables finds the sample table ────────────────────────── */

static void test_tables(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);
    assert_int_equal(SQLTables(s, NULL, 0, NULL, 0, NULL, 0,
                               (SQLCHAR *)"TABLE", SQL_NTS), SQL_SUCCESS);

    int found = 0;
    char name[128];
    SQLLEN ind;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLGetData(s, 3, SQL_C_CHAR, name, sizeof(name), &ind);  /* TABLE_NAME */
        if (strcmp(name, "baseballStats") == 0) found = 1;
    }
    assert_int_equal(found, 1);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── A bad query surfaces the real Pinot error ───────────────── */

static void test_error_message(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);
    SQLRETURN rc = SQLExecDirect(s, (SQLCHAR *)
        "SELECT * FROM no_such_table_xyz", SQL_NTS);
    assert_int_not_equal(rc, SQL_SUCCESS);

    SQLCHAR sqlstate[6], msg[512];
    SQLINTEGER native;
    SQLSMALLINT len;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, sqlstate, &native,
                                   msg, sizeof(msg), &len), SQL_SUCCESS);
    /* Pinot reports a TableDoesNotExistError for an unknown table. */
    assert_non_null(strstr((char *)msg, "TableDoesNotExist"));
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_baseball),
        cmocka_unit_test(test_select_numeric_native),
        cmocka_unit_test(test_columns),
        cmocka_unit_test(test_tables),
        cmocka_unit_test(test_error_message),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
