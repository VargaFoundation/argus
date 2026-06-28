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
 * Integration tests: queries + catalog ops against a real InfluxDB 3 Core via
 * the Arrow Flight SQL backend (seeded with the "cpu" table in db "testdb").
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d --wait
 */

static const char *fsql_host(void)
{
    const char *h = getenv("FLIGHTSQL_HOST");
    return h ? h : "localhost";
}

static int fsql_port(void)
{
    const char *p = getenv("FLIGHTSQL_PORT");
    return p ? atoi(p) : 8181;
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
             "HOST=%s;PORT=%d;Backend=flightsql;Database=testdb",
             fsql_host(), fsql_port());

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

/* ── SELECT the seeded rows (string + double + int) ──────────── */

static void test_select_cpu(void **state)
{
    (void)state;
    SQLHSTMT s;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(s,
        (SQLCHAR *)"SELECT host, usage, cores FROM cpu ORDER BY host", SQL_NTS),
        SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 3);

    SQLCHAR buf[64];
    SQLLEN ind;

    /* First row (ordered by host): alpha / 0.5 / 4 */
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    SQLGetData(s, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "alpha");
    SQLGetData(s, 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "0.5");

    int rows = 1;
    while (SQLFetch(s) == SQL_SUCCESS) rows++;
    assert_int_equal(rows, 3);

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLColumns returns the cpu table's columns ──────────────── */

static void test_columns(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    assert_int_equal(SQLColumns(s, NULL, 0, NULL, 0,
                                (SQLCHAR *)"cpu", SQL_NTS, NULL, 0),
                     SQL_SUCCESS);
    int found_usage = 0, rows = 0;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLCHAR name[128];
        SQLLEN ind;
        SQLGetData(s, 4, SQL_C_CHAR, name, sizeof(name), &ind);  /* COLUMN_NAME */
        if (strcmp((char *)name, "usage") == 0) found_usage = 1;
        rows++;
    }
    assert_true(rows >= 4);
    assert_int_equal(found_usage, 1);

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLTables finds the cpu table (TABLE vs BASE TABLE) ──────── */

static void test_tables(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    assert_int_equal(SQLTables(s, NULL, 0, NULL, 0, (SQLCHAR *)"cpu", SQL_NTS,
                               (SQLCHAR *)"TABLE", SQL_NTS),
                     SQL_SUCCESS);
    int found = 0;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLCHAR name[128];
        SQLLEN ind;
        SQLGetData(s, 3, SQL_C_CHAR, name, sizeof(name), &ind);
        if (strcmp((char *)name, "cpu") == 0) found = 1;
    }
    assert_int_equal(found, 1);

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── A bad query surfaces the real server error message ──────── */

static void test_error_message(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    SQLRETURN r = SQLExecDirect(s,
        (SQLCHAR *)"SELECT * FROM no_such_table_xyz", SQL_NTS);
    assert_int_equal(r, SQL_ERROR);

    SQLCHAR sqlstate[6], msg[512];
    SQLINTEGER native;
    SQLSMALLINT len;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, sqlstate,
                                   &native, msg, sizeof(msg), &len),
                     SQL_SUCCESS);
    assert_non_null(strstr((char *)msg, "no_such_table_xyz"));

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_cpu),
        cmocka_unit_test(test_columns),
        cmocka_unit_test(test_tables),
        cmocka_unit_test(test_error_message),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
