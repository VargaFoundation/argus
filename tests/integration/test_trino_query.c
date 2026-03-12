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
 * Integration tests: Execute queries against a real Trino instance.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 */

static const char *get_trino_host(void)
{
    const char *h = getenv("TRINO_HOST");
    return h ? h : "localhost";
}

static int get_trino_port(void)
{
    const char *p = getenv("TRINO_PORT");
    return p ? atoi(p) : 8080;
}

/* Shared handles */
static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;

static int setup(void **state)
{
    (void)state;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION,
                  (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=trino;Database=memory",
             get_trino_host(), get_trino_port());

    SQLRETURN ret = SQLDriverConnect(g_dbc, NULL,
                                     (SQLCHAR *)conn_str, SQL_NTS,
                                     NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS)
        return -1;

    /* Create schema for tests */
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt) == SQL_SUCCESS) {
        SQLExecDirect(stmt,
            (SQLCHAR *)"CREATE SCHEMA IF NOT EXISTS memory.argus_test",
            SQL_NTS);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }

    return 0;
}

static int teardown(void **state)
{
    (void)state;

    /* Clean up test table */
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt) == SQL_SUCCESS) {
        SQLExecDirect(stmt,
            (SQLCHAR *)"DROP TABLE IF EXISTS memory.argus_test.argus_test_table",
            SQL_NTS);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }

    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

/* ── Test: Simple SELECT ─────────────────────────────────────── */

static void test_select_literal(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1 AS num, 'hello' AS msg",
                        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    ret = SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ncols, 2);

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLCHAR buf[64];
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "1");

    ret = SQLGetData(stmt, 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "hello");

    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: CREATE TABLE, INSERT, SELECT ──────────────────────── */

static void test_create_and_query_table(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Create table in memory catalog */
    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"CREATE TABLE IF NOT EXISTS memory.argus_test.argus_test_table "
                   "(id INTEGER, name VARCHAR)",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLFreeStmt(stmt, SQL_CLOSE);

    /* Insert data */
    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"INSERT INTO memory.argus_test.argus_test_table "
                   "VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLFreeStmt(stmt, SQL_CLOSE);

    /* Select and verify */
    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT id, name FROM memory.argus_test.argus_test_table "
                   "ORDER BY id",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ncols, 2);

    int row_count = 0;
    SQLCHAR id_buf[16], name_buf[64];
    SQLLEN id_ind, name_ind;

    while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, id_buf, sizeof(id_buf), &id_ind);
        SQLGetData(stmt, 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &name_ind);

        row_count++;
        if (row_count == 1) {
            assert_string_equal((char *)id_buf, "1");
            assert_string_equal((char *)name_buf, "alpha");
        }
    }
    assert_int_equal(row_count, 3);

    SQLFreeStmt(stmt, SQL_CLOSE);

    /* Drop table */
    SQLExecDirect(stmt,
        (SQLCHAR *)"DROP TABLE IF EXISTS memory.argus_test.argus_test_table",
        SQL_NTS);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLTables ─────────────────────────────────────────── */

static void test_get_tables(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLTables(stmt,
                              (SQLCHAR *)"memory", SQL_NTS,
                              (SQLCHAR *)"%", SQL_NTS,
                              (SQLCHAR *)"%", SQL_NTS,
                              (SQLCHAR *)"TABLE", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_true(ncols >= 5);

    ret = SQLFetch(stmt);
    assert_true(ret == SQL_SUCCESS || ret == SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLColumns ────────────────────────────────────────── */

static void test_get_columns(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    /* Create a table to inspect */
    SQLExecDirect(stmt,
        (SQLCHAR *)"CREATE TABLE IF NOT EXISTS memory.argus_test.argus_col_test "
                   "(id INTEGER, name VARCHAR)",
        SQL_NTS);
    SQLFreeStmt(stmt, SQL_CLOSE);

    SQLRETURN ret = SQLColumns(stmt,
                               (SQLCHAR *)"memory", SQL_NTS,
                               (SQLCHAR *)"argus_test", SQL_NTS,
                               (SQLCHAR *)"argus_col_test", SQL_NTS,
                               NULL, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_true(ncols >= 4);

    ret = SQLFetch(stmt);
    assert_true(ret == SQL_SUCCESS || ret == SQL_NO_DATA);

    SQLFreeStmt(stmt, SQL_CLOSE);

    /* Clean up */
    SQLExecDirect(stmt,
        (SQLCHAR *)"DROP TABLE IF EXISTS memory.argus_test.argus_col_test",
        SQL_NTS);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_literal),
        cmocka_unit_test(test_create_and_query_table),
        cmocka_unit_test(test_get_tables),
        cmocka_unit_test(test_get_columns),
    };
    return cmocka_run_group_tests_name("trino_query", tests, setup, teardown);
}
