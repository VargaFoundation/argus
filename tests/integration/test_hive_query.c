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
 * Integration tests: Execute queries against a real HiveServer2 instance.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 */

static const char *get_hive_host(void) {
    const char *h = getenv("HIVE_HOST");
    return h ? h : "localhost";
}

static int get_hive_port(void) {
    const char *p = getenv("HIVE_PORT");
    return p ? atoi(p) : 10000;
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
             "HOST=%s;PORT=%d;AuthMech=NOSASL;Database=default",
             get_hive_host(), get_hive_port());

    SQLRETURN ret = SQLDriverConnect(g_dbc, NULL,
                                      (SQLCHAR *)conn_str, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    return (ret == SQL_SUCCESS) ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;

    /* Clean up test table */
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt) == SQL_SUCCESS) {
        SQLExecDirect(stmt,
            (SQLCHAR *)"DROP TABLE IF EXISTS argus_test_table", SQL_NTS);
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

    /* Get first column as string */
    SQLCHAR buf[64];
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "1");

    /* Get second column */
    ret = SQLGetData(stmt, 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "hello");

    /* No more rows */
    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: CREATE TABLE, INSERT, SELECT ──────────────────────── */

static void test_create_insert_select(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Create table */
    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"CREATE TABLE IF NOT EXISTS argus_test_table "
                   "(id INT, name STRING, value DOUBLE)",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLFreeStmt(stmt, SQL_CLOSE);

    /* Insert data */
    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"INSERT INTO argus_test_table VALUES "
                   "(1, 'alpha', 3.14), (2, 'beta', 2.71), (3, 'gamma', 1.62)",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLFreeStmt(stmt, SQL_CLOSE);

    /* Select and verify */
    ret = SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT id, name, value FROM argus_test_table ORDER BY id",
        SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ncols, 3);

    /* Describe columns */
    SQLCHAR col_name[128];
    SQLSMALLINT name_len, data_type, nullable;
    SQLULEN col_size;
    SQLSMALLINT decimal_digits;

    ret = SQLDescribeCol(stmt, 1, col_name, sizeof(col_name), &name_len,
                          &data_type, &col_size, &decimal_digits, &nullable);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)col_name, "id");
    assert_int_equal(data_type, SQL_INTEGER);

    /* Fetch rows */
    int row_count = 0;
    SQLCHAR id_buf[16], name_buf[64], val_buf[32];
    SQLLEN id_ind, name_ind, val_ind;

    while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, id_buf, sizeof(id_buf), &id_ind);
        SQLGetData(stmt, 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &name_ind);
        SQLGetData(stmt, 3, SQL_C_CHAR, val_buf, sizeof(val_buf), &val_ind);

        row_count++;
        if (row_count == 1) {
            assert_string_equal((char *)id_buf, "1");
            assert_string_equal((char *)name_buf, "alpha");
        }
    }
    assert_int_equal(row_count, 3);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLTables ─────────────────────────────────────────── */

static void test_tables(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLTables(stmt, NULL, 0,
                               (SQLCHAR *)"%", SQL_NTS,
                               (SQLCHAR *)"%", SQL_NTS,
                               (SQLCHAR *)"TABLE", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_true(ncols >= 5); /* TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS */

    /* Just verify we can fetch at least one row */
    ret = SQLFetch(stmt);
    /* May be SQL_SUCCESS or SQL_NO_DATA depending on state */
    assert_true(ret == SQL_SUCCESS || ret == SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Test: SQLPrepare + SQLExecute ───────────────────────────── */

static void test_prepare_execute(void **state)
{
    (void)state;

    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt);

    SQLRETURN ret = SQLPrepare(stmt, (SQLCHAR *)"SELECT 42 AS answer",
                                SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLExecute(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    SQLCHAR buf[32];
    SQLLEN ind;
    ret = SQLFetch(stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "42");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_literal),
        cmocka_unit_test(test_create_insert_select),
        cmocka_unit_test(test_tables),
        cmocka_unit_test(test_prepare_execute),
    };
    return cmocka_run_group_tests_name("hive_query", tests, setup, teardown);
}
