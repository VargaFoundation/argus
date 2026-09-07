/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>
#include <stdbool.h>

/*
 * Integration tests: queries + catalog ops against a real MariaDB via the
 * MySQL-wire backend (seeded by mysql-init/01-schema.sql -> table "products").
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d
 */

static const char *mysql_host(void)
{
    const char *h = getenv("MYSQL_HOST");
    return h ? h : "localhost";
}

static int mysql_port(void)
{
    const char *p = getenv("MYSQL_PORT");
    return p ? atoi(p) : 3306;
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
             "HOST=%s;PORT=%d;UID=root;PWD=root123;Backend=mysql;Database=testdb",
             mysql_host(), mysql_port());

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

/* ── SELECT with several types + a NULL ──────────────────────── */

static void test_select_products(void **state)
{
    (void)state;
    SQLHSTMT s;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(s,
        (SQLCHAR *)"SELECT id, name, price, qty, notes FROM products ORDER BY id",
        SQL_NTS), SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 5);

    SQLCHAR buf[64];
    SQLLEN ind;

    /* Row 1: Widget / 19.99 / notes present */
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    SQLGetData(s, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "1");
    SQLGetData(s, 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "Widget");
    SQLGetData(s, 3, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal((char *)buf, "19.99");

    /* Row 2: notes is NULL */
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    SQLGetData(s, 5, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_int_equal(ind, SQL_NULL_DATA);

    assert_int_equal(SQLFetch(s), SQL_SUCCESS);   /* row 3 */
    assert_int_equal(SQLFetch(s), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLColumns returns the table's columns ──────────────────── */

static void test_columns(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    assert_int_equal(SQLColumns(s, NULL, 0, NULL, 0,
                                (SQLCHAR *)"products", SQL_NTS, NULL, 0),
                     SQL_SUCCESS);
    int rows = 0;
    while (SQLFetch(s) == SQL_SUCCESS) rows++;
    assert_int_equal(rows, 7);   /* id, name, price, qty, rating, created, notes */

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLPrimaryKeys returns the PK column ────────────────────── */

static void test_primary_keys(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    assert_int_equal(SQLPrimaryKeys(s, NULL, 0, NULL, 0,
                                    (SQLCHAR *)"products", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);

    SQLCHAR col[64];
    SQLLEN ind;
    SQLGetData(s, 4, SQL_C_CHAR, col, sizeof(col), &ind);  /* COLUMN_NAME */
    assert_string_equal((char *)col, "id");

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLStatistics: the PRIMARY KEY is an index ──────────────── */

/* This used to be an empty result set: the backend had no get_statistics,
 * so an application asking which columns are indexed learned nothing. Now
 * information_schema.statistics answers: a SQL_TABLE_STAT row for the
 * table, then the PRIMARY index on `id`. */
static void test_statistics(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    assert_int_equal(SQLStatistics(s, NULL, 0, NULL, 0,
                                   (SQLCHAR *)"products", SQL_NTS,
                                   SQL_INDEX_ALL, SQL_QUICK),
                     SQL_SUCCESS);

    bool saw_table_stat = false, saw_primary = false;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLSMALLINT type = -1;
        SQLLEN ind = 0;
        SQLGetData(s, 7, SQL_C_SSHORT, &type, sizeof(type), &ind);   /* TYPE */
        if (type == SQL_TABLE_STAT) {
            saw_table_stat = true;
            continue;
        }
        SQLCHAR idx[64] = {0}, col[64] = {0};
        SQLGetData(s, 6, SQL_C_CHAR, idx, sizeof(idx), &ind);   /* INDEX_NAME */
        SQLGetData(s, 9, SQL_C_CHAR, col, sizeof(col), &ind);   /* COLUMN_NAME */
        if (strcmp((char *)idx, "PRIMARY") == 0 && strcmp((char *)col, "id") == 0) {
            SQLSMALLINT non_unique = -1;
            SQLGetData(s, 4, SQL_C_SSHORT, &non_unique, sizeof(non_unique), &ind);
            assert_int_equal(non_unique, 0);
            saw_primary = true;
        }
    }
    assert_true(saw_table_stat);
    assert_true(saw_primary);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    /* Unique-only still lists the PRIMARY KEY. */
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);
    assert_int_equal(SQLStatistics(s, NULL, 0, NULL, 0,
                                   (SQLCHAR *)"products", SQL_NTS,
                                   SQL_INDEX_UNIQUE, SQL_QUICK),
                     SQL_SUCCESS);
    saw_primary = false;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLCHAR idx[64] = {0};
        SQLLEN ind = 0;
        SQLGetData(s, 6, SQL_C_CHAR, idx, sizeof(idx), &ind);
        if (strcmp((char *)idx, "PRIMARY") == 0) saw_primary = true;
    }
    assert_true(saw_primary);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLTables finds the seeded table (TABLE vs BASE TABLE) ──── */

static void test_tables(void **state)
{
    (void)state;
    SQLHSTMT s;
    SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s);

    assert_int_equal(SQLTables(s, (SQLCHAR *)"testdb", SQL_NTS, NULL, 0,
                               (SQLCHAR *)"products", SQL_NTS,
                               (SQLCHAR *)"TABLE", SQL_NTS),
                     SQL_SUCCESS);
    int found = 0;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLCHAR name[128];
        SQLLEN ind;
        SQLGetData(s, 3, SQL_C_CHAR, name, sizeof(name), &ind);
        if (strcmp((char *)name, "products") == 0) found = 1;
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
    /* Should carry the real MySQL message, not a generic one. */
    assert_non_null(strstr((char *)msg, "no_such_table_xyz"));

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLCancel while the server is busy ─────────────────────── */

typedef struct {
    SQLHSTMT  stmt;
    SQLRETURN ret;
    gint64    elapsed_ms;
} sleeping_call_t;

static gpointer run_sleep(gpointer data)
{
    sleeping_call_t *c = data;
    gint64 t0 = g_get_monotonic_time();
    c->ret = SQLExecDirect(c->stmt, (SQLCHAR *)"SELECT SLEEP(30)", SQL_NTS);
    c->elapsed_ms = (g_get_monotonic_time() - t0) / 1000;
    return NULL;
}

/*
 * The wire protocol has no out-of-band cancel, so the backend opens a second
 * session and sends KILL QUERY for the first one's id. The proof is the
 * clock: a statement the server will hold for 30 seconds comes back HY008
 * within a few, from a cancel sent on another thread. Before this, SQLCancel
 * returned SQL_SUCCESS and the call sat out the full 30 seconds.
 */
static void test_cancel_interrupts_a_sleeping_query(void **state)
{
    (void)state;
    SQLHSTMT stmt;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    sleeping_call_t c = { .stmt = stmt };
    GThread *t = g_thread_new("sleep", run_sleep, &c);
    g_usleep(500 * 1000);                      /* let it reach the server */

    assert_int_equal(SQLCancel(stmt), SQL_SUCCESS);
    g_thread_join(t);

    print_message("SELECT SLEEP(30) returned after %ld ms\n", (long)c.elapsed_ms);
    assert_int_equal(c.ret, SQL_ERROR);
    SQLCHAR st[6] = {0}, msg[256];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native,
                                   msg, sizeof(msg), &len), SQL_SUCCESS);
    assert_string_equal((const char *)st, "HY008");
    assert_true(c.elapsed_ms < 10000);

    /* The session is intact: the next statement runs. */
    assert_int_equal(SQLExecDirect(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_products),
        cmocka_unit_test(test_columns),
        cmocka_unit_test(test_primary_keys),
        cmocka_unit_test(test_statistics),
        cmocka_unit_test(test_tables),
        cmocka_unit_test(test_error_message),
        cmocka_unit_test(test_cancel_interrupts_a_sleeping_query),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
