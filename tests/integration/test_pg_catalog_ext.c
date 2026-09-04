/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

/*
 * The catalog functions that used to return an empty result set for every
 * backend: SQLForeignKeys, SQLProcedures, SQLProcedureColumns,
 * SQLTablePrivileges, SQLColumnPrivileges and SQLSpecialColumns.
 *
 * Empty was the correct answer for Trino, Hive, Impala, Pinot and the rest —
 * those engines have no foreign keys, no stored procedures and no per-column
 * privileges, and inventing metadata a BI tool would then trust is worse than
 * returning nothing. It stopped being correct when PostgreSQL arrived, which
 * has all of them.
 *
 * Also covers the bug that came out while wiring these up: on a *connected*
 * statement the empty-result paths never set fetch_started, so SQLFetch went
 * to the backend with a NULL operation handle and failed instead of returning
 * SQL_NO_DATA. Any BI tool that called SQLForeignKeys and then fetched hit it.
 */

static const char *env_or(const char *key, const char *dflt)
{
    const char *v = getenv(key);
    return (v && *v) ? v : dflt;
}

static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;

static int setup(void **state)
{
    (void)state;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_env);
    SQLSetEnvAttr(g_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, g_env, &g_dbc);

    char cs[512];
    snprintf(cs, sizeof(cs),
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=postgres;Database=%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
             env_or("PG_DB", "argusdb"));

    SQLRETURN rc = SQLDriverConnect(g_dbc, NULL, (SQLCHAR *)cs, SQL_NTS,
                                    NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    return (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

static SQLHSTMT new_stmt(void)
{
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    return s;
}

static void col_str(SQLHSTMT s, SQLUSMALLINT col, char *out, size_t n)
{
    SQLLEN ind = 0;
    out[0] = '\0';
    SQLGetData(s, col, SQL_C_CHAR, out, (SQLLEN)n, &ind);
    if (ind == SQL_NULL_DATA) out[0] = '\0';
}

static SQLSMALLINT col_i16(SQLHSTMT s, SQLUSMALLINT col)
{
    SQLSMALLINT v = -32768;
    SQLLEN ind = 0;
    SQLGetData(s, col, SQL_C_SSHORT, &v, sizeof(v), &ind);
    return (ind == SQL_NULL_DATA) ? -32768 : v;
}

/* ── SQLForeignKeys ──────────────────────────────────────────── */

static void test_foreign_keys(void **state)
{
    (void)state;
    SQLHSTMT s = new_stmt();

    /* orders.customer_id references customers.id ON DELETE CASCADE. */
    assert_int_equal(SQLForeignKeys(s,
                                    NULL, 0, NULL, 0, NULL, 0,             /* PK side: any */
                                    NULL, 0,
                                    (SQLCHAR *)"argus_test", SQL_NTS,
                                    (SQLCHAR *)"orders", SQL_NTS),
                     SQL_SUCCESS);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 14);

    assert_int_equal(SQLFetch(s), SQL_SUCCESS);

    char buf[128];
    col_str(s, 2, buf, sizeof(buf)); assert_string_equal(buf, "argus_test");
    col_str(s, 3, buf, sizeof(buf)); assert_string_equal(buf, "customers");
    col_str(s, 4, buf, sizeof(buf)); assert_string_equal(buf, "id");
    col_str(s, 6, buf, sizeof(buf)); assert_string_equal(buf, "argus_test");
    col_str(s, 7, buf, sizeof(buf)); assert_string_equal(buf, "orders");
    col_str(s, 8, buf, sizeof(buf)); assert_string_equal(buf, "customer_id");
    assert_int_equal(col_i16(s, 9), 1);                 /* KEY_SEQ */
    assert_int_equal(col_i16(s, 11), SQL_CASCADE);      /* DELETE_RULE */
    assert_int_equal(col_i16(s, 10), SQL_NO_ACTION);    /* UPDATE_RULE */
    assert_int_equal(col_i16(s, 14), SQL_NOT_DEFERRABLE);

    assert_int_equal(SQLFetch(s), SQL_NO_DATA);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    /* The other direction: asking by primary-key table must find the same
     * relationship from the other end. */
    s = new_stmt();
    assert_int_equal(SQLForeignKeys(s,
                                    NULL, 0,
                                    (SQLCHAR *)"argus_test", SQL_NTS,
                                    (SQLCHAR *)"customers", SQL_NTS,
                                    NULL, 0, NULL, 0, NULL, 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    col_str(s, 7, buf, sizeof(buf)); assert_string_equal(buf, "orders");
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* A table with no foreign keys: still SQL_SUCCESS, 14 columns, no rows —
 * and SQLFetch must say NO_DATA rather than failing. */
static void test_foreign_keys_none(void **state)
{
    (void)state;
    SQLHSTMT s = new_stmt();
    assert_int_equal(SQLForeignKeys(s, NULL, 0, NULL, 0, NULL, 0,
                                    NULL, 0,
                                    (SQLCHAR *)"argus_test", SQL_NTS,
                                    (SQLCHAR *)"all_types", SQL_NTS),
                     SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 14);
    assert_int_equal(SQLFetch(s), SQL_NO_DATA);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLProcedures / SQLProcedureColumns ─────────────────────── */

static void test_procedures(void **state)
{
    (void)state;
    SQLHSTMT s = new_stmt();

    assert_int_equal(SQLProcedures(s, NULL, 0,
                                   (SQLCHAR *)"argus_test", SQL_NTS,
                                   (SQLCHAR *)"order_count", SQL_NTS),
                     SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 8);

    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    char buf[128];
    col_str(s, 2, buf, sizeof(buf)); assert_string_equal(buf, "argus_test");
    col_str(s, 3, buf, sizeof(buf)); assert_string_equal(buf, "order_count");
    /* It returns a value, so it is a function, not a procedure. */
    assert_int_equal(col_i16(s, 8), SQL_PT_FUNCTION);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    s = new_stmt();
    assert_int_equal(SQLProcedureColumns(s, NULL, 0,
                                         (SQLCHAR *)"argus_test", SQL_NTS,
                                         (SQLCHAR *)"order_count", SQL_NTS,
                                         NULL, 0),
                     SQL_SUCCESS);
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 19);

    /* The return value and the one input argument, in that order. */
    bool saw_return = false, saw_arg = false;
    while (SQLFetch(s) == SQL_SUCCESS) {
        SQLSMALLINT kind = col_i16(s, 5);       /* COLUMN_TYPE */
        SQLSMALLINT dtype = col_i16(s, 6);      /* DATA_TYPE */
        if (kind == SQL_RETURN_VALUE) {
            saw_return = true;
            assert_int_equal(dtype, SQL_BIGINT);
        } else if (kind == SQL_PARAM_INPUT) {
            saw_arg = true;
            col_str(s, 4, buf, sizeof(buf));
            assert_string_equal(buf, "cust");
            assert_int_equal(dtype, SQL_INTEGER);
        }
    }
    assert_true(saw_return);
    assert_true(saw_arg);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLTablePrivileges / SQLColumnPrivileges ────────────────── */

static void test_privileges(void **state)
{
    (void)state;
    SQLHSTMT s = new_stmt();

    assert_int_equal(SQLTablePrivileges(s, NULL, 0,
                                        (SQLCHAR *)"argus_test", SQL_NTS,
                                        (SQLCHAR *)"customers", SQL_NTS),
                     SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 7);

    bool saw_select = false;
    char buf[128];
    while (SQLFetch(s) == SQL_SUCCESS) {
        col_str(s, 6, buf, sizeof(buf));     /* PRIVILEGE */
        if (strcmp(buf, "SELECT") == 0) saw_select = true;
    }
    assert_true(saw_select);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    s = new_stmt();
    assert_int_equal(SQLColumnPrivileges(s, NULL, 0,
                                         (SQLCHAR *)"argus_test", SQL_NTS,
                                         (SQLCHAR *)"customers", SQL_NTS,
                                         (SQLCHAR *)"region", SQL_NTS),
                     SQL_SUCCESS);
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 8);

    saw_select = false;
    while (SQLFetch(s) == SQL_SUCCESS) {
        col_str(s, 4, buf, sizeof(buf));
        assert_string_equal(buf, "region");
        col_str(s, 7, buf, sizeof(buf));
        if (strcmp(buf, "SELECT") == 0) saw_select = true;
    }
    assert_true(saw_select);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQLSpecialColumns ───────────────────────────────────────── */

static void test_special_columns_best_rowid(void **state)
{
    (void)state;
    SQLHSTMT s = new_stmt();

    /* customers has a single-column primary key, which is the best row id. */
    assert_int_equal(SQLSpecialColumns(s, SQL_BEST_ROWID, NULL, 0,
                                       (SQLCHAR *)"argus_test", SQL_NTS,
                                       (SQLCHAR *)"customers", SQL_NTS,
                                       SQL_SCOPE_SESSION, SQL_NO_NULLS),
                     SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 8);

    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    char buf[128];
    col_str(s, 2, buf, sizeof(buf));
    assert_string_equal(buf, "id");
    assert_int_equal(col_i16(s, 3), SQL_INTEGER);
    assert_int_equal(col_i16(s, 8), SQL_PC_NOT_PSEUDO);
    assert_int_equal(SQLFetch(s), SQL_NO_DATA);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    /*
     * A table with no primary key and no all-NOT-NULL unique index has no best
     * row id. ctid is deliberately NOT offered: it is invalidated by UPDATE and
     * VACUUM FULL, so it does not satisfy ODBC's "identifies the row for the
     * requested scope" and a tool that used it for positioned updates would
     * corrupt data.
     */
    s = new_stmt();
    assert_int_equal(SQLSpecialColumns(s, SQL_BEST_ROWID, NULL, 0,
                                       (SQLCHAR *)"argus_test", SQL_NTS,
                                       (SQLCHAR *)"all_types", SQL_NTS,
                                       SQL_SCOPE_SESSION, SQL_NULLABLE),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(s), SQL_NO_DATA);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* SQL_ROWVER: xmin is a real row version, but it wraps around, so it is only
 * offered when explicitly asked for. */
static void test_special_columns_rowver(void **state)
{
    (void)state;
    SQLHSTMT s = new_stmt();
    assert_int_equal(SQLSpecialColumns(s, SQL_ROWVER, NULL, 0,
                                       (SQLCHAR *)"argus_test", SQL_NTS,
                                       (SQLCHAR *)"customers", SQL_NTS,
                                       SQL_SCOPE_TRANSACTION, SQL_NO_NULLS),
                     SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(s, &ncols);
    assert_int_equal(ncols, 8);
    /* Off by default; ARGUS_PG_ROW_VERSIONING=1 turns it on. */
    assert_int_equal(SQLFetch(s), SQL_NO_DATA);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── SQL_PROCEDURES ──────────────────────────────────────────── */

static void test_procedures_getinfo(void **state)
{
    (void)state;
    /*
     * "Y" promises two things: the engine has procedures, and the driver
     * accepts ODBC's {call ...} invocation syntax. Both are true here —
     * SQLProcedures reads pg_proc and escape.c renders {call f(a)} as
     * SELECT * FROM f(a). The answer is derived from the dialect's call
     * template, so it cannot drift from the second half.
     */
    SQLCHAR buf[8] = {0};
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_PROCEDURES, buf, sizeof(buf), &len),
                     SQL_SUCCESS);
    assert_string_equal((char *)buf, "Y");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_foreign_keys),
        cmocka_unit_test(test_foreign_keys_none),
        cmocka_unit_test(test_procedures),
        cmocka_unit_test(test_privileges),
        cmocka_unit_test(test_special_columns_best_rowid),
        cmocka_unit_test(test_special_columns_rowver),
        cmocka_unit_test(test_procedures_getinfo),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
