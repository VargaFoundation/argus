/*
 * Unit tests for the length arguments of the narrow ODBC entry points.
 *
 * A length that is neither a byte count nor SQL_NTS used to be cast to
 * size_t: malloc(0) followed by a memcpy of "SIZE_MAX" bytes, reachable
 * from SQLExecDirect, SQLPrepare, SQLDriverConnect, SQLConnect and every
 * catalog function. ODBC's answer is HY090, before the buffer is touched.
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <stdlib.h>
#include "argus/handle.h"

static void expect_state(SQLSMALLINT type, SQLHANDLE h, const char *want)
{
    SQLCHAR state[6] = {0};
    SQLINTEGER native = 0;
    SQLCHAR msg[256];
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(type, h, 1, state, &native,
                                   msg, sizeof(msg), &len), SQL_SUCCESS);
    assert_string_equal((const char *)state, want);
}

/* ── argus_str_dup: the helper itself never trusts a bad length ── */

static void test_str_dup_rejects_negative_lengths(void **state)
{
    (void)state;
    const SQLCHAR *s = (const SQLCHAR *)"SELECT 1";

    char *d = argus_str_dup(s, SQL_NTS);
    assert_non_null(d);
    assert_string_equal(d, "SELECT 1");
    free(d);

    d = argus_str_dup(s, 6);
    assert_non_null(d);
    assert_string_equal(d, "SELECT");
    free(d);

    d = argus_str_dup(s, 0);
    assert_non_null(d);
    assert_string_equal(d, "");
    free(d);

    assert_null(argus_str_dup(s, -2));
    assert_null(argus_str_dup(s, -12345));
    assert_null(argus_str_dup_short(s, -2));
    assert_null(argus_str_dup_short(s, (SQLSMALLINT)-32768));
    assert_null(argus_str_dup(NULL, SQL_NTS));

    assert_true(argus_odbc_len_valid(SQL_NTS));
    assert_true(argus_odbc_len_valid(0));
    assert_true(argus_odbc_len_valid(32767));
    assert_false(argus_odbc_len_valid(-2));
    assert_false(argus_odbc_len_valid(SQL_NULL_DATA)); /* -1 == SQL_NULL_DATA */
}

/* ── Statement entry points ─────────────────────────────────────── */

typedef struct { SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt; } handles_t;

static void handles_open(handles_t *h)
{
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &h->env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(h->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, h->env, &h->dbc),
                     SQL_SUCCESS);
    /* Statement allocation requires an open connection; fake the state the
     * way the other backend-less unit tests do. */
    ((argus_dbc_t *)h->dbc)->connected = true;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, h->dbc, &h->stmt),
                     SQL_SUCCESS);
}

static void handles_close(handles_t *h)
{
    SQLFreeHandle(SQL_HANDLE_STMT, h->stmt);
    ((argus_dbc_t *)h->dbc)->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, h->dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, h->env);
}

static void test_exec_direct_and_prepare(void **state)
{
    (void)state;
    handles_t h;
    handles_open(&h);
    SQLCHAR *sql = (SQLCHAR *)"SELECT 1";

    assert_int_equal(SQLExecDirect(h.stmt, sql, -7), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");

    assert_int_equal(SQLPrepare(h.stmt, sql, -7), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");

    /* A NULL text is a different caller bug, and stays HY009. */
    assert_int_equal(SQLPrepare(h.stmt, NULL, SQL_NTS), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY009");

    /* An explicit byte count still works: the statement is stored. */
    assert_int_equal(SQLPrepare(h.stmt, (SQLCHAR *)"SELECT 1 -- tail", 8),
                     SQL_SUCCESS);
    assert_string_equal(((argus_stmt_t *)h.stmt)->query, "SELECT 1");

    handles_close(&h);
}

static void test_cursor_name(void **state)
{
    (void)state;
    handles_t h;
    handles_open(&h);

    assert_int_equal(SQLSetCursorName(h.stmt, (SQLCHAR *)"c1", -9), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLSetCursorName(h.stmt, (SQLCHAR *)"c1xx", 2),
                     SQL_SUCCESS);
    assert_string_equal(((argus_stmt_t *)h.stmt)->cursor_name, "c1");

    handles_close(&h);
}

static void test_catalog_functions(void **state)
{
    (void)state;
    handles_t h;
    handles_open(&h);
    SQLCHAR *t = (SQLCHAR *)"t";
    SQLSMALLINT bad = -9;

    /* Each length position of each function is checked: a bad length in the
     * last argument is as fatal as one in the first. */
    assert_int_equal(SQLTables(h.stmt, NULL, 0, NULL, 0, t, SQL_NTS, t, bad),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLColumns(h.stmt, t, bad, NULL, 0, t, SQL_NTS, NULL, 0),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLStatistics(h.stmt, NULL, 0, t, bad, t, SQL_NTS,
                                   SQL_INDEX_ALL, SQL_QUICK), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLSpecialColumns(h.stmt, SQL_BEST_ROWID, NULL, 0, NULL,
                                       0, t, bad, SQL_SCOPE_SESSION,
                                       SQL_NULLABLE), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLPrimaryKeys(h.stmt, NULL, 0, NULL, 0, t, bad),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLForeignKeys(h.stmt, NULL, 0, NULL, 0, t, SQL_NTS,
                                    NULL, 0, NULL, 0, t, bad), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLProcedures(h.stmt, NULL, 0, t, bad, t, SQL_NTS),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLProcedureColumns(h.stmt, NULL, 0, NULL, 0, t, SQL_NTS,
                                         t, bad), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLTablePrivileges(h.stmt, t, bad, NULL, 0, t, SQL_NTS),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");
    assert_int_equal(SQLColumnPrivileges(h.stmt, NULL, 0, NULL, 0, t, SQL_NTS,
                                         t, bad), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY090");

    handles_close(&h);
}

/* ── Connection entry points ────────────────────────────────────── */

static void test_connection_functions(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    SQLCHAR *cs = (SQLCHAR *)"DRIVER=Argus;HOST=localhost";
    SQLCHAR out[64];
    SQLSMALLINT out_len = 0;

    assert_int_equal(SQLDriverConnect(dbc, NULL, cs, -4, out, sizeof(out),
                                      &out_len, SQL_DRIVER_NOPROMPT),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_DBC, dbc, "HY090");

    assert_int_equal(SQLBrowseConnect(dbc, cs, -4, out, sizeof(out), &out_len),
                     SQL_ERROR);
    expect_state(SQL_HANDLE_DBC, dbc, "HY090");

    /* SQLConnect treats a NULL argument as "not provided"; a bad length must
     * not be mistaken for that. All three positions. */
    SQLCHAR *n = (SQLCHAR *)"x";
    assert_int_equal(SQLConnect(dbc, n, -4, n, SQL_NTS, n, SQL_NTS), SQL_ERROR);
    expect_state(SQL_HANDLE_DBC, dbc, "HY090");
    assert_int_equal(SQLConnect(dbc, n, SQL_NTS, n, -4, n, SQL_NTS), SQL_ERROR);
    expect_state(SQL_HANDLE_DBC, dbc, "HY090");
    assert_int_equal(SQLConnect(dbc, n, SQL_NTS, n, SQL_NTS, n, -4), SQL_ERROR);
    expect_state(SQL_HANDLE_DBC, dbc, "HY090");
    /* Nothing was stored on the way to the error. */
    assert_null(((argus_dbc_t *)dbc)->username);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}


/*
 * With SQL_ATTR_METADATA_ID set, a catalog argument is an identifier, so a
 * null pointer for one is HY009 -- not, as it used to be, an unfiltered
 * query over every schema on the server.
 */
static void test_metadata_id_rejects_null_identifiers(void **state)
{
    (void)state;
    handles_t h;
    handles_open(&h);
    SQLCHAR *t = (SQLCHAR *)"t";

    /* Without the attribute a null argument means "not filtered", so the
     * call gets as far as the (absent) backend: 08003, not HY009. */
    assert_int_equal(SQLSetStmtAttr(h.stmt, SQL_ATTR_METADATA_ID,
                                    (SQLPOINTER)SQL_FALSE, 0), SQL_SUCCESS);
    assert_int_equal(SQLTables(h.stmt, NULL, 0, NULL, 0, t, SQL_NTS,
                               NULL, 0), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "08003");

    assert_int_equal(SQLSetStmtAttr(h.stmt, SQL_ATTR_METADATA_ID,
                                    (SQLPOINTER)SQL_TRUE, 0), SQL_SUCCESS);
    assert_int_equal(SQLTables(h.stmt, NULL, 0, NULL, 0, t, SQL_NTS,
                               NULL, 0), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY009");

    assert_int_equal(SQLColumns(h.stmt, t, SQL_NTS, t, SQL_NTS, NULL, 0,
                                t, SQL_NTS), SQL_ERROR);
    expect_state(SQL_HANDLE_STMT, h.stmt, "HY009");

    handles_close(&h);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_str_dup_rejects_negative_lengths),
        cmocka_unit_test(test_exec_direct_and_prepare),
        cmocka_unit_test(test_cursor_name),
        cmocka_unit_test(test_catalog_functions),
        cmocka_unit_test(test_metadata_id_rejects_null_identifiers),
        cmocka_unit_test(test_connection_functions),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
