/*
 * Unit tests for the Unicode (W) entry points in src/odbc/unicode.c that work
 * without a live backend: attribute get/set, diagnostics, driver-level
 * SQLGetInfoW, cursor names, column metadata on a synthetic statement, and
 * the SQLDriverConnectW error path (UTF-16 in, UTF-16 diag out).
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include "argus/handle.h"

/* Argus provides W variants of the env-attr calls too; they are not part of
 * the standard sqlucode.h surface, so declare them here. */
SQLRETURN SQL_API SQLSetEnvAttrW(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
SQLRETURN SQL_API SQLGetEnvAttrW(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER,
                                 SQLINTEGER *);

/* Build a UTF-16 literal from ASCII at runtime (SQLWCHAR is 16-bit here). */
static SQLWCHAR *w(const char *ascii, SQLWCHAR *buf, size_t n)
{
    size_t i = 0;
    for (; ascii[i] && i + 1 < n; i++) buf[i] = (SQLWCHAR)(unsigned char)ascii[i];
    buf[i] = 0;
    return buf;
}

static char *narrow(const SQLWCHAR *ws)
{
    static char out[512];
    size_t i = 0;
    for (; ws[i] && i + 1 < sizeof(out); i++) out[i] = (char)ws[i];
    out[i] = '\0';
    return out;
}

static void test_env_attr_w(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttrW(env, SQL_ATTR_ODBC_VERSION,
                                    (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    SQLINTEGER v = 0;
    assert_int_equal(SQLGetEnvAttrW(env, SQL_ATTR_ODBC_VERSION, &v, 0, NULL),
                     SQL_SUCCESS);
    assert_int_equal(v, SQL_OV_ODBC3);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_connect_attr_info_and_diag_w(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    /* Set/get a connection attribute through the W surface. */
    assert_int_equal(SQLSetConnectAttrW(dbc, SQL_ATTR_LOGIN_TIMEOUT,
                                        (SQLPOINTER)(intptr_t)17, 0),
                     SQL_SUCCESS);
    SQLUINTEGER to = 0;
    assert_int_equal(SQLGetConnectAttrW(dbc, SQL_ATTR_LOGIN_TIMEOUT, &to,
                                        sizeof(to), NULL), SQL_SUCCESS);
    assert_int_equal(to, 17);

    /* Driver-level SQLGetInfoW works without a connection. */
    SQLWCHAR info[128];
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfoW(dbc, SQL_DRIVER_ODBC_VER, info, sizeof(info),
                                &len);
    assert_true(SQL_SUCCEEDED(ret));
    assert_string_equal(narrow(info), "03.80");

    /* SQLDriverConnectW error path: prompting is rejected, diag readable
     * through the W diagnostics (record + field). */
    SQLWCHAR conn[64];
    ret = SQLDriverConnectW(dbc, NULL, w("HOST=x", conn, 64), SQL_NTS,
                            NULL, 0, NULL, SQL_DRIVER_PROMPT);
    assert_int_equal(ret, SQL_ERROR);

    SQLWCHAR sqlstate[6], msg[256];
    SQLINTEGER native = 0;
    SQLSMALLINT mlen = 0;
    assert_true(SQL_SUCCEEDED(SQLGetDiagRecW(SQL_HANDLE_DBC, dbc, 1, sqlstate,
                                             &native, msg, 256, &mlen)));
    assert_int_equal((int)strlen(narrow(sqlstate)), 5);
    assert_non_null(strstr(narrow(msg), "Argus"));

    SQLWCHAR field[64];
    SQLSMALLINT flen = 0;
    assert_true(SQL_SUCCEEDED(SQLGetDiagFieldW(SQL_HANDLE_DBC, dbc, 1,
                                               SQL_DIAG_SQLSTATE, field,
                                               sizeof(field), &flen)));

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* Synthetic executed statement with one described column (no backend). */
static argus_stmt_t *make_fake_stmt(void)
{
    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    stmt->signature = ARGUS_STMT_SIGNATURE;
    stmt->row_count = -1;
    stmt->row_array_size = 1;
    stmt->executed = true;
    stmt->num_cols = 1;
    stmt->metadata_fetched = true;
    argus_diag_clear(&stmt->diag);
    argus_stmt_ensure_columns(stmt, 8);
    argus_stmt_ensure_bindings(stmt, 8);
    strncpy((char *)stmt->columns[0].name, "col_w", ARGUS_MAX_COLUMN_NAME);
    stmt->columns[0].name_len = 5;
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->columns[0].column_size = 64;
    stmt->columns[0].nullable = SQL_NULLABLE;
    return stmt;
}

static void free_fake_stmt(argus_stmt_t *stmt)
{
    free(stmt->columns);
    free(stmt->bindings);
    stmt->signature = 0;
    free(stmt);
}

static void test_stmt_metadata_w(void **state)
{
    (void)state;
    argus_stmt_t *stmt = make_fake_stmt();

    /* Cursor name round-trip — a name may only be set before execution. */
    stmt->executed = false;
    SQLWCHAR cname[32], got[32];
    SQLSMALLINT glen = 0;
    assert_true(SQL_SUCCEEDED(SQLSetCursorNameW(stmt, w("CUR_W", cname, 32),
                                                SQL_NTS)));
    assert_true(SQL_SUCCEEDED(SQLGetCursorNameW(stmt, got, 32, &glen)));
    assert_string_equal(narrow(got), "CUR_W");
    stmt->executed = true;

    SQLWCHAR name[64];
    SQLSMALLINT nlen = 0, dtype = 0, digits = 0, nullable = 0;
    SQLULEN size = 0;
    assert_true(SQL_SUCCEEDED(SQLDescribeColW(stmt, 1, name, 64, &nlen,
                                              &dtype, &size, &digits,
                                              &nullable)));
    assert_string_equal(narrow(name), "col_w");
    assert_int_equal(dtype, SQL_VARCHAR);

    SQLWCHAR attr[64];
    SQLSMALLINT alen = 0;
    SQLLEN num = 0;
    assert_true(SQL_SUCCEEDED(SQLColAttributeW(stmt, 1, SQL_DESC_NAME, attr,
                                               sizeof(attr), &alen, &num)));
    assert_string_equal(narrow(attr), "col_w");

    /* Statement attributes through the W surface. */
    assert_true(SQL_SUCCEEDED(SQLSetStmtAttrW(stmt, SQL_ATTR_MAX_ROWS,
                                              (SQLPOINTER)(intptr_t)42, 0)));
    SQLULEN mr = 0;
    assert_true(SQL_SUCCEEDED(SQLGetStmtAttrW(stmt, SQL_ATTR_MAX_ROWS, &mr,
                                              sizeof(mr), NULL)));
    assert_int_equal((int)mr, 42);

    free_fake_stmt(stmt);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_env_attr_w),
        cmocka_unit_test(test_connect_attr_info_and_diag_w),
        cmocka_unit_test(test_stmt_metadata_w),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
