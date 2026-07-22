/*
 * Integration test: the static-cursor materialisation cap (MAXSCROLLROWS).
 *
 * A static (scrollable) cursor buffers the whole result set in memory; an
 * unbounded SELECT would OOM the process. Past the cap the driver must fail
 * cleanly with SQLSTATE HY001 and an actionable message — never crash or
 * truncate silently. Under the cap, scrolling works and sees every row.
 *
 * Requires a live Trino (docker compose -f tests/integration/docker-compose.yml).
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static const char *trino_host(void)
{
    const char *h = getenv("TRINO_HOST");
    return h ? h : "localhost";
}

static int trino_port(void)
{
    const char *p = getenv("TRINO_PORT");
    return p ? atoi(p) : 8080;
}

static SQLHDBC connect_scroll(SQLHENV *env_out, long max_scroll_rows)
{
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%d;UID=test;Backend=trino;Database=memory;"
             "MAXSCROLLROWS=%ld",
             trino_host(), trino_port(), max_scroll_rows);
    assert_true(SQL_SUCCEEDED(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str,
                                               SQL_NTS, NULL, 0, NULL,
                                               SQL_DRIVER_NOPROMPT)));
    *env_out = env;
    return dbc;
}

static SQLHSTMT static_cursor_exec(SQLHDBC dbc, const char *sql)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_true(SQL_SUCCEEDED(SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE,
                                             (SQLPOINTER)SQL_CURSOR_STATIC, 0)));
    assert_true(SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS)));
    return stmt;
}

/* 10 rows > cap of 5: the first scroll materialises and must refuse. */
static void test_cap_exceeded_fails_cleanly(void **state)
{
    (void)state;
    SQLHENV env;
    SQLHDBC dbc = connect_scroll(&env, 5);
    SQLHSTMT stmt = static_cursor_exec(
        dbc, "SELECT * FROM UNNEST(SEQUENCE(1, 10)) AS t(n)");

    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_FIRST, 0);
    assert_int_equal(ret, SQL_ERROR);

    SQLCHAR sqlstate[6] = {0}, msg[256] = {0};
    SQLINTEGER native = 0;
    SQLSMALLINT mlen = 0;
    assert_true(SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1,
                                            sqlstate, &native, msg,
                                            sizeof(msg), &mlen)));
    assert_string_equal((char *)sqlstate, "HY001");
    assert_non_null(strstr((char *)msg, "MaxScrollRows"));

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* Under the cap the static cursor scrolls to both ends. */
static void test_under_cap_scrolls(void **state)
{
    (void)state;
    SQLHENV env;
    SQLHDBC dbc = connect_scroll(&env, 100);
    SQLHSTMT stmt = static_cursor_exec(
        dbc, "SELECT * FROM UNNEST(SEQUENCE(1, 10)) AS t(n)");

    long val = 0;
    SQLLEN ind = 0;
    assert_true(SQL_SUCCEEDED(SQLBindCol(stmt, 1, SQL_C_SLONG, &val,
                                         sizeof(val), &ind)));
    assert_true(SQL_SUCCEEDED(SQLFetchScroll(stmt, SQL_FETCH_LAST, 0)));
    assert_int_equal(val, 10);
    assert_true(SQL_SUCCEEDED(SQLFetchScroll(stmt, SQL_FETCH_FIRST, 0)));
    assert_int_equal(val, 1);
    assert_true(SQL_SUCCEEDED(SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, 7)));
    assert_int_equal(val, 7);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_cap_exceeded_fails_cleanly),
        cmocka_unit_test(test_under_cap_scrolls),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
