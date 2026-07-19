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
 * Block cursors: column-wise vs row-wise binding.
 *
 * ODBC lets an application receive a rowset either as one array per column
 * (SQL_BIND_BY_COLUMN) or as an array of structs (SQL_ATTR_ROW_BIND_TYPE set to
 * the struct size). The two need different pointer arithmetic, and getting it
 * wrong is silent: the driver writes inside the caller's buffer, just at the
 * wrong offset, so there is no crash and no diagnostic — only wrong data.
 *
 * The driver used to accept SQL_ATTR_ROW_BIND_TYPE and then ignore it, always
 * writing column-wise. These tests assert the layout actually requested.
 *
 * Defaults target Trino's tpch.tiny.nation (25 rows, stable content).
 * Override with BI_HOST/BI_PORT/BI_BACKEND/BI_DATABASE.
 */

static const char *env_or(const char *name, const char *dflt)
{
    const char *v = getenv(name);
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

    char conn[512];
    snprintf(conn, sizeof(conn),
             "HOST=%s;PORT=%s;UID=test;Backend=%s;Database=%s",
             env_or("BI_HOST", "localhost"), env_or("BI_PORT", "8080"),
             env_or("BI_BACKEND", "trino"), env_or("BI_DATABASE", "tpch"));

    if (SQLDriverConnect(g_dbc, NULL, (SQLCHAR *)conn, SQL_NTS,
                         NULL, 0, NULL, SQL_DRIVER_NOPROMPT) != SQL_SUCCESS)
        return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    SQLDisconnect(g_dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, g_dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, g_env);
    return 0;
}

#define ROWSET 5
#define NAME_LEN 32

static const char *QUERY =
    "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey < 5 "
    "ORDER BY nationkey";

/* Column-wise: one array per column. This is what every BI tool uses, and what
 * the driver did unconditionally. */
static void test_column_wise_block_fetch(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    SQLULEN rows_fetched = 0;
    SQLUSMALLINT status[ROWSET];
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE,
                                    (SQLPOINTER)SQL_BIND_BY_COLUMN, 0), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                                    (SQLPOINTER)(uintptr_t)ROWSET, 0), SQL_SUCCESS);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, status, 0);

    SQLINTEGER keys[ROWSET];
    SQLCHAR    names[ROWSET][NAME_LEN];
    SQLLEN     key_ind[ROWSET], name_ind[ROWSET];
    memset(keys, 0, sizeof(keys));
    memset(names, 0, sizeof(names));

    assert_int_equal(SQLBindCol(stmt, 1, SQL_C_SLONG, keys,
                                sizeof(SQLINTEGER), key_ind), SQL_SUCCESS);
    assert_int_equal(SQLBindCol(stmt, 2, SQL_C_CHAR, names,
                                NAME_LEN, name_ind), SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(stmt, (SQLCHAR *)QUERY, SQL_NTS), SQL_SUCCESS);

    SQLRETURN r = SQLFetch(stmt);
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    assert_int_equal(rows_fetched, ROWSET);

    /* nationkey 0..4 in order, each name non-empty. A column-wise write to a
     * row-wise layout would scatter these. */
    for (int i = 0; i < ROWSET; i++) {
        assert_int_equal(status[i], SQL_ROW_SUCCESS);
        assert_int_equal(keys[i], i);
        assert_true(name_ind[i] > 0);
        assert_true(names[i][0] != '\0');
    }
    assert_string_equal((const char *)names[0], "ALGERIA");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* Row-wise: one struct per row. Both the value and the indicator advance by
 * sizeof(struct), not by the column's buffer length. */
typedef struct {
    SQLINTEGER key;
    SQLLEN     key_ind;
    SQLCHAR    name[NAME_LEN];
    SQLLEN     name_ind;
} nation_row_t;

static void test_row_wise_block_fetch(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    nation_row_t rows[ROWSET];
    memset(rows, 0, sizeof(rows));

    SQLULEN rows_fetched = 0;
    SQLUSMALLINT status[ROWSET];

    /* Row-wise is requested by setting the bind type to the struct size. */
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE,
                                    (SQLPOINTER)(uintptr_t)sizeof(nation_row_t), 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                                    (SQLPOINTER)(uintptr_t)ROWSET, 0), SQL_SUCCESS);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, status, 0);

    /* Row-wise binds the FIRST row's members; the driver strides from there. */
    assert_int_equal(SQLBindCol(stmt, 1, SQL_C_SLONG, &rows[0].key,
                                sizeof(SQLINTEGER), &rows[0].key_ind), SQL_SUCCESS);
    assert_int_equal(SQLBindCol(stmt, 2, SQL_C_CHAR, rows[0].name,
                                NAME_LEN, &rows[0].name_ind), SQL_SUCCESS);

    assert_int_equal(SQLExecDirect(stmt, (SQLCHAR *)QUERY, SQL_NTS), SQL_SUCCESS);

    SQLRETURN r = SQLFetch(stmt);
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    assert_int_equal(rows_fetched, ROWSET);

    /* Every struct must hold its own row. Column-wise arithmetic here writes
     * row 1's key at &rows[0].key + 4 — inside rows[0], not rows[1] — so
     * rows[1..4] would stay zeroed and this fails. */
    for (int i = 0; i < ROWSET; i++) {
        assert_int_equal(status[i], SQL_ROW_SUCCESS);
        assert_int_equal(rows[i].key, i);
        assert_true(rows[i].name_ind > 0);
        assert_true(rows[i].name[0] != '\0');
    }
    assert_string_equal((const char *)rows[0].name, "ALGERIA");
    assert_string_equal((const char *)rows[1].name, "ARGENTINA");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* The attribute must read back as set, not as a hard-coded default. */
static void test_row_bind_type_round_trips(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    SQLULEN v = 0;
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, &v, 0, NULL),
                     SQL_SUCCESS);
    assert_int_equal(v, SQL_BIND_BY_COLUMN);

    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE,
                                    (SQLPOINTER)(uintptr_t)sizeof(nation_row_t), 0),
                     SQL_SUCCESS);
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, &v, 0, NULL),
                     SQL_SUCCESS);
    assert_int_equal(v, sizeof(nation_row_t));

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* Row-wise must work through the static cursor's scroll path too — it has its
 * own delivery function, and the same bug lived in both. */
static void test_row_wise_with_static_cursor(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    nation_row_t rows[ROWSET];
    memset(rows, 0, sizeof(rows));
    SQLULEN rows_fetched = 0;

    SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE,
                   (SQLPOINTER)(uintptr_t)sizeof(nation_row_t), 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(uintptr_t)ROWSET, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);

    SQLBindCol(stmt, 1, SQL_C_SLONG, &rows[0].key, sizeof(SQLINTEGER), &rows[0].key_ind);
    SQLBindCol(stmt, 2, SQL_C_CHAR, rows[0].name, NAME_LEN, &rows[0].name_ind);

    assert_int_equal(SQLExecDirect(stmt, (SQLCHAR *)QUERY, SQL_NTS), SQL_SUCCESS);

    SQLRETURN r = SQLFetchScroll(stmt, SQL_FETCH_FIRST, 0);
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    assert_int_equal(rows_fetched, ROWSET);

    for (int i = 0; i < ROWSET; i++)
        assert_int_equal(rows[i].key, i);
    assert_string_equal((const char *)rows[1].name, "ARGENTINA");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/*
 * A static (scrollable) cursor buffers the whole result in memory, so an
 * unbounded SELECT would OOM. MaxScrollRows caps it and the driver fails
 * cleanly with an actionable diagnostic instead of crashing. Prove it: a cap of
 * 3 over the 25-row nation table must be refused, not truncated silently.
 */
static void test_static_cursor_row_cap(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    char conn[512];
    snprintf(conn, sizeof(conn),
             "HOST=%s;PORT=%s;UID=test;Backend=%s;Database=%s;MaxScrollRows=3",
             env_or("BI_HOST", "localhost"), env_or("BI_PORT", "8080"),
             env_or("BI_BACKEND", "trino"), env_or("BI_DATABASE", "tpch"));
    assert_int_equal(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn, SQL_NTS,
                                      NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
                     SQL_SUCCESS);

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, 0);

    assert_int_equal(SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT nationkey FROM tpch.tiny.nation", SQL_NTS),
        SQL_SUCCESS);

    /* Building the scroll cache exceeds the cap → clean error, not an OOM. */
    SQLRETURN r = SQLFetchScroll(stmt, SQL_FETCH_FIRST, 0);
    assert_int_equal(r, SQL_ERROR);

    SQLCHAR st[6] = {0}, msg[256] = {0};
    SQLINTEGER native; SQLSMALLINT l;
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &l);
    assert_non_null(strstr((const char *)msg, "MaxScrollRows"));
    assert_non_null(strstr((const char *)msg, "forward-only"));

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_row_bind_type_round_trips),
        cmocka_unit_test(test_column_wise_block_fetch),
        cmocka_unit_test(test_row_wise_block_fetch),
        cmocka_unit_test(test_row_wise_with_static_cursor),
        cmocka_unit_test(test_static_cursor_row_cap),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
