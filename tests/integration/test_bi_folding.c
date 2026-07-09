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
 * BI query-folding probe.
 *
 * Power BI's Power Query engine reads the driver's dialect from SQLGetInfo and
 * then GENERATES SQL from it — the folding path. If the advertised dialect is
 * wrong, the generated SQL fails on the server even though hand-written queries
 * work (this is exactly how the backtick-vs-ANSI quote-char bug hid). This test
 * reproduces that path at the ODBC level: it reads the quote char the driver
 * advertises, builds the queries Power Query would generate WITH it, and runs
 * them against a live server. A regression in the advertised dialect breaks the
 * generated SQL and fails the test.
 *
 * Defaults target Trino's built-in tpch.tiny.nation table. Override with
 * BI_HOST/BI_PORT/BI_BACKEND/BI_TABLE/BI_COL/BI_NUMCOL.
 */

static const char *env_or(const char *name, const char *dflt)
{
    const char *v = getenv(name);
    return (v && *v) ? v : dflt;
}

static SQLHENV g_env = SQL_NULL_HENV;
static SQLHDBC g_dbc = SQL_NULL_HDBC;
static char    g_q[4] = "\"";   /* advertised identifier quote char */

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

    /* Read the dialect quote char the driver advertises — the queries below
     * are built with it, so this is what Power Query would actually emit. */
    SQLSMALLINT len = 0;
    SQLGetInfo(g_dbc, SQL_IDENTIFIER_QUOTE_CHAR, g_q, sizeof(g_q), &len);
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

/* Run one folded query and assert the server accepts it. */
static void run_folded(const char *label, const char *sql)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6], msg[512];
        SQLINTEGER native;
        SQLSMALLINT l;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &l);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("folded query rejected [%s]: %s\n  SQL: %s", label, msg, sql);
    }
    /* Drain any rows so the statement completes cleanly. */
    while (SQLFetch(stmt) == SQL_SUCCESS) { /* consume */ }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── The queries Power Query generates when folding ──────────── */

static void test_folding_patterns(void **state)
{
    (void)state;
    const char *q     = g_q;
    const char *table = env_or("BI_TABLE",  "tpch.tiny.nation");
    const char *col   = env_or("BI_COL",    "name");
    const char *ncol  = env_or("BI_NUMCOL", "nationkey");
    char sql[1024];

    /* Schema detection: Power Query probes columns with a no-row query. */
    snprintf(sql, sizeof(sql),
             "SELECT %s%s%s, %s%s%s FROM %s WHERE 1 = 0",
             q, col, q, q, ncol, q, table);
    run_folded("schema WHERE 1=0", sql);

    /* Applied filter — the step that first breaks a wrong quote char. */
    snprintf(sql, sizeof(sql),
             "SELECT %s%s%s FROM %s WHERE %s%s%s < 5",
             q, col, q, table, q, ncol, q);
    run_folded("filter", sql);

    /* GROUP BY aggregation. */
    snprintf(sql, sizeof(sql),
             "SELECT %s%s%s, COUNT(*) FROM %s GROUP BY %s%s%s",
             q, ncol, q, table, q, ncol, q);
    run_folded("group by", sql);

    /* Top-N via ANSI OFFSET…FETCH with an ORDER BY on a quoted column. */
    snprintf(sql, sizeof(sql),
             "SELECT %s%s%s FROM %s ORDER BY %s%s%s "
             "OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY",
             q, col, q, table, q, col, q);
    run_folded("order by + offset/fetch", sql);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_folding_patterns),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
