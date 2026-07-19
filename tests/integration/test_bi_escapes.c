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
 * ODBC escape sequence probe.
 *
 * Tableau, Excel, Qlik and Alteryx read SQLGetInfo's scalar-function bitmaps
 * and then emit {fn UCASE(x)}, {ts '...'}, {oj ...}. The ODBC spec makes those
 * the driver's to translate, so advertising a function is a promise to accept
 * the escape. This test exercises that promise against a live server: a wrong
 * or missing translation reaches the server as a literal brace and is rejected
 * with "mismatched input '{'", which is precisely what used to happen.
 *
 * Unit tests (tests/unit/test_escape.c) pin the translation's *shape*; only a
 * live server can say whether the rendering is actually valid for the engine —
 * Trino's repeat() returning an array instead of a string was caught here, not
 * there.
 *
 * Defaults target Trino's built-in tpch.tiny.nation. Override with
 * BI_HOST/BI_PORT/BI_BACKEND/BI_DATABASE.
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

/*
 * Execute `sql` and assert the first column of the first row equals `expect`.
 * Checking the value, not just the return code, is what catches a translation
 * that parses and runs but means something else — DAYOFWEEK's 1=Sunday, say.
 */
static void assert_scalar(const char *label, const char *sql, const char *expect)
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
        fail_msg("escape rejected [%s]: %s\n  SQL: %s", label, msg, sql);
    }

    r = SQLFetch(stmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("no row returned [%s]\n  SQL: %s", label, sql);
    }

    SQLCHAR val[256] = {0};
    SQLLEN  ind      = 0;
    r = SQLGetData(stmt, 1, SQL_C_CHAR, val, sizeof(val), &ind);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("could not read column [%s]\n  SQL: %s", label, sql);
    }

    if (strcmp((const char *)val, expect) != 0) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("wrong value [%s]: got '%s', expected '%s'\n  SQL: %s",
                 label, (const char *)val, expect, sql);
    }

    while (SQLFetch(stmt) == SQL_SUCCESS) { /* drain */ }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* The escapes Tableau actually emits, with the value each must produce. */
static void test_scalar_function_escapes(void **state)
{
    (void)state;

    assert_scalar("UCASE",  "SELECT {fn UCASE('peru')}",  "PERU");
    assert_scalar("LCASE",  "SELECT {fn LCASE('PERU')}",  "peru");
    assert_scalar("LENGTH", "SELECT {fn LENGTH('PERU')}", "4");
    assert_scalar("CONCAT", "SELECT {fn CONCAT('PE', 'RU')}", "PERU");
    assert_scalar("LTRIM",  "SELECT {fn LTRIM('  PERU')}", "PERU");
    assert_scalar("IFNULL", "SELECT {fn IFNULL(NULL, 'PERU')}", "PERU");

    /* Argument order differs from the native function: ODBC LOCATE takes
     * (needle, haystack), Trino's strpos takes (haystack, needle). */
    assert_scalar("LOCATE", "SELECT {fn LOCATE('E', 'PERU')}", "2");

    /* Functions the engine does not have natively, rendered via others. */
    assert_scalar("LEFT",   "SELECT {fn LEFT('PERU', 2)}",  "PE");
    assert_scalar("RIGHT",  "SELECT {fn RIGHT('PERU', 2)}", "RU");
    assert_scalar("SPACE",  "SELECT {fn LENGTH({fn SPACE(3)})}", "3");
    assert_scalar("REPEAT", "SELECT {fn REPEAT('ab', 2)}", "abab");

    assert_scalar("nested", "SELECT {fn UCASE({fn LTRIM('  peru')})}", "PERU");
}

static void test_datetime_escapes(void **state)
{
    (void)state;

    assert_scalar("{d} literal",  "SELECT {fn YEAR({d '2024-01-31'})}", "2024");
    assert_scalar("{ts} literal", "SELECT {fn HOUR({ts '2024-01-31 15:00:00'})}", "15");
    assert_scalar("MONTH",        "SELECT {fn MONTH({d '2024-01-31'})}", "1");
    assert_scalar("DAYOFMONTH",   "SELECT {fn DAYOFMONTH({d '2024-01-31'})}", "31");
    assert_scalar("QUARTER",      "SELECT {fn QUARTER({d '2024-05-01'})}", "2");

    /* ODBC numbers days 1=Sunday..7=Saturday; Trino's day_of_week is 1=Monday.
     * 2024-01-07 is a Sunday, so a dialect that forwards day_of_week unchanged
     * answers 7 and this fails. */
    assert_scalar("DAYOFWEEK Sunday",   "SELECT {fn DAYOFWEEK({d '2024-01-07'})}", "1");
    assert_scalar("DAYOFWEEK Saturday", "SELECT {fn DAYOFWEEK({d '2024-01-06'})}", "7");
}

static void test_join_and_like_escapes(void **state)
{
    (void)state;

    assert_scalar("{oj}",
                  "SELECT n.name FROM {oj tpch.tiny.nation n "
                  "LEFT OUTER JOIN tpch.tiny.region r ON n.regionkey = r.regionkey} "
                  "WHERE n.name = 'PERU'",
                  "PERU");

    assert_scalar("{escape}",
                  "SELECT name FROM tpch.tiny.nation "
                  "WHERE name LIKE 'PER%' {escape '!'}",
                  "PERU");
}

/* A brace inside a literal is data. Getting this wrong corrupts queries
 * silently rather than failing loudly, so it is worth a live check. */
static void test_braces_in_literals_survive(void **state)
{
    (void)state;
    assert_scalar("literal brace", "SELECT '{fn UCASE(x)}'", "{fn UCASE(x)}");
}

/*
 * SQLNativeSql is what a BI tool's "view native query" calls. It must show the
 * SQL the server really receives — no escapes left in it.
 */
static void test_native_sql_reports_translated_text(void **state)
{
    (void)state;

    SQLCHAR    out[512] = {0};
    SQLINTEGER len      = 0;
    const char *in = "SELECT {fn UCASE(name)} FROM tpch.tiny.nation "
                     "WHERE {fn LENGTH(name)} > 5";

    SQLRETURN r = SQLNativeSql(g_dbc, (SQLCHAR *)in, SQL_NTS,
                               out, sizeof(out), &len);
    assert_int_equal(r, SQL_SUCCESS);

    assert_null(strstr((const char *)out, "{fn"));
    assert_non_null(strstr((const char *)out, "upper(name)"));
    assert_non_null(strstr((const char *)out, "length(name)"));
    assert_int_equal(len, (SQLINTEGER)strlen((const char *)out));
}

/*
 * The contract runs both ways: a function SQLGetInfo does NOT advertise must be
 * refused by the driver, not forwarded for the server to choke on. Trino has no
 * SOUNDEX, so the trino dialect has no mapping for it.
 */
static void test_unadvertised_function_is_refused_by_driver(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)"SELECT {fn SOUNDEX('PERU')}", SQL_NTS);
    assert_int_equal(r, SQL_ERROR);

    SQLCHAR st[6] = {0}, msg[512] = {0};
    SQLINTEGER native;
    SQLSMALLINT l;
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &l);

    /* Rejected by us with a syntax-error SQLSTATE and a message naming the
     * function — not an opaque error relayed from the server. */
    assert_string_equal((const char *)st, "42000");
    assert_non_null(strstr((const char *)msg, "SOUNDEX"));

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* SQL_ATTR_NOSCAN is how an application says "my SQL is already native". */
static void test_noscan_disables_translation(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_NOSCAN,
                                    (SQLPOINTER)SQL_NOSCAN_ON, 0), SQL_SUCCESS);

    SQLULEN ns = 0;
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_NOSCAN, &ns, 0, NULL), SQL_SUCCESS);
    assert_int_equal(ns, SQL_NOSCAN_ON);

    /* Untranslated, the brace reaches Trino and it says so. */
    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)"SELECT {fn UCASE('peru')}", SQL_NTS);
    assert_true(r == SQL_ERROR);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/*
 * SQL_DBMS_VER must be the server's real version. BI tools gate features on it,
 * so the old hard-coded "04.00.0000" had them gating on a fiction. ODBC's shape
 * is "##.##.####" plus an optional vendor suffix, which is where Trino's bare
 * major ("467") survives intact.
 */
static void test_dbms_version_is_the_real_server_version(void **state)
{
    (void)state;

    SQLCHAR ver[128] = {0};
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_DBMS_VER, ver, sizeof(ver), &len),
                     SQL_SUCCESS);

    /* Not the invented constant, and not the "unknown" fallback. */
    assert_string_not_equal((const char *)ver, "04.00.0000");
    assert_string_not_equal((const char *)ver, "00.00.0000");

    /* Shape: ##.##.#### then the server's own string. */
    unsigned major = 0, minor = 0, release = 0;
    assert_int_equal(sscanf((const char *)ver, "%u.%u.%u", &major, &minor, &release), 3);
    assert_true(major > 0);

    /* Trino versions are a bare major, and the suffix carries it verbatim. */
    char suffix[64] = {0};
    assert_int_equal(sscanf((const char *)ver, "%*u.%*u.%*u %63s", suffix), 1);
    assert_int_equal((unsigned)atoi(suffix), major);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_dbms_version_is_the_real_server_version),
        cmocka_unit_test(test_scalar_function_escapes),
        cmocka_unit_test(test_datetime_escapes),
        cmocka_unit_test(test_join_and_like_escapes),
        cmocka_unit_test(test_braces_in_literals_survive),
        cmocka_unit_test(test_native_sql_reports_translated_text),
        cmocka_unit_test(test_unadvertised_function_is_refused_by_driver),
        cmocka_unit_test(test_noscan_disables_translation),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
