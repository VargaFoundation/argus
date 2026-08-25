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
 * Every entry of the PostgreSQL dialect table, executed against a live server
 * and its value checked.
 *
 * src/odbc/dialect.c states the rule: reporting a bit in SQLGetInfo's scalar
 * bitmaps is a promise that {fn NAME(...)} will be translated into something
 * the engine accepts *and* that means what ODBC says it means. The generic
 * probe (test_bi_escapes.c) checks the subset every backend shares; this one
 * exists because the PostgreSQL table has entries no other backend has, and
 * several of them are exactly the cases where reading the documentation is not
 * enough — round() and trunc() not existing for double precision, log() being
 * base 10, to_char() blank-padding day names.
 *
 * Run against the compose `postgres` service, or any PostgreSQL:
 *   PG_HOST=... PG_PORT=... PG_USER=... PG_PASS=... PG_DB=... ./test_postgres_escapes
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
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=%s;Database=%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"),     env_or("PG_PASS", "argus"),
             env_or("PG_BACKEND", "postgres"), env_or("PG_DB", "argusdb"));

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

/* Execute and compare the first column of the first row. */
static void probe(const char *label, const char *sql, const char *expect)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0;
        SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &len);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("[%s] rejected: %s %s\n  SQL: %s", label, st, msg, sql);
    }

    if (SQLFetch(stmt) != SQL_SUCCESS) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("[%s] returned no rows\n  SQL: %s", label, sql);
    }

    SQLCHAR val[256] = {0};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, val, sizeof(val), &ind);

    if (strcmp((const char *)val, expect) != 0) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("[%s] got '%s', expected '%s'\n  SQL: %s",
                 label, (const char *)val, expect, sql);
    }

    while (SQLFetch(stmt) == SQL_SUCCESS) { /* drain */ }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* Non-deterministic entries: assert only that the engine accepts them. */
static void probe_runs(const char *label, const char *sql)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0;
        SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &len);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("[%s] rejected: %s %s\n  SQL: %s", label, st, msg, sql);
    }
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/* ── String functions ────────────────────────────────────────── */

static void test_string_functions(void **state)
{
    (void)state;
    probe("CONCAT",    "SELECT {fn CONCAT('PE','RU')}",        "PERU");
    probe("CONCAT/3",  "SELECT {fn CONCAT('a','b','c')}",      "abc");
    probe("LENGTH",    "SELECT {fn LENGTH('PERU')}",           "4");
    probe("SUBSTRING", "SELECT {fn SUBSTRING('PERUVIAN',1,4)}","PERU");
    probe("SUBSTRING2","SELECT {fn SUBSTRING('PERUVIAN',5)}",  "VIAN");
    probe("LTRIM",     "SELECT {fn LTRIM('  PERU')}",          "PERU");
    probe("RTRIM",     "SELECT {fn RTRIM('PERU  ')}",          "PERU");
    probe("LCASE",     "SELECT {fn LCASE('PERU')}",            "peru");
    probe("UCASE",     "SELECT {fn UCASE('peru')}",            "PERU");
    probe("REPLACE",   "SELECT {fn REPLACE('PERU','RU','TE')}","PETE");
    probe("LEFT",      "SELECT {fn LEFT('PERU',2)}",           "PE");
    probe("RIGHT",     "SELECT {fn RIGHT('PERU',2)}",          "RU");
    probe("REPEAT",    "SELECT {fn REPEAT('ab',2)}",           "abab");
    probe("SPACE",     "SELECT {fn LENGTH({fn SPACE(3)})}",    "3");
    probe("ASCII",     "SELECT {fn ASCII('A')}",               "65");
    probe("CHAR",      "SELECT {fn CHAR(65)}",                 "A");

    /* ODBC LOCATE is (needle, haystack); strpos() is (haystack, needle).
     * Both are 1-based and both answer 0 when absent, so the swap is the
     * entire adjustment — no offset, unlike Pinot's 0-based strpos. */
    probe("LOCATE",         "SELECT {fn LOCATE('E','PERU')}",  "2");
    probe("LOCATE missing", "SELECT {fn LOCATE('Z','PERU')}",  "0");

    probe("nested", "SELECT {fn UCASE({fn LTRIM('  peru')})}", "PERU");
}

/* ── Numeric functions ───────────────────────────────────────── */

static void test_numeric_functions(void **state)
{
    (void)state;
    probe("ABS",     "SELECT {fn ABS(-3)}",          "3");
    probe("CEILING", "SELECT {fn CEILING(1.2)}",     "2");
    probe("FLOOR",   "SELECT {fn FLOOR(1.8)}",       "1");
    probe("MOD",     "SELECT {fn MOD(7,3)}",         "1");
    probe("SQRT",    "SELECT {fn SQRT(9)}",          "3");
    probe("POWER",   "SELECT {fn POWER(2,10)}",      "1024");
    probe("SIGN",    "SELECT {fn SIGN(-5)}",         "-1");
    probe("EXP",     "SELECT {fn ROUND({fn EXP(0)},0)}", "1");

    /* PostgreSQL's one-argument log() is base 10 and ln() is natural — the
     * opposite of the naming in most engines, so both are checked. */
    probe("LOG10",   "SELECT {fn LOG10(1000)}",      "3");
    probe("LOG",     "SELECT {fn ROUND({fn LOG(1)},0)}", "0");

    /*
     * round()/trunc() with two arguments exist only for numeric in
     * PostgreSQL: round(double precision, integer) does not exist. The
     * dialect casts the first argument, and this is the probe that proves
     * it — without the cast these two fail with 42883.
     */
    probe("ROUND numeric",  "SELECT {fn ROUND(1.2345, 2)}", "1.23");
    probe("ROUND float col",
          "SELECT {fn ROUND(c_float8, 1)} FROM argus_test.all_types "
          "WHERE c_bool IS NOT NULL", "2.3");
    probe("TRUNCATE numeric", "SELECT {fn TRUNCATE(1.2999, 2)}", "1.29");
    probe("TRUNCATE float col",
          "SELECT {fn TRUNCATE(c_float8, 1)} FROM argus_test.all_types "
          "WHERE c_bool IS NOT NULL", "2.2");

    probe("DEGREES", "SELECT {fn ROUND({fn DEGREES({fn PI()})},0)}", "180");
    probe("RADIANS", "SELECT {fn ROUND({fn RADIANS(180)} - {fn PI()}, 6)}", "0.000000");
    probe("ATAN2",   "SELECT {fn ROUND({fn ATAN2(0,1)},0)}", "0");
    probe("ACOS",    "SELECT {fn ROUND({fn ACOS(1)},0)}",    "0");
    probe("ASIN",    "SELECT {fn ROUND({fn ASIN(0)},0)}",    "0");
    probe("ATAN",    "SELECT {fn ROUND({fn ATAN(0)},0)}",    "0");
    probe("COS",     "SELECT {fn ROUND({fn COS(0)},0)}",     "1");
    probe("SIN",     "SELECT {fn ROUND({fn SIN(0)},0)}",     "0");
    probe("TAN",     "SELECT {fn ROUND({fn TAN(0)},0)}",     "0");
    probe_runs("COT",  "SELECT {fn COT(1)}");
    probe_runs("RAND", "SELECT {fn RAND()}");
    probe_runs("PI",   "SELECT {fn PI()}");
}

/* ── System functions ────────────────────────────────────────── */

static void test_system_functions(void **state)
{
    (void)state;
    probe("IFNULL", "SELECT {fn IFNULL(NULL,'PERU')}", "PERU");
    probe_runs("USERNAME", "SELECT {fn USERNAME()}");
    probe("DBNAME", "SELECT {fn DBNAME()}", env_or("PG_DB", "argusdb"));
}

/* ── Date/time functions ─────────────────────────────────────── */

static void test_datetime_functions(void **state)
{
    (void)state;
    probe("YEAR",       "SELECT {fn YEAR({d '2024-01-31'})}",       "2024");
    probe("QUARTER",    "SELECT {fn QUARTER({d '2024-05-01'})}",    "2");
    probe("MONTH",      "SELECT {fn MONTH({d '2024-01-31'})}",      "1");
    probe("DAYOFMONTH", "SELECT {fn DAYOFMONTH({d '2024-01-31'})}", "31");
    probe("DAYOFYEAR",  "SELECT {fn DAYOFYEAR({d '2024-03-01'})}",  "61");
    probe("HOUR",       "SELECT {fn HOUR({ts '2024-01-31 15:04:05'})}",   "15");
    probe("MINUTE",     "SELECT {fn MINUTE({ts '2024-01-31 15:04:05'})}", "4");

    /* extract(second) is numeric with a fractional part in PostgreSQL; the
     * dialect floors it so an ODBC caller gets the integer ODBC promises. */
    probe("SECOND",     "SELECT {fn SECOND({ts '2024-01-31 15:04:05'})}", "5");

    /* ODBC numbers days 1=Sunday..7=Saturday; extract(dow) is 0=Sunday. */
    probe("DAYOFWEEK Sunday",   "SELECT {fn DAYOFWEEK({d '2024-01-07'})}", "1");
    probe("DAYOFWEEK Saturday", "SELECT {fn DAYOFWEEK({d '2024-01-06'})}", "7");

    /* to_char() blank-pads to nine characters without the FM prefix, which
     * surfaces as trailing spaces in every report that uses it. */
    probe("DAYNAME",   "SELECT {fn DAYNAME({d '2024-01-07'})}",   "Sunday");
    probe("MONTHNAME", "SELECT {fn MONTHNAME({d '2024-05-01'})}", "May");

    probe_runs("NOW",               "SELECT {fn NOW()}");
    probe_runs("CURDATE",           "SELECT {fn CURDATE()}");
    probe_runs("CURRENT_DATE",      "SELECT {fn CURRENT_DATE()}");
    probe_runs("CURTIME",           "SELECT {fn CURTIME()}");
    probe_runs("CURRENT_TIME",      "SELECT {fn CURRENT_TIME()}");
    probe_runs("CURRENT_TIMESTAMP", "SELECT {fn CURRENT_TIMESTAMP()}");
}

/*
 * The other half of the contract: a function the table does not carry must be
 * refused by the driver with 42000, never forwarded with the braces intact.
 * WEEK is the interesting case — PostgreSQL has extract(week), but it is ISO
 * 8601, so 2021-01-01 falls in week 53 of the previous year while ODBC's WEEK
 * puts January 1st in week 1. Advertising it would produce quietly wrong week
 * numbers in a report, so it is deliberately absent.
 */
static void test_unadvertised_functions_are_refused(void **state)
{
    (void)state;
    const char *refused[] = {
        "SELECT {fn WEEK({d '2021-01-01'})}",
        "SELECT {fn SOUNDEX('peru')}",
        "SELECT {fn TIMESTAMPADD(SQL_TSI_DAY, 1, {d '2024-01-01'})}",
    };

    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        SQLHSTMT stmt = SQL_NULL_HSTMT;
        assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

        SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)refused[i], SQL_NTS);
        assert_int_equal(r, SQL_ERROR);

        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0;
        SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &len);
        /* Refused by the driver (42000), not forwarded and refused by the
         * server (which would be a syntax error mentioning a brace). */
        assert_string_equal((char *)st, "42000");

        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
}

/*
 * {call ...} against a live server, which is what makes SQL_PROCEDURES = "Y"
 * honest: the info type promises both that the engine has procedures and that
 * the driver accepts ODBC's invocation syntax.
 */
static void test_procedure_call_escape(void **state)
{
    (void)state;

    /* order_count(cust) counts the seeded orders for a customer. */
    probe("{call}", "SELECT * FROM ({call argus_test.order_count(1)}) t(c)", "250");

    /* The ?= form binds the return value, which is what this rendering already
     * produces, so it must be accepted rather than refused. */
    probe("{?= call}",
          "SELECT * FROM ({?= call argus_test.order_count(1)}) t(c)", "250");

    /* A nested {fn} inside the argument list must be translated too. */
    probe("{call} nested",
          "SELECT * FROM ({call argus_test.order_count({fn ABS(-1)})}) t(c)",
          "250");
}

/*
 * A bound parameter carrying a backslash must arrive unchanged.
 *
 * The dialect table's backslash_escapes decides whether the driver doubles
 * '\' when rendering a bound value. PostgreSQL has defaulted
 * standard_conforming_strings to on since 9.1, so '\' inside a literal is an
 * ordinary character and doubling it would deliver 'C:\\path' where the
 * application bound 'C:\path' — a silent corruption, not an error. This is the
 * live check on that flag being false for the PostgreSQL family.
 */
static void test_bound_backslash_survives(void **state)
{
    (void)state;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &stmt), SQL_SUCCESS);

    const char *bound = "C:\\path\\to\\file";
    SQLLEN ind = SQL_NTS;
    assert_int_equal(
        SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                         strlen(bound), 0, (SQLPOINTER)bound, 0, &ind),
        SQL_SUCCESS);

    SQLRETURN r = SQLExecDirect(stmt, (SQLCHAR *)"SELECT ?::text", SQL_NTS);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0;
        SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &len);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("bound backslash rejected: %s %s", st, msg);
    }

    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLCHAR got[256] = {0};
    SQLLEN out = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, got, sizeof(got), &out);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    assert_string_equal((char *)got, bound);
}

static void test_sql_procedures_is_now_yes(void **state)
{
    (void)state;
    SQLCHAR buf[8] = {0};
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_PROCEDURES, buf, sizeof(buf), &len),
                     SQL_SUCCESS);
    assert_string_equal((char *)buf, "Y");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_string_functions),
        cmocka_unit_test(test_numeric_functions),
        cmocka_unit_test(test_system_functions),
        cmocka_unit_test(test_datetime_functions),
        cmocka_unit_test(test_procedure_call_escape),
        cmocka_unit_test(test_bound_backslash_survives),
        cmocka_unit_test(test_sql_procedures_is_now_yes),
        cmocka_unit_test(test_unadvertised_functions_are_refused),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
