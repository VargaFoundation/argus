#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/*
 * SQLDescribeParam, and the statement-level SQLSTATEs that arrived with it.
 *
 * Everywhere else the driver can only answer SQL_VARCHAR/255 for a parameter,
 * because there is nothing to ask. The PostgreSQL family parses the statement
 * server-side and reports the types PostgreSQL inferred, which is what
 * SQL_DESCRIBE_PARAMETER = "Y" is a promise about.
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

static SQLHSTMT prepared(const char *sql)
{
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    SQLRETURN rc = SQLPrepare(s, (SQLCHAR *)sql, SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER n = 0; SQLSMALLINT l = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, st, &n, msg, sizeof(msg), &l);
        SQLFreeHandle(SQL_HANDLE_STMT, s);
        fail_msg("SQLPrepare failed: %s %s\n  SQL: %s", st, msg, sql);
    }
    return s;
}

static void test_describe_parameter_is_advertised(void **state)
{
    (void)state;
    SQLCHAR buf[8] = {0};
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_DESCRIBE_PARAMETER, buf,
                                sizeof(buf), &len), SQL_SUCCESS);
    assert_string_equal((char *)buf, "Y");
}

/* Types inferred from the columns the parameters are compared against. */
static void test_parameter_types_come_from_the_server(void **state)
{
    (void)state;
    SQLHSTMT s = prepared(
        "SELECT * FROM argus_test.customers WHERE id = ? AND name = ?");

    SQLSMALLINT n = 0;
    assert_int_equal(SQLNumParams(s, &n), SQL_SUCCESS);
    assert_int_equal(n, 2);

    SQLSMALLINT dtype = 0, ddigits = 0, nullable = 0;
    SQLULEN size = 0;

    assert_int_equal(SQLDescribeParam(s, 1, &dtype, &size, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_int_equal(dtype, SQL_INTEGER);       /* customers.id */

    /* customers.name is varchar(50), but PostgreSQL resolves `varchar = $2`
     * through the text operator, so the inferred parameter type is text. A
     * character *parameter* is reported as SQL_VARCHAR either way: an
     * application binding a value does not need to be told to stream it. */
    assert_int_equal(SQLDescribeParam(s, 2, &dtype, &size, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_int_equal(dtype, SQL_VARCHAR);

    /* Out of range is an error, not a silent generic answer. */
    assert_int_equal(SQLDescribeParam(s, 3, &dtype, &size, &ddigits, &nullable),
                     SQL_ERROR);

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

static void test_parameter_types_numeric_and_date(void **state)
{
    (void)state;
    SQLHSTMT s = prepared(
        "SELECT * FROM argus_test.orders "
        "WHERE amount > ? AND placed_on < ? AND order_id = ?");

    SQLSMALLINT dtype = 0, ddigits = 0, nullable = 0;
    SQLULEN size = 0;

    assert_int_equal(SQLDescribeParam(s, 1, &dtype, &size, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_int_equal(dtype, SQL_NUMERIC);       /* orders.amount numeric(10,2) */

    assert_int_equal(SQLDescribeParam(s, 2, &dtype, &size, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_int_equal(dtype, SQL_TYPE_DATE);

    assert_int_equal(SQLDescribeParam(s, 3, &dtype, &size, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_int_equal(dtype, SQL_BIGINT);

    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* A statement with no parameters: zero, and no server round trip wasted. */
static void test_no_parameters(void **state)
{
    (void)state;
    SQLHSTMT s = prepared("SELECT 1");
    SQLSMALLINT n = -1;
    assert_int_equal(SQLNumParams(s, &n), SQL_SUCCESS);
    assert_int_equal(n, 0);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/*
 * A question mark inside a literal is data, not a marker. Getting this wrong
 * would make the driver send PostgreSQL a $1 it never asked for.
 */
static void test_question_mark_in_literal_is_not_a_parameter(void **state)
{
    (void)state;
    SQLHSTMT s = prepared("SELECT 'why?' , ? ::int");
    SQLSMALLINT n = -1;
    assert_int_equal(SQLNumParams(s, &n), SQL_SUCCESS);
    assert_int_equal(n, 1);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    s = prepared("SELECT 1 -- what?\n , ? ::int");
    assert_int_equal(SQLNumParams(s, &n), SQL_SUCCESS);
    assert_int_equal(n, 1);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    s = prepared("SELECT /* really? */ ? ::int");
    assert_int_equal(SQLNumParams(s, &n), SQL_SUCCESS);
    assert_int_equal(n, 1);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/*
 * A statement the server cannot parse must not surface as an error from
 * SQLDescribeParam: the description is best-effort and falls back to the
 * generic answer, which is what every other backend gives.
 */
static void test_undescribable_statement_falls_back(void **state)
{
    (void)state;
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    SQLPrepare(s, (SQLCHAR *)"SELECT * FROM not_a_table_at_all WHERE x = ?",
               SQL_NTS);

    SQLSMALLINT dtype = 0, ddigits = 0, nullable = 0;
    SQLULEN size = 0;
    assert_int_equal(SQLDescribeParam(s, 1, &dtype, &size, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_int_equal(dtype, SQL_VARCHAR);
    assert_int_equal((int)size, 255);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    /* And the failed describe must not poison the next statement: libpq's
     * error state is sticky, and the ODBC layer fails any statement whose
     * backend reports an outstanding error. */
    s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(s, (SQLCHAR *)"SELECT 1", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(s), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

/* ── Statement-level SQLSTATEs ───────────────────────────────── */

/*
 * The server's own SQLSTATE reaches the application. Every other backend can
 * only report HY000, which is the one part of a diagnostic a BI tool can
 * actually branch on.
 */
static void test_server_sqlstate_reaches_the_application(void **state)
{
    (void)state;

    struct { const char *sql; const char *state; } cases[] = {
        /* undefined_table */
        { "SELECT * FROM no_such_relation_anywhere",            "42P01" },
        /* undefined_column */
        { "SELECT no_such_column FROM argus_test.customers",    "42703" },
        /* syntax_error */
        { "SELEC 1",                                            "42601" },
        /* division_by_zero */
        { "SELECT 1/0",                                         "22012" },
        /* unique_violation */
        { "INSERT INTO argus_test.customers VALUES (1, 'dup')", "23505" },
        /* invalid_text_representation */
        { "SELECT 'abc'::integer",                              "22P02" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        SQLHSTMT s = SQL_NULL_HSTMT;
        assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
        assert_int_equal(SQLExecDirect(s, (SQLCHAR *)cases[i].sql, SQL_NTS),
                         SQL_ERROR);

        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0; SQLSMALLINT len = 0;
        assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, st, &native,
                                       msg, sizeof(msg), &len), SQL_SUCCESS);
        if (strcmp((char *)st, cases[i].state) != 0)
            fail_msg("expected SQLSTATE %s, got %s (%s)\n  SQL: %s",
                     cases[i].state, st, msg, cases[i].sql);

        SQLFreeHandle(SQL_HANDLE_STMT, s);
    }
}

/* A cancelled query reports ODBC's own state, not PostgreSQL's 57014 — the
 * one place where passing the server code through would be wrong. */
static void test_cancellation_maps_to_hy008(void **state)
{
    (void)state;
    /* Provoke the server-side equivalent without racing a real cancel: a
     * statement_timeout produces the same 57014. */
    SQLHSTMT s = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    SQLExecDirect(s, (SQLCHAR *)"SET statement_timeout = 50", SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(s, (SQLCHAR *)"SELECT pg_sleep(5)", SQL_NTS),
                     SQL_ERROR);

    SQLCHAR st[6] = {0}, msg[512] = {0};
    SQLINTEGER native = 0; SQLSMALLINT len = 0;
    SQLGetDiagRec(SQL_HANDLE_STMT, s, 1, st, &native, msg, sizeof(msg), &len);
    assert_string_equal((char *)st, "HY008");
    SQLFreeHandle(SQL_HANDLE_STMT, s);

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, g_dbc, &s), SQL_SUCCESS);
    SQLExecDirect(s, (SQLCHAR *)"SET statement_timeout = 0", SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, s);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_describe_parameter_is_advertised),
        cmocka_unit_test(test_parameter_types_come_from_the_server),
        cmocka_unit_test(test_parameter_types_numeric_and_date),
        cmocka_unit_test(test_no_parameters),
        cmocka_unit_test(test_question_mark_in_literal_is_not_a_parameter),
        cmocka_unit_test(test_undescribable_statement_falls_back),
        cmocka_unit_test(test_server_sqlstate_reaches_the_application),
        cmocka_unit_test(test_cancellation_maps_to_hy008),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
