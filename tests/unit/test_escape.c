/*
 * ODBC escape sequence translation.
 *
 * These are the sequences Tableau, Excel, Qlik and Alteryx actually emit once
 * SQLGetInfo tells them the driver handles them. Before this existed the driver
 * passed {fn UCASE(x)} straight to the server, which rejected it.
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <glib.h>
#include "argus/dialect.h"
#include "argus/error.h"

/* Translate with `backend`'s dialect, asserting success. Caller g_free()s. */
static char *xlat(const char *backend, const char *sql)
{
    argus_diag_t diag = {0};   /* stack diag: zero-init, records allocate lazily */
    char        *out = NULL;

    argus_diag_clear(&diag);
    argus_escape_result_t r =
        argus_escape_translate(argus_dialect_by_name(backend), sql, &out, &diag);

    assert_int_equal(r, ARGUS_ESCAPE_OK);
    assert_non_null(out);
    argus_diag_dispose(&diag);
    return out;
}

/* Translate and compare against the expected native SQL. */
static void assert_translates(const char *backend, const char *sql,
                              const char *expect)
{
    char *out = xlat(backend, sql);
    assert_string_equal(out, expect);
    g_free(out);
}

/* Translate expecting rejection; returns the SQLSTATE. */
static void assert_rejected(const char *backend, const char *sql,
                            const char *expect_sqlstate)
{
    argus_diag_t diag = {0};   /* stack diag: zero-init, records allocate lazily */
    char        *out = NULL;

    argus_diag_clear(&diag);
    argus_escape_result_t r =
        argus_escape_translate(argus_dialect_by_name(backend), sql, &out, &diag);

    assert_int_equal(r, ARGUS_ESCAPE_ERROR);
    assert_true(diag.count > 0);
    assert_string_equal((const char *)diag.records[0].sqlstate, expect_sqlstate);
    argus_diag_dispose(&diag);
}

/* Escape-free SQL must not even be copied. */
static void test_no_escape_is_left_alone(void **state)
{
    (void)state;

    argus_diag_t diag = {0};   /* stack diag: zero-init, records allocate lazily */
    char        *out = NULL;

    argus_diag_clear(&diag);
    assert_int_equal(argus_escape_translate(argus_dialect_by_name("trino"),
                                            "SELECT a FROM t WHERE b > 1",
                                            &out, &diag),
                     ARGUS_ESCAPE_NONE);
    assert_null(out);
    argus_diag_dispose(&diag);
}

/* The same escape must render differently per backend — that is the whole
 * reason the dialect is per-connection. */
static void test_scalar_function_is_dialect_specific(void **state)
{
    (void)state;

    char *trino = xlat("trino", "SELECT {fn UCASE(name)} FROM t");
    assert_string_equal(trino, "SELECT upper(name) FROM t");
    g_free(trino);

    /* druid is on the conservative ANSI fallback (its functions have not been
     * verified against a live server), which renders SQL-92 spellings. */
    char *ansi = xlat("druid", "SELECT {fn UCASE(name)} FROM t");
    assert_string_equal(ansi, "SELECT UPPER(name) FROM t");
    g_free(ansi);

    /* Pinot has its own probed table: same function, different rendering, and
     * a LOCATE that compensates for strpos() being 0-based. */
    char *pinot = xlat("pinot", "SELECT {fn UCASE(name)} FROM t");
    assert_string_equal(pinot, "SELECT upper(name) FROM t");
    g_free(pinot);

    char *pinot_loc = xlat("pinot", "SELECT {fn LOCATE('x', name)}");
    assert_string_equal(pinot_loc, "SELECT (strpos(name, 'x') + 1)");
    g_free(pinot_loc);

    /* ODBC's LOG is the natural log; Trino spells that ln(), MySQL log(). */
    char *t_log = xlat("trino", "SELECT {fn LOG(x)}");
    assert_string_equal(t_log, "SELECT ln(x)");
    g_free(t_log);

    char *m_log = xlat("mysql", "SELECT {fn LOG(x)}");
    assert_string_equal(m_log, "SELECT log(x)");
    g_free(m_log);
}

/* Templates must be able to reorder and rewrite, not just rename. */
static void test_template_rewrites(void **state)
{
    (void)state;

    /* ODBC LOCATE(needle, haystack); Trino strpos(haystack, needle). */
    char *loc = xlat("trino", "SELECT {fn LOCATE('x', name)}");
    assert_string_equal(loc, "SELECT strpos(name, 'x')");
    g_free(loc);

    /* Trino has no LEFT/RIGHT. */
    char *left = xlat("trino", "SELECT {fn LEFT(name, 3)}");
    assert_string_equal(left, "SELECT substr(name, 1, 3)");
    g_free(left);

    char *right = xlat("trino", "SELECT {fn RIGHT(name, 3)}");
    assert_string_equal(right, "SELECT substr(name, -(3))");
    g_free(right);

    /* ODBC DAYOFWEEK is 1=Sunday; Trino's day_of_week is 1=Monday. */
    char *dow = xlat("trino", "SELECT {fn DAYOFWEEK(d)}");
    assert_string_equal(dow, "SELECT ((day_of_week(d) % 7) + 1)");
    g_free(dow);

    /* Zero-argument functions, with and without a native call syntax. */
    char *now = xlat("trino", "SELECT {fn NOW()}");
    assert_string_equal(now, "SELECT current_timestamp");
    g_free(now);

    char *rand = xlat("trino", "SELECT {fn RAND()}");
    assert_string_equal(rand, "SELECT random()");
    g_free(rand);
}

/* Variadic and optional arguments go through $*. */
static void test_variadic_and_optional_args(void **state)
{
    (void)state;

    char *cat = xlat("trino", "SELECT {fn CONCAT(a, b, c)}");
    assert_string_equal(cat, "SELECT concat(a, b, c)");
    g_free(cat);

    char *r1 = xlat("trino", "SELECT {fn ROUND(x)}");
    assert_string_equal(r1, "SELECT round(x)");
    g_free(r1);

    char *r2 = xlat("trino", "SELECT {fn ROUND(x, 2)}");
    assert_string_equal(r2, "SELECT round(x, 2)");
    g_free(r2);
}

static void test_nested_escapes(void **state)
{
    (void)state;

    char *out = xlat("trino", "SELECT {fn UCASE({fn LTRIM(name)})} FROM t");
    assert_string_equal(out, "SELECT upper(ltrim(name)) FROM t");
    g_free(out);

    /* A comma inside a nested call must not split the outer argument list. */
    char *out2 = xlat("trino", "SELECT {fn UCASE({fn SUBSTRING(a, 1, 2)})}");
    assert_string_equal(out2, "SELECT upper(substr(a, 1, 2))");
    g_free(out2);
}

static void test_datetime_literals(void **state)
{
    (void)state;

    char *d = xlat("trino", "WHERE d = {d '2024-01-31'}");
    assert_string_equal(d, "WHERE d = DATE '2024-01-31'");
    g_free(d);

    char *ts = xlat("trino", "WHERE t = {ts '2024-01-31 12:00:00'}");
    assert_string_equal(ts, "WHERE t = TIMESTAMP '2024-01-31 12:00:00'");
    g_free(ts);

    char *t = xlat("hive", "WHERE t = {t '12:00:00'}");
    assert_string_equal(t, "WHERE t = TIME '12:00:00'");
    g_free(t);
}

static void test_like_escape_and_outer_join(void **state)
{
    (void)state;

    char *e = xlat("trino", "WHERE a LIKE '%x!%' {escape '!'}");
    assert_string_equal(e, "WHERE a LIKE '%x!%' ESCAPE '!'");
    g_free(e);

    char *oj = xlat("trino", "FROM {oj a LEFT OUTER JOIN b ON a.id = b.id}");
    assert_string_equal(oj, "FROM a LEFT OUTER JOIN b ON a.id = b.id");
    g_free(oj);

    /* Dialects that don't claim outer-join support must refuse it rather than
     * emit something the server will reject. */
    assert_rejected("pinot", "FROM {oj a LEFT OUTER JOIN b ON a.id = b.id}", "42000");
}

/* A brace inside a literal is data, not an escape. Getting this wrong would
 * silently corrupt queries. */
static void test_escapes_inside_literals_are_untouched(void **state)
{
    (void)state;

    char *out = xlat("trino", "SELECT '{fn UCASE(x)}', {fn UCASE(y)}");
    assert_string_equal(out, "SELECT '{fn UCASE(x)}', upper(y)");
    g_free(out);

    /* Doubled quotes keep the literal open. */
    char *out2 = xlat("trino", "SELECT 'it''s {d ''x''}', {fn LCASE(y)}");
    assert_string_equal(out2, "SELECT 'it''s {d ''x''}', lower(y)");
    g_free(out2);

    /* Quoted identifiers too. */
    char *out3 = xlat("trino", "SELECT \"{weird}\" FROM t");
    assert_string_equal(out3, "SELECT \"{weird}\" FROM t");
    g_free(out3);
}

/* The literal is read as the dialect lexes it: a backslash-escaped quote
 * keeps it open on Hive and MySQL wire (and so does E'...' on PostgreSQL),
 * a dollar-quoted string is one run, and a brace in a comment is not an
 * escape. On an ANSI engine the same backslash text closes the literal, and
 * the brace after it is an escape. */
static void test_literals_follow_the_dialect_lexer(void **state)
{
    (void)state;

    assert_translates("hive", "SELECT 'a\\'{fn UCASE(x)}', {fn LCASE(y)}",
                      "SELECT 'a\\'{fn UCASE(x)}', lower(y)");
    assert_translates("mysql", "SELECT \"a\\\"{x}\", `{y}`, {fn LCASE(y)}",
                      "SELECT \"a\\\"{x}\", `{y}`, lower(y)");
    /* ANSI: the literal closed at \', so the brace is an escape and the
     * last quote opens a literal that never closes. */
    assert_rejected("trino", "SELECT 'a\\'{fn UCASE(x)}'", "42000");
    assert_translates("trino", "SELECT 'a\\'{fn UCASE(x)}",
                      "SELECT 'a\\'upper(x)");

    assert_translates("postgres", "SELECT E'a\\'{fn UCASE(x)}', {fn LCASE(y)}",
                      "SELECT E'a\\'{fn UCASE(x)}', lower(y)");
    assert_translates("postgres",
                      "SELECT $$ {fn UCASE(x)} $$, $t$'{y}'$t$, {fn LCASE(y)}",
                      "SELECT $$ {fn UCASE(x)} $$, $t$'{y}'$t$, lower(y)");
    assert_rejected("postgres", "SELECT $$ {fn UCASE(x)}", "42000");

    assert_translates("trino", "SELECT 1 -- {fn UCASE(x)}\n, /* {d '1'} */ {fn LCASE(y)}",
                      "SELECT 1 -- {fn UCASE(x)}\n, /* {d '1'} */ lower(y)");
    /* Inside an escape too. */
    assert_translates("trino", "SELECT {fn LCASE(y /* {x} */)}",
                      "SELECT lower(y /* {x} */)");
}

/*
 * The contract that motivates this whole file: a function the dialect cannot
 * render is one SQLGetInfo never advertised, so it is an application error and
 * must be reported as such — never forwarded and left for the server to choke
 * on with an unhelpful message.
 */
static void test_unsupported_function_is_rejected_not_passed_through(void **state)
{
    (void)state;

    assert_rejected("trino", "SELECT {fn SOUNDEX(name)}", "42000");

    /* SPACE exists for Trino but not in the conservative ANSI set. */
    assert_rejected("druid", "SELECT {fn SPACE(4)}", "42000");

    /* Pinot has no random() and its date functions take epoch millis rather
     * than a temporal value, so neither is advertised and both must be
     * refused rather than sent for the server to reject. */
    assert_rejected("pinot", "SELECT {fn RAND()}", "42000");
    assert_rejected("pinot", "SELECT {fn NOW()}", "42000");

    /* Pinot's concat() reads a third argument as a separator, so the variadic
     * form is deliberately not offered — this must fail here, not silently
     * return "a-b" from the server. */
    assert_rejected("pinot", "SELECT {fn CONCAT(a, b, c)}", "42000");
}

static void test_arity_is_checked(void **state)
{
    (void)state;

    assert_rejected("trino", "SELECT {fn UCASE(a, b)}", "42000");
    assert_rejected("trino", "SELECT {fn UCASE()}", "42000");
    assert_rejected("trino", "SELECT {fn ROUND(a, b, c)}", "42000");  /* max 2 */
    assert_rejected("trino", "SELECT {fn CONCAT()}", "42000");        /* variadic, min 1 */
}

/* The diagnostic has to name the function and describe the real arity — a
 * variadic minimum must not read as "takes 1..-1 argument(s)". */
static void test_arity_message_is_readable(void **state)
{
    (void)state;

    argus_diag_t diag = {0};   /* stack diag: zero-init, records allocate lazily */
    char        *out = NULL;

    argus_diag_clear(&diag);
    argus_escape_translate(argus_dialect_by_name("trino"),
                           "SELECT {fn CONCAT()}", &out, &diag);
    assert_true(diag.count > 0);
    assert_non_null(strstr((const char *)diag.records[0].message, "CONCAT"));
    assert_non_null(strstr((const char *)diag.records[0].message, "at least 1"));
    assert_null(strstr((const char *)diag.records[0].message, "-1"));

    argus_diag_clear(&diag);
    argus_escape_translate(argus_dialect_by_name("trino"),
                           "SELECT {fn UCASE(a, b)}", &out, &diag);
    assert_true(diag.count > 0);
    assert_non_null(strstr((const char *)diag.records[0].message, "takes 1 argument"));
    argus_diag_dispose(&diag);
}

/*
 * {call ...} is translated where the dialect can express it and refused where
 * it cannot — and SQL_PROCEDURES is derived from the same field, so the two
 * cannot disagree. The engines with no procedures still get HYC00, which is
 * what they always got.
 */
static void test_procedure_calls_follow_the_dialect(void **state)
{
    (void)state;

    assert_rejected("trino", "{call foo(1)}", "HYC00");
    assert_rejected("trino", "{?= call foo(1)}", "HYC00");
    assert_rejected("hive",  "{call foo(1)}", "HYC00");

    /* PostgreSQL: what returns a result set is a function, so this is what an
     * ODBC application means by {call} — and what psqlODBC generates. */
    assert_translates("postgres", "{call order_count(1)}",
                      "SELECT * FROM order_count(1)");
    assert_translates("greenplum", "{call f(1, 'a')}",
                      "SELECT * FROM f(1, 'a')");

    /* The ?= form binds the function's return value, which this rendering
     * already produces, so the marker is consumed rather than refused. */
    assert_translates("postgres", "{?= call order_count(1)}",
                      "SELECT * FROM order_count(1)");

    /* A parameter marker and a nested escape inside the argument list must
     * survive; the body is copied through, not re-parsed as an expression. */
    assert_translates("postgres", "{call f(?)}", "SELECT * FROM f(?)");
    assert_translates("postgres", "{call f({fn UCASE('a')})}",
                      "SELECT * FROM f(upper('a'))");
}

static void test_malformed_escapes_are_rejected(void **state)
{
    (void)state;

    assert_rejected("trino", "SELECT {bogus 1}", "42000");
    assert_rejected("trino", "SELECT {fn UCASE(a}", "42000");
    assert_rejected("trino", "SELECT {d 2024-01-01}", "42000");

    /* An unterminated literal swallows the brace, so no escape can be parsed
     * out of what follows. */
    assert_rejected("trino", "SELECT 'unterminated {fn UCASE(x)}", "42000");
}

/*
 * The translator only judges what it was asked to translate. SQL with no escape
 * in it is passed through untouched even when it is plainly broken — diagnosing
 * that is the server's job, not this file's.
 */
static void test_translator_does_not_validate_sql(void **state)
{
    (void)state;

    argus_diag_t diag = {0};   /* stack diag: zero-init, records allocate lazily */
    char        *out = NULL;

    argus_diag_clear(&diag);
    assert_int_equal(argus_escape_translate(argus_dialect_by_name("trino"),
                                            "SELECT 'unterminated", &out, &diag),
                     ARGUS_ESCAPE_NONE);
    assert_null(out);

    argus_diag_clear(&diag);
    assert_int_equal(argus_escape_translate(argus_dialect_by_name("trino"),
                                            "THIS IS NOT SQL AT ALL", &out, &diag),
                     ARGUS_ESCAPE_NONE);
    assert_null(out);
    argus_diag_dispose(&diag);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_no_escape_is_left_alone),
        cmocka_unit_test(test_scalar_function_is_dialect_specific),
        cmocka_unit_test(test_template_rewrites),
        cmocka_unit_test(test_variadic_and_optional_args),
        cmocka_unit_test(test_nested_escapes),
        cmocka_unit_test(test_datetime_literals),
        cmocka_unit_test(test_like_escape_and_outer_join),
        cmocka_unit_test(test_escapes_inside_literals_are_untouched),
        cmocka_unit_test(test_literals_follow_the_dialect_lexer),
        cmocka_unit_test(test_unsupported_function_is_rejected_not_passed_through),
        cmocka_unit_test(test_arity_is_checked),
        cmocka_unit_test(test_arity_message_is_readable),
        cmocka_unit_test(test_procedure_calls_follow_the_dialect),
        cmocka_unit_test(test_malformed_escapes_are_rejected),
        cmocka_unit_test(test_translator_does_not_validate_sql),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
