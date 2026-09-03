/*
 * argus_sql_quote_literal / argus_sql_quote_ident: the one way a value
 * reaches query text (parameters and catalog search patterns alike).
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include "argus/types.h"

static void assert_quoted(const char *value, bool backslash,
                          const char *expected)
{
    char *out = argus_sql_quote_literal(value, backslash);
    assert_non_null(out);
    assert_string_equal(out, expected);
    free(out);
}

static void test_literal_doubles_quotes(void **state)
{
    (void)state;
    assert_quoted("abc", false, "'abc'");
    assert_quoted("", false, "''");
    assert_quoted("O'Brien", false, "'O''Brien'");
    assert_quoted("'", false, "''''");
    /* The classic break-out: closing quote, injected clause, comment. */
    assert_quoted("x' OR 1=1 --", false, "'x'' OR 1=1 --'");
    /* Percent and underscore are LIKE metacharacters, not literal ones. */
    assert_quoted("t%_", false, "'t%_'");
    /* UTF-8 passes through untouched. */
    assert_quoted("caf\xc3\xa9", false, "'caf\xc3\xa9'");
}

static void test_literal_backslash_by_dialect(void **state)
{
    (void)state;
    /* ANSI engines (Trino, Druid, PostgreSQL): a backslash is a character. */
    assert_quoted("a\\'b", false, "'a\\''b'");
    /* Backslash-escape engines (Hive, MySQL wire, BigQuery): doubled, so
     * "\'" cannot turn the doubled quote back into a terminator. */
    assert_quoted("a\\'b", true, "'a\\\\''b'");
    assert_quoted("C:\\dir", true, "'C:\\\\dir'");
}

static void test_literal_rejects_embedded_nul(void **state)
{
    (void)state;
    assert_null(argus_sql_quote_literal_n("ab\0cd", 5, false));
    char *out = argus_sql_quote_literal_n("ab\0cd", 2, false);
    assert_non_null(out);
    assert_string_equal(out, "'ab'");
    free(out);
    assert_null(argus_sql_quote_literal(NULL, false));
    assert_null(argus_sql_quote_literal_n(NULL, 0, false));
}

static void test_ident_doubles_delimiter(void **state)
{
    (void)state;
    char *out = argus_sql_quote_ident("orders", '"');
    assert_string_equal(out, "\"orders\"");
    free(out);
    out = argus_sql_quote_ident("we\"ird", '"');
    assert_string_equal(out, "\"we\"\"ird\"");
    free(out);
    /* A single quote inside an identifier is not special. */
    out = argus_sql_quote_ident("it's", '"');
    assert_string_equal(out, "\"it's\"");
    free(out);
    out = argus_sql_quote_ident("a`b", '`');
    assert_string_equal(out, "`a``b`");
    free(out);
    assert_null(argus_sql_quote_ident(NULL, '"'));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_literal_doubles_quotes),
        cmocka_unit_test(test_literal_backslash_by_dialect),
        cmocka_unit_test(test_literal_rejects_embedded_nul),
        cmocka_unit_test(test_ident_doubles_delimiter),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
