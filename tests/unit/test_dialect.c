/*
 * Dialect table invariants.
 *
 * The point of the table is that SQLGetInfo's scalar-function bitmaps and the
 * escape translator read the same data, so the driver cannot advertise a
 * function it has no rendering for. These tests pin that property down, plus
 * the per-backend facts that BI tools depend on (quote character above all).
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/dialect.h"

/* Every entry must be renderable and self-consistent. This is the guard that
 * makes over-claiming in SQLGetInfo structurally impossible: the bitmap is
 * derived from these same entries. */
static void test_fn_map_entries_are_wellformed(void **state)
{
    (void)state;

    for (size_t i = 0; i < argus_dialect_count(); i++) {
        const argus_dialect_t *d = argus_dialect_at(i);

        assert_non_null(d->name);
        assert_non_null(d->quote_char);
        assert_non_null(d->fn_map);

        for (const argus_fn_entry_t *e = d->fn_map; e->odbc_name; e++) {
            assert_non_null(e->tmpl);
            assert_true(e->bit != 0);
            assert_true(e->min_args >= 0);
            if (e->max_args != ARGUS_FN_VARIADIC)
                assert_true(e->max_args >= e->min_args);

            /* Name lookup must find the very entry we are looking at, or the
             * translator would reject SQL that SQLGetInfo promised to accept. */
            assert_ptr_equal(argus_dialect_find_fn(d, e->odbc_name), e);
        }
    }
}

/* Whatever a bitmap advertises must be resolvable back to an entry. */
static void test_bitmaps_derive_from_fn_map(void **state)
{
    (void)state;

    const argus_fn_group_t groups[] = {
        ARGUS_FN_GROUP_STRING, ARGUS_FN_GROUP_NUMERIC,
        ARGUS_FN_GROUP_TIMEDATE, ARGUS_FN_GROUP_SYSTEM
    };

    for (size_t i = 0; i < argus_dialect_count(); i++) {
        const argus_dialect_t *d = argus_dialect_at(i);

        for (size_t g = 0; g < sizeof(groups) / sizeof(groups[0]); g++) {
            SQLUINTEGER mask = argus_dialect_fn_bitmap(d, groups[g]);
            SQLUINTEGER seen = 0;

            for (const argus_fn_entry_t *e = d->fn_map; e->odbc_name; e++) {
                if (e->group == groups[g]) seen |= e->bit;
            }
            assert_int_equal(mask, seen);
        }
    }
}

/* Case-insensitive lookup: applications spell {fn ucase(x)} either way. */
static void test_find_fn_is_case_insensitive(void **state)
{
    (void)state;

    const argus_dialect_t *trino = argus_dialect_by_name("trino");

    assert_non_null(argus_dialect_find_fn(trino, "UCASE"));
    assert_non_null(argus_dialect_find_fn(trino, "ucase"));
    assert_non_null(argus_dialect_find_fn(trino, "UcAsE"));
    assert_null(argus_dialect_find_fn(trino, "NO_SUCH_FUNCTION"));
}

/* A wrong quote char makes every generated query fail on the server; this is
 * the regression that already bit Power BI once. */
static void test_quote_chars(void **state)
{
    (void)state;

    assert_string_equal(argus_dialect_by_name("hive")->quote_char, "`");
    assert_string_equal(argus_dialect_by_name("impala")->quote_char, "`");
    assert_string_equal(argus_dialect_by_name("mysql")->quote_char, "`");
    assert_string_equal(argus_dialect_by_name("bigquery")->quote_char, "`");

    assert_string_equal(argus_dialect_by_name("trino")->quote_char, "\"");
    assert_string_equal(argus_dialect_by_name("phoenix")->quote_char, "\"");
    assert_string_equal(argus_dialect_by_name("pinot")->quote_char, "\"");
    assert_string_equal(argus_dialect_by_name("druid")->quote_char, "\"");
}

/* An unknown backend must degrade to the conservative ANSI dialect, never to
 * NULL and never to another backend's grammar. */
static void test_unknown_backend_falls_back_to_ansi(void **state)
{
    (void)state;

    const argus_dialect_t *d = argus_dialect_by_name("no-such-backend");
    assert_non_null(d);
    assert_string_equal(d->name, "ansi");

    d = argus_dialect_by_name(NULL);
    assert_non_null(d);
    assert_string_equal(d->name, "ansi");

    /* NULL dbc (not connected yet) is the same situation. */
    assert_string_equal(argus_dialect_for(NULL)->name, "ansi");
}

/* Backends whose function set has not been verified against a live server
 * advertise a short list rather than the 48 functions they used to claim and
 * could not translate. */
static void test_unverified_backends_underclaim(void **state)
{
    (void)state;

    const argus_dialect_t *trino = argus_dialect_by_name("trino");
    const argus_dialect_t *druid = argus_dialect_by_name("druid");

    SQLUINTEGER trino_str = argus_dialect_fn_bitmap(trino, ARGUS_FN_GROUP_STRING);
    SQLUINTEGER druid_str = argus_dialect_fn_bitmap(druid, ARGUS_FN_GROUP_STRING);

    assert_true(trino_str & SQL_FN_STR_UCASE);
    assert_true(druid_str & SQL_FN_STR_UCASE);

    /* Druid is on the ANSI fallback: a strict subset of what a verified
     * dialect claims. */
    assert_int_equal(druid_str & ~trino_str, 0);
    assert_true(trino_str != druid_str);

    /* Trino has no native LEFT/RIGHT but renders them via substr, so the bits
     * are honest; Druid's set is unverified, so it claims neither. */
    assert_true(trino_str & SQL_FN_STR_LEFT);
    assert_false(druid_str & SQL_FN_STR_LEFT);

    /* The ANSI fallback claims nothing about date/time: probing Pinot showed
     * even CURRENT_TIMESTAMP is not universal among Calcite-based engines. */
    assert_int_equal(argus_dialect_fn_bitmap(druid, ARGUS_FN_GROUP_TIMEDATE), 0);
}

/* Pinot's table was built from live probing, and the semantics it does NOT
 * share with its neighbours are the point of having a table per backend. */
static void test_pinot_dialect_reflects_probed_semantics(void **state)
{
    (void)state;

    const argus_dialect_t *pinot = argus_dialect_by_name("pinot");

    /* Pinot's concat() takes a separator as its third argument, so only the
     * 2-argument form is safe — a variadic mapping would silently mis-render. */
    const argus_fn_entry_t *concat = argus_dialect_find_fn(pinot, "CONCAT");
    assert_non_null(concat);
    assert_int_equal(concat->min_args, 2);
    assert_int_equal(concat->max_args, 2);

    /* round() is not decimal rounding in Pinot; roundDecimal() is. */
    const argus_fn_entry_t *round = argus_dialect_find_fn(pinot, "ROUND");
    assert_non_null(round);
    assert_non_null(strstr(round->tmpl, "roundDecimal"));

    /* strpos() is 0-based; ODBC's LOCATE is 1-based. */
    const argus_fn_entry_t *locate = argus_dialect_find_fn(pinot, "LOCATE");
    assert_non_null(locate);
    assert_non_null(strstr(locate->tmpl, "+ 1"));

    /* No date/time functions: Pinot's take epoch millis, not temporal values. */
    assert_int_equal(argus_dialect_fn_bitmap(pinot, ARGUS_FN_GROUP_TIMEDATE), 0);
    /* And no RAND: Pinot has no random(). */
    assert_false(argus_dialect_fn_bitmap(pinot, ARGUS_FN_GROUP_NUMERIC) & SQL_FN_NUM_RAND);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_fn_map_entries_are_wellformed),
        cmocka_unit_test(test_bitmaps_derive_from_fn_map),
        cmocka_unit_test(test_find_fn_is_case_insensitive),
        cmocka_unit_test(test_quote_chars),
        cmocka_unit_test(test_unknown_backend_falls_back_to_ansi),
        cmocka_unit_test(test_unverified_backends_underclaim),
        cmocka_unit_test(test_pinot_dialect_reflects_probed_semantics),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
