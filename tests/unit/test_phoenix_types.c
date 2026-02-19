#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>

/* Phoenix type mapping functions (defined in phoenix_types.c) */
SQLSMALLINT phoenix_type_to_sql_type(const char *phoenix_type);
SQLULEN     phoenix_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT phoenix_type_decimal_digits(SQLSMALLINT sql_type);

/* ── Test: Basic type mapping ────────────────────────────────── */

static void test_phoenix_basic_types(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_to_sql_type("BOOLEAN"),   SQL_BIT);
    assert_int_equal(phoenix_type_to_sql_type("TINYINT"),   SQL_TINYINT);
    assert_int_equal(phoenix_type_to_sql_type("SMALLINT"),  SQL_SMALLINT);
    assert_int_equal(phoenix_type_to_sql_type("INTEGER"),   SQL_INTEGER);
    assert_int_equal(phoenix_type_to_sql_type("INT"),       SQL_INTEGER);
    assert_int_equal(phoenix_type_to_sql_type("BIGINT"),    SQL_BIGINT);
    assert_int_equal(phoenix_type_to_sql_type("FLOAT"),     SQL_REAL);
    assert_int_equal(phoenix_type_to_sql_type("REAL"),      SQL_REAL);
    assert_int_equal(phoenix_type_to_sql_type("DOUBLE"),    SQL_DOUBLE);
    assert_int_equal(phoenix_type_to_sql_type("VARCHAR"),   SQL_VARCHAR);
    assert_int_equal(phoenix_type_to_sql_type("CHAR"),      SQL_CHAR);
    assert_int_equal(phoenix_type_to_sql_type("VARBINARY"), SQL_VARBINARY);
    assert_int_equal(phoenix_type_to_sql_type("BINARY"),    SQL_BINARY);
    assert_int_equal(phoenix_type_to_sql_type("DATE"),      SQL_TYPE_DATE);
    assert_int_equal(phoenix_type_to_sql_type("TIMESTAMP"), SQL_TYPE_TIMESTAMP);
    assert_int_equal(phoenix_type_to_sql_type("DECIMAL"),   SQL_DECIMAL);
    assert_int_equal(phoenix_type_to_sql_type("NUMERIC"),   SQL_DECIMAL);
}

/* ── Test: Case insensitivity ────────────────────────────────── */

static void test_phoenix_case_insensitive(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_to_sql_type("boolean"),   SQL_BIT);
    assert_int_equal(phoenix_type_to_sql_type("Boolean"),   SQL_BIT);
    assert_int_equal(phoenix_type_to_sql_type("integer"),   SQL_INTEGER);
    assert_int_equal(phoenix_type_to_sql_type("bigint"),    SQL_BIGINT);
    assert_int_equal(phoenix_type_to_sql_type("varchar"),   SQL_VARCHAR);
}

/* ── Test: Phoenix unsigned types ────────────────────────────── */

static void test_phoenix_unsigned_types(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_TINYINT"),   SQL_TINYINT);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_SMALLINT"),  SQL_SMALLINT);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_INT"),       SQL_INTEGER);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_LONG"),      SQL_BIGINT);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_FLOAT"),     SQL_REAL);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_DOUBLE"),    SQL_DOUBLE);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_DATE"),      SQL_TYPE_DATE);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_TIME"),      SQL_TYPE_TIMESTAMP);
    assert_int_equal(phoenix_type_to_sql_type("UNSIGNED_TIMESTAMP"), SQL_TYPE_TIMESTAMP);
}

/* ── Test: Parameterized types ───────────────────────────────── */

static void test_phoenix_parameterized_types(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_to_sql_type("VARCHAR(255)"),    SQL_VARCHAR);
    assert_int_equal(phoenix_type_to_sql_type("CHAR(10)"),        SQL_CHAR);
    assert_int_equal(phoenix_type_to_sql_type("DECIMAL(18,6)"),   SQL_DECIMAL);
    assert_int_equal(phoenix_type_to_sql_type("TIMESTAMP(3)"),    SQL_TYPE_TIMESTAMP);
    assert_int_equal(phoenix_type_to_sql_type("VARBINARY(1024)"), SQL_VARBINARY);
    assert_int_equal(phoenix_type_to_sql_type("BINARY(16)"),      SQL_BINARY);
}

/* ── Test: NULL and unknown types ────────────────────────────── */

static void test_phoenix_null_and_unknown(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_to_sql_type(NULL),        SQL_VARCHAR);
    assert_int_equal(phoenix_type_to_sql_type("unknown"),   SQL_VARCHAR);
    assert_int_equal(phoenix_type_to_sql_type(""),          SQL_VARCHAR);
}

/* ── Test: Column sizes ──────────────────────────────────────── */

static void test_phoenix_column_sizes(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_column_size(SQL_BIT),            1);
    assert_int_equal(phoenix_type_column_size(SQL_TINYINT),        3);
    assert_int_equal(phoenix_type_column_size(SQL_SMALLINT),       5);
    assert_int_equal(phoenix_type_column_size(SQL_INTEGER),        10);
    assert_int_equal(phoenix_type_column_size(SQL_BIGINT),         19);
    assert_int_equal(phoenix_type_column_size(SQL_REAL),           7);
    assert_int_equal(phoenix_type_column_size(SQL_DOUBLE),         15);
    assert_int_equal(phoenix_type_column_size(SQL_DECIMAL),        38);
    assert_int_equal(phoenix_type_column_size(SQL_VARCHAR),        65535);
    assert_int_equal(phoenix_type_column_size(SQL_VARBINARY),      65535);
    assert_int_equal(phoenix_type_column_size(SQL_BINARY),         65535);
    assert_int_equal(phoenix_type_column_size(SQL_TYPE_DATE),      10);
    assert_int_equal(phoenix_type_column_size(SQL_TYPE_TIMESTAMP), 29);
}

/* ── Test: Decimal digits ────────────────────────────────────── */

static void test_phoenix_decimal_digits(void **state)
{
    (void)state;

    assert_int_equal(phoenix_type_decimal_digits(SQL_REAL),           7);
    assert_int_equal(phoenix_type_decimal_digits(SQL_DOUBLE),         15);
    assert_int_equal(phoenix_type_decimal_digits(SQL_DECIMAL),        18);
    assert_int_equal(phoenix_type_decimal_digits(SQL_TYPE_TIMESTAMP), 9);
    assert_int_equal(phoenix_type_decimal_digits(SQL_INTEGER),        0);
    assert_int_equal(phoenix_type_decimal_digits(SQL_VARCHAR),        0);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_phoenix_basic_types),
        cmocka_unit_test(test_phoenix_case_insensitive),
        cmocka_unit_test(test_phoenix_unsigned_types),
        cmocka_unit_test(test_phoenix_parameterized_types),
        cmocka_unit_test(test_phoenix_null_and_unknown),
        cmocka_unit_test(test_phoenix_column_sizes),
        cmocka_unit_test(test_phoenix_decimal_digits),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
