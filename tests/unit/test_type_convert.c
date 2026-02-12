#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>

/* We test the type mapping functions declared in hive_types.c.
 * Since they're linked into the shared library, we declare them here. */
extern SQLSMALLINT hive_type_to_sql_type(const char *hive_type);
extern SQLULEN     hive_type_column_size(SQLSMALLINT sql_type);
extern SQLSMALLINT hive_type_decimal_digits(SQLSMALLINT sql_type);

/* ── Test: Primitive Hive types ──────────────────────────────── */

static void test_primitive_types(void **state)
{
    (void)state;

    assert_int_equal(hive_type_to_sql_type("BOOLEAN"),   SQL_BIT);
    assert_int_equal(hive_type_to_sql_type("TINYINT"),   SQL_TINYINT);
    assert_int_equal(hive_type_to_sql_type("SMALLINT"),  SQL_SMALLINT);
    assert_int_equal(hive_type_to_sql_type("INT"),        SQL_INTEGER);
    assert_int_equal(hive_type_to_sql_type("INTEGER"),    SQL_INTEGER);
    assert_int_equal(hive_type_to_sql_type("BIGINT"),     SQL_BIGINT);
    assert_int_equal(hive_type_to_sql_type("FLOAT"),      SQL_FLOAT);
    assert_int_equal(hive_type_to_sql_type("DOUBLE"),     SQL_DOUBLE);
    assert_int_equal(hive_type_to_sql_type("STRING"),     SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type("VARCHAR"),    SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type("CHAR"),       SQL_CHAR);
    assert_int_equal(hive_type_to_sql_type("TIMESTAMP"),  SQL_TYPE_TIMESTAMP);
    assert_int_equal(hive_type_to_sql_type("DATE"),       SQL_TYPE_DATE);
    assert_int_equal(hive_type_to_sql_type("BINARY"),     SQL_BINARY);
    assert_int_equal(hive_type_to_sql_type("DECIMAL"),    SQL_DECIMAL);
}

/* ── Test: Case insensitive matching ─────────────────────────── */

static void test_case_insensitive(void **state)
{
    (void)state;

    assert_int_equal(hive_type_to_sql_type("boolean"),   SQL_BIT);
    assert_int_equal(hive_type_to_sql_type("Boolean"),   SQL_BIT);
    assert_int_equal(hive_type_to_sql_type("int"),        SQL_INTEGER);
    assert_int_equal(hive_type_to_sql_type("String"),     SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type("timestamp"),  SQL_TYPE_TIMESTAMP);
}

/* ── Test: Complex types map to VARCHAR ──────────────────────── */

static void test_complex_types(void **state)
{
    (void)state;

    assert_int_equal(hive_type_to_sql_type("ARRAY<INT>"),     SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type("MAP<STRING,INT>"), SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type("STRUCT<a:INT>"),   SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type("UNIONTYPE<INT,STRING>"), SQL_VARCHAR);
}

/* ── Test: NULL type defaults to VARCHAR ─────────────────────── */

static void test_null_type(void **state)
{
    (void)state;

    assert_int_equal(hive_type_to_sql_type(NULL), SQL_VARCHAR);
}

/* ── Test: Unknown type defaults to VARCHAR ──────────────────── */

static void test_unknown_type(void **state)
{
    (void)state;

    assert_int_equal(hive_type_to_sql_type("UNKNOWN_TYPE"), SQL_VARCHAR);
    assert_int_equal(hive_type_to_sql_type(""), SQL_VARCHAR);
}

/* ── Test: Column sizes ──────────────────────────────────────── */

static void test_column_sizes(void **state)
{
    (void)state;

    assert_int_equal(hive_type_column_size(SQL_BIT), 1);
    assert_int_equal(hive_type_column_size(SQL_TINYINT), 3);
    assert_int_equal(hive_type_column_size(SQL_SMALLINT), 5);
    assert_int_equal(hive_type_column_size(SQL_INTEGER), 10);
    assert_int_equal(hive_type_column_size(SQL_BIGINT), 19);
    assert_int_equal(hive_type_column_size(SQL_FLOAT), 7);
    assert_int_equal(hive_type_column_size(SQL_DOUBLE), 15);
    assert_int_equal(hive_type_column_size(SQL_DECIMAL), 38);
    assert_int_equal(hive_type_column_size(SQL_VARCHAR), 65535);
    assert_int_equal(hive_type_column_size(SQL_TYPE_DATE), 10);
    assert_int_equal(hive_type_column_size(SQL_TYPE_TIMESTAMP), 29);
}

/* ── Test: Decimal digits ────────────────────────────────────── */

static void test_decimal_digits(void **state)
{
    (void)state;

    assert_int_equal(hive_type_decimal_digits(SQL_INTEGER), 0);
    assert_int_equal(hive_type_decimal_digits(SQL_BIGINT), 0);
    assert_int_equal(hive_type_decimal_digits(SQL_VARCHAR), 0);
    assert_int_equal(hive_type_decimal_digits(SQL_FLOAT), 7);
    assert_int_equal(hive_type_decimal_digits(SQL_DOUBLE), 15);
    assert_int_equal(hive_type_decimal_digits(SQL_DECIMAL), 18);
    assert_int_equal(hive_type_decimal_digits(SQL_TYPE_TIMESTAMP), 9);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_primitive_types),
        cmocka_unit_test(test_case_insensitive),
        cmocka_unit_test(test_complex_types),
        cmocka_unit_test(test_null_type),
        cmocka_unit_test(test_unknown_type),
        cmocka_unit_test(test_column_sizes),
        cmocka_unit_test(test_decimal_digits),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
