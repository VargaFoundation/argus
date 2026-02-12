#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>

/* Trino type mapping functions (defined in trino_types.c) */
SQLSMALLINT trino_type_to_sql_type(const char *trino_type);
SQLULEN     trino_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT trino_type_decimal_digits(SQLSMALLINT sql_type);

/* ── Test: Basic type mapping ────────────────────────────────── */

static void test_trino_basic_types(void **state)
{
    (void)state;

    assert_int_equal(trino_type_to_sql_type("boolean"),   SQL_BIT);
    assert_int_equal(trino_type_to_sql_type("tinyint"),   SQL_TINYINT);
    assert_int_equal(trino_type_to_sql_type("smallint"),  SQL_SMALLINT);
    assert_int_equal(trino_type_to_sql_type("integer"),   SQL_INTEGER);
    assert_int_equal(trino_type_to_sql_type("int"),       SQL_INTEGER);
    assert_int_equal(trino_type_to_sql_type("bigint"),    SQL_BIGINT);
    assert_int_equal(trino_type_to_sql_type("real"),      SQL_REAL);
    assert_int_equal(trino_type_to_sql_type("double"),    SQL_DOUBLE);
    assert_int_equal(trino_type_to_sql_type("varchar"),   SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("char"),      SQL_CHAR);
    assert_int_equal(trino_type_to_sql_type("varbinary"), SQL_VARBINARY);
    assert_int_equal(trino_type_to_sql_type("date"),      SQL_TYPE_DATE);
    assert_int_equal(trino_type_to_sql_type("timestamp"), SQL_TYPE_TIMESTAMP);
    assert_int_equal(trino_type_to_sql_type("decimal"),   SQL_DECIMAL);
}

/* ── Test: Case insensitivity ────────────────────────────────── */

static void test_trino_case_insensitive(void **state)
{
    (void)state;

    assert_int_equal(trino_type_to_sql_type("BOOLEAN"),   SQL_BIT);
    assert_int_equal(trino_type_to_sql_type("Boolean"),   SQL_BIT);
    assert_int_equal(trino_type_to_sql_type("INTEGER"),   SQL_INTEGER);
    assert_int_equal(trino_type_to_sql_type("BIGINT"),    SQL_BIGINT);
    assert_int_equal(trino_type_to_sql_type("VARCHAR"),   SQL_VARCHAR);
}

/* ── Test: Parameterized types ───────────────────────────────── */

static void test_trino_parameterized_types(void **state)
{
    (void)state;

    assert_int_equal(trino_type_to_sql_type("varchar(255)"),    SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("char(10)"),        SQL_CHAR);
    assert_int_equal(trino_type_to_sql_type("decimal(18,6)"),   SQL_DECIMAL);
    assert_int_equal(trino_type_to_sql_type("timestamp(3)"),    SQL_TYPE_TIMESTAMP);
    assert_int_equal(trino_type_to_sql_type("time(6)"),         SQL_TYPE_TIMESTAMP);
    assert_int_equal(trino_type_to_sql_type("varbinary(1024)"), SQL_VARBINARY);
}

/* ── Test: Trino-specific types ──────────────────────────────── */

static void test_trino_specific_types(void **state)
{
    (void)state;

    assert_int_equal(trino_type_to_sql_type("json"),       SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("uuid"),       SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("ipaddress"),  SQL_VARCHAR);
}

/* ── Test: Complex types map to VARCHAR ──────────────────────── */

static void test_trino_complex_types(void **state)
{
    (void)state;

    assert_int_equal(trino_type_to_sql_type("array(integer)"),               SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("map(varchar, integer)"),        SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("row(a integer, b varchar)"),    SQL_VARCHAR);
}

/* ── Test: NULL and unknown types ────────────────────────────── */

static void test_trino_null_and_unknown(void **state)
{
    (void)state;

    assert_int_equal(trino_type_to_sql_type(NULL),        SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type("unknown"),   SQL_VARCHAR);
    assert_int_equal(trino_type_to_sql_type(""),          SQL_VARCHAR);
}

/* ── Test: Column sizes ──────────────────────────────────────── */

static void test_trino_column_sizes(void **state)
{
    (void)state;

    assert_int_equal(trino_type_column_size(SQL_BIT),            1);
    assert_int_equal(trino_type_column_size(SQL_TINYINT),        3);
    assert_int_equal(trino_type_column_size(SQL_SMALLINT),       5);
    assert_int_equal(trino_type_column_size(SQL_INTEGER),        10);
    assert_int_equal(trino_type_column_size(SQL_BIGINT),         19);
    assert_int_equal(trino_type_column_size(SQL_REAL),           7);
    assert_int_equal(trino_type_column_size(SQL_DOUBLE),         15);
    assert_int_equal(trino_type_column_size(SQL_DECIMAL),        38);
    assert_int_equal(trino_type_column_size(SQL_VARCHAR),        65535);
    assert_int_equal(trino_type_column_size(SQL_VARBINARY),      65535);
    assert_int_equal(trino_type_column_size(SQL_TYPE_DATE),      10);
    assert_int_equal(trino_type_column_size(SQL_TYPE_TIMESTAMP), 29);
}

/* ── Test: Decimal digits ────────────────────────────────────── */

static void test_trino_decimal_digits(void **state)
{
    (void)state;

    assert_int_equal(trino_type_decimal_digits(SQL_REAL),           7);
    assert_int_equal(trino_type_decimal_digits(SQL_DOUBLE),         15);
    assert_int_equal(trino_type_decimal_digits(SQL_DECIMAL),        18);
    assert_int_equal(trino_type_decimal_digits(SQL_TYPE_TIMESTAMP), 9);
    assert_int_equal(trino_type_decimal_digits(SQL_INTEGER),        0);
    assert_int_equal(trino_type_decimal_digits(SQL_VARCHAR),        0);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_trino_basic_types),
        cmocka_unit_test(test_trino_case_insensitive),
        cmocka_unit_test(test_trino_parameterized_types),
        cmocka_unit_test(test_trino_specific_types),
        cmocka_unit_test(test_trino_complex_types),
        cmocka_unit_test(test_trino_null_and_unknown),
        cmocka_unit_test(test_trino_column_sizes),
        cmocka_unit_test(test_trino_decimal_digits),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
