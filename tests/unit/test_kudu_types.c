#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>

/* Kudu type mapping functions (defined in kudu_types.c) */
SQLSMALLINT kudu_type_to_sql_type(int kudu_type);
SQLSMALLINT kudu_type_name_to_sql_type(const char *type_name);
SQLULEN     kudu_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT kudu_type_decimal_digits(SQLSMALLINT sql_type);
const char *kudu_type_id_to_name(int kudu_type);

/* Kudu type IDs (must match kudu_internal.h) */
#define KUDU_TYPE_INT8               0
#define KUDU_TYPE_INT16              1
#define KUDU_TYPE_INT32              2
#define KUDU_TYPE_INT64              3
#define KUDU_TYPE_STRING             4
#define KUDU_TYPE_BOOL               5
#define KUDU_TYPE_FLOAT              6
#define KUDU_TYPE_DOUBLE             7
#define KUDU_TYPE_BINARY             8
#define KUDU_TYPE_UNIXTIME_MICROS    9
#define KUDU_TYPE_DECIMAL            10
#define KUDU_TYPE_VARCHAR            11
#define KUDU_TYPE_DATE               12

/* ── Test: Integer type ID mapping ───────────────────────────── */

static void test_kudu_type_id_mapping(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_INT8),   SQL_TINYINT);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_INT16),  SQL_SMALLINT);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_INT32),  SQL_INTEGER);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_INT64),  SQL_BIGINT);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_FLOAT),  SQL_REAL);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_DOUBLE), SQL_DOUBLE);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_BOOL),   SQL_BIT);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_STRING), SQL_VARCHAR);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_BINARY), SQL_VARBINARY);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_UNIXTIME_MICROS),
                     SQL_TYPE_TIMESTAMP);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_DECIMAL), SQL_DECIMAL);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_VARCHAR), SQL_VARCHAR);
    assert_int_equal(kudu_type_to_sql_type(KUDU_TYPE_DATE),    SQL_TYPE_DATE);
}

/* ── Test: Unknown type ID defaults to VARCHAR ───────────────── */

static void test_kudu_type_id_unknown(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_to_sql_type(99),  SQL_VARCHAR);
    assert_int_equal(kudu_type_to_sql_type(-1),  SQL_VARCHAR);
}

/* ── Test: String name mapping ───────────────────────────────── */

static void test_kudu_type_name_mapping(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_name_to_sql_type("INT8"),    SQL_TINYINT);
    assert_int_equal(kudu_type_name_to_sql_type("INT16"),   SQL_SMALLINT);
    assert_int_equal(kudu_type_name_to_sql_type("INT32"),   SQL_INTEGER);
    assert_int_equal(kudu_type_name_to_sql_type("INT64"),   SQL_BIGINT);
    assert_int_equal(kudu_type_name_to_sql_type("STRING"),  SQL_VARCHAR);
    assert_int_equal(kudu_type_name_to_sql_type("BOOL"),    SQL_BIT);
    assert_int_equal(kudu_type_name_to_sql_type("FLOAT"),   SQL_REAL);
    assert_int_equal(kudu_type_name_to_sql_type("DOUBLE"),  SQL_DOUBLE);
    assert_int_equal(kudu_type_name_to_sql_type("BINARY"),  SQL_VARBINARY);
    assert_int_equal(kudu_type_name_to_sql_type("UNIXTIME_MICROS"),
                     SQL_TYPE_TIMESTAMP);
    assert_int_equal(kudu_type_name_to_sql_type("DECIMAL"), SQL_DECIMAL);
    assert_int_equal(kudu_type_name_to_sql_type("VARCHAR"), SQL_VARCHAR);
    assert_int_equal(kudu_type_name_to_sql_type("DATE"),    SQL_TYPE_DATE);
}

/* ── Test: Alias name mapping ────────────────────────────────── */

static void test_kudu_type_name_aliases(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_name_to_sql_type("TINYINT"),   SQL_TINYINT);
    assert_int_equal(kudu_type_name_to_sql_type("SMALLINT"),  SQL_SMALLINT);
    assert_int_equal(kudu_type_name_to_sql_type("INT"),       SQL_INTEGER);
    assert_int_equal(kudu_type_name_to_sql_type("INTEGER"),   SQL_INTEGER);
    assert_int_equal(kudu_type_name_to_sql_type("BIGINT"),    SQL_BIGINT);
    assert_int_equal(kudu_type_name_to_sql_type("BOOLEAN"),   SQL_BIT);
    assert_int_equal(kudu_type_name_to_sql_type("TIMESTAMP"), SQL_TYPE_TIMESTAMP);
}

/* ── Test: NULL and unknown name ─────────────────────────────── */

static void test_kudu_type_name_unknown(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_name_to_sql_type(NULL),      SQL_VARCHAR);
    assert_int_equal(kudu_type_name_to_sql_type("unknown"), SQL_VARCHAR);
    assert_int_equal(kudu_type_name_to_sql_type(""),        SQL_VARCHAR);
}

/* ── Test: Column sizes ──────────────────────────────────────── */

static void test_kudu_column_sizes(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_column_size(SQL_BIT),            1);
    assert_int_equal(kudu_type_column_size(SQL_TINYINT),        3);
    assert_int_equal(kudu_type_column_size(SQL_SMALLINT),       5);
    assert_int_equal(kudu_type_column_size(SQL_INTEGER),        10);
    assert_int_equal(kudu_type_column_size(SQL_BIGINT),         19);
    assert_int_equal(kudu_type_column_size(SQL_REAL),           7);
    assert_int_equal(kudu_type_column_size(SQL_DOUBLE),         15);
    assert_int_equal(kudu_type_column_size(SQL_DECIMAL),        38);
    assert_int_equal(kudu_type_column_size(SQL_VARCHAR),        65535);
    assert_int_equal(kudu_type_column_size(SQL_VARBINARY),      65535);
    assert_int_equal(kudu_type_column_size(SQL_TYPE_DATE),      10);
    assert_int_equal(kudu_type_column_size(SQL_TYPE_TIMESTAMP), 29);
}

/* ── Test: Decimal digits ────────────────────────────────────── */

static void test_kudu_decimal_digits(void **state)
{
    (void)state;

    assert_int_equal(kudu_type_decimal_digits(SQL_REAL),           7);
    assert_int_equal(kudu_type_decimal_digits(SQL_DOUBLE),         15);
    assert_int_equal(kudu_type_decimal_digits(SQL_DECIMAL),        18);
    assert_int_equal(kudu_type_decimal_digits(SQL_TYPE_TIMESTAMP), 6);
    assert_int_equal(kudu_type_decimal_digits(SQL_INTEGER),        0);
    assert_int_equal(kudu_type_decimal_digits(SQL_VARCHAR),        0);
}

/* ── Test: Type ID to name ───────────────────────────────────── */

static void test_kudu_type_id_to_name(void **state)
{
    (void)state;

    assert_string_equal(kudu_type_id_to_name(KUDU_TYPE_INT8), "INT8");
    assert_string_equal(kudu_type_id_to_name(KUDU_TYPE_INT32), "INT32");
    assert_string_equal(kudu_type_id_to_name(KUDU_TYPE_STRING), "STRING");
    assert_string_equal(kudu_type_id_to_name(KUDU_TYPE_BOOL), "BOOL");
    assert_string_equal(kudu_type_id_to_name(KUDU_TYPE_UNIXTIME_MICROS),
                        "UNIXTIME_MICROS");
    assert_string_equal(kudu_type_id_to_name(99), "UNKNOWN");
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_kudu_type_id_mapping),
        cmocka_unit_test(test_kudu_type_id_unknown),
        cmocka_unit_test(test_kudu_type_name_mapping),
        cmocka_unit_test(test_kudu_type_name_aliases),
        cmocka_unit_test(test_kudu_type_name_unknown),
        cmocka_unit_test(test_kudu_column_sizes),
        cmocka_unit_test(test_kudu_decimal_digits),
        cmocka_unit_test(test_kudu_type_id_to_name),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
