#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>

#include "bigquery_internal.h"

/* ── Type mapping ────────────────────────────────────────────── */

static void test_type_mapping(void **state)
{
    (void)state;
    assert_int_equal(bq_type_to_sql_type("INT64"), SQL_BIGINT);
    assert_int_equal(bq_type_to_sql_type("INTEGER"), SQL_BIGINT);
    assert_int_equal(bq_type_to_sql_type("FLOAT64"), SQL_DOUBLE);
    assert_int_equal(bq_type_to_sql_type("FLOAT"), SQL_DOUBLE);
    assert_int_equal(bq_type_to_sql_type("BOOL"), SQL_BIT);
    assert_int_equal(bq_type_to_sql_type("BOOLEAN"), SQL_BIT);
    assert_int_equal(bq_type_to_sql_type("NUMERIC"), SQL_DECIMAL);
    assert_int_equal(bq_type_to_sql_type("BIGNUMERIC"), SQL_DECIMAL);
    assert_int_equal(bq_type_to_sql_type("TIMESTAMP"), SQL_TYPE_TIMESTAMP);
    assert_int_equal(bq_type_to_sql_type("DATETIME"), SQL_TYPE_TIMESTAMP);
    assert_int_equal(bq_type_to_sql_type("DATE"), SQL_TYPE_DATE);
    assert_int_equal(bq_type_to_sql_type("TIME"), SQL_TYPE_TIME);
    assert_int_equal(bq_type_to_sql_type("STRING"), SQL_VARCHAR);
    assert_int_equal(bq_type_to_sql_type("BYTES"), SQL_VARCHAR);
    assert_int_equal(bq_type_to_sql_type("GEOGRAPHY"), SQL_VARCHAR);
    assert_int_equal(bq_type_to_sql_type("JSON"), SQL_VARCHAR);
    assert_int_equal(bq_type_to_sql_type(NULL), SQL_VARCHAR);
}

static void test_type_sizes(void **state)
{
    (void)state;
    assert_int_equal(bq_type_column_size("INT64"), 19);
    assert_int_equal(bq_type_column_size("DATE"), 10);
    assert_int_equal(bq_type_column_size("TIMESTAMP"), 26);
    assert_int_equal(bq_type_column_size("NUMERIC"), 38);
    assert_int_equal(bq_type_column_size("BIGNUMERIC"), 76);
    assert_int_equal(bq_type_decimal_digits("NUMERIC"), 9);
    assert_int_equal(bq_type_decimal_digits("BIGNUMERIC"), 38);
    assert_int_equal(bq_type_decimal_digits("TIMESTAMP"), 6);
    assert_int_equal(bq_type_decimal_digits("INT64"), 0);
}

/* ── Cell conversion ─────────────────────────────────────────── */

static void test_cell_int64_native(void **state)
{
    (void)state;
    argus_cell_t cell;
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "INT64", "42");
    assert_int_equal(cell.native_kind, ARGUS_NATIVE_I64);
    assert_true(cell.native.i64 == 42);
    assert_false(cell.is_null);
}

static void test_cell_float_native(void **state)
{
    (void)state;
    argus_cell_t cell;
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "FLOAT64", "3.25");
    assert_int_equal(cell.native_kind, ARGUS_NATIVE_F64);
    assert_true(cell.native.f64 > 3.24 && cell.native.f64 < 3.26);
}

static void test_cell_bool(void **state)
{
    (void)state;
    argus_cell_t cell;
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "BOOL", "true");
    assert_string_equal(cell.data, "1");
    free(cell.data);

    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "BOOL", "false");
    assert_string_equal(cell.data, "0");
    free(cell.data);
}

static void test_cell_timestamp_epoch(void **state)
{
    (void)state;
    argus_cell_t cell;

    /* 1718000000 = 2024-06-10T06:13:20Z */
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "TIMESTAMP", "1718000000.5");
    assert_string_equal(cell.data, "2024-06-10 06:13:20.500000");
    free(cell.data);

    /* Scientific notation, as the REST API actually sends it */
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "TIMESTAMP", "1.718E9");
    assert_string_equal(cell.data, "2024-06-10 06:13:20");
    free(cell.data);
}

static void test_cell_datetime(void **state)
{
    (void)state;
    argus_cell_t cell;
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "DATETIME", "2024-06-10T12:34:56.789");
    assert_string_equal(cell.data, "2024-06-10 12:34:56.789");
    free(cell.data);
}

static void test_cell_null(void **state)
{
    (void)state;
    argus_cell_t cell;
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "STRING", NULL);
    assert_true(cell.is_null);
}

static void test_cell_string_passthrough(void **state)
{
    (void)state;
    argus_cell_t cell;
    memset(&cell, 0, sizeof(cell));
    bq_fill_cell(&cell, "STRING", "hello");
    assert_string_equal(cell.data, "hello");
    assert_int_equal(cell.data_len, 5);
    free(cell.data);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_type_mapping),
        cmocka_unit_test(test_type_sizes),
        cmocka_unit_test(test_cell_int64_native),
        cmocka_unit_test(test_cell_float_native),
        cmocka_unit_test(test_cell_bool),
        cmocka_unit_test(test_cell_timestamp_epoch),
        cmocka_unit_test(test_cell_datetime),
        cmocka_unit_test(test_cell_null),
        cmocka_unit_test(test_cell_string_passthrough),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
