/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Unit tests for Druid type mapping (src/backend/druid/druid_types.c).
 * Druid had zero test coverage; this exercises the pure type-name → ODBC
 * mapping and column-size table with no live broker.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "druid_internal.h"

SQLSMALLINT druid_type_to_sql_type(const char *t);
SQLULEN     druid_type_column_size(SQLSMALLINT sql_type);

static void test_druid_basic_types(void **state)
{
    (void)state;
    assert_int_equal(druid_type_to_sql_type("BOOLEAN"),   SQL_BIT);
    assert_int_equal(druid_type_to_sql_type("TINYINT"),   SQL_TINYINT);
    assert_int_equal(druid_type_to_sql_type("SMALLINT"),  SQL_SMALLINT);
    assert_int_equal(druid_type_to_sql_type("INTEGER"),   SQL_INTEGER);
    assert_int_equal(druid_type_to_sql_type("BIGINT"),    SQL_BIGINT);
    assert_int_equal(druid_type_to_sql_type("FLOAT"),     SQL_REAL);
    assert_int_equal(druid_type_to_sql_type("REAL"),      SQL_REAL);
    assert_int_equal(druid_type_to_sql_type("DOUBLE"),    SQL_DOUBLE);
    assert_int_equal(druid_type_to_sql_type("DECIMAL"),   SQL_DECIMAL);
    assert_int_equal(druid_type_to_sql_type("TIMESTAMP"), SQL_TYPE_TIMESTAMP);
    assert_int_equal(druid_type_to_sql_type("DATE"),      SQL_TYPE_DATE);
    assert_int_equal(druid_type_to_sql_type("CHAR"),      SQL_CHAR);
    assert_int_equal(druid_type_to_sql_type("VARCHAR"),   SQL_VARCHAR);
}

static void test_druid_varchar_prefix_and_fallback(void **state)
{
    (void)state;
    /* VARCHAR carries a length suffix in Druid metadata. */
    assert_int_equal(druid_type_to_sql_type("VARCHAR(255)"), SQL_VARCHAR);
    /* Complex/array/unknown types fall back to text; NULL is safe. */
    assert_int_equal(druid_type_to_sql_type("COMPLEX<json>"), SQL_VARCHAR);
    assert_int_equal(druid_type_to_sql_type("ARRAY<BIGINT>"), SQL_VARCHAR);
    assert_int_equal(druid_type_to_sql_type(NULL), SQL_VARCHAR);
    /* Case-sensitive: Druid reports upper-case, lower-case is not a keyword. */
    assert_int_equal(druid_type_to_sql_type("bigint"), SQL_VARCHAR);
}

static void test_druid_column_size(void **state)
{
    (void)state;
    assert_int_equal(druid_type_column_size(SQL_BIT), 1);
    assert_int_equal(druid_type_column_size(SQL_INTEGER), 10);
    assert_int_equal(druid_type_column_size(SQL_BIGINT), 19);
    assert_int_equal(druid_type_column_size(SQL_DOUBLE), 15);
    assert_int_equal(druid_type_column_size(SQL_DECIMAL), 38);
    assert_int_equal(druid_type_column_size(SQL_TYPE_TIMESTAMP), 23);
    assert_int_equal(druid_type_column_size(SQL_VARCHAR), 255);   /* default */
}

/* GET /status backs SQL_DBMS_VER; only its "version" is taken, and a
 * document without one leaves the driver saying "unknown" rather than
 * inventing something. */
static void test_druid_status_version(void **state)
{
    (void)state;
    char v[64];
    assert_true(druid_parse_status_version(
        "{\"version\":\"30.0.1\",\"modules\":[],\"memory\":{}}", v, sizeof(v)));
    assert_string_equal(v, "30.0.1");
    assert_false(druid_parse_status_version("{\"modules\":[]}", v, sizeof(v)));
    assert_false(druid_parse_status_version("{\"version\":\"\"}", v, sizeof(v)));
    assert_false(druid_parse_status_version("not json", v, sizeof(v)));
    assert_false(druid_parse_status_version("[\"30.0.1\"]", v, sizeof(v)));
    assert_false(druid_parse_status_version(NULL, v, sizeof(v)));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_druid_basic_types),
        cmocka_unit_test(test_druid_varchar_prefix_and_fallback),
        cmocka_unit_test(test_druid_column_size),
        cmocka_unit_test(test_druid_status_version),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
