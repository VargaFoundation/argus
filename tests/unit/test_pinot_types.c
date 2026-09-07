/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Unit tests for Pinot type mapping (src/backend/pinot/pinot_types.c).
 * Pinot's columnDataType strings → ODBC SQL types; no live broker needed.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "pinot_internal.h"

SQLSMALLINT pinot_type_to_sql_type(const char *t);
SQLULEN     pinot_type_column_size(SQLSMALLINT sql_type);

static void test_pinot_basic_types(void **state)
{
    (void)state;
    assert_int_equal(pinot_type_to_sql_type("INT"),         SQL_INTEGER);
    assert_int_equal(pinot_type_to_sql_type("LONG"),        SQL_BIGINT);
    assert_int_equal(pinot_type_to_sql_type("FLOAT"),       SQL_REAL);
    assert_int_equal(pinot_type_to_sql_type("DOUBLE"),      SQL_DOUBLE);
    assert_int_equal(pinot_type_to_sql_type("BIG_DECIMAL"), SQL_DECIMAL);
    assert_int_equal(pinot_type_to_sql_type("BOOLEAN"),     SQL_BIT);
    assert_int_equal(pinot_type_to_sql_type("TIMESTAMP"),   SQL_TYPE_TIMESTAMP);
    assert_int_equal(pinot_type_to_sql_type("STRING"),      SQL_VARCHAR);
    assert_int_equal(pinot_type_to_sql_type("JSON"),        SQL_LONGVARCHAR);
    assert_int_equal(pinot_type_to_sql_type("BYTES"),       SQL_VARBINARY);
}

static void test_pinot_multivalue_and_fallback(void **state)
{
    (void)state;
    /* Multi-value columns are surfaced as their JSON text form. */
    assert_int_equal(pinot_type_to_sql_type("INT_ARRAY"),    SQL_VARCHAR);
    assert_int_equal(pinot_type_to_sql_type("STRING_ARRAY"), SQL_VARCHAR);
    assert_int_equal(pinot_type_to_sql_type("UNKNOWN"),      SQL_VARCHAR);
    assert_int_equal(pinot_type_to_sql_type(NULL),           SQL_VARCHAR);
}

static void test_pinot_column_size(void **state)
{
    (void)state;
    assert_int_equal(pinot_type_column_size(SQL_BIT), 1);
    assert_int_equal(pinot_type_column_size(SQL_INTEGER), 10);
    assert_int_equal(pinot_type_column_size(SQL_BIGINT), 19);
    assert_int_equal(pinot_type_column_size(SQL_DOUBLE), 15);
    assert_int_equal(pinot_type_column_size(SQL_DECIMAL), 38);
    assert_int_equal(pinot_type_column_size(SQL_TYPE_TIMESTAMP), 19);
    assert_int_equal(pinot_type_column_size(SQL_VARCHAR), 255);   /* default */
}

/* The controller's GET /version is a map of component to version; the
 * broker's entry is preferred, any version-looking value is accepted, and a
 * document with none leaves SQL_DBMS_VER "unknown". */
static void test_pinot_version_document(void **state)
{
    (void)state;
    char v[64];
    assert_true(pinot_parse_version_json(
        "{\"pinot-common\":\"1.2.0\",\"pinot-broker\":\"1.2.0-SNAPSHOT\"}",
        v, sizeof(v)));
    assert_string_equal(v, "1.2.0-SNAPSHOT");
    assert_true(pinot_parse_version_json("{\"something-else\":\"0.12.1\"}",
                                         v, sizeof(v)));
    assert_string_equal(v, "0.12.1");
    assert_false(pinot_parse_version_json("{\"pinot-broker\":\"unknown\"}",
                                          v, sizeof(v)));
    assert_false(pinot_parse_version_json("{}", v, sizeof(v)));
    assert_false(pinot_parse_version_json("garbage", v, sizeof(v)));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_pinot_basic_types),
        cmocka_unit_test(test_pinot_multivalue_and_fallback),
        cmocka_unit_test(test_pinot_column_size),
        cmocka_unit_test(test_pinot_version_document),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
