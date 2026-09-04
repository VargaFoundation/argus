/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>

#include "pg_common.h"

/*
 * PostgreSQL OID → ODBC type mapping, without a server.
 *
 * The half that needs the most care is atttypmod. A driver that ignores it
 * reports every varchar(20) as unbounded and every numeric(12,3) as an
 * unconstrained decimal, and a BI tool then sizes its buffers for the worst
 * case. The encoding is PostgreSQL's own and is not obvious, so each shape is
 * pinned here.
 */

/* atttypmod as PostgreSQL stores it. */
#define TYPMOD_LEN(n)          ((n) + 4)
#define TYPMOD_NUMERIC(p, s)   (((((p) << 16) | (s))) + 4)

static void test_integer_types(void **state)
{
    (void)state;
    assert_int_equal(pg_oid_to_sql_type(PG_OID_BOOL, -1), SQL_BIT);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_INT2, -1), SQL_SMALLINT);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_INT4, -1), SQL_INTEGER);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_INT8, -1), SQL_BIGINT);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_OID,  -1), SQL_INTEGER);

    assert_int_equal((int)pg_column_size(PG_OID_BOOL, -1), 1);
    assert_int_equal((int)pg_column_size(PG_OID_INT2, -1), 5);
    assert_int_equal((int)pg_column_size(PG_OID_INT4, -1), 10);
    assert_int_equal((int)pg_column_size(PG_OID_INT8, -1), 19);

    assert_int_equal(pg_decimal_digits(PG_OID_INT4, -1), 0);
}

static void test_float_types(void **state)
{
    (void)state;
    assert_int_equal(pg_oid_to_sql_type(PG_OID_FLOAT4, -1), SQL_REAL);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_FLOAT8, -1), SQL_DOUBLE);
    assert_int_equal((int)pg_column_size(PG_OID_FLOAT4, -1), 7);
    assert_int_equal((int)pg_column_size(PG_OID_FLOAT8, -1), 15);
}

static void test_numeric_typmod(void **state)
{
    (void)state;
    assert_int_equal(pg_oid_to_sql_type(PG_OID_NUMERIC, -1), SQL_NUMERIC);

    /* numeric(12,3) */
    int mod = TYPMOD_NUMERIC(12, 3);
    assert_int_equal((int)pg_column_size(PG_OID_NUMERIC, mod), 12);
    assert_int_equal(pg_decimal_digits(PG_OID_NUMERIC, mod), 3);

    /* numeric(38,0) — scale zero must not be confused with "undeclared" */
    mod = TYPMOD_NUMERIC(38, 0);
    assert_int_equal((int)pg_column_size(PG_OID_NUMERIC, mod), 38);
    assert_int_equal(pg_decimal_digits(PG_OID_NUMERIC, mod), 0);

    /* Undeclared numeric: PostgreSQL allows 1000 digits, which no ODBC
     * application allocates for. 38 is the practical ceiling reported. */
    assert_int_equal((int)pg_column_size(PG_OID_NUMERIC, -1), 38);
    assert_int_equal(pg_decimal_digits(PG_OID_NUMERIC, -1), 0);
}

static void test_character_typmod(void **state)
{
    (void)state;
    /* varchar(20) */
    assert_int_equal(pg_oid_to_sql_type(PG_OID_VARCHAR, TYPMOD_LEN(20)),
                     SQL_VARCHAR);
    assert_int_equal((int)pg_column_size(PG_OID_VARCHAR, TYPMOD_LEN(20)), 20);

    /* varchar with no declared length has no meaningful size, so it is a long
     * type rather than a varchar of unknown width. */
    assert_int_equal(pg_oid_to_sql_type(PG_OID_VARCHAR, -1), SQL_LONGVARCHAR);

    /* char(5) */
    assert_int_equal(pg_oid_to_sql_type(PG_OID_BPCHAR, TYPMOD_LEN(5)), SQL_CHAR);
    assert_int_equal((int)pg_column_size(PG_OID_BPCHAR, TYPMOD_LEN(5)), 5);

    assert_int_equal(pg_oid_to_sql_type(PG_OID_TEXT, -1), SQL_LONGVARCHAR);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_NAME, -1), SQL_VARCHAR);
    assert_int_equal((int)pg_column_size(PG_OID_NAME, -1), 63);
}

static void test_temporal_typmod(void **state)
{
    (void)state;
    assert_int_equal(pg_oid_to_sql_type(PG_OID_DATE, -1), SQL_TYPE_DATE);
    assert_int_equal((int)pg_column_size(PG_OID_DATE, -1), 10);
    assert_int_equal(pg_decimal_digits(PG_OID_DATE, -1), 0);

    /* timestamp with no declared precision is microseconds: 6 digits, and
     * "YYYY-MM-DD HH:MM:SS.ffffff" is 26 characters. */
    assert_int_equal(pg_oid_to_sql_type(PG_OID_TIMESTAMP, -1), SQL_TYPE_TIMESTAMP);
    assert_int_equal((int)pg_column_size(PG_OID_TIMESTAMP, -1), 26);
    assert_int_equal(pg_decimal_digits(PG_OID_TIMESTAMP, -1), 6);

    /* timestamp(3) */
    assert_int_equal((int)pg_column_size(PG_OID_TIMESTAMP, 3), 23);
    assert_int_equal(pg_decimal_digits(PG_OID_TIMESTAMP, 3), 3);

    /* timestamp(0): no fractional part, so no separator either. */
    assert_int_equal((int)pg_column_size(PG_OID_TIMESTAMP, 0), 19);
    assert_int_equal(pg_decimal_digits(PG_OID_TIMESTAMP, 0), 0);

    assert_int_equal(pg_oid_to_sql_type(PG_OID_TIME, 3), SQL_TYPE_TIME);
    assert_int_equal((int)pg_column_size(PG_OID_TIME, 3), 12);   /* HH:MM:SS.fff */

    /* timetz maps to TIME; the offset has nowhere to go in ODBC's struct. */
    assert_int_equal(pg_oid_to_sql_type(PG_OID_TIMETZ, -1), SQL_TYPE_TIME);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_TIMESTAMPTZ, -1),
                     SQL_TYPE_TIMESTAMP);
}

static void test_other_types(void **state)
{
    (void)state;
    assert_int_equal(pg_oid_to_sql_type(PG_OID_BYTEA, -1), SQL_LONGVARBINARY);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_UUID,  -1), SQL_GUID);
    assert_int_equal((int)pg_column_size(PG_OID_UUID, -1), 36);

    assert_int_equal(pg_oid_to_sql_type(PG_OID_JSON,  -1), SQL_LONGVARCHAR);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_JSONB, -1), SQL_LONGVARCHAR);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_XML,   -1), SQL_LONGVARCHAR);

    /* No ODBC interval type matches PostgreSQL's month/day/microsecond
     * triple, and money carries a locale currency symbol: both are text. */
    assert_int_equal(pg_oid_to_sql_type(PG_OID_INTERVAL, -1), SQL_VARCHAR);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_MONEY,    -1), SQL_VARCHAR);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_BIT,      -1), SQL_VARCHAR);
    assert_int_equal(pg_oid_to_sql_type(PG_OID_VARBIT,   -1), SQL_VARCHAR);

    /* Arrays, composites, enums, ranges and every extension type: the text
     * form is what PostgreSQL always has, so nothing is invented. 1007 is
     * _int4, the array of integer. */
    assert_int_equal(pg_oid_to_sql_type(1007, -1), SQL_LONGVARCHAR);
    assert_int_equal(pg_oid_to_sql_type(999999, -1), SQL_LONGVARCHAR);
}

/*
 * The native fast path must be claimed only where the text form and the
 * numeric value cannot disagree. Anything whose text carries information the
 * number does not — numeric's exact scale, a timestamp's rendering — has to
 * stay on the text path.
 */
static void test_native_kind(void **state)
{
    (void)state;
    assert_int_equal(pg_native_kind(PG_OID_INT2), ARGUS_NATIVE_I64);
    assert_int_equal(pg_native_kind(PG_OID_INT4), ARGUS_NATIVE_I64);
    assert_int_equal(pg_native_kind(PG_OID_INT8), ARGUS_NATIVE_I64);
    assert_int_equal(pg_native_kind(PG_OID_BOOL), ARGUS_NATIVE_I64);
    assert_int_equal(pg_native_kind(PG_OID_OID),  ARGUS_NATIVE_I64);

    assert_int_equal(pg_native_kind(PG_OID_FLOAT4), ARGUS_NATIVE_F64);
    assert_int_equal(pg_native_kind(PG_OID_FLOAT8), ARGUS_NATIVE_F64);

    /* numeric exceeds double: narrowing a money column silently is not an
     * acceptable optimisation. */
    assert_int_equal(pg_native_kind(PG_OID_NUMERIC), ARGUS_NATIVE_NONE);
    assert_int_equal(pg_native_kind(PG_OID_TEXT), ARGUS_NATIVE_NONE);
    assert_int_equal(pg_native_kind(PG_OID_TIMESTAMP), ARGUS_NATIVE_NONE);
    assert_int_equal(pg_native_kind(PG_OID_DATE), ARGUS_NATIVE_NONE);
}

/*
 * SQLColumns reports DATA_TYPE as a numeric ODBC code produced by SQL, so the
 * SQL CASE and the C mapping are two expressions of one table and can drift.
 * This pins the SQL side against the C side for every OID that appears in it.
 */
static void test_sql_case_matches_c_mapping(void **state)
{
    (void)state;

    GString *sql = g_string_new(NULL);
    pg_append_odbc_type_case(sql, "t.oid", "a.atttypmod");
    const char *s = sql->str;

    struct { Oid oid; const char *fragment; } expect[] = {
        { PG_OID_BOOL,        "WHEN 16 THEN -7" },
        { PG_OID_BYTEA,       "WHEN 17 THEN -4" },
        { PG_OID_INT8,        "WHEN 20 THEN -5" },
        { PG_OID_INT2,        "WHEN 21 THEN 5" },
        { PG_OID_INT4,        "WHEN 23 THEN 4" },
        { PG_OID_FLOAT4,      "WHEN 700 THEN 7" },
        { PG_OID_FLOAT8,      "WHEN 701 THEN 8" },
        { PG_OID_BPCHAR,      "WHEN 1042 THEN 1" },
        { PG_OID_DATE,        "WHEN 1082 THEN 91" },
        { PG_OID_TIME,        "WHEN 1083 THEN 92" },
        { PG_OID_TIMESTAMP,   "WHEN 1114 THEN 93" },
        { PG_OID_TIMESTAMPTZ, "WHEN 1184 THEN 93" },
        { PG_OID_NUMERIC,     "WHEN 1700 THEN 2" },
        { PG_OID_UUID,        "WHEN 2950 THEN -11" },
    };

    for (size_t i = 0; i < sizeof(expect) / sizeof(expect[0]); i++) {
        if (!strstr(s, expect[i].fragment))
            fail_msg("SQL CASE is missing '%s'", expect[i].fragment);

        /* And the fragment must agree with what the C mapping answers. */
        int sql_code = atoi(strstr(expect[i].fragment, "THEN ") + 5);
        assert_int_equal(sql_code, pg_oid_to_sql_type(expect[i].oid, -1));
    }

    /* The caller's typmod expression must actually be interpolated, or the
     * varchar branch silently reports every varchar as a long type. */
    assert_non_null(strstr(s, "a.atttypmod"));

    g_string_free(sql, TRUE);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_integer_types),
        cmocka_unit_test(test_float_types),
        cmocka_unit_test(test_numeric_typmod),
        cmocka_unit_test(test_character_typmod),
        cmocka_unit_test(test_temporal_typmod),
        cmocka_unit_test(test_other_types),
        cmocka_unit_test(test_native_kind),
        cmocka_unit_test(test_sql_case_matches_c_mapping),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
