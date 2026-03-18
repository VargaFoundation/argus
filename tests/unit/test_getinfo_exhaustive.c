#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/*
 * Test that all commonly-queried SQLGetInfo types return SQL_SUCCESS.
 * BI tools (Tableau, Power BI, DBeaver) query these during connection setup.
 */

static argus_dbc_t *create_test_dbc(void)
{
    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);
    dbc->host = strdup("testhost");
    dbc->database = strdup("testdb");
    dbc->username = strdup("testuser");

    extern void argus_backends_init(void);
    extern const argus_backend_t *argus_backend_find(const char *name);
    argus_backends_init();

    dbc->backend = argus_backend_find("hive");
    if (!dbc->backend) dbc->backend = argus_backend_find("trino");

    return dbc;
}

static void free_test_dbc(argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: all string info types return SQL_SUCCESS ──────────── */

static void test_string_info_types(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    SQLCHAR buf[512];
    SQLSMALLINT len;

    SQLUSMALLINT string_types[] = {
        SQL_DRIVER_NAME, SQL_DRIVER_VER, SQL_DRIVER_ODBC_VER,
        SQL_ODBC_VER, SQL_DATA_SOURCE_NAME, SQL_SERVER_NAME,
        SQL_DATABASE_NAME, SQL_DBMS_NAME, SQL_DBMS_VER,
        SQL_IDENTIFIER_QUOTE_CHAR, SQL_CATALOG_NAME_SEPARATOR,
        SQL_CATALOG_TERM, SQL_SCHEMA_TERM, SQL_TABLE_TERM,
        SQL_PROCEDURE_TERM, SQL_SEARCH_PATTERN_ESCAPE,
        SQL_LIKE_ESCAPE_CLAUSE, SQL_SPECIAL_CHARACTERS,
        SQL_COLUMN_ALIAS, SQL_ORDER_BY_COLUMNS_IN_SELECT,
        SQL_EXPRESSIONS_IN_ORDERBY, SQL_MULT_RESULT_SETS,
        SQL_MULTIPLE_ACTIVE_TXN, SQL_OUTER_JOINS,
        SQL_ACCESSIBLE_TABLES, SQL_ACCESSIBLE_PROCEDURES,
        SQL_MAX_ROW_SIZE_INCLUDES_LONG, SQL_NEED_LONG_DATA_LEN,
        SQL_ROW_UPDATES, SQL_DESCRIBE_PARAMETER, SQL_INTEGRITY,
        SQL_DATA_SOURCE_READ_ONLY, SQL_CATALOG_NAME,
        SQL_COLLATION_SEQ, SQL_USER_NAME, SQL_KEYWORDS,
        SQL_PROCEDURES,
    };

    for (size_t i = 0; i < sizeof(string_types) / sizeof(string_types[0]); i++) {
        memset(buf, 0, sizeof(buf));
        SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, string_types[i],
                                    buf, sizeof(buf), &len);
        assert_int_equal(ret, SQL_SUCCESS);
    }

    free_test_dbc(dbc);
}

/* ── Test: all integer info types return SQL_SUCCESS ─────────── */

static void test_integer_info_types(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    SQLUINTEGER val;
    SQLSMALLINT len;

    SQLUSMALLINT uint_types[] = {
        SQL_GETDATA_EXTENSIONS, SQL_SCROLL_OPTIONS,
        SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
        SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
        SQL_TXN_ISOLATION_OPTION, SQL_DEFAULT_TXN_ISOLATION,
        SQL_CONVERT_FUNCTIONS, SQL_STRING_FUNCTIONS,
        SQL_NUMERIC_FUNCTIONS, SQL_SYSTEM_FUNCTIONS,
        SQL_TIMEDATE_FUNCTIONS, SQL_OJ_CAPABILITIES,
        SQL_SUBQUERIES, SQL_UNION,
        SQL_MAX_ROW_SIZE, SQL_MAX_STATEMENT_LEN,
        SQL_MAX_CHAR_LITERAL_LEN, SQL_MAX_BINARY_LITERAL_LEN,
        SQL_ALTER_TABLE, SQL_SQL_CONFORMANCE,
        SQL_ODBC_INTERFACE_CONFORMANCE,
        SQL_SQL92_PREDICATES, SQL_SQL92_VALUE_EXPRESSIONS,
        SQL_SQL92_RELATIONAL_JOIN_OPERATORS,
        SQL_AGGREGATE_FUNCTIONS, SQL_CATALOG_USAGE,
        SQL_SCHEMA_USAGE, SQL_BATCH_SUPPORT,
        SQL_CREATE_TABLE, SQL_CREATE_VIEW,
        SQL_DROP_TABLE, SQL_DROP_VIEW,
        SQL_INSERT_STATEMENT, SQL_BOOKMARK_PERSISTENCE,
        SQL_MAX_INDEX_SIZE, SQL_POS_OPERATIONS,
        SQL_CURSOR_SENSITIVITY,
    };

    for (size_t i = 0; i < sizeof(uint_types) / sizeof(uint_types[0]); i++) {
        val = 0xDEADBEEF;
        SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, uint_types[i],
                                    &val, sizeof(val), &len);
        assert_int_equal(ret, SQL_SUCCESS);
    }

    free_test_dbc(dbc);
}

/* ── Test: unknown info types return SQL_SUCCESS (fallback) ──── */

static void test_unknown_info_types_fallback(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[64];
    SQLSMALLINT len;
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, 9999,
                                buf, sizeof(buf), &len);
    assert_int_equal(ret, SQL_SUCCESS);

    free_test_dbc(dbc);
}

/* ── Test: specific BI-critical values ───────────────────────── */

static void test_bi_critical_values(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();

    /* SQL_PROCEDURES should be "N" */
    SQLCHAR buf[32];
    SQLGetInfo((SQLHDBC)dbc, SQL_PROCEDURES, buf, sizeof(buf), NULL);
    assert_string_equal((char *)buf, "N");

    /* SQL_MAX_IDENTIFIER_LEN should be 128 */
    SQLUSMALLINT id_len = 0;
    SQLGetInfo((SQLHDBC)dbc, SQL_MAX_IDENTIFIER_LEN, &id_len, sizeof(id_len), NULL);
    assert_int_equal(id_len, 128);

    /* SQL_MAX_COLUMN_NAME_LEN should be 256 */
    SQLUSMALLINT col_len = 0;
    SQLGetInfo((SQLHDBC)dbc, SQL_MAX_COLUMN_NAME_LEN, &col_len, sizeof(col_len), NULL);
    assert_int_equal(col_len, 256);

    /* SQL_GROUP_BY should be SQL_GB_GROUP_BY_CONTAINS_SELECT */
    SQLUSMALLINT gb = 0;
    SQLGetInfo((SQLHDBC)dbc, SQL_GROUP_BY, &gb, sizeof(gb), NULL);
    assert_int_equal(gb, SQL_GB_GROUP_BY_CONTAINS_SELECT);

    /* SQL_AGGREGATE_FUNCTIONS bitmask should include common functions */
    SQLUINTEGER agg = 0;
    SQLGetInfo((SQLHDBC)dbc, SQL_AGGREGATE_FUNCTIONS, &agg, sizeof(agg), NULL);
    assert_true(agg & SQL_AF_AVG);
    assert_true(agg & SQL_AF_COUNT);
    assert_true(agg & SQL_AF_SUM);

    free_test_dbc(dbc);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_string_info_types),
        cmocka_unit_test(test_integer_info_types),
        cmocka_unit_test(test_unknown_info_types_fallback),
        cmocka_unit_test(test_bi_critical_values),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
