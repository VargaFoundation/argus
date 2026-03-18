#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/*
 * Test SQLColAttribute returns TABLE_NAME, SCHEMA_NAME, CATALOG_NAME,
 * BASE_TABLE_NAME, and BASE_COLUMN_NAME.
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

    extern void argus_backends_init(void);
    extern const argus_backend_t *argus_backend_find(const char *name);
    argus_backends_init();
    dbc->backend = argus_backend_find("hive");
    if (!dbc->backend) dbc->backend = argus_backend_find("trino");
    dbc->connected = true;

    return dbc;
}

static void free_test_dbc(argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    dbc->connected = false;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: TABLE_NAME, SCHEMA_NAME, CATALOG_NAME from column metadata ── */

static void test_col_metadata_fields(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    argus_stmt_t *stmt = NULL;
    SQLRETURN ret = argus_alloc_stmt(dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Manually set up a column with metadata */
    stmt->num_cols = 1;
    stmt->executed = true;
    strncpy((char *)stmt->columns[0].name, "my_column",
            ARGUS_MAX_COLUMN_NAME - 1);
    stmt->columns[0].name_len = 9;
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->columns[0].column_size = 100;
    stmt->columns[0].nullable = SQL_NULLABLE;

    strncpy((char *)stmt->columns[0].table_name, "my_table",
            ARGUS_MAX_COLUMN_NAME - 1);
    strncpy((char *)stmt->columns[0].schema_name, "my_schema",
            ARGUS_MAX_COLUMN_NAME - 1);
    strncpy((char *)stmt->columns[0].catalog_name, "my_catalog",
            ARGUS_MAX_COLUMN_NAME - 1);

    SQLCHAR buf[256];
    SQLSMALLINT len;

    /* SQL_DESC_TABLE_NAME */
    ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_TABLE_NAME,
                            buf, sizeof(buf), &len, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "my_table");

    /* SQL_DESC_SCHEMA_NAME */
    ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_SCHEMA_NAME,
                            buf, sizeof(buf), &len, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "my_schema");

    /* SQL_DESC_CATALOG_NAME */
    ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_CATALOG_NAME,
                            buf, sizeof(buf), &len, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "my_catalog");

    /* SQL_DESC_BASE_COLUMN_NAME returns column name */
    ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_BASE_COLUMN_NAME,
                            buf, sizeof(buf), &len, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "my_column");

    /* SQL_DESC_BASE_TABLE_NAME returns table name */
    ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_BASE_TABLE_NAME,
                            buf, sizeof(buf), &len, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "my_table");

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Test: empty metadata returns empty strings (not errors) ─── */

static void test_col_metadata_empty(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    stmt->num_cols = 1;
    stmt->executed = true;
    strncpy((char *)stmt->columns[0].name, "col1", ARGUS_MAX_COLUMN_NAME - 1);
    stmt->columns[0].name_len = 4;
    stmt->columns[0].sql_type = SQL_INTEGER;
    stmt->columns[0].column_size = 10;
    /* table_name, schema_name, catalog_name left as zeros (empty) */

    SQLCHAR buf[256];
    SQLSMALLINT len;

    SQLRETURN ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_TABLE_NAME,
                                      buf, sizeof(buf), &len, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "");

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_col_metadata_fields),
        cmocka_unit_test(test_col_metadata_empty),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
