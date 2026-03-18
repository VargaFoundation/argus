#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/*
 * Test the sequence of ODBC calls that BI tools (Tableau, Power BI, DBeaver)
 * make during connection setup. Verifies that all calls succeed without errors.
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

/* ── Test: Tableau-style connection sequence ─────────────────── */

static void test_tableau_connect_sequence(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    SQLRETURN ret;

    /* Phase 1: Tableau queries SQLGetInfo extensively */
    SQLCHAR sbuf[256];
    SQLUINTEGER uval;
    SQLUSMALLINT usval;

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_DBMS_NAME, sbuf, sizeof(sbuf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_DBMS_VER, sbuf, sizeof(sbuf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_DRIVER_NAME, sbuf, sizeof(sbuf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_IDENTIFIER_QUOTE_CHAR, sbuf, sizeof(sbuf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_CATALOG_NAME_SEPARATOR, sbuf, sizeof(sbuf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_CATALOG_LOCATION, &usval, sizeof(usval), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(usval, SQL_CL_START);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_CATALOG_USAGE, &uval, sizeof(uval), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(uval & SQL_CU_DML_STATEMENTS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_SCHEMA_USAGE, &uval, sizeof(uval), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_AGGREGATE_FUNCTIONS, &uval, sizeof(uval), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(uval & SQL_AF_COUNT);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_SQL92_PREDICATES, &uval, sizeof(uval), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLGetInfo((SQLHDBC)dbc, SQL_SQL92_RELATIONAL_JOIN_OPERATORS, &uval, sizeof(uval), NULL);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Phase 2: Tableau checks SQLGetFunctions */
    SQLUSMALLINT supported[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE];
    ret = SQLGetFunctions((SQLHDBC)dbc, SQL_API_ODBC3_ALL_FUNCTIONS,
                            supported);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Phase 3: Tableau allocates a statement and queries catalogs */
    argus_stmt_t *stmt = NULL;
    ret = argus_alloc_stmt(dbc, &stmt);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Set statement attributes like Tableau does */
    ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_QUERY_TIMEOUT,
                           (SQLPOINTER)30, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_MAX_ROWS,
                           (SQLPOINTER)0, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_METADATA_ID,
                           (SQLPOINTER)(uintptr_t)SQL_FALSE, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    /* Phase 4: Verify SQLDescribeCol returns proper default sizes */
    stmt->num_cols = 1;
    stmt->executed = true;
    strncpy((char *)stmt->columns[0].name, "test_col",
            ARGUS_MAX_COLUMN_NAME - 1);
    stmt->columns[0].name_len = 8;
    stmt->columns[0].sql_type = SQL_TYPE_TIMESTAMP;
    stmt->columns[0].column_size = 0; /* backend didn't set it */
    stmt->columns[0].nullable = SQL_NULLABLE;

    SQLULEN col_size = 0;
    SQLSMALLINT data_type = 0;
    ret = SQLDescribeCol((SQLHSTMT)stmt, 1, sbuf, sizeof(sbuf), NULL,
                           &data_type, &col_size, NULL, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(col_size, 26); /* TIMESTAMP default */

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Test: Bookmarks attribute handling ──────────────────────── */

static void test_bookmark_handling(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    /* SQL_UB_OFF should be accepted */
    SQLRETURN ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_USE_BOOKMARKS,
                                     (SQLPOINTER)(uintptr_t)SQL_UB_OFF, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    /* SQL_UB_VARIABLE should be rejected with HYC00 */
    ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_USE_BOOKMARKS,
                           (SQLPOINTER)(uintptr_t)SQL_UB_VARIABLE, 0);
    assert_int_equal(ret, SQL_ERROR);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Test: metadata_id attribute ─────────────────────────────── */

static void test_metadata_id(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    /* Default should be SQL_FALSE */
    SQLULEN val = 99;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_METADATA_ID,
                     &val, sizeof(val), NULL);
    assert_int_equal(val, SQL_FALSE);

    /* Set to SQL_TRUE */
    SQLRETURN ret = SQLSetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_METADATA_ID,
                                     (SQLPOINTER)(uintptr_t)SQL_TRUE, 0);
    assert_int_equal(ret, SQL_SUCCESS);

    val = 0;
    SQLGetStmtAttr((SQLHSTMT)stmt, SQL_ATTR_METADATA_ID,
                     &val, sizeof(val), NULL);
    assert_int_equal(val, SQL_TRUE);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_tableau_connect_sequence),
        cmocka_unit_test(test_bookmark_handling),
        cmocka_unit_test(test_metadata_id),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
