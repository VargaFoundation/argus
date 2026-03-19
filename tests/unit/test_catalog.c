/*
 * Unit tests for catalog function metadata
 *
 * Verifies that empty catalog result sets return the correct number
 * of columns with proper ODBC-mandated metadata.
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/* ── Helper: create a minimal connected stmt ─────────────────── */

static argus_stmt_t *create_test_stmt(void)
{
    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    stmt->signature = ARGUS_STMT_SIGNATURE;
    stmt->row_count = -1;
    stmt->row_array_size = 1;
    argus_diag_clear(&stmt->diag);
    argus_row_cache_init(&stmt->row_cache);
    argus_stmt_ensure_columns(stmt, 64);
    argus_stmt_ensure_bindings(stmt, 64);
    return stmt;
}

static void destroy_test_stmt(argus_stmt_t *stmt)
{
    argus_row_cache_free(&stmt->row_cache);
    free(stmt->query);
    free(stmt->columns);
    free(stmt->bindings);
    stmt->signature = 0;
    free(stmt);
}

/* ── Test: SQLSpecialColumns returns 8 columns ───────────────── */

static void test_special_columns_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLSpecialColumns(
        (SQLHSTMT)stmt, SQL_BEST_ROWID,
        NULL, 0, NULL, 0, NULL, 0,
        SQL_SCOPE_SESSION, SQL_NULLABLE);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 8);
    assert_true(stmt->metadata_fetched);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols((SQLHSTMT)stmt, &ncols);
    assert_int_equal(ncols, 8);

    /* Verify column names */
    assert_string_equal((char *)stmt->columns[0].name, "SCOPE");
    assert_string_equal((char *)stmt->columns[1].name, "COLUMN_NAME");
    assert_string_equal((char *)stmt->columns[7].name, "PSEUDO_COLUMN");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLForeignKeys returns 14 columns ─────────────────── */

static void test_foreign_keys_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLForeignKeys(
        (SQLHSTMT)stmt,
        NULL, 0, NULL, 0, NULL, 0,
        NULL, 0, NULL, 0, NULL, 0);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 14);
    assert_string_equal((char *)stmt->columns[0].name, "PKTABLE_CAT");
    assert_string_equal((char *)stmt->columns[13].name, "DEFERRABILITY");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLProcedures returns 8 columns ───────────────────── */

static void test_procedures_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLProcedures(
        (SQLHSTMT)stmt, NULL, 0, NULL, 0, NULL, 0);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 8);
    assert_string_equal((char *)stmt->columns[0].name, "PROCEDURE_CAT");
    assert_string_equal((char *)stmt->columns[7].name, "PROCEDURE_TYPE");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLProcedureColumns returns 13 columns ────────────── */

static void test_procedure_columns_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLProcedureColumns(
        (SQLHSTMT)stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 13);
    assert_string_equal((char *)stmt->columns[0].name, "PROCEDURE_CAT");
    assert_string_equal((char *)stmt->columns[12].name, "REMARKS");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLTablePrivileges returns 7 columns ──────────────── */

static void test_table_privileges_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLTablePrivileges(
        (SQLHSTMT)stmt, NULL, 0, NULL, 0, NULL, 0);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 7);
    assert_string_equal((char *)stmt->columns[0].name, "TABLE_CAT");
    assert_string_equal((char *)stmt->columns[6].name, "IS_GRANTABLE");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLColumnPrivileges returns 8 columns ─────────────── */

static void test_column_privileges_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLColumnPrivileges(
        (SQLHSTMT)stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 8);
    assert_string_equal((char *)stmt->columns[0].name, "TABLE_CAT");
    assert_string_equal((char *)stmt->columns[7].name, "IS_GRANTABLE");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLPrimaryKeys returns 6 columns ──────────────────── */

static void test_primary_keys_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLPrimaryKeys(
        (SQLHSTMT)stmt, NULL, 0, NULL, 0, NULL, 0);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 6);
    assert_string_equal((char *)stmt->columns[0].name, "TABLE_CAT");
    assert_string_equal((char *)stmt->columns[5].name, "PK_NAME");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLStatistics returns 13 columns ──────────────────── */

static void test_statistics_metadata(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_test_stmt();

    SQLRETURN ret = SQLStatistics(
        (SQLHSTMT)stmt, NULL, 0, NULL, 0, NULL, 0,
        SQL_INDEX_ALL, SQL_QUICK);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(stmt->num_cols, 13);
    assert_string_equal((char *)stmt->columns[0].name, "TABLE_CAT");
    assert_string_equal((char *)stmt->columns[12].name, "FILTER_CONDITION");

    destroy_test_stmt(stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_special_columns_metadata),
        cmocka_unit_test(test_foreign_keys_metadata),
        cmocka_unit_test(test_procedures_metadata),
        cmocka_unit_test(test_procedure_columns_metadata),
        cmocka_unit_test(test_table_privileges_metadata),
        cmocka_unit_test(test_column_privileges_metadata),
        cmocka_unit_test(test_primary_keys_metadata),
        cmocka_unit_test(test_statistics_metadata),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
