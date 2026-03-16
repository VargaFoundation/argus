/*
 * Unit tests for SQLGetDescField / SQLGetDescRec
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/* ── Helper: create a test stmt with column metadata ──────────── */

static argus_stmt_t *create_stmt_with_metadata(void)
{
    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    stmt->signature = ARGUS_STMT_SIGNATURE;
    stmt->row_count = -1;
    stmt->row_array_size = 1;
    stmt->executed = true;
    stmt->metadata_fetched = true;
    argus_diag_clear(&stmt->diag);
    argus_row_cache_init(&stmt->row_cache);

    /* Set up 3 columns */
    stmt->num_cols = 3;

    strncpy((char *)stmt->columns[0].name, "id", ARGUS_MAX_COLUMN_NAME);
    stmt->columns[0].name_len = 2;
    stmt->columns[0].sql_type = SQL_INTEGER;
    stmt->columns[0].column_size = 10;
    stmt->columns[0].decimal_digits = 0;
    stmt->columns[0].nullable = SQL_NO_NULLS;

    strncpy((char *)stmt->columns[1].name, "name", ARGUS_MAX_COLUMN_NAME);
    stmt->columns[1].name_len = 4;
    stmt->columns[1].sql_type = SQL_VARCHAR;
    stmt->columns[1].column_size = 255;
    stmt->columns[1].decimal_digits = 0;
    stmt->columns[1].nullable = SQL_NULLABLE;

    strncpy((char *)stmt->columns[2].name, "price", ARGUS_MAX_COLUMN_NAME);
    stmt->columns[2].name_len = 5;
    stmt->columns[2].sql_type = SQL_DOUBLE;
    stmt->columns[2].column_size = 15;
    stmt->columns[2].decimal_digits = 2;
    stmt->columns[2].nullable = SQL_NULLABLE;

    return stmt;
}

static void destroy_test_stmt(argus_stmt_t *stmt)
{
    argus_row_cache_free(&stmt->row_cache);
    free(stmt->query);
    stmt->signature = 0;
    free(stmt);
}

/* ── Test: SQLGetDescField header SQL_DESC_COUNT ─────────────── */

static void test_desc_count(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLSMALLINT count = 0;
    SQLRETURN ret = SQLGetDescField(
        (SQLHDESC)stmt, 0, SQL_DESC_COUNT,
        &count, sizeof(count), NULL);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(count, 3);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLGetDescField record SQL_DESC_NAME ──────────────── */

static void test_desc_name(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLCHAR name_buf[64];
    SQLINTEGER len = 0;

    SQLRETURN ret = SQLGetDescField(
        (SQLHDESC)stmt, 1, SQL_DESC_NAME,
        name_buf, sizeof(name_buf), &len);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)name_buf, "id");
    assert_int_equal(len, 2);

    ret = SQLGetDescField(
        (SQLHDESC)stmt, 2, SQL_DESC_NAME,
        name_buf, sizeof(name_buf), &len);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)name_buf, "name");

    destroy_test_stmt(stmt);
}

/* ── Test: SQLGetDescField record SQL_DESC_TYPE ──────────────── */

static void test_desc_type(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLSMALLINT type = 0;
    SQLRETURN ret = SQLGetDescField(
        (SQLHDESC)stmt, 1, SQL_DESC_TYPE,
        &type, sizeof(type), NULL);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(type, SQL_INTEGER);

    ret = SQLGetDescField(
        (SQLHDESC)stmt, 3, SQL_DESC_TYPE,
        &type, sizeof(type), NULL);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(type, SQL_DOUBLE);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLGetDescField record SQL_DESC_NULLABLE ──────────── */

static void test_desc_nullable(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLSMALLINT nullable = 0;
    SQLGetDescField(
        (SQLHDESC)stmt, 1, SQL_DESC_NULLABLE,
        &nullable, sizeof(nullable), NULL);

    assert_int_equal(nullable, SQL_NO_NULLS);

    SQLGetDescField(
        (SQLHDESC)stmt, 2, SQL_DESC_NULLABLE,
        &nullable, sizeof(nullable), NULL);

    assert_int_equal(nullable, SQL_NULLABLE);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLGetDescField out-of-range record ───────────────── */

static void test_desc_invalid_record(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLSMALLINT type = 0;
    SQLRETURN ret = SQLGetDescField(
        (SQLHDESC)stmt, 5, SQL_DESC_TYPE,
        &type, sizeof(type), NULL);

    assert_int_equal(ret, SQL_ERROR);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLGetDescRec ─────────────────────────────────────── */

static void test_get_desc_rec(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLCHAR name[64];
    SQLSMALLINT name_len = 0, type = 0, sub = 0, prec = 0, scale = 0, nullable = 0;
    SQLLEN length = 0;

    SQLRETURN ret = SQLGetDescRec(
        (SQLHDESC)stmt, 2,
        name, sizeof(name), &name_len,
        &type, &sub, &length, &prec, &scale, &nullable);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)name, "name");
    assert_int_equal(type, SQL_VARCHAR);
    assert_int_equal(length, 255);
    assert_int_equal(nullable, SQL_NULLABLE);

    destroy_test_stmt(stmt);
}

/* ── Test: SQLGetDescRec out of range returns SQL_NO_DATA ────── */

static void test_get_desc_rec_no_data(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_metadata();

    SQLCHAR name[64];
    SQLSMALLINT name_len;

    SQLRETURN ret = SQLGetDescRec(
        (SQLHDESC)stmt, 10,
        name, sizeof(name), &name_len,
        NULL, NULL, NULL, NULL, NULL, NULL);

    assert_int_equal(ret, SQL_NO_DATA);

    destroy_test_stmt(stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_desc_count),
        cmocka_unit_test(test_desc_name),
        cmocka_unit_test(test_desc_type),
        cmocka_unit_test(test_desc_nullable),
        cmocka_unit_test(test_desc_invalid_record),
        cmocka_unit_test(test_get_desc_rec),
        cmocka_unit_test(test_get_desc_rec_no_data),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
