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
    argus_stmt_ensure_columns(stmt, 64);
    argus_stmt_ensure_bindings(stmt, 64);

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
    free(stmt->columns);
    free(stmt->bindings);
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

/* ── Real (handle-allocated) statement, with the four descriptors ──
 * Unlike create_stmt_with_metadata (a bare calloc), this goes through the
 * allocation path so the embedded descriptors are initialised — needed to test
 * the descriptor handles themselves. */
static argus_env_t *g_env;
static argus_dbc_t *g_dbc;

static argus_stmt_t *create_real_stmt(void)
{
    argus_alloc_env(&g_env);
    g_env->odbc_version = SQL_OV_ODBC3;
    argus_alloc_dbc(g_env, &g_dbc);
    g_dbc->connected = true;   /* alloc_stmt requires an open connection */

    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(g_dbc, &stmt);
    return stmt;
}

static void destroy_real_stmt(argus_stmt_t *stmt)
{
    argus_free_stmt(stmt);
    g_dbc->connected = false;
    argus_free_dbc(g_dbc);
    argus_free_env(g_env);
}

/* The four statement descriptors must be four DISTINCT handles. Returning the
 * same stmt pointer for all four (the previous behaviour) made an application
 * unable to tell ARD from IRD. */
static void test_four_descriptors_are_distinct(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_real_stmt();

    SQLHDESC ard = NULL, apd = NULL, ird = NULL, ipd = NULL;
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, &ard, 0, NULL), SQL_SUCCESS);
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_APP_PARAM_DESC, &apd, 0, NULL), SQL_SUCCESS);
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_IMP_ROW_DESC, &ird, 0, NULL), SQL_SUCCESS);
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_IMP_PARAM_DESC, &ipd, 0, NULL), SQL_SUCCESS);

    assert_non_null(ard); assert_non_null(apd);
    assert_non_null(ird); assert_non_null(ipd);
    assert_ptr_not_equal(ard, apd);
    assert_ptr_not_equal(ard, ird);
    assert_ptr_not_equal(ard, ipd);
    assert_ptr_not_equal(apd, ird);
    assert_ptr_not_equal(ird, ipd);
    /* And none of them is the statement handle itself. */
    assert_ptr_not_equal(ard, (SQLHDESC)stmt);

    destroy_real_stmt(stmt);
}

/* SQLAllocHandle(SQL_HANDLE_DESC) must succeed and SQLFreeHandle release it —
 * this returned SQL_ERROR before, a Core-conformance gap. */
static void test_explicit_descriptor_alloc_free(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_real_stmt();

    SQLHDESC desc = NULL;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)g_dbc, &desc), SQL_SUCCESS);
    assert_non_null(desc);

    /* A fresh descriptor is usable via SQLSetDescField before association. */
    int marker = 0;
    assert_int_equal(SQLSetDescField(desc, 1, SQL_DESC_DATA_PTR, &marker, 0), SQL_SUCCESS);
    SQLPOINTER got = NULL;
    assert_int_equal(SQLGetDescField(desc, 1, SQL_DESC_DATA_PTR, &got, sizeof(got), NULL),
                     SQL_SUCCESS);
    assert_ptr_equal(got, &marker);

    assert_int_equal(SQLFreeHandle(SQL_HANDLE_DESC, desc), SQL_SUCCESS);
    destroy_real_stmt(stmt);
}

/* Associating an explicit ARD must make it the statement's active row
 * descriptor: SQLGetStmtAttr then returns the explicit handle, and freeing the
 * descriptor detaches it cleanly (no dangling stmt->bindings). */
static void test_explicit_ard_association(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_real_stmt();

    SQLHDESC implicit_ard = NULL;
    SQLGetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, &implicit_ard, 0, NULL);

    SQLHDESC expl = NULL;
    SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)g_dbc, &expl);

    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, expl, 0), SQL_SUCCESS);

    SQLHDESC active = NULL;
    SQLGetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, &active, 0, NULL);
    assert_ptr_equal(active, expl);
    assert_ptr_not_equal(active, implicit_ard);

    /* Reverting to the implicit descriptor (SQL_NULL_HDESC) restores it. */
    assert_int_equal(SQLSetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, SQL_NULL_HDESC, 0), SQL_SUCCESS);
    SQLGetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, &active, 0, NULL);
    assert_ptr_equal(active, implicit_ard);

    SQLFreeHandle(SQL_HANDLE_DESC, expl);
    destroy_real_stmt(stmt);
}

/* Freeing an explicit descriptor while it is still the active ARD must detach
 * it, so the statement reverts to its own array rather than reading freed
 * memory on the next fetch. */
static void test_free_associated_descriptor_detaches(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_real_stmt();

    SQLHDESC expl = NULL;
    SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)g_dbc, &expl);
    SQLSetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, expl, 0);
    assert_ptr_equal(stmt->active_ard, (argus_desc_t *)expl);

    SQLFreeHandle(SQL_HANDLE_DESC, expl);

    /* Reverted to the implicit ARD and its own binding array. */
    assert_ptr_equal(stmt->active_ard, &stmt->desc_ard);
    assert_ptr_equal(stmt->bindings, stmt->implicit_bindings);

    destroy_real_stmt(stmt);
}

/* SQLCopyDesc copies binding records between descriptors. */
static void test_copy_desc(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_real_stmt();

    SQLHDESC a = NULL, b = NULL;
    SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)g_dbc, &a);
    SQLAllocHandle(SQL_HANDLE_DESC, (SQLHANDLE)g_dbc, &b);

    int marker = 0;
    SQLSetDescField(a, 1, SQL_DESC_DATA_PTR, &marker, 0);
    SQLSetDescField(a, 1, SQL_DESC_TYPE, (SQLPOINTER)(intptr_t)SQL_C_LONG, 0);

    assert_int_equal(SQLCopyDesc(a, b), SQL_SUCCESS);

    SQLPOINTER got = NULL;
    SQLGetDescField(b, 1, SQL_DESC_DATA_PTR, &got, sizeof(got), NULL);
    assert_ptr_equal(got, &marker);

    SQLFreeHandle(SQL_HANDLE_DESC, a);
    SQLFreeHandle(SQL_HANDLE_DESC, b);
    destroy_real_stmt(stmt);
}

/* The interface conformance now claims Level 1, matching the commercial drivers
 * for these engines. */
static void test_interface_conformance_is_level1(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_real_stmt();

    SQLUINTEGER conf = 0;
    assert_int_equal(SQLGetInfo(g_dbc, SQL_ODBC_INTERFACE_CONFORMANCE,
                                &conf, sizeof(conf), NULL), SQL_SUCCESS);
    assert_int_equal(conf, SQL_OIC_LEVEL1);

    destroy_real_stmt(stmt);
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
        cmocka_unit_test(test_four_descriptors_are_distinct),
        cmocka_unit_test(test_explicit_descriptor_alloc_free),
        cmocka_unit_test(test_explicit_ard_association),
        cmocka_unit_test(test_free_associated_descriptor_detaches),
        cmocka_unit_test(test_copy_desc),
        cmocka_unit_test(test_interface_conformance_is_level1),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
