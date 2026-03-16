/*
 * Unit tests for UTF-8 / UTF-16 conversion and Unicode W functions
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <glib.h>
#include "argus/handle.h"

/* ── Helper: create a test stmt with a cell for SQLGetData ───── */

static argus_stmt_t *create_stmt_with_cell(const char *utf8_data)
{
    argus_stmt_t *stmt = calloc(1, sizeof(argus_stmt_t));
    stmt->signature = ARGUS_STMT_SIGNATURE;
    stmt->row_count = -1;
    stmt->row_array_size = 1;
    stmt->executed = true;
    stmt->fetch_started = true;
    stmt->num_cols = 1;
    stmt->metadata_fetched = true;
    argus_diag_clear(&stmt->diag);

    /* Set up column descriptor */
    strncpy((char *)stmt->columns[0].name, "col1", ARGUS_MAX_COLUMN_NAME);
    stmt->columns[0].name_len = 4;
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->columns[0].column_size = 256;
    stmt->columns[0].nullable = SQL_NULLABLE;

    /* Set up row cache with one row */
    stmt->row_cache.num_cols = 1;
    stmt->row_cache.num_rows = 1;
    stmt->row_cache.current_row = 1; /* "just fetched" position */
    stmt->row_cache.rows = calloc(1, sizeof(argus_row_t));
    stmt->row_cache.rows[0].cells = calloc(1, sizeof(argus_cell_t));
    stmt->row_cache.rows[0].cells[0].data = strdup(utf8_data);
    stmt->row_cache.rows[0].cells[0].data_len = strlen(utf8_data);
    stmt->row_cache.rows[0].cells[0].is_null = false;

    return stmt;
}

static void destroy_test_stmt(argus_stmt_t *stmt)
{
    argus_row_cache_free(&stmt->row_cache);
    free(stmt->query);
    stmt->signature = 0;
    free(stmt);
}

/* ── Test: ASCII string via SQLGetData SQL_C_WCHAR ───────────── */

static void test_wchar_ascii(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_cell("Hello");

    SQLWCHAR buf[32];
    SQLLEN ind = 0;

    SQLRETURN ret = SQLGetData(
        (SQLHSTMT)stmt, 1, SQL_C_WCHAR,
        buf, sizeof(buf), &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ind, 5 * (SQLLEN)sizeof(SQLWCHAR));

    /* Verify content */
    assert_int_equal(buf[0], 'H');
    assert_int_equal(buf[1], 'e');
    assert_int_equal(buf[2], 'l');
    assert_int_equal(buf[3], 'l');
    assert_int_equal(buf[4], 'o');
    assert_int_equal(buf[5], 0);

    destroy_test_stmt(stmt);
}

/* ── Test: Multi-byte UTF-8 (accented chars) → UTF-16 ────────── */

static void test_wchar_multibyte(void **state)
{
    (void)state;
    /* "café" in UTF-8 is: c(63) a(61) f(66) é(c3 a9) */
    argus_stmt_t *stmt = create_stmt_with_cell("caf\xc3\xa9");

    SQLWCHAR buf[32];
    SQLLEN ind = 0;

    SQLRETURN ret = SQLGetData(
        (SQLHSTMT)stmt, 1, SQL_C_WCHAR,
        buf, sizeof(buf), &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    /* "café" = 4 UTF-16 code units */
    assert_int_equal(ind, 4 * (SQLLEN)sizeof(SQLWCHAR));

    assert_int_equal(buf[0], 'c');
    assert_int_equal(buf[1], 'a');
    assert_int_equal(buf[2], 'f');
    assert_int_equal(buf[3], 0x00E9); /* é */
    assert_int_equal(buf[4], 0);

    destroy_test_stmt(stmt);
}

/* ── Test: CJK characters → UTF-16 ──────────────────────────── */

static void test_wchar_cjk(void **state)
{
    (void)state;
    /* 日本 in UTF-8: e6 97 a5 e6 9c ac */
    argus_stmt_t *stmt = create_stmt_with_cell("\xe6\x97\xa5\xe6\x9c\xac");

    SQLWCHAR buf[16];
    SQLLEN ind = 0;

    SQLRETURN ret = SQLGetData(
        (SQLHSTMT)stmt, 1, SQL_C_WCHAR,
        buf, sizeof(buf), &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ind, 2 * (SQLLEN)sizeof(SQLWCHAR));
    assert_int_equal(buf[0], 0x65E5); /* 日 */
    assert_int_equal(buf[1], 0x672C); /* 本 */
    assert_int_equal(buf[2], 0);

    destroy_test_stmt(stmt);
}

/* ── Test: Truncation when buffer is too small ───────────────── */

static void test_wchar_truncation(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_cell("Hello World");

    /* Buffer for 4 wide chars + NUL = 5 * sizeof(SQLWCHAR) */
    SQLWCHAR buf[5];
    SQLLEN ind = 0;

    SQLRETURN ret = SQLGetData(
        (SQLHSTMT)stmt, 1, SQL_C_WCHAR,
        buf, sizeof(buf), &ind);

    assert_int_equal(ret, SQL_SUCCESS_WITH_INFO);
    /* Total data length */
    assert_int_equal(ind, 11 * (SQLLEN)sizeof(SQLWCHAR));
    /* Only 4 chars copied */
    assert_int_equal(buf[0], 'H');
    assert_int_equal(buf[1], 'e');
    assert_int_equal(buf[2], 'l');
    assert_int_equal(buf[3], 'l');
    assert_int_equal(buf[4], 0);

    destroy_test_stmt(stmt);
}

/* ── Test: Empty string ──────────────────────────────────────── */

static void test_wchar_empty(void **state)
{
    (void)state;
    argus_stmt_t *stmt = create_stmt_with_cell("");

    SQLWCHAR buf[8];
    SQLLEN ind = 0;

    SQLRETURN ret = SQLGetData(
        (SQLHSTMT)stmt, 1, SQL_C_WCHAR,
        buf, sizeof(buf), &ind);

    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ind, 0);
    assert_int_equal(buf[0], 0);

    destroy_test_stmt(stmt);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_wchar_ascii),
        cmocka_unit_test(test_wchar_multibyte),
        cmocka_unit_test(test_wchar_cjk),
        cmocka_unit_test(test_wchar_truncation),
        cmocka_unit_test(test_wchar_empty),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
