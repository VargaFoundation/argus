#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <stdlib.h>
#include "argus/handle.h"

/*
 * Test SQLGetData multi-call for long data (LOB support).
 * When the buffer is too small, SQLGetData returns SQL_SUCCESS_WITH_INFO
 * with SQLSTATE 01004, and subsequent calls return the remaining data.
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

static argus_stmt_t *setup_stmt_with_data(argus_dbc_t *dbc,
                                            const char *data)
{
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    /* Set up one column */
    stmt->num_cols = 1;
    stmt->executed = true;
    stmt->fetch_started = true;
    strncpy((char *)stmt->columns[0].name, "long_text",
            ARGUS_MAX_COLUMN_NAME - 1);
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->columns[0].column_size = 65535;

    /* Set up row cache with one row */
    stmt->row_cache.rows = calloc(1, sizeof(argus_row_t));
    stmt->row_cache.num_rows = 1;
    stmt->row_cache.num_cols = 1;
    stmt->row_cache.current_row = 1; /* already fetched */
    stmt->row_cache.exhausted = true;

    stmt->row_cache.rows[0].cells = calloc(1, sizeof(argus_cell_t));
    stmt->row_cache.rows[0].cells[0].data = strdup(data);
    stmt->row_cache.rows[0].cells[0].data_len = strlen(data);
    stmt->row_cache.rows[0].cells[0].is_null = false;

    return stmt;
}

/* ── Test: small buffer triggers multi-call ──────────────────── */

static void test_getdata_multi_call(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = setup_stmt_with_data(dbc, "Hello, World! This is long data.");

    SQLCHAR buf[10];
    SQLLEN ind;

    /* First call: buffer too small, should get first 9 chars + NUL */
    SQLRETURN ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR,
                                 buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 32); /* total data length */
    assert_int_equal(strlen((char *)buf), 9);
    assert_memory_equal(buf, "Hello, Wo", 9);

    /* Second call: should get next 9 chars */
    ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR,
                      buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 23); /* remaining data length */
    assert_memory_equal(buf, "rld! This", 9);

    /* Third call: get next 9 chars */
    ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR,
                      buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 14); /* remaining data length */
    assert_memory_equal(buf, " is long ", 9);

    /* Fourth call: remaining 5 chars fit in buffer */
    ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR,
                      buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ind, 5);
    assert_memory_equal(buf, "data.", 5);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Test: NULL data returns SQL_NULL_DATA ───────────────────── */

static void test_getdata_null(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    stmt->num_cols = 1;
    stmt->executed = true;
    stmt->fetch_started = true;
    stmt->columns[0].sql_type = SQL_VARCHAR;

    stmt->row_cache.rows = calloc(1, sizeof(argus_row_t));
    stmt->row_cache.num_rows = 1;
    stmt->row_cache.num_cols = 1;
    stmt->row_cache.current_row = 1;
    stmt->row_cache.exhausted = true;

    stmt->row_cache.rows[0].cells = calloc(1, sizeof(argus_cell_t));
    stmt->row_cache.rows[0].cells[0].is_null = true;

    SQLCHAR buf[32];
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR,
                                 buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ind, SQL_NULL_DATA);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Test: data fits in one call ─────────────────────────────── */

static void test_getdata_single_call(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = setup_stmt_with_data(dbc, "short");

    SQLCHAR buf[64];
    SQLLEN ind;
    SQLRETURN ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR,
                                 buf, sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(ind, 5);
    assert_string_equal((char *)buf, "short");

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_getdata_multi_call),
        cmocka_unit_test(test_getdata_null),
        cmocka_unit_test(test_getdata_single_call),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
