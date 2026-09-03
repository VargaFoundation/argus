/*
 * test_thread_safety.c — hammer the newly-locked ODBC surface from several
 * threads against SHARED handles. No backend needed: attribute get/set,
 * cursor names, descriptor fields and diagnostic reads/writes all operate on
 * driver-local state. Run under ASan/TSan this catches lock regressions
 * (races on the lazily-allocated diag/param arrays were the motivating bug);
 * without sanitizers it still catches deadlocks (the test would hang) and
 * crashes.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <glib.h>
#include "argus/handle.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define THREADS 8
#define ITERS   3000

typedef struct {
    SQLHENV  env;
    SQLHDBC  dbc;
    SQLHSTMT stmt;
    int      id;
} ctx_t;

static gpointer worker(gpointer data)
{
    ctx_t *c = data;

    for (int i = 0; i < ITERS; i++) {
        switch ((i + c->id) % 6) {
        case 0: {
            SQLSetStmtAttr(c->stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                           (SQLPOINTER)(uintptr_t)(1 + (i % 32)), 0);
            break;
        }
        case 1: {
            SQLULEN v = 0;
            SQLGetStmtAttr(c->stmt, SQL_ATTR_ROW_ARRAY_SIZE, &v, 0, NULL);
            break;
        }
        case 2: {
            /* Provoke a diagnostic write (invalid attribute) then read it —
             * this is the exact writer/reader pair that raced before. */
            SQLSetStmtAttr(c->stmt, (SQLINTEGER)0x7fff0000, (SQLPOINTER)1, 0);
            SQLCHAR st[6]; SQLINTEGER nat; SQLCHAR msg[256]; SQLSMALLINT len;
            SQLGetDiagRec(SQL_HANDLE_STMT, c->stmt, 1, st, &nat,
                          msg, sizeof(msg), &len);
            break;
        }
        case 3: {
            char name[32];
            snprintf(name, sizeof(name), "cur_%d_%d", c->id, i);
            SQLSetCursorName(c->stmt, (SQLCHAR *)name, SQL_NTS);
            SQLCHAR out[64]; SQLSMALLINT olen;
            SQLGetCursorName(c->stmt, out, sizeof(out), &olen);
            break;
        }
        case 4: {
            SQLHDESC ard = SQL_NULL_HDESC;
            SQLGetStmtAttr(c->stmt, SQL_ATTR_APP_ROW_DESC, &ard, 0, NULL);
            if (ard) {
                SQLSMALLINT cnt = 0;
                SQLGetDescField(ard, 0, SQL_DESC_COUNT, &cnt,
                                sizeof(cnt), NULL);
            }
            break;
        }
        case 5: {
            SQLUINTEGER dead = 0;
            SQLGetConnectAttr(c->dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
            SQLGetEnvAttr(c->env, SQL_ATTR_ODBC_VERSION, &dead, 0, NULL);
            break;
        }
        }
    }
    return NULL;
}

static void test_concurrent_handle_access(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    /* Statement allocation requires an open connection; fake the state the
     * way the other backend-less unit tests do. */
    ((argus_dbc_t *)dbc)->connected = true;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    ctx_t ctx[THREADS];
    GThread *th[THREADS];
    for (int t = 0; t < THREADS; t++) {
        ctx[t] = (ctx_t){ env, dbc, stmt, t };
        th[t] = g_thread_new("odbc-hammer", worker, &ctx[t]);
    }
    for (int t = 0; t < THREADS; t++) g_thread_join(th[t]);

    /* The handles must still be coherent after the storm. */
    SQLULEN v = 0;
    assert_int_equal(SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, &v, 0, NULL),
                     SQL_SUCCESS);
    assert_true(v >= 1 && v <= 32);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    ((argus_dbc_t *)dbc)->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Metadata cache: one table per connection, statements on many threads ── */

#define CACHE_ITERS 2000

typedef struct {
    argus_dbc_t  *dbc;
    argus_stmt_t *stmt;
    int           id;
} cache_ctx_t;

/* Give the statement a one-column result of `n` rows, as a catalog function
 * leaves behind before it stores the result. */
static void fill_result(argus_stmt_t *stmt, int n, int tag)
{
    argus_row_cache_free(&stmt->row_cache);
    argus_row_cache_init(&stmt->row_cache);
    stmt->row_cache.rows = calloc((size_t)n, sizeof(argus_row_t));
    stmt->row_cache.num_rows = (size_t)n;
    stmt->row_cache.capacity = (size_t)n;
    stmt->row_cache.num_cols = 1;
    stmt->row_cache.exhausted = true;
    for (int r = 0; r < n; r++) {
        argus_cell_t *cell = calloc(1, sizeof(argus_cell_t));
        char buf[32];
        snprintf(buf, sizeof(buf), "t%d_r%d", tag, r);
        cell->data = strdup(buf);
        cell->data_len = strlen(buf);
        stmt->row_cache.rows[r].cells = cell;
    }
    snprintf((char *)stmt->columns[0].name, sizeof(stmt->columns[0].name),
             "TABLE_NAME");
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->num_cols = 1;
}

static gpointer cache_worker(gpointer data)
{
    cache_ctx_t *c = data;
    char table[32];

    for (int i = 0; i < CACHE_ITERS; i++) {
        /* A handful of keys per thread, shared with no other thread, so a
         * hit can be checked against what this thread stored. */
        snprintf(table, sizeof(table), "t%d_%d", c->id, i % 4);
        fill_result(c->stmt, 1 + (i % 3), c->id);
        argus_metadata_cache_store(c->dbc, c->stmt, "SQLTables",
                                   NULL, "s", table, NULL);

        argus_row_cache_free(&c->stmt->row_cache);
        argus_row_cache_init(&c->stmt->row_cache);
        if (argus_metadata_cache_lookup(c->dbc, c->stmt, "SQLTables",
                                        NULL, "s", table, NULL)) {
            assert_int_equal(c->stmt->num_cols, 1);
            assert_true(c->stmt->row_cache.num_rows >= 1);
            char expect[32];
            snprintf(expect, sizeof(expect), "t%d_r0", c->id);
            assert_string_equal(c->stmt->row_cache.rows[0].cells[0].data,
                                expect);
        }
        /* Every thread also clears now and then, as SQLEndTran does. */
        if (i % 97 == c->id)
            argus_metadata_cache_clear(c->dbc);
    }
    argus_row_cache_free(&c->stmt->row_cache);
    argus_row_cache_init(&c->stmt->row_cache);
    return NULL;
}

/* SQLTables/SQLColumns on several statements of one connection store into
 * and read from the same table; it used to be mutated under the statement
 * locks only, which do not exclude each other. */
static void test_concurrent_metadata_cache(void **state)
{
    (void)state;
    argus_env_t *env = NULL;
    argus_dbc_t *dbc = NULL;
    assert_int_equal(argus_alloc_env(&env), SQL_SUCCESS);
    assert_int_equal(argus_alloc_dbc(env, &dbc), SQL_SUCCESS);
    dbc->connected = true;

    cache_ctx_t ctx[THREADS];
    GThread *th[THREADS];
    for (int t = 0; t < THREADS; t++) {
        ctx[t] = (cache_ctx_t){ dbc, NULL, t };
        assert_int_equal(argus_alloc_stmt(dbc, &ctx[t].stmt), SQL_SUCCESS);
    }
    for (int t = 0; t < THREADS; t++)
        th[t] = g_thread_new("cache-hammer", cache_worker, &ctx[t]);
    for (int t = 0; t < THREADS; t++) g_thread_join(th[t]);

    /* The table is still coherent: a fresh store is found again. */
    fill_result(ctx[0].stmt, 2, 99);
    argus_metadata_cache_store(dbc, ctx[0].stmt, "SQLColumns",
                               NULL, "s", "final", NULL);
    argus_row_cache_free(&ctx[0].stmt->row_cache);
    argus_row_cache_init(&ctx[0].stmt->row_cache);
    assert_true(argus_metadata_cache_lookup(dbc, ctx[0].stmt, "SQLColumns",
                                            NULL, "s", "final", NULL));
    assert_int_equal(ctx[0].stmt->row_cache.num_rows, 2);
    assert_string_equal(ctx[0].stmt->row_cache.rows[1].cells[0].data,
                        "t99_r1");

    for (int t = 0; t < THREADS; t++) argus_free_stmt(ctx[t].stmt);
    dbc->connected = false;
    assert_int_equal(argus_free_dbc(dbc), SQL_SUCCESS);
    argus_free_env(env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_concurrent_handle_access),
        cmocka_unit_test(test_concurrent_metadata_cache),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
