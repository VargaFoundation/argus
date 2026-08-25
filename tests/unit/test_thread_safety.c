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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_concurrent_handle_access),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
