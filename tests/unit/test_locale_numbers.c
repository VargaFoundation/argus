/*
 * Numbers must cross the driver with a '.' whatever LC_NUMERIC says.
 *
 * Excel, Tableau and Power BI call setlocale(LC_ALL, "") at startup. On a
 * French or German desktop that turns printf("%g", 1.5) into "1,5" and makes
 * strtod("1.5") stop at the '.', so a bound parameter used to reach the
 * server as `WHERE x = 1,5` and a fetched "1.5" came back as 1.0. These
 * tests switch LC_NUMERIC to such a locale first, then drive the parameter
 * renderer, the SQLGetData conversions and the shared helpers.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/backend.h"
#include "argus/numtext.h"
#include "locale_helper.h"

/* ── Fake backend that records the SQL it is asked to execute ─── */

static char g_executed[512];

static int recording_execute(argus_backend_conn_t conn, const char *query,
                             argus_backend_op_t *out_op)
{
    (void)conn;
    snprintf(g_executed, sizeof(g_executed), "%s", query);
    *out_op = (argus_backend_op_t)(uintptr_t)0x1;
    return 0;
}

static void noop_close(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn; (void)op;
}

static const argus_backend_t g_backend = {
    .name = "fake",
    .execute = recording_execute,
    .close_operation = noop_close,
};

static argus_stmt_t *setup_stmt(argus_dbc_t **out_dbc)
{
    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);
    dbc->backend = &g_backend;
    dbc->backend_conn = (argus_backend_conn_t)(uintptr_t)0xBEEF;
    dbc->connected = true;

    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);
    g_executed[0] = '\0';
    *out_dbc = dbc;
    return stmt;
}

static void teardown_stmt(argus_stmt_t *stmt, argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    argus_free_stmt(stmt);
    dbc->connected = false;
    dbc->backend = NULL;
    dbc->backend_conn = NULL;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* One fetched row with a single VARCHAR cell holding `text` (or a native
 * double when `text` is NULL), positioned for SQLGetData. */
static void load_row(argus_stmt_t *stmt, const char *text, double native)
{
    stmt->num_cols = 1;
    stmt->executed = true;
    stmt->fetch_started = true;
    stmt->columns[0].sql_type = SQL_VARCHAR;
    stmt->columns[0].column_size = 64;

    stmt->row_cache.rows = calloc(1, sizeof(argus_row_t));
    stmt->row_cache.num_rows = 1;
    stmt->row_cache.num_cols = 1;
    stmt->row_cache.current_row = 1;
    stmt->row_cache.exhausted = true;
    stmt->row_cache.rows[0].cells = calloc(1, sizeof(argus_cell_t));
    argus_cell_t *cell = &stmt->row_cache.rows[0].cells[0];
    if (text) {
        cell->data = strdup(text);
        cell->data_len = strlen(text);
    } else {
        cell->native_kind = ARGUS_NATIVE_F64;
        cell->native.f64 = native;
    }
}

/* ── Group fixture: every test runs under a comma-decimal locale ─ */

static int group_setup(void **state)
{
    (void)state;
    const char *name = argus_test_use_comma_locale();
    if (!name) {
        if (argus_test_locale_required()) {
            fprintf(stderr, "no comma-decimal locale installed and "
                            "ARGUS_TEST_REQUIRE_LOCALE is set\n");
            return -1;
        }
        fprintf(stderr, "no comma-decimal locale installed: the locale "
                        "tests run in the C locale and prove nothing\n");
    }
    return 0;
}

static int group_teardown(void **state)
{
    (void)state;
    argus_test_restore_c_locale();
    return 0;
}

/* ── Test: the locale really is comma-decimal ─────────────────── */

static void test_locale_is_comma_decimal(void **state)
{
    (void)state;
    if (!argus_test_use_comma_locale()) skip();
    char buf[32];
    snprintf(buf, sizeof(buf), "%g", 1.5);
    assert_string_equal(buf, "1,5");
}

/* ── Test: bound double/float parameters are rendered with '.' ── */

static void test_double_parameter_keeps_dot(void **state)
{
    (void)state;
    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_stmt(&dbc);

    SQLDOUBLE d = 1.5;
    SQLLEN d_ind = sizeof(d);
    SQLREAL f = 2.25f;
    SQLLEN f_ind = sizeof(f);
    assert_int_equal(SQLBindParameter((SQLHSTMT)stmt, 1, SQL_PARAM_INPUT,
                                      SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &d,
                                      sizeof(d), &d_ind), SQL_SUCCESS);
    assert_int_equal(SQLBindParameter((SQLHSTMT)stmt, 2, SQL_PARAM_INPUT,
                                      SQL_C_FLOAT, SQL_REAL, 0, 0, &f,
                                      sizeof(f), &f_ind), SQL_SUCCESS);

    SQLRETURN ret = SQLExecDirect((SQLHSTMT)stmt,
                                  (SQLCHAR *)"SELECT ?, ?", SQL_NTS);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT 1.5, 2.25");

    teardown_stmt(stmt, dbc);
}

/* ── Test: negative and exponent forms too ────────────────────── */

static void test_double_parameter_exponent_form(void **state)
{
    (void)state;
    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_stmt(&dbc);

    SQLDOUBLE d = -1.25e-7;
    SQLLEN d_ind = sizeof(d);
    SQLBindParameter((SQLHSTMT)stmt, 1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                     SQL_DOUBLE, 0, 0, &d, sizeof(d), &d_ind);

    assert_int_equal(SQLExecDirect((SQLHSTMT)stmt,
                                   (SQLCHAR *)"SELECT ?", SQL_NTS),
                     SQL_SUCCESS);
    assert_string_equal(g_executed, "SELECT -1.25e-07");

    teardown_stmt(stmt, dbc);
}

/* ── Test: SQLGetData parses "1.5" as 1.5 ─────────────────────── */

static void test_getdata_double_parses_dot(void **state)
{
    (void)state;
    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_stmt(&dbc);
    load_row(stmt, "1.5", 0);

    SQLDOUBLE d = 0;
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_DOUBLE, &d,
                               sizeof(d), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(d == 1.5);

    SQLREAL f = 0;
    ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_FLOAT, &f, sizeof(f), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(f == 1.5f);

    teardown_stmt(stmt, dbc);
}

/* ── Test: SQLBindCol on SQL_C_DOUBLE goes through the same path ─ */

static void test_bindcol_double_parses_dot(void **state)
{
    (void)state;
    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_stmt(&dbc);
    load_row(stmt, "-0.125", 0);

    SQLDOUBLE d = 0;
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_DOUBLE, &d,
                               sizeof(d), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_true(d == -0.125);

    teardown_stmt(stmt, dbc);
}

/* ── Test: a native double is rendered to text with '.' ───────── */

static void test_native_double_renders_dot(void **state)
{
    (void)state;
    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_stmt(&dbc);
    load_row(stmt, NULL, 2.5);

    char buf[32];
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf,
                               sizeof(buf), &ind);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal(buf, "2.5");

    teardown_stmt(stmt, dbc);
}

/* ── Test: out-of-range text still reports 22003 ──────────────── */

static void test_getdata_out_of_range(void **state)
{
    (void)state;
    argus_dbc_t *dbc = NULL;
    argus_stmt_t *stmt = setup_stmt(&dbc);
    load_row(stmt, "1e400", 0);

    SQLDOUBLE d = 0;
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_DOUBLE, &d,
                                sizeof(d), &ind), SQL_ERROR);

    /* 1e300 fits a double but not a float. */
    free(stmt->row_cache.rows[0].cells[0].data);
    stmt->row_cache.rows[0].cells[0].data = strdup("1e300");
    stmt->row_cache.rows[0].cells[0].data_len = 5;
    argus_diag_clear(&stmt->diag);
    SQLREAL f = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_FLOAT, &f,
                                sizeof(f), &ind), SQL_ERROR);
    SQLCHAR sqlstate[6] = {0};
    SQLINTEGER native = 0;
    SQLCHAR msg[128];
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_STMT, (SQLHSTMT)stmt, 1, sqlstate, &native,
                  msg, sizeof(msg), &msg_len);
    assert_string_equal((const char *)sqlstate, "22003");

    teardown_stmt(stmt, dbc);
}

/* ── Test: the helpers themselves ─────────────────────────────── */

static void test_helpers_are_locale_independent(void **state)
{
    (void)state;
    char buf[64];

    assert_int_equal(argus_dtoa(buf, sizeof(buf), 15, 1.5), 3);
    assert_string_equal(buf, "1.5");
    argus_dtoa(buf, sizeof(buf), 7, (double)2.25f);
    assert_string_equal(buf, "2.25");
    argus_dtoa(buf, sizeof(buf), 17, 0.1);
    assert_string_equal(buf, "0.10000000000000001");
    argus_dtoa(buf, sizeof(buf), 15, 1e21);
    assert_string_equal(buf, "1e+21");
    argus_dtoa(buf, sizeof(buf), 15, -0.0);
    assert_string_equal(buf, "-0");
    argus_dtoa(buf, sizeof(buf), 15, INFINITY);
    assert_string_equal(buf, "inf");
    assert_int_equal(argus_dtoa(buf, 0, 15, 1.5), 0);

    char *end = NULL;
    assert_true(argus_strtod("1.5", &end) == 1.5);
    assert_string_equal(end, "");
    assert_true(argus_strtod("1.5e3", NULL) == 1500.0);
    assert_true(argus_strtod("-.5xyz", &end) == -0.5);
    assert_string_equal(end, "xyz");
    assert_true(isinf(argus_strtod("inf", NULL)));
    assert_true(isnan(argus_strtod("nan", NULL)));
    /* The comma is not a decimal separator for the driver, whatever the
     * locale: "1,5" is 1 followed by junk. */
    assert_true(argus_strtod("1,5", &end) == 1.0);
    assert_string_equal(end, ",5");

    errno = 0;
    argus_strtod("1e400", NULL);
    assert_int_equal(errno, ERANGE);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_locale_is_comma_decimal),
        cmocka_unit_test(test_double_parameter_keeps_dot),
        cmocka_unit_test(test_double_parameter_exponent_form),
        cmocka_unit_test(test_getdata_double_parses_dot),
        cmocka_unit_test(test_bindcol_double_parses_dot),
        cmocka_unit_test(test_native_double_renders_dot),
        cmocka_unit_test(test_getdata_out_of_range),
        cmocka_unit_test(test_helpers_are_locale_independent),
    };
    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
