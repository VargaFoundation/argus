#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/*
 * Helper: create a connected-looking DBC for SQLGetInfo tests.
 * SQLGetInfo doesn't actually require a live backend connection.
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
    return dbc;
}

static void free_test_dbc(argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

/* ── Test: Driver name ───────────────────────────────────────── */

static void test_driver_name(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[256];
    SQLSMALLINT len;
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_DRIVER_NAME,
                                buf, sizeof(buf), &len);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "libargus_odbc.so");

    free_test_dbc(dbc);
}

/* ── Test: DBMS name ─────────────────────────────────────────── */

static void test_dbms_name(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[256];
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_DBMS_NAME,
                                buf, sizeof(buf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "Apache Hive");

    free_test_dbc(dbc);
}

/* ── Test: Server name ───────────────────────────────────────── */

static void test_server_name(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[256];
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_SERVER_NAME,
                                buf, sizeof(buf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "testhost");

    free_test_dbc(dbc);
}

/* ── Test: User name ─────────────────────────────────────────── */

static void test_user_name(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[256];
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_USER_NAME,
                                buf, sizeof(buf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "testuser");

    free_test_dbc(dbc);
}

/* ── Test: Identifier quote char ─────────────────────────────── */

static void test_identifier_quote(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[32];
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_IDENTIFIER_QUOTE_CHAR,
                                buf, sizeof(buf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "`");

    free_test_dbc(dbc);
}

/* ── Test: Cursor capabilities ───────────────────────────────── */

static void test_scroll_options(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLUINTEGER scroll_opts = 0;
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_SCROLL_OPTIONS,
                                &scroll_opts, sizeof(scroll_opts), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(scroll_opts, SQL_SO_FORWARD_ONLY);

    free_test_dbc(dbc);
}

/* ── Test: Transaction support ───────────────────────────────── */

static void test_txn_capable(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLUSMALLINT txn_capable = 0;
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_TXN_CAPABLE,
                                &txn_capable, sizeof(txn_capable), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(txn_capable, SQL_TC_NONE);

    free_test_dbc(dbc);
}

/* ── Test: SQLGetFunctions - individual function check ───────── */

static void test_get_functions_individual(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLUSMALLINT supported = SQL_FALSE;

    SQLRETURN ret = SQLGetFunctions((SQLHDBC)dbc,
                                     SQL_API_SQLCONNECT, &supported);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(supported, SQL_TRUE);

    ret = SQLGetFunctions((SQLHDBC)dbc,
                           SQL_API_SQLDRIVERCONNECT, &supported);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(supported, SQL_TRUE);

    ret = SQLGetFunctions((SQLHDBC)dbc,
                           SQL_API_SQLEXECDIRECT, &supported);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(supported, SQL_TRUE);

    free_test_dbc(dbc);
}

/* ── Test: SQLGetFunctions - unsupported function ────────────── */

static void test_get_functions_unsupported(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLUSMALLINT supported = SQL_TRUE;
    SQLRETURN ret = SQLGetFunctions((SQLHDBC)dbc, 9999, &supported);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(supported, SQL_FALSE);

    free_test_dbc(dbc);
}

/* ── Test: ODBC version ──────────────────────────────────────── */

static void test_odbc_ver(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[32];
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_DRIVER_ODBC_VER,
                                buf, sizeof(buf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "03.80");

    free_test_dbc(dbc);
}

/* ── Test: Database name ─────────────────────────────────────── */

static void test_database_name(void **state)
{
    (void)state;

    argus_dbc_t *dbc = create_test_dbc();

    SQLCHAR buf[256];
    SQLRETURN ret = SQLGetInfo((SQLHDBC)dbc, SQL_DATABASE_NAME,
                                buf, sizeof(buf), NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)buf, "testdb");

    free_test_dbc(dbc);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_name),
        cmocka_unit_test(test_dbms_name),
        cmocka_unit_test(test_server_name),
        cmocka_unit_test(test_user_name),
        cmocka_unit_test(test_identifier_quote),
        cmocka_unit_test(test_scroll_options),
        cmocka_unit_test(test_txn_capable),
        cmocka_unit_test(test_get_functions_individual),
        cmocka_unit_test(test_get_functions_unsupported),
        cmocka_unit_test(test_odbc_ver),
        cmocka_unit_test(test_database_name),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
