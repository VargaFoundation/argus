/* SPDX-License-Identifier: Apache-2.0 */
/*
 * The driver's side of Driver-Manager connection pooling.
 *
 * The driver no longer pools connections itself: SQL_ATTR_CONNECTION_POOLING
 * on the environment is the Driver Manager's business, and every
 * SQLDriverConnect authenticates against the backend. What the driver owes
 * the Driver Manager's pool is two things — a truthful SQL_ATTR_CONNECTION_DEAD
 * (backed by the backend's probe) and SQL_ATTR_RESET_CONNECTION (ODBC 3.8),
 * which clears session state before a connection is parked.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include "argus/handle.h"
#include "argus/backend.h"

#ifndef SQL_ATTR_RESET_CONNECTION
#define SQL_ATTR_RESET_CONNECTION 116
#define SQL_RESET_CONNECTION_YES  1UL
#endif

static int  g_connect_calls;
static int  g_disconnect_calls;
static int  g_reset_calls;
static bool g_alive = true;
static bool g_reset_ok = true;

static int fake_connect(argus_dbc_t *dbc, const char *host, int port,
                        const char *username, const char *password,
                        const char *database, const char *auth_mechanism,
                        argus_backend_conn_t *out_conn)
{
    (void)dbc; (void)host; (void)port; (void)username;
    (void)database; (void)auth_mechanism;
    g_connect_calls++;
    if (!password || strcmp(password, "right") != 0) return -1;
    *out_conn = (argus_backend_conn_t)(uintptr_t)0xD00D;
    return 0;
}

static void fake_disconnect(argus_backend_conn_t conn)
{
    (void)conn;
    g_disconnect_calls++;
}

static bool fake_is_alive(argus_backend_conn_t conn)
{
    (void)conn;
    return g_alive;
}

static bool fake_reset_session(argus_backend_conn_t conn)
{
    (void)conn;
    g_reset_calls++;
    return g_reset_ok;
}

static bool fake_get_last_error(argus_backend_conn_t conn, char *buf, size_t n)
{
    (void)conn; (void)buf; (void)n;
    return false;
}

static const argus_backend_t fake_backend = {
    .name           = "dmpool",
    .connect        = fake_connect,
    .disconnect     = fake_disconnect,
    .is_alive       = fake_is_alive,
    .reset_session  = fake_reset_session,
    .get_last_error = fake_get_last_error,
};

/* Same engine without a reset hook: nothing to clear, and it says so. */
static const argus_backend_t bare_backend = {
    .name       = "dmpool-bare",
    .connect    = fake_connect,
    .disconnect = fake_disconnect,
    .is_alive   = fake_is_alive,
};

static int group_setup(void **state)
{
    (void)state;
    argus_backend_register(&fake_backend);
    argus_backend_register(&bare_backend);
    return 0;
}

static int test_setup(void **state)
{
    (void)state;
    g_connect_calls = g_disconnect_calls = g_reset_calls = 0;
    g_alive = true;
    g_reset_ok = true;
    return 0;
}

static SQLHENV pooled_env(void)
{
    SQLHENV env = SQL_NULL_HENV;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_SUCCESS);
    /* What unixODBC and Tableau set; used to switch the driver's own pool on. */
    assert_int_equal(SQLSetEnvAttr(env, SQL_ATTR_CONNECTION_POOLING,
                                   (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0),
                     SQL_SUCCESS);
    return env;
}

static SQLRETURN connect_with(SQLHDBC dbc, const char *connstr)
{
    return SQLDriverConnect(dbc, NULL, (SQLCHAR *)connstr, SQL_NTS,
                            NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
}

static void assert_sqlstate(SQLHDBC dbc, const char *expected)
{
    SQLCHAR state[6] = {0};
    SQLINTEGER native = 0;
    SQLCHAR msg[256];
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, &native,
                                   msg, sizeof(msg), &len), SQL_SUCCESS);
    assert_string_equal((const char *)state, expected);
}

/* The bug this replaces: with pooling on, a second SQLDriverConnect to the
 * same host/user was served from the driver's cache without a password
 * check, so a wrong password got somebody else's authenticated session. */
static void test_every_connect_authenticates(void **state)
{
    (void)state;
    SQLHENV env = pooled_env();
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    assert_int_equal(connect_with(dbc, "BACKEND=dmpool;HOST=h;UID=u;PWD=right"),
                     SQL_SUCCESS);
    assert_int_equal(g_connect_calls, 1);
    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    /* Disconnect closes the session instead of parking it. */
    assert_int_equal(g_disconnect_calls, 1);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    assert_int_equal(connect_with(dbc, "BACKEND=dmpool;HOST=h;UID=u;PWD=wrong"),
                     SQL_ERROR);
    assert_int_equal(g_connect_calls, 2);
    SQLUINTEGER dead = SQL_CD_FALSE;
    SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
    assert_int_equal(dead, SQL_CD_TRUE);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);

    /* The right password still connects, and again through the backend. */
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    assert_int_equal(connect_with(dbc, "BACKEND=dmpool;HOST=h;UID=u;PWD=right"),
                     SQL_SUCCESS);
    assert_int_equal(g_connect_calls, 3);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_connection_dead_asks_the_backend(void **state)
{
    (void)state;
    SQLHENV env = pooled_env();
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    SQLUINTEGER dead = 42;
    SQLINTEGER len = 0;
    assert_int_equal(SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead,
                                       0, &len), SQL_SUCCESS);
    assert_int_equal(dead, SQL_CD_TRUE);
    assert_int_equal(len, sizeof(SQLUINTEGER));

    assert_int_equal(connect_with(dbc, "BACKEND=dmpool;HOST=h;PWD=right"),
                     SQL_SUCCESS);
    SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
    assert_int_equal(dead, SQL_CD_FALSE);

    /* The server dropped the session while the connection sat parked: the
     * connected flag is still set, and the probe is what tells the truth. */
    g_alive = false;
    SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
    assert_int_equal(dead, SQL_CD_TRUE);
    g_alive = true;
    SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
    assert_int_equal(dead, SQL_CD_FALSE);

    SQLDisconnect(dbc);
    SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
    assert_int_equal(dead, SQL_CD_TRUE);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_reset_connection_runs_the_hook(void **state)
{
    (void)state;
    SQLHENV env = pooled_env();
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    /* Nothing to reset before a connection exists. */
    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_RESET_CONNECTION,
                                       (SQLPOINTER)SQL_RESET_CONNECTION_YES, 0),
                     SQL_ERROR);
    assert_sqlstate(dbc, "08003");

    assert_int_equal(connect_with(dbc, "BACKEND=dmpool;HOST=h;PWD=right"),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_RESET_CONNECTION,
                                       (SQLPOINTER)SQL_RESET_CONNECTION_YES, 0),
                     SQL_SUCCESS);
    assert_int_equal(g_reset_calls, 1);

    /* Only SQL_RESET_CONNECTION_YES is defined. */
    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_RESET_CONNECTION,
                                       (SQLPOINTER)0, 0), SQL_ERROR);
    assert_sqlstate(dbc, "HY024");
    assert_int_equal(g_reset_calls, 1);

    /* A reset that fails must not hand the connection to the next borrower:
     * SQL_ERROR is what makes the Driver Manager discard it. */
    g_reset_ok = false;
    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_RESET_CONNECTION,
                                       (SQLPOINTER)SQL_RESET_CONNECTION_YES, 0),
                     SQL_ERROR);
    assert_sqlstate(dbc, "HY000");
    assert_int_equal(g_reset_calls, 2);

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void test_reset_connection_without_hook_succeeds(void **state)
{
    (void)state;
    SQLHENV env = pooled_env();
    SQLHDBC dbc = SQL_NULL_HDBC;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);
    assert_int_equal(connect_with(dbc, "BACKEND=dmpool-bare;HOST=h;PWD=right"),
                     SQL_SUCCESS);
    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_RESET_CONNECTION,
                                       (SQLPOINTER)SQL_RESET_CONNECTION_YES, 0),
                     SQL_SUCCESS);
    assert_int_equal(g_reset_calls, 0);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup(test_every_connect_authenticates, test_setup),
        cmocka_unit_test_setup(test_connection_dead_asks_the_backend, test_setup),
        cmocka_unit_test_setup(test_reset_connection_runs_the_hook, test_setup),
        cmocka_unit_test_setup(test_reset_connection_without_hook_succeeds, test_setup),
    };
    return cmocka_run_group_tests(tests, group_setup, NULL);
}
