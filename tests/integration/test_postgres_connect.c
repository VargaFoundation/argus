/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/*
 * Integration tests: connect to a real PostgreSQL through BACKEND=postgres.
 * Requires: docker compose -f tests/integration/docker-compose.yml up -d postgres
 *           (or any PostgreSQL seeded with postgres-init/01-seed.sql)
 *
 * Override with PG_HOST / PG_PORT / PG_USER / PG_PASS / PG_DB.
 */

static const char *env_or(const char *key, const char *dflt)
{
    const char *v = getenv(key);
    return (v && *v) ? v : dflt;
}

static void make_conn_str(char *buf, size_t n, const char *extra)
{
    snprintf(buf, n,
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=postgres;Database=%s%s%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
             env_or("PG_DB", "argusdb"),
             extra ? ";" : "", extra ? extra : "");
}

static SQLRETURN connect_with(SQLHENV *env, SQLHDBC *dbc, const char *extra)
{
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, *env, dbc), SQL_SUCCESS);

    char conn_str[512];
    make_conn_str(conn_str, sizeof(conn_str), extra);

    SQLCHAR out[1024];
    SQLSMALLINT out_len;
    return SQLDriverConnect(*dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                            out, sizeof(out), &out_len, SQL_DRIVER_NOPROMPT);
}

static void disconnect(SQLHENV env, SQLHDBC dbc)
{
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── connect / disconnect ────────────────────────────────────── */

static void test_driver_connect(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    assert_int_equal(connect_with(&env, &dbc, NULL), SQL_SUCCESS);
    assert_int_equal(SQLDisconnect(dbc), SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/*
 * The ODBC layer substitutes "default" when DATABASE is absent, which is not a
 * database any PostgreSQL has. The backend has to read that as "unset" — this
 * test is the guard on that, because getting it wrong breaks every DSN that
 * does not name a database.
 */
static void test_connect_without_database(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=postgres",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"));

    SQLCHAR out[1024];
    SQLSMALLINT out_len;
    SQLRETURN rc = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                    out, sizeof(out), &out_len,
                                    SQL_DRIVER_NOPROMPT);
    assert_true(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO);
    disconnect(env, dbc);
}

/* ── SQLGetInfo reports the real server ──────────────────────── */

static void test_dbms_name_and_version(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    assert_int_equal(connect_with(&env, &dbc, NULL), SQL_SUCCESS);

    SQLCHAR name[128] = {0};
    SQLSMALLINT len = 0;
    assert_int_equal(SQLGetInfo(dbc, SQL_DBMS_NAME, name, sizeof(name), &len),
                     SQL_SUCCESS);
    /* The engine's display name from its capability descriptor, not the
     * backend keyword — this is what a BI tool shows the user. */
    assert_string_equal((char *)name, "PostgreSQL");

    SQLCHAR ver[128] = {0};
    assert_int_equal(SQLGetInfo(dbc, SQL_DBMS_VER, ver, sizeof(ver), &len),
                     SQL_SUCCESS);
    /* A real version, not the "unknown" placeholder. */
    assert_string_not_equal((char *)ver, "00.00.0000");

    SQLCHAR quote[8] = {0};
    assert_int_equal(SQLGetInfo(dbc, SQL_IDENTIFIER_QUOTE_CHAR, quote,
                                sizeof(quote), &len), SQL_SUCCESS);
    assert_string_equal((char *)quote, "\"");

    disconnect(env, dbc);
}

/* ── A wrong password must fail with a real SQLSTATE ─────────── */

static void test_bad_password_diagnostics(void **state)
{
    (void)state;
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%s;UID=%s;PWD=definitely-not-the-password;"
             "Backend=postgres;Database=%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_DB", "argusdb"));

    SQLCHAR out[1024];
    SQLSMALLINT out_len;
    SQLRETURN rc = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                    out, sizeof(out), &out_len,
                                    SQL_DRIVER_NOPROMPT);
    assert_int_equal(rc, SQL_ERROR);

    SQLCHAR state_buf[6] = {0}, msg[512] = {0};
    SQLINTEGER native = 0;
    SQLSMALLINT msg_len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state_buf, &native,
                                   msg, sizeof(msg), &msg_len), SQL_SUCCESS);
    /* Connection-level failure: ODBC's "client unable to establish
     * connection", and the server's own words in the message. */
    assert_string_equal((char *)state_buf, "08001");
    assert_true(msg[0] != '\0');

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── SSL=0 must really mean plaintext, not libpq's "prefer" ──── */

static void test_ssl_disabled_connects(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    assert_int_equal(connect_with(&env, &dbc, "SSL=0"), SQL_SUCCESS);
    disconnect(env, dbc);
}

/*
 * BACKEND=greenplum against a plain PostgreSQL.
 *
 * People do this — a DSN copied between environments, a test cluster that is
 * not really Greenplum. It must not fail: the connection is perfectly usable
 * and only the engine-specific half of the catalog is missing. What it must do
 * is say so, once, with SQL_SUCCESS_WITH_INFO and a 01000 record, and then
 * behave like the PostgreSQL backend rather than emitting SQL against
 * gp_distribution_policy and failing every SQLTables call.
 */
static void test_mpp_backend_against_plain_postgres(void **state)
{
    (void)state;

    const char *backends[] = { "greenplum", "cloudberry" };
    for (size_t i = 0; i < sizeof(backends) / sizeof(backends[0]); i++) {
        SQLHENV env = SQL_NULL_HENV;
        SQLHDBC dbc = SQL_NULL_HDBC;

        assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env),
                         SQL_SUCCESS);
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
        assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc), SQL_SUCCESS);

        char conn_str[512];
        snprintf(conn_str, sizeof(conn_str),
                 "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=%s;Database=%s",
                 env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
                 env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
                 backends[i], env_or("PG_DB", "argusdb"));

        SQLCHAR out[1024];
        SQLSMALLINT out_len;
        SQLRETURN rc = SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                        out, sizeof(out), &out_len,
                                        SQL_DRIVER_NOPROMPT);
        assert_int_equal(rc, SQL_SUCCESS_WITH_INFO);

        SQLCHAR state_buf[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0;
        SQLSMALLINT msg_len = 0;
        assert_int_equal(SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state_buf,
                                       &native, msg, sizeof(msg), &msg_len),
                         SQL_SUCCESS);
        assert_string_equal((char *)state_buf, "01000");
        assert_non_null(strstr((char *)msg, "PostgreSQL"));

        /* The catalog must still work, degraded to plain-PostgreSQL rules —
         * still hiding the 24 partition children. */
        SQLHSTMT stmt = SQL_NULL_HSTMT;
        assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
        assert_int_equal(SQLTables(stmt, NULL, 0,
                                   (SQLCHAR *)"argus_test", SQL_NTS,
                                   (SQLCHAR *)"events%", SQL_NTS,
                                   NULL, 0), SQL_SUCCESS);
        long n = 0;
        while (SQLFetch(stmt) == SQL_SUCCESS) n++;
        assert_int_equal(n, 1);

        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        disconnect(env, dbc);
    }
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_driver_connect),
        cmocka_unit_test(test_connect_without_database),
        cmocka_unit_test(test_dbms_name_and_version),
        cmocka_unit_test(test_bad_password_diagnostics),
        cmocka_unit_test(test_ssl_disabled_connects),
        cmocka_unit_test(test_mpp_backend_against_plain_postgres),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
