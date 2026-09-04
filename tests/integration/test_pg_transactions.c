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
#include <stdbool.h>

/*
 * Transactions, against a real PostgreSQL.
 *
 * The PostgreSQL family is the first Argus backend that reports
 * SQL_TXN_CAPABLE = SQL_TC_ALL, so this is where the driver has to mean it.
 * Two connections are used throughout: the only way to prove a transaction is
 * isolated is to have somebody else fail to see it.
 *
 * The pooling test at the end is the one that matters most. A connection
 * parked mid-transaction poisons whoever borrows it next, and the symptom —
 * every statement failing with 25P02 on a connection the application never
 * touched — is close to undiagnosable from the application's side.
 */

static const char *env_or(const char *key, const char *dflt)
{
    const char *v = getenv(key);
    return (v && *v) ? v : dflt;
}

static void conn_str(char *buf, size_t n)
{
    snprintf(buf, n,
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=postgres;Database=%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
             env_or("PG_DB", "argusdb"));
}

static void open_conn(SQLHENV *env, SQLHDBC *dbc)
{
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, *env, dbc), SQL_SUCCESS);

    char cs[512];
    conn_str(cs, sizeof(cs));
    SQLRETURN rc = SQLDriverConnect(*dbc, NULL, (SQLCHAR *)cs, SQL_NTS,
                                    NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    assert_true(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO);
}

static void close_conn(SQLHENV env, SQLHDBC dbc)
{
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void exec_ok(SQLHDBC dbc, const char *sql)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    SQLRETURN rc = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0; SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &len);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("failed: %s %s\n  SQL: %s", st, msg, sql);
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

static long scalar(SQLHDBC dbc, const char *sql)
{
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    SQLRETURN rc = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR st[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0; SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, st, &native, msg, sizeof(msg), &len);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        fail_msg("failed: %s %s\n  SQL: %s", st, msg, sql);
    }
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLINTEGER v = 0; SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_SLONG, &v, sizeof(v), &ind);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return v;
}

static void set_autocommit(SQLHDBC dbc, SQLUINTEGER mode)
{
    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT,
                                       (SQLPOINTER)(uintptr_t)mode, 0),
                     SQL_SUCCESS);
}

static int setup(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);
    exec_ok(dbc, "DROP TABLE IF EXISTS argus_test.txn_probe");
    exec_ok(dbc, "CREATE TABLE argus_test.txn_probe (id integer)");
    close_conn(env, dbc);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);
    exec_ok(dbc, "DROP TABLE IF EXISTS argus_test.txn_probe");
    close_conn(env, dbc);
    return 0;
}

static void clear_probe(void)
{
    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);
    exec_ok(dbc, "DELETE FROM argus_test.txn_probe");
    close_conn(env, dbc);
}

/* ── SQLGetInfo now tells the truth ──────────────────────────── */

static void test_txn_capable_is_reported(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);

    SQLUSMALLINT tc = 0;
    assert_int_equal(SQLGetInfo(dbc, SQL_TXN_CAPABLE, &tc, sizeof(tc), NULL),
                     SQL_SUCCESS);
    assert_int_equal(tc, SQL_TC_ALL);

    SQLUINTEGER iso = 0;
    assert_int_equal(SQLGetInfo(dbc, SQL_DEFAULT_TXN_ISOLATION, &iso,
                                sizeof(iso), NULL), SQL_SUCCESS);
    assert_int_equal(iso, SQL_TXN_READ_COMMITTED);

    close_conn(env, dbc);
}

/* ── COMMIT makes the work visible; nothing before it does ───── */

static void test_commit_is_visible_only_after_commit(void **state)
{
    (void)state;
    clear_probe();

    SQLHENV env1, env2; SQLHDBC w, r;
    open_conn(&env1, &w);
    open_conn(&env2, &r);

    set_autocommit(w, SQL_AUTOCOMMIT_OFF);
    exec_ok(w, "INSERT INTO argus_test.txn_probe VALUES (1), (2), (3)");

    /* The writer sees its own uncommitted rows; the reader must not. */
    assert_int_equal(scalar(w, "SELECT count(*) FROM argus_test.txn_probe"), 3);
    assert_int_equal(scalar(r, "SELECT count(*) FROM argus_test.txn_probe"), 0);

    assert_int_equal(SQLEndTran(SQL_HANDLE_DBC, w, SQL_COMMIT), SQL_SUCCESS);
    assert_int_equal(scalar(r, "SELECT count(*) FROM argus_test.txn_probe"), 3);

    close_conn(env1, w);
    close_conn(env2, r);
}

static void test_rollback_discards(void **state)
{
    (void)state;
    clear_probe();

    SQLHENV env1, env2; SQLHDBC w, r;
    open_conn(&env1, &w);
    open_conn(&env2, &r);

    set_autocommit(w, SQL_AUTOCOMMIT_OFF);
    exec_ok(w, "INSERT INTO argus_test.txn_probe VALUES (9)");
    assert_int_equal(SQLEndTran(SQL_HANDLE_DBC, w, SQL_ROLLBACK), SQL_SUCCESS);

    assert_int_equal(scalar(w, "SELECT count(*) FROM argus_test.txn_probe"), 0);
    assert_int_equal(scalar(r, "SELECT count(*) FROM argus_test.txn_probe"), 0);

    close_conn(env1, w);
    close_conn(env2, r);
}

/* ODBC: switching autocommit back ON commits whatever is open. Discarding it
 * instead would silently lose the application's work. */
static void test_autocommit_on_commits_open_transaction(void **state)
{
    (void)state;
    clear_probe();

    SQLHENV env1, env2; SQLHDBC w, r;
    open_conn(&env1, &w);
    open_conn(&env2, &r);

    set_autocommit(w, SQL_AUTOCOMMIT_OFF);
    exec_ok(w, "INSERT INTO argus_test.txn_probe VALUES (7)");
    assert_int_equal(scalar(r, "SELECT count(*) FROM argus_test.txn_probe"), 0);

    set_autocommit(w, SQL_AUTOCOMMIT_ON);
    assert_int_equal(scalar(r, "SELECT count(*) FROM argus_test.txn_probe"), 1);

    close_conn(env1, w);
    close_conn(env2, r);
}

/* With autocommit on (the default) each statement stands alone. */
static void test_autocommit_on_is_the_default(void **state)
{
    (void)state;
    clear_probe();

    SQLHENV env1, env2; SQLHDBC w, r;
    open_conn(&env1, &w);
    open_conn(&env2, &r);

    SQLUINTEGER mode = 0;
    assert_int_equal(SQLGetConnectAttr(w, SQL_ATTR_AUTOCOMMIT, &mode,
                                       sizeof(mode), NULL), SQL_SUCCESS);
    assert_int_equal(mode, SQL_AUTOCOMMIT_ON);

    exec_ok(w, "INSERT INTO argus_test.txn_probe VALUES (5)");
    assert_int_equal(scalar(r, "SELECT count(*) FROM argus_test.txn_probe"), 1);

    close_conn(env1, w);
    close_conn(env2, r);
}

/*
 * A failed statement puts PostgreSQL in an aborted transaction where
 * everything but ROLLBACK fails with 25P02. The driver must let the
 * application out of it, and the connection must be usable afterwards.
 */
static void test_aborted_transaction_recovers(void **state)
{
    (void)state;
    clear_probe();

    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);
    set_autocommit(dbc, SQL_AUTOCOMMIT_OFF);

    exec_ok(dbc, "INSERT INTO argus_test.txn_probe VALUES (1)");

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(stmt,
        (SQLCHAR *)"SELECT * FROM definitely_not_a_table", SQL_NTS), SQL_ERROR);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    assert_int_equal(SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK), SQL_SUCCESS);

    /* Out of the aborted state, and the insert is gone with it. */
    assert_int_equal(scalar(dbc, "SELECT count(*) FROM argus_test.txn_probe"), 0);

    close_conn(env, dbc);
}

/* SQLEndTran with nothing open is legal and common. */
static void test_end_tran_without_transaction(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);
    assert_int_equal(SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT), SQL_SUCCESS);
    assert_int_equal(SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK), SQL_SUCCESS);
    close_conn(env, dbc);
}

static void test_isolation_level_round_trip(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc;
    open_conn(&env, &dbc);

    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_TXN_ISOLATION,
                                       (SQLPOINTER)(uintptr_t)SQL_TXN_SERIALIZABLE,
                                       0), SQL_SUCCESS);

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(stmt,
        (SQLCHAR *)"SHOW default_transaction_isolation", SQL_NTS), SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    char buf[64] = {0}; SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    assert_string_equal(buf, "serializable");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    close_conn(env, dbc);
}

/*
 * The pooling hazard.
 *
 * A connection handed back to a pool with a transaction still open, or with a
 * modified search_path, poisons the next borrower. The Driver Manager's pool
 * asks the driver to clean a connection before parking it with
 * SQL_ATTR_RESET_CONNECTION (ODBC 3.8). Dirty the session, ask for the reset,
 * and check the connection is clean: no uncommitted rows, no leftover session
 * state, and a statement that simply works.
 */
#ifndef SQL_ATTR_RESET_CONNECTION
#define SQL_ATTR_RESET_CONNECTION 116
#define SQL_RESET_CONNECTION_YES  1UL
#endif

static void test_reset_connection_cleans_the_session(void **state)
{
    (void)state;
    clear_probe();

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    open_conn(&env, &dbc);

    /* Dirty the session the way an application returning a connection to
     * the pool mid-work would. */
    set_autocommit(dbc, SQL_AUTOCOMMIT_OFF);
    exec_ok(dbc, "INSERT INTO argus_test.txn_probe VALUES (42)");
    exec_ok(dbc, "SET search_path TO pg_catalog");

    assert_int_equal(SQLSetConnectAttr(dbc, SQL_ATTR_RESET_CONNECTION,
                                       (SQLPOINTER)SQL_RESET_CONNECTION_YES, 0),
                     SQL_SUCCESS);

    /* The uncommitted insert was rolled back, not inherited. */
    assert_int_equal(scalar(dbc, "SELECT count(*) FROM argus_test.txn_probe"), 0);

    /* And the session state went with it: search_path is back to the default,
     * so an unqualified name resolves the way the next application expects. */
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLExecDirect(stmt, (SQLCHAR *)"SHOW search_path", SQL_NTS),
                     SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    char sp[128] = {0}; SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, sp, sizeof(sp), &ind);
    assert_null(strstr(sp, "pg_catalog,"));
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    /* Autocommit is back on: a statement commits by itself. */
    exec_ok(dbc, "INSERT INTO argus_test.txn_probe VALUES (7)");
    assert_int_equal(scalar(dbc, "SELECT count(*) FROM argus_test.txn_probe"), 1);

    close_conn(env, dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_txn_capable_is_reported),
        cmocka_unit_test(test_commit_is_visible_only_after_commit),
        cmocka_unit_test(test_rollback_discards),
        cmocka_unit_test(test_autocommit_on_commits_open_transaction),
        cmocka_unit_test(test_autocommit_on_is_the_default),
        cmocka_unit_test(test_aborted_transaction_recovers),
        cmocka_unit_test(test_end_tran_without_transaction),
        cmocka_unit_test(test_isolation_level_round_trip),
        cmocka_unit_test(test_reset_connection_cleans_the_session),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
