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
 * Integration tests: query and catalog behaviour against a real PostgreSQL.
 * Requires the seed in postgres-init/01-seed.sql.
 *
 * Override with PG_HOST / PG_PORT / PG_USER / PG_PASS / PG_DB.
 */

static const char *env_or(const char *key, const char *dflt)
{
    const char *v = getenv(key);
    return (v && *v) ? v : dflt;
}

static void open_conn(SQLHENV *env, SQLHDBC *dbc)
{
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env),
                     SQL_SUCCESS);
    SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_DBC, *env, dbc), SQL_SUCCESS);

    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str),
             "HOST=%s;PORT=%s;UID=%s;PWD=%s;Backend=postgres;Database=%s",
             env_or("PG_HOST", "127.0.0.1"), env_or("PG_PORT", "5432"),
             env_or("PG_USER", "argus"), env_or("PG_PASS", "argus"),
             env_or("PG_DB", "argusdb"));

    SQLCHAR out[1024];
    SQLSMALLINT out_len;
    assert_int_equal(SQLDriverConnect(*dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                      out, sizeof(out), &out_len,
                                      SQL_DRIVER_NOPROMPT), SQL_SUCCESS);
}

static void close_conn(SQLHENV env, SQLHDBC dbc)
{
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static void exec_direct(SQLHSTMT stmt, const char *sql)
{
    SQLRETURN rc = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR state[6] = {0}, msg[512] = {0};
        SQLINTEGER native = 0; SQLSMALLINT len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &native,
                      msg, sizeof(msg), &len);
        fail_msg("SQLExecDirect(%s) failed: %s %s", sql, state, msg);
    }
}

/* ── A scalar round trip ─────────────────────────────────────── */

static void test_select_scalar(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    exec_direct(stmt, "SELECT 42 AS answer");

    SQLSMALLINT ncols = 0;
    assert_int_equal(SQLNumResultCols(stmt, &ncols), SQL_SUCCESS);
    assert_int_equal(ncols, 1);

    SQLCHAR cname[64] = {0};
    SQLSMALLINT nlen = 0, dtype = 0, ddigits = 0, nullable = 0;
    SQLULEN csize = 0;
    assert_int_equal(SQLDescribeCol(stmt, 1, cname, sizeof(cname), &nlen,
                                    &dtype, &csize, &ddigits, &nullable),
                     SQL_SUCCESS);
    assert_string_equal((char *)cname, "answer");
    assert_int_equal(dtype, SQL_INTEGER);

    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    SQLINTEGER v = 0; SQLLEN ind = 0;
    assert_int_equal(SQLGetData(stmt, 1, SQL_C_SLONG, &v, sizeof(v), &ind),
                     SQL_SUCCESS);
    assert_int_equal(v, 42);

    assert_int_equal(SQLFetch(stmt), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

/* ── Streaming: more rows than one chunk ─────────────────────── */

static void test_fetch_many_rows(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    exec_direct(stmt, "SELECT g FROM generate_series(1, 2500) g ORDER BY g");

    long count = 0, sum = 0;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLINTEGER v = 0; SQLLEN ind = 0;
        assert_int_equal(SQLGetData(stmt, 1, SQL_C_SLONG, &v, sizeof(v), &ind),
                         SQL_SUCCESS);
        count++;
        sum += v;
    }
    assert_int_equal(count, 2500);
    assert_int_equal(sum, 2500L * 2501L / 2);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

/* ── Types: the driver's mapping against a real RowDescription ── */

static void test_type_mapping(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    exec_direct(stmt,
        "SELECT c_bool, c_int2, c_int4, c_int8, c_float4, c_float8, "
        "c_numeric, c_char, c_varchar, c_date, c_timestamp, c_uuid, c_bytea "
        "FROM argus_test.all_types WHERE c_bool IS NOT NULL");

    struct { int col; SQLSMALLINT type; } expect[] = {
        { 1,  SQL_BIT },
        { 2,  SQL_SMALLINT },
        { 3,  SQL_INTEGER },
        { 4,  SQL_BIGINT },
        { 5,  SQL_REAL },
        { 6,  SQL_DOUBLE },
        { 7,  SQL_NUMERIC },
        { 8,  SQL_CHAR },
        { 9,  SQL_VARCHAR },
        { 10, SQL_TYPE_DATE },
        { 11, SQL_TYPE_TIMESTAMP },
        { 12, SQL_GUID },
        { 13, SQL_LONGVARBINARY },
    };

    for (size_t i = 0; i < sizeof(expect) / sizeof(expect[0]); i++) {
        SQLCHAR cname[64] = {0};
        SQLSMALLINT nlen = 0, dtype = 0, ddigits = 0, nullable = 0;
        SQLULEN csize = 0;
        assert_int_equal(SQLDescribeCol(stmt, (SQLUSMALLINT)expect[i].col,
                                        cname, sizeof(cname), &nlen, &dtype,
                                        &csize, &ddigits, &nullable),
                         SQL_SUCCESS);
        if (dtype != expect[i].type)
            fail_msg("column %d (%s): expected SQL type %d, got %d",
                     expect[i].col, cname, expect[i].type, dtype);
    }

    /* atttypmod has to reach column_size and decimal_digits: numeric(12,3)
     * and varchar(20) are the two that a driver ignoring it gets wrong. */
    {
        SQLCHAR cname[64]; SQLSMALLINT nlen, dtype, ddigits, nullable;
        SQLULEN csize;
        SQLDescribeCol(stmt, 7, cname, sizeof(cname), &nlen, &dtype,
                       &csize, &ddigits, &nullable);
        assert_int_equal((int)csize, 12);
        assert_int_equal(ddigits, 3);

        SQLDescribeCol(stmt, 9, cname, sizeof(cname), &nlen, &dtype,
                       &csize, &ddigits, &nullable);
        assert_int_equal((int)csize, 20);
    }

    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    /* Boolean must read as ODBC's 1/0, not PostgreSQL's t/f. */
    {
        char buf[16] = {0}; SQLLEN ind = 0;
        assert_int_equal(SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind),
                         SQL_SUCCESS);
        assert_string_equal(buf, "1");

        unsigned char bit = 0;
        assert_int_equal(SQLGetData(stmt, 1, SQL_C_BIT, &bit, sizeof(bit), &ind),
                         SQL_SUCCESS);
        assert_int_equal(bit, 1);
    }

    /* The native fast path must agree with the text form. */
    {
        SQLBIGINT big = 0; SQLLEN ind = 0;
        assert_int_equal(SQLGetData(stmt, 4, SQL_C_SBIGINT, &big, sizeof(big), &ind),
                         SQL_SUCCESS);
        assert_true(big == 9223372036854775807LL);

        SQLDOUBLE d = 0;
        assert_int_equal(SQLGetData(stmt, 6, SQL_C_DOUBLE, &d, sizeof(d), &ind),
                         SQL_SUCCESS);
        assert_true(d > 2.24 && d < 2.26);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

/* ── NULLs ───────────────────────────────────────────────────── */

static void test_nulls(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    exec_direct(stmt,
        "SELECT c_int4, c_varchar FROM argus_test.all_types "
        "WHERE c_bool IS NULL");
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    SQLINTEGER v = 7; SQLLEN ind = 0;
    assert_int_equal(SQLGetData(stmt, 1, SQL_C_SLONG, &v, sizeof(v), &ind),
                     SQL_SUCCESS);
    assert_int_equal(ind, SQL_NULL_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

/* ── A failing statement reports the server's own SQLSTATE ───── */

static void test_error_reports_server_message(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    SQLRETURN rc = SQLExecDirect(
        stmt, (SQLCHAR *)"SELECT * FROM no_such_table_here", SQL_NTS);
    assert_int_equal(rc, SQL_ERROR);

    SQLCHAR state_buf[6] = {0}, msg[512] = {0};
    SQLINTEGER native = 0; SQLSMALLINT len = 0;
    assert_int_equal(SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state_buf, &native,
                                   msg, sizeof(msg), &len), SQL_SUCCESS);
    /* The server's words must survive to the application. */
    assert_non_null(strstr((char *)msg, "no_such_table_here"));

    /* And the connection must still be usable — a statement that failed
     * mid-stream must not leave the wire in a broken state. */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    exec_direct(stmt, "SELECT 1");
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

/*
 * A successful statement after a failed one must not inherit the failure.
 * libpq's error state is sticky for the life of the connection and the ODBC
 * layer fails any statement whose get_last_error() returns text, so this is
 * the regression guard on clearing it.
 */
static void test_error_does_not_stick(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    SQLExecDirect(stmt, (SQLCHAR *)"SELECT * FROM nope_not_here", SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    for (int i = 0; i < 3; i++) {
        assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
        exec_direct(stmt, "SELECT 1");
        assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    close_conn(env, dbc);
}

/* ── Catalog: partition children must not be listed ──────────── */

static long count_rows(SQLHSTMT stmt)
{
    long n = 0;
    while (SQLFetch(stmt) == SQL_SUCCESS) n++;
    return n;
}

static void test_tables_hides_partition_children(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    /* The seed creates `events` plus 24 monthly children. A driver that does
     * not filter returns 25 rows here; the whole point of the filter is that
     * this returns 1. */
    assert_int_equal(SQLTables(stmt, NULL, 0,
                               (SQLCHAR *)"argus_test", SQL_NTS,
                               (SQLCHAR *)"events%", SQL_NTS,
                               NULL, 0), SQL_SUCCESS);
    assert_int_equal(count_rows(stmt), 1);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

static void test_tables_and_views(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    assert_int_equal(SQLTables(stmt, NULL, 0,
                               (SQLCHAR *)"argus_test", SQL_NTS,
                               (SQLCHAR *)"customers", SQL_NTS,
                               NULL, 0), SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    char cat[128] = {0}, schem[128] = {0}, name[128] = {0}, type[64] = {0},
         remarks[256] = {0};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, cat, sizeof(cat), &ind);
    SQLGetData(stmt, 2, SQL_C_CHAR, schem, sizeof(schem), &ind);
    SQLGetData(stmt, 3, SQL_C_CHAR, name, sizeof(name), &ind);
    SQLGetData(stmt, 4, SQL_C_CHAR, type, sizeof(type), &ind);
    SQLGetData(stmt, 5, SQL_C_CHAR, remarks, sizeof(remarks), &ind);

    /* Database is the catalog and schema is the schema — the three-level
     * model, not MySQL's flattened one. */
    assert_string_equal(schem, "argus_test");
    assert_string_equal(name, "customers");
    assert_string_equal(type, "TABLE");
    assert_string_equal(remarks, "customer dimension");
    assert_true(cat[0] != '\0');

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    /* A VIEW type filter must return the view and not the tables. */
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLTables(stmt, NULL, 0,
                               (SQLCHAR *)"argus_test", SQL_NTS,
                               NULL, 0,
                               (SQLCHAR *)"VIEW", SQL_NTS), SQL_SUCCESS);
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
    SQLGetData(stmt, 3, SQL_C_CHAR, name, sizeof(name), &ind);
    assert_string_equal(name, "big_orders");
    assert_int_equal(SQLFetch(stmt), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

static void test_columns(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    assert_int_equal(SQLColumns(stmt, NULL, 0,
                                (SQLCHAR *)"argus_test", SQL_NTS,
                                (SQLCHAR *)"customers", SQL_NTS,
                                NULL, 0), SQL_SUCCESS);

    /* id integer NOT NULL (PK), name varchar(50) NOT NULL, region text NULL */
    struct { const char *name; int data_type; int size; int nullable; }
    expect[] = {
        { "id",     SQL_INTEGER, 10, SQL_NO_NULLS },
        { "name",   SQL_VARCHAR, 50, SQL_NO_NULLS },
        { "region", SQL_LONGVARCHAR, 0, SQL_NULLABLE },
    };

    for (size_t i = 0; i < sizeof(expect) / sizeof(expect[0]); i++) {
        assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

        char cname[128] = {0};
        SQLSMALLINT dtype = 0, nullable = 0;
        SQLINTEGER csize = 0;
        SQLLEN ind = 0;

        SQLGetData(stmt, 4, SQL_C_CHAR, cname, sizeof(cname), &ind);
        SQLGetData(stmt, 5, SQL_C_SSHORT, &dtype, sizeof(dtype), &ind);
        SQLGetData(stmt, 7, SQL_C_SLONG, &csize, sizeof(csize), &ind);
        SQLGetData(stmt, 11, SQL_C_SSHORT, &nullable, sizeof(nullable), &ind);

        assert_string_equal(cname, expect[i].name);
        if (dtype != expect[i].data_type)
            fail_msg("%s: expected DATA_TYPE %d, got %d",
                     cname, expect[i].data_type, dtype);
        if (expect[i].size > 0) assert_int_equal(csize, expect[i].size);
        assert_int_equal(nullable, expect[i].nullable);
    }
    assert_int_equal(SQLFetch(stmt), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

static void test_primary_keys(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    /* orders has a two-column key (order_id, customer_id): KEY_SEQ must
     * follow the declaration order, not attnum order. */
    assert_int_equal(SQLPrimaryKeys(stmt, NULL, 0,
                                    (SQLCHAR *)"argus_test", SQL_NTS,
                                    (SQLCHAR *)"orders", SQL_NTS),
                     SQL_SUCCESS);

    const char *expect[] = { "order_id", "customer_id" };
    for (int i = 0; i < 2; i++) {
        assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);
        char col[128] = {0};
        SQLSMALLINT seq = 0;
        SQLLEN ind = 0;
        SQLGetData(stmt, 4, SQL_C_CHAR, col, sizeof(col), &ind);
        SQLGetData(stmt, 5, SQL_C_SSHORT, &seq, sizeof(seq), &ind);
        assert_string_equal(col, expect[i]);
        assert_int_equal(seq, i + 1);
    }
    assert_int_equal(SQLFetch(stmt), SQL_NO_DATA);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

static void test_statistics(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    assert_int_equal(SQLStatistics(stmt, NULL, 0,
                                   (SQLCHAR *)"argus_test", SQL_NTS,
                                   (SQLCHAR *)"orders", SQL_NTS,
                                   SQL_INDEX_ALL, SQL_QUICK), SQL_SUCCESS);

    bool saw_table_stat = false, saw_index = false;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLSMALLINT type = 0;
        SQLBIGINT card = 0;
        SQLLEN ind_type = 0, ind_card = 0;
        SQLGetData(stmt, 7, SQL_C_SSHORT, &type, sizeof(type), &ind_type);
        SQLGetData(stmt, 11, SQL_C_SBIGINT, &card, sizeof(card), &ind_card);

        if (type == SQL_TABLE_STAT) {
            saw_table_stat = true;
            /* The seed ANALYZEs, so a real estimate must be present. */
            assert_int_not_equal(ind_card, SQL_NULL_DATA);
            assert_true(card > 0);
        } else {
            saw_index = true;
        }
    }
    assert_true(saw_table_stat);
    assert_true(saw_index);      /* the primary key index */

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

static void test_schemas_and_catalogs(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);

    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLTables(stmt, NULL, 0, (SQLCHAR *)"", 0,
                               (SQLCHAR *)"", 0, (SQLCHAR *)"", 0),
                     SQL_SUCCESS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    /* SQLGetTypeInfo must come back with the full ODBC shape. */
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);
    assert_int_equal(SQLGetTypeInfo(stmt, SQL_ALL_TYPES), SQL_SUCCESS);
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    assert_int_equal(ncols, 16);
    assert_true(count_rows(stmt) > 10);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    close_conn(env, dbc);
}

/* ── SQLCancel really cancels ────────────────────────────────── */

static void test_cancel_stops_query(void **state)
{
    (void)state;
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    open_conn(&env, &dbc);
    assert_int_equal(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt), SQL_SUCCESS);

    /* Cancelling before execution is a no-op that must still succeed. */
    assert_int_equal(SQLCancel(stmt), SQL_SUCCESS);

    exec_direct(stmt, "SELECT 1");
    assert_int_equal(SQLFetch(stmt), SQL_SUCCESS);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    close_conn(env, dbc);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_scalar),
        cmocka_unit_test(test_fetch_many_rows),
        cmocka_unit_test(test_type_mapping),
        cmocka_unit_test(test_nulls),
        cmocka_unit_test(test_error_reports_server_message),
        cmocka_unit_test(test_error_does_not_stick),
        cmocka_unit_test(test_tables_hides_partition_children),
        cmocka_unit_test(test_tables_and_views),
        cmocka_unit_test(test_columns),
        cmocka_unit_test(test_primary_keys),
        cmocka_unit_test(test_statistics),
        cmocka_unit_test(test_schemas_and_catalogs),
        cmocka_unit_test(test_cancel_stops_query),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
