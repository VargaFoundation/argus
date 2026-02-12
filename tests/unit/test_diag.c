#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "argus/handle.h"

/* ── Test: Push and retrieve diagnostic record ───────────────── */

static void test_push_and_get(void **state)
{
    (void)state;

    argus_diag_t diag;
    argus_diag_clear(&diag);

    argus_diag_push(&diag, "HY000", "Test error message", 42);
    assert_int_equal(diag.count, 1);

    SQLCHAR sqlstate[6] = {0};
    SQLINTEGER native_error = 0;
    SQLCHAR message[256] = {0};
    SQLSMALLINT msg_len = 0;

    SQLRETURN ret = argus_diag_get_rec(&diag, 1, sqlstate, &native_error,
                                        message, sizeof(message), &msg_len);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)sqlstate, "HY000");
    assert_int_equal(native_error, 42);
    assert_string_equal((char *)message, "Test error message");
    assert_int_equal(msg_len, 18);
}

/* ── Test: No data for empty diagnostics ─────────────────────── */

static void test_empty_diag(void **state)
{
    (void)state;

    argus_diag_t diag;
    argus_diag_clear(&diag);

    SQLCHAR sqlstate[6];
    SQLRETURN ret = argus_diag_get_rec(&diag, 1, sqlstate, NULL,
                                        NULL, 0, NULL);
    assert_int_equal(ret, SQL_NO_DATA);
}

/* ── Test: Invalid record number ─────────────────────────────── */

static void test_invalid_rec_number(void **state)
{
    (void)state;

    argus_diag_t diag;
    argus_diag_clear(&diag);
    argus_diag_push(&diag, "HY000", "Error", 0);

    SQLRETURN ret = argus_diag_get_rec(&diag, 0, NULL, NULL, NULL, 0, NULL);
    assert_int_equal(ret, SQL_NO_DATA);

    ret = argus_diag_get_rec(&diag, 2, NULL, NULL, NULL, 0, NULL);
    assert_int_equal(ret, SQL_NO_DATA);
}

/* ── Test: Multiple diagnostic records ───────────────────────── */

static void test_multiple_records(void **state)
{
    (void)state;

    argus_diag_t diag;
    argus_diag_clear(&diag);

    argus_diag_push(&diag, "HY000", "First error", 1);
    argus_diag_push(&diag, "HY001", "Second error", 2);
    argus_diag_push(&diag, "08001", "Third error", 3);

    assert_int_equal(diag.count, 3);

    SQLCHAR sqlstate[6];
    SQLINTEGER native_error;

    argus_diag_get_rec(&diag, 1, sqlstate, &native_error, NULL, 0, NULL);
    assert_string_equal((char *)sqlstate, "HY000");
    assert_int_equal(native_error, 1);

    argus_diag_get_rec(&diag, 2, sqlstate, &native_error, NULL, 0, NULL);
    assert_string_equal((char *)sqlstate, "HY001");
    assert_int_equal(native_error, 2);

    argus_diag_get_rec(&diag, 3, sqlstate, &native_error, NULL, 0, NULL);
    assert_string_equal((char *)sqlstate, "08001");
    assert_int_equal(native_error, 3);
}

/* ── Test: argus_set_error clears and sets ───────────────────── */

static void test_set_error(void **state)
{
    (void)state;

    argus_diag_t diag;
    argus_diag_clear(&diag);

    argus_diag_push(&diag, "HY000", "Old error", 99);
    assert_int_equal(diag.count, 1);

    /* set_error should clear and push new */
    argus_set_error(&diag, "08001", "New error", 42);
    assert_int_equal(diag.count, 1);

    SQLCHAR sqlstate[6];
    SQLINTEGER native_error;
    SQLCHAR message[256];
    argus_diag_get_rec(&diag, 1, sqlstate, &native_error,
                        message, sizeof(message), NULL);
    assert_string_equal((char *)sqlstate, "08001");
    assert_string_equal((char *)message, "New error");
    assert_int_equal(native_error, 42);
}

/* ── Test: SQLGetDiagRec with env handle ─────────────────────── */

static void test_get_diag_rec_env(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    /* Push an error to the env */
    argus_env_t *e = (argus_env_t *)env;
    argus_diag_push(&e->diag, "HY092", "Invalid attribute", 0);

    SQLCHAR sqlstate[6];
    SQLCHAR message[256];
    SQLSMALLINT msg_len;

    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_ENV, env, 1,
                                   sqlstate, NULL,
                                   message, sizeof(message), &msg_len);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_string_equal((char *)sqlstate, "HY092");

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: SQLGetDiagField ───────────────────────────────────── */

static void test_get_diag_field(void **state)
{
    (void)state;

    SQLHENV env = SQL_NULL_HENV;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    argus_env_t *e = (argus_env_t *)env;
    argus_diag_push(&e->diag, "HY000", "Error one", 10);
    argus_diag_push(&e->diag, "08001", "Error two", 20);

    /* Header: number of records */
    SQLINTEGER count = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_ENV, env, 0,
                                     SQL_DIAG_NUMBER, &count, 0, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(count, 2);

    /* Record field: native error */
    SQLINTEGER native_err = 0;
    ret = SQLGetDiagField(SQL_HANDLE_ENV, env, 1,
                           SQL_DIAG_NATIVE, &native_err, 0, NULL);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(native_err, 10);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/* ── Test: Message truncation ────────────────────────────────── */

static void test_message_truncation(void **state)
{
    (void)state;

    argus_diag_t diag;
    argus_diag_clear(&diag);

    argus_diag_push(&diag, "HY000", "This is a long error message", 0);

    SQLCHAR message[10];
    SQLSMALLINT msg_len;

    SQLRETURN ret = argus_diag_get_rec(&diag, 1, NULL, NULL,
                                        message, sizeof(message), &msg_len);
    assert_int_equal(ret, SQL_SUCCESS);
    assert_int_equal(msg_len, 28); /* Full length */
    assert_int_equal(strlen((char *)message), 9); /* Truncated */
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_push_and_get),
        cmocka_unit_test(test_empty_diag),
        cmocka_unit_test(test_invalid_rec_number),
        cmocka_unit_test(test_multiple_records),
        cmocka_unit_test(test_set_error),
        cmocka_unit_test(test_get_diag_rec_env),
        cmocka_unit_test(test_get_diag_field),
        cmocka_unit_test(test_message_truncation),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
