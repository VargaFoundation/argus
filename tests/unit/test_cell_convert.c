/* SPDX-License-Identifier: Apache-2.0 */
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
 * Conversions from the row cache to the application's C types: binary
 * values (decoded by the backend, never guessed from the text), the ODBC
 * rules for character data reaching numeric targets (22018 / 22003 /
 * 01S07), exponents in SQL_C_NUMERIC, zone suffixes on timestamps, and
 * SQLGetData continuation that never splits a UTF-8 sequence, a surrogate
 * pair or a hex byte and ends with SQL_NO_DATA.
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

/* One fetched row with one cell holding `data` (len bytes; text unless
 * `binary`). */
static argus_stmt_t *stmt_with_cell(argus_dbc_t *dbc, const char *data,
                                    size_t len, bool binary)
{
    argus_stmt_t *stmt = NULL;
    argus_alloc_stmt(dbc, &stmt);

    stmt->num_cols = 1;
    stmt->executed = true;
    stmt->fetch_started = true;
    stmt->columns[0].sql_type = binary ? SQL_VARBINARY : SQL_VARCHAR;

    stmt->row_cache.rows = calloc(1, sizeof(argus_row_t));
    stmt->row_cache.num_rows = 1;
    stmt->row_cache.num_cols = 1;
    stmt->row_cache.current_row = 1;
    stmt->row_cache.exhausted = true;

    argus_cell_t *cell = calloc(1, sizeof(argus_cell_t));
    stmt->row_cache.rows[0].cells = cell;
    cell->data = malloc(len + 1);
    memcpy(cell->data, data, len);
    cell->data[len] = '\0';
    cell->data_len = len;
    cell->native_kind = binary ? ARGUS_NATIVE_BINARY : ARGUS_NATIVE_NONE;
    return stmt;
}

static argus_stmt_t *stmt_with_text(argus_dbc_t *dbc, const char *text)
{
    return stmt_with_cell(dbc, text, strlen(text), false);
}

static void check_state(argus_stmt_t *stmt, const char *expected)
{
    SQLCHAR state[6] = {0}, msg[256];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    SQLRETURN r = SQLGetDiagRec(SQL_HANDLE_STMT, (SQLHSTMT)stmt, 1, state,
                                &native, msg, sizeof(msg), &len);
    assert_true(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO);
    assert_string_equal((char *)state, expected);
}

/* ── Decoders ─────────────────────────────────────────────────── */

static void test_decode_hex(void **state)
{
    (void)state;
    argus_cell_t c;
    const char *cases[] = { "48656c6c6f", "48656C6C6F", "0x48656c6c6f", "\\x48656c6c6f" };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        memset(&c, 0, sizeof(c));
        c.data = strdup(cases[i]);
        c.data_len = strlen(c.data);
        assert_int_equal(argus_cell_decode_hex(&c), 0);
        assert_int_equal(c.native_kind, ARGUS_NATIVE_BINARY);
        assert_int_equal(c.data_len, 5);
        assert_memory_equal(c.data, "Hello", 5);
        assert_int_equal(c.data[5], 0);
        free(c.data);
    }

    /* Bytes that are not text survive, NULs included. */
    memset(&c, 0, sizeof(c));
    c.data = strdup("00ff10");
    c.data_len = 6;
    assert_int_equal(argus_cell_decode_hex(&c), 0);
    assert_int_equal(c.data_len, 3);
    assert_memory_equal(c.data, "\x00\xff\x10", 3);
    free(c.data);

    /* Empty value: zero bytes. */
    memset(&c, 0, sizeof(c));
    c.data = strdup("");
    assert_int_equal(argus_cell_decode_hex(&c), 0);
    assert_int_equal(c.native_kind, ARGUS_NATIVE_BINARY);
    assert_int_equal(c.data_len, 0);
    free(c.data);

    /* Not hex: the cell is left as it was. */
    const char *bad[] = { "abc", "zz", "12 34" };
    for (size_t i = 0; i < 3; i++) {
        memset(&c, 0, sizeof(c));
        c.data = strdup(bad[i]);
        c.data_len = strlen(c.data);
        assert_int_equal(argus_cell_decode_hex(&c), -1);
        assert_int_equal(c.native_kind, ARGUS_NATIVE_NONE);
        assert_string_equal(c.data, bad[i]);
        free(c.data);
    }

    /* NULL and already-binary cells are untouched. */
    memset(&c, 0, sizeof(c));
    c.is_null = true;
    assert_int_equal(argus_cell_decode_hex(&c), 0);
    assert_int_equal(c.native_kind, ARGUS_NATIVE_NONE);
    memset(&c, 0, sizeof(c));
    c.data = strdup("zz");
    c.data_len = 2;
    c.native_kind = ARGUS_NATIVE_BINARY;
    assert_int_equal(argus_cell_decode_hex(&c), 0);
    assert_string_equal(c.data, "zz");
    free(c.data);
}

static void test_decode_base64(void **state)
{
    (void)state;
    argus_cell_t c;
    struct { const char *in; const char *out; size_t len; } cases[] = {
        { "SGVsbG8=", "Hello", 5 },
        { "SGVsbG8", "Hello", 5 },          /* unpadded */
        { "SGVsbG8h", "Hello!", 6 },
        { "AP8Q", "\x00\xff\x10", 3 },
        { "+/8=", "\xfb\xff", 2 },
        { "-_8", "\xfb\xff", 2 },            /* URL alphabet */
        { "", "", 0 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        memset(&c, 0, sizeof(c));
        c.data = strdup(cases[i].in);
        c.data_len = strlen(c.data);
        assert_int_equal(argus_cell_decode_base64(&c), 0);
        assert_int_equal(c.native_kind, ARGUS_NATIVE_BINARY);
        assert_int_equal(c.data_len, cases[i].len);
        assert_memory_equal(c.data, cases[i].out, cases[i].len);
        assert_int_equal(c.data[cases[i].len], 0);
        free(c.data);
    }

    const char *bad[] = { "S", "SGVs bG8=", "SGV$" };
    for (size_t i = 0; i < 3; i++) {
        memset(&c, 0, sizeof(c));
        c.data = strdup(bad[i]);
        c.data_len = strlen(c.data);
        assert_int_equal(argus_cell_decode_base64(&c), -1);
        assert_int_equal(c.native_kind, ARGUS_NATIVE_NONE);
        assert_string_equal(c.data, bad[i]);
        free(c.data);
    }
}

/* ── Binary cells ─────────────────────────────────────────────── */

static void test_binary_cell_targets(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_cell(dbc, "\x01\x00\xab", 3, true);

    /* SQL_C_BINARY: the bytes, NUL included. */
    unsigned char buf[8] = {0x55, 0x55, 0x55, 0x55};
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf,
                                sizeof(buf), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 3);
    assert_memory_equal(buf, "\x01\x00\xab", 3);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf,
                                sizeof(buf), &ind), SQL_NO_DATA);

    /* SQL_C_DEFAULT on a binary value is the bytes too (fresh column state
     * after a fetch is simulated by resetting the continuation). */
    argus_getdata_reset(&stmt->getdata);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_DEFAULT, buf,
                                sizeof(buf), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 3);

    /* SQL_C_CHAR: hex, upper case, NUL-terminated. */
    argus_getdata_reset(&stmt->getdata);
    char text[16];
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, text,
                                sizeof(text), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 6);
    assert_string_equal(text, "0100AB");

    /* SQL_C_WCHAR: the same digits as UTF-16. */
    argus_getdata_reset(&stmt->getdata);
    SQLWCHAR wide[16];
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, wide,
                                sizeof(wide), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 6 * (SQLLEN)sizeof(SQLWCHAR));
    assert_int_equal(wide[0], '0');
    assert_int_equal(wide[5], 'B');
    assert_int_equal(wide[6], 0);

    /* Anything else is a restricted conversion. */
    argus_getdata_reset(&stmt->getdata);
    SQLINTEGER n = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_SLONG, &n,
                                sizeof(n), &ind), SQL_ERROR);
    check_state(stmt, "07006");

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* Hex text in a VARCHAR is text: no sniffing on the way to SQL_C_BINARY. */
static void test_text_to_binary_is_bytes(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_text(dbc, "123456");

    unsigned char buf[8];
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf,
                                sizeof(buf), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 6);
    assert_memory_equal(buf, "123456", 6);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* Hex chunks end between bytes, never inside one. */
static void test_binary_to_char_chunks(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_cell(dbc, "\x01\x02\x03", 3, true);

    char buf[4];           /* room for one byte (two digits) and the NUL */
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 4, &ind),
                     SQL_SUCCESS_WITH_INFO);
    check_state(stmt, "01004");
    assert_int_equal(ind, 6);
    assert_string_equal(buf, "01");
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 4, &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 4);
    assert_string_equal(buf, "02");
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 4, &ind),
                     SQL_SUCCESS);
    assert_int_equal(ind, 2);
    assert_string_equal(buf, "03");
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 4, &ind),
                     SQL_NO_DATA);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

static void test_binary_chunks(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_cell(dbc, "abcde", 5, true);

    unsigned char buf[2];
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf, 2, &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 5);
    assert_memory_equal(buf, "ab", 2);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf, 2, &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 3);
    assert_memory_equal(buf, "cd", 2);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf, 2, &ind),
                     SQL_SUCCESS);
    assert_int_equal(ind, 1);
    assert_memory_equal(buf, "e", 1);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_BINARY, buf, 2, &ind),
                     SQL_NO_DATA);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Character continuation ───────────────────────────────────── */

static void test_char_chunks_keep_utf8_whole(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_text(dbc, "a\xc3\xa9" "b");   /* "aéb" */

    char buf[3];           /* two bytes and the NUL */
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 3, &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 4);
    assert_string_equal(buf, "a");                /* not "a" + half of é */
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 3, &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 3);
    assert_string_equal(buf, "\xc3\xa9");
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 3, &ind),
                     SQL_SUCCESS);
    assert_int_equal(ind, 1);
    assert_string_equal(buf, "b");
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 3, &ind),
                     SQL_NO_DATA);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

static void test_wchar_chunks(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    /* "a", U+1F600 (a surrogate pair in UTF-16), "b" */
    argus_stmt_t *stmt = stmt_with_text(dbc, "a\xf0\x9f\x98\x80" "b");

    SQLWCHAR buf[3];       /* two units and the NUL */
    SQLLEN ind = 0;
    SQLLEN unit = (SQLLEN)sizeof(SQLWCHAR);

    /* 'a' fits, the pair would not: the chunk stops before it. */
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, buf,
                                sizeof(buf), &ind), SQL_SUCCESS_WITH_INFO);
    check_state(stmt, "01004");
    assert_int_equal(ind, 4 * unit);
    assert_int_equal(buf[0], 'a');
    assert_int_equal(buf[1], 0);

    /* The pair, whole. */
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, buf,
                                sizeof(buf), &ind), SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 3 * unit);
    assert_int_equal(buf[0], 0xD83D);
    assert_int_equal(buf[1], 0xDE00);
    assert_int_equal(buf[2], 0);

    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, buf,
                                sizeof(buf), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 1 * unit);
    assert_int_equal(buf[0], 'b');
    assert_int_equal(buf[1], 0);

    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, buf,
                                sizeof(buf), &ind), SQL_NO_DATA);

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

static void test_wchar_whole_value(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_text(dbc, "h\xc3\xa9llo");   /* "héllo" */

    SQLWCHAR buf[16];
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, buf,
                                sizeof(buf), &ind), SQL_SUCCESS);
    assert_int_equal(ind, 5 * (SQLLEN)sizeof(SQLWCHAR));
    assert_int_equal(buf[1], 0xE9);
    assert_int_equal(buf[5], 0);

    /* Length probe: no buffer, the indicator carries the size. */
    argus_getdata_reset(&stmt->getdata);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, NULL, 0, &ind),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(ind, 5 * (SQLLEN)sizeof(SQLWCHAR));
    /* ...and the value is still all there for the next call. */
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_WCHAR, buf,
                                sizeof(buf), &ind), SQL_SUCCESS);
    assert_int_equal(buf[4], 'o');

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* After the whole value went out, the same column still reads into a
 * fixed-length type; only the character/binary re-read is SQL_NO_DATA. */
static void test_no_data_then_fixed_reread(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_text(dbc, "42");

    char buf[8];
    SQLLEN ind = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 8, &ind),
                     SQL_SUCCESS);
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, 8, &ind),
                     SQL_NO_DATA);
    SQLINTEGER n = 0;
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_SLONG, &n, 4, &ind),
                     SQL_SUCCESS);
    assert_int_equal(n, 42);

    /* A negative buffer length is the application's bug. */
    assert_int_equal(SQLGetData((SQLHSTMT)stmt, 1, SQL_C_CHAR, buf, -5, &ind),
                     SQL_ERROR);
    check_state(stmt, "HY090");

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Integers ─────────────────────────────────────────────────── */

static SQLRETURN get_as(argus_stmt_t *stmt, SQLSMALLINT type, void *out,
                        SQLLEN size)
{
    SQLLEN ind = 0;
    argus_getdata_reset(&stmt->getdata);
    return SQLGetData((SQLHSTMT)stmt, 1, type, out, size, &ind);
}

static void test_integer_rules(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    SQLINTEGER i32 = 0;
    SQLBIGINT i64 = 0;
    SQLUINTEGER u32 = 0;
    unsigned char bit = 0;

    /* Out of range for the C type, not silently wrapped. */
    argus_stmt_t *stmt = stmt_with_text(dbc, "2147483648");
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_ERROR);
    check_state(stmt, "22003");
    assert_int_equal(get_as(stmt, SQL_C_SBIGINT, &i64, 8), SQL_SUCCESS);
    assert_true(i64 == 2147483648LL);
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "-2147483648");
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_SUCCESS);
    assert_true(i32 == -2147483647 - 1);
    assert_int_equal(get_as(stmt, SQL_C_ULONG, &u32, 4), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    /* Not a number at all. */
    stmt = stmt_with_text(dbc, "abc");
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_ERROR);
    check_state(stmt, "22018");
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "12abc");
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_ERROR);
    check_state(stmt, "22018");
    argus_free_stmt(stmt);

    /* A fraction is dropped with a warning. */
    stmt = stmt_with_text(dbc, "12.75");
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_SUCCESS_WITH_INFO);
    check_state(stmt, "01S07");
    assert_int_equal(i32, 12);
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "12.00");
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_SUCCESS);
    assert_int_equal(i32, 12);
    argus_free_stmt(stmt);

    /* Hive prints doubles with an exponent. */
    stmt = stmt_with_text(dbc, "1.0E10");
    assert_int_equal(get_as(stmt, SQL_C_SBIGINT, &i64, 8), SQL_SUCCESS);
    assert_true(i64 == 10000000000LL);
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    /* Blanks around the number are fine; an unsigned target rejects a sign. */
    stmt = stmt_with_text(dbc, " 42 ");
    assert_int_equal(get_as(stmt, SQL_C_ULONG, &u32, 4), SQL_SUCCESS);
    assert_int_equal(u32, 42);
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "-1");
    assert_int_equal(get_as(stmt, SQL_C_ULONG, &u32, 4), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    /* The full unsigned 64-bit range. */
    SQLUBIGINT u64 = 0;
    stmt = stmt_with_text(dbc, "18446744073709551615");
    assert_int_equal(get_as(stmt, SQL_C_UBIGINT, &u64, 8), SQL_SUCCESS);
    assert_true(u64 == 18446744073709551615ULL);
    assert_int_equal(get_as(stmt, SQL_C_SBIGINT, &i64, 8), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    /* BIT: booleans and 0/1 only. */
    stmt = stmt_with_text(dbc, "true");
    assert_int_equal(get_as(stmt, SQL_C_BIT, &bit, 1), SQL_SUCCESS);
    assert_int_equal(bit, 1);
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_SUCCESS);
    assert_int_equal(i32, 1);
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "2");
    assert_int_equal(get_as(stmt, SQL_C_BIT, &bit, 1), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    free_test_dbc(dbc);
}

static void test_native_integer_rules(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_text(dbc, "4000000000");
    argus_cell_t *cell = &stmt->row_cache.rows[0].cells[0];
    cell->native_kind = ARGUS_NATIVE_I64;
    cell->native.i64 = 4000000000LL;

    SQLINTEGER i32 = 0;
    SQLBIGINT i64 = 0;
    SQLSMALLINT i16 = 0;
    assert_int_equal(get_as(stmt, SQL_C_SLONG, &i32, 4), SQL_ERROR);
    check_state(stmt, "22003");
    assert_int_equal(get_as(stmt, SQL_C_SBIGINT, &i64, 8), SQL_SUCCESS);
    assert_true(i64 == 4000000000LL);

    cell->native_kind = ARGUS_NATIVE_F64;
    cell->native.f64 = 2.5;
    assert_int_equal(get_as(stmt, SQL_C_SSHORT, &i16, 2), SQL_SUCCESS_WITH_INFO);
    check_state(stmt, "01S07");
    assert_int_equal(i16, 2);

    cell->native.f64 = -1e30;
    assert_int_equal(get_as(stmt, SQL_C_SBIGINT, &i64, 8), SQL_ERROR);
    check_state(stmt, "22003");

    /* A native value with no text form is rendered on demand. */
    free(cell->data);
    cell->data = NULL;
    cell->data_len = 0;
    cell->native.f64 = 0.1;
    char buf[32];
    assert_int_equal(get_as(stmt, SQL_C_CHAR, buf, sizeof(buf)), SQL_SUCCESS);
    assert_string_equal(buf, "0.1");
    cell->native.f64 = 0.1 + 0.2;
    assert_int_equal(get_as(stmt, SQL_C_CHAR, buf, sizeof(buf)), SQL_SUCCESS);
    assert_string_equal(buf, "0.30000000000000004");

    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}

/* ── Floating point ───────────────────────────────────────────── */

static void test_float_rules(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    SQLDOUBLE d = 0;
    SQLREAL f = 0;

    argus_stmt_t *stmt = stmt_with_text(dbc, "1e400");
    assert_int_equal(get_as(stmt, SQL_C_DOUBLE, &d, 8), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "1e300");
    assert_int_equal(get_as(stmt, SQL_C_DOUBLE, &d, 8), SQL_SUCCESS);
    assert_true(d == 1e300);
    assert_int_equal(get_as(stmt, SQL_C_FLOAT, &f, 4), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "3.5x");
    assert_int_equal(get_as(stmt, SQL_C_DOUBLE, &d, 8), SQL_ERROR);
    check_state(stmt, "22018");
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, " -3.5 ");
    assert_int_equal(get_as(stmt, SQL_C_FLOAT, &f, 4), SQL_SUCCESS);
    assert_true(f == -3.5f);
    argus_free_stmt(stmt);

    free_test_dbc(dbc);
}

/* ── SQL_C_NUMERIC ────────────────────────────────────────────── */

static unsigned long long numeric_lo(const SQL_NUMERIC_STRUCT *n)
{
    unsigned long long lo;
    memcpy(&lo, n->val, 8);
    return lo;
}

static void test_numeric_rules(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    SQL_NUMERIC_STRUCT num;

    struct {
        const char *text;
        unsigned char sign;
        unsigned long long lo;
        int scale;
        int precision;
    } cases[] = {
        { "1E3",      1, 1000, 0, 4 },
        { "1.5E-3",   1, 15,   4, 4 },
        { "-12.50",   0, 1250, 2, 4 },
        { "0.001",    1, 1,    3, 3 },
        { "0",        1, 0,    0, 1 },
        { "1234.5e1", 1, 12345, 0, 5 },
        { "+7",       1, 7,    0, 1 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        argus_stmt_t *stmt = stmt_with_text(dbc, cases[i].text);
        memset(&num, 0, sizeof(num));
        assert_int_equal(get_as(stmt, SQL_C_NUMERIC, &num, sizeof(num)),
                         SQL_SUCCESS);
        assert_int_equal(num.sign, cases[i].sign);
        assert_true(numeric_lo(&num) == cases[i].lo);
        assert_int_equal(num.scale, cases[i].scale);
        assert_int_equal(num.precision, cases[i].precision);
        argus_free_stmt(stmt);
    }

    /* 128 bits: 2^64 needs the high word. */
    argus_stmt_t *stmt = stmt_with_text(dbc, "18446744073709551616");
    assert_int_equal(get_as(stmt, SQL_C_NUMERIC, &num, sizeof(num)), SQL_SUCCESS);
    unsigned long long hi;
    memcpy(&hi, num.val + 8, 8);
    assert_true(numeric_lo(&num) == 0 && hi == 1);
    argus_free_stmt(stmt);

    /* Past 38 digits, or not a number. */
    stmt = stmt_with_text(dbc, "1E40");
    assert_int_equal(get_as(stmt, SQL_C_NUMERIC, &num, sizeof(num)), SQL_ERROR);
    check_state(stmt, "22003");
    argus_free_stmt(stmt);
    stmt = stmt_with_text(dbc, "1.2.3");
    assert_int_equal(get_as(stmt, SQL_C_NUMERIC, &num, sizeof(num)), SQL_ERROR);
    check_state(stmt, "22018");
    argus_free_stmt(stmt);

    free_test_dbc(dbc);
}

/* ── Date and time ────────────────────────────────────────────── */

static void test_datetime_rules(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    SQL_TIMESTAMP_STRUCT ts;
    SQL_DATE_STRUCT date;
    SQL_TIME_STRUCT time;

    /* Zone suffixes are read past; the wall-clock value is delivered. */
    const char *stamps[] = {
        "2026-07-01T10:20:30.5Z",
        "2026-07-01 10:20:30.500 +02:00",
        "2026-07-01 10:20:30.500000 Europe/Paris",
        "2026-07-01 10:20:30.5-0800",
        "2026-07-01 10:20:30.5 UTC",
    };
    for (size_t i = 0; i < sizeof(stamps) / sizeof(stamps[0]); i++) {
        argus_stmt_t *stmt = stmt_with_text(dbc, stamps[i]);
        memset(&ts, 0, sizeof(ts));
        assert_int_equal(get_as(stmt, SQL_C_TYPE_TIMESTAMP, &ts, sizeof(ts)),
                         SQL_SUCCESS);
        assert_int_equal(ts.year, 2026);
        assert_int_equal(ts.month, 7);
        assert_int_equal(ts.day, 1);
        assert_int_equal(ts.hour, 10);
        assert_int_equal(ts.minute, 20);
        assert_int_equal(ts.second, 30);
        assert_int_equal(ts.fraction, 500000000);
        argus_free_stmt(stmt);
    }

    /* A date alone is midnight; a date read from a timestamp with a time
     * part warns that the time was dropped. */
    argus_stmt_t *stmt = stmt_with_text(dbc, "2026-07-01");
    assert_int_equal(get_as(stmt, SQL_C_TYPE_TIMESTAMP, &ts, sizeof(ts)), SQL_SUCCESS);
    assert_int_equal(ts.hour, 0);
    assert_int_equal(ts.fraction, 0);
    assert_int_equal(get_as(stmt, SQL_C_TYPE_DATE, &date, sizeof(date)), SQL_SUCCESS);
    assert_int_equal(date.day, 1);
    argus_free_stmt(stmt);

    stmt = stmt_with_text(dbc, "2026-07-01 10:20:30");
    assert_int_equal(get_as(stmt, SQL_C_TYPE_DATE, &date, sizeof(date)),
                     SQL_SUCCESS_WITH_INFO);
    check_state(stmt, "01S07");
    assert_int_equal(date.month, 7);
    assert_int_equal(get_as(stmt, SQL_C_TYPE_TIME, &time, sizeof(time)), SQL_SUCCESS);
    assert_int_equal(time.hour, 10);
    assert_int_equal(time.second, 30);
    /* ODBC 2.x names of the same structs. */
    assert_int_equal(get_as(stmt, SQL_C_DATE, &date, sizeof(date)),
                     SQL_SUCCESS_WITH_INFO);
    assert_int_equal(get_as(stmt, SQL_C_TIMESTAMP, &ts, sizeof(ts)), SQL_SUCCESS);
    argus_free_stmt(stmt);

    /* A time alone, with fractional seconds SQL_TIME_STRUCT cannot hold. */
    stmt = stmt_with_text(dbc, "10:20:30.25");
    assert_int_equal(get_as(stmt, SQL_C_TYPE_TIME, &time, sizeof(time)),
                     SQL_SUCCESS_WITH_INFO);
    check_state(stmt, "01S07");
    assert_int_equal(time.minute, 20);
    assert_int_equal(get_as(stmt, SQL_C_TYPE_DATE, &date, sizeof(date)), SQL_ERROR);
    check_state(stmt, "22007");
    assert_int_equal(get_as(stmt, SQL_C_TYPE_TIMESTAMP, &ts, sizeof(ts)), SQL_SUCCESS);
    assert_int_equal(ts.hour, 10);
    assert_true(ts.year >= 2026);
    argus_free_stmt(stmt);

    /* Garbage and out-of-range fields. (A trailing word is read as a zone
     * name — "Japan" and "Poland" are IANA zones — so the junk here is
     * something no zone looks like.) */
    const char *bad[] = { "yesterday", "2026-13-01", "2026-07-01 25:00:00",
                          "2026-07-01 10:20:30 5pm", "2026-07-01 10:20:30 +",
                          "2026-07-01 10:20:30.", "2026-07-01 10:20" "x" };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        stmt = stmt_with_text(dbc, bad[i]);
        assert_int_equal(get_as(stmt, SQL_C_TYPE_TIMESTAMP, &ts, sizeof(ts)), SQL_ERROR);
        check_state(stmt, "22007");
        argus_free_stmt(stmt);
    }

    free_test_dbc(dbc);
}

/* An unknown C type is the application's bug, not a string copy. */
static void test_unknown_target_type(void **state)
{
    (void)state;
    argus_dbc_t *dbc = create_test_dbc();
    argus_stmt_t *stmt = stmt_with_text(dbc, "x");
    char buf[8];
    assert_int_equal(get_as(stmt, (SQLSMALLINT)12345, buf, sizeof(buf)), SQL_ERROR);
    check_state(stmt, "HY003");
    argus_free_stmt(stmt);
    free_test_dbc(dbc);
}


/* The whole batch at once: which columns are binary comes from the column
 * descriptors, the spelling from the backend — never from the value. */
static void test_cache_decode_binary(void **state)
{
    (void)state;
    argus_column_desc_t cols[3];
    memset(cols, 0, sizeof(cols));
    cols[0].sql_type = SQL_VARCHAR;
    cols[1].sql_type = SQL_VARBINARY;
    cols[2].sql_type = SQL_LONGVARBINARY;

    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));
    cache.num_cols = 3;
    cache.num_rows = 2;
    cache.capacity = 2;
    cache.rows = calloc(2, sizeof(argus_row_t));
    assert_non_null(cache.rows);
    for (int r = 0; r < 2; r++) {
        cache.rows[r].cells = calloc(3, sizeof(argus_cell_t));
        assert_non_null(cache.rows[r].cells);
    }
    /* A VARCHAR that happens to read as hex, and the binary columns. */
    cache.rows[0].cells[0].data = strdup("48656c6c6f");
    cache.rows[0].cells[0].data_len = 10;
    cache.rows[0].cells[1].data = strdup("48656c6c6f");
    cache.rows[0].cells[1].data_len = 10;
    cache.rows[0].cells[2].data = strdup("00ff10");
    cache.rows[0].cells[2].data_len = 6;
    cache.rows[1].cells[1].is_null = true;
    cache.rows[1].cells[2].data = strdup("nothex");
    cache.rows[1].cells[2].data_len = 6;

    argus_cache_decode_binary(&cache, cols, 3, ARGUS_BINARY_HEX);

    /* The VARCHAR is left alone even though it would have decoded. */
    assert_int_equal(cache.rows[0].cells[0].native_kind, ARGUS_NATIVE_NONE);
    assert_string_equal(cache.rows[0].cells[0].data, "48656c6c6f");
    /* The binary columns are bytes. */
    assert_int_equal(cache.rows[0].cells[1].native_kind, ARGUS_NATIVE_BINARY);
    assert_int_equal(cache.rows[0].cells[1].data_len, 5);
    assert_memory_equal(cache.rows[0].cells[1].data, "Hello", 5);
    assert_int_equal(cache.rows[0].cells[2].data_len, 3);
    assert_memory_equal(cache.rows[0].cells[2].data, "\x00\xff\x10", 3);
    /* A NULL stays NULL, and text the decoder rejects keeps its text. */
    assert_true(cache.rows[1].cells[1].is_null);
    assert_int_equal(cache.rows[1].cells[2].native_kind, ARGUS_NATIVE_NONE);
    assert_string_equal(cache.rows[1].cells[2].data, "nothex");

    /* Running it again changes nothing: decoding is idempotent. */
    argus_cache_decode_binary(&cache, cols, 3, ARGUS_BINARY_HEX);
    assert_int_equal(cache.rows[0].cells[1].data_len, 5);
    assert_memory_equal(cache.rows[0].cells[1].data, "Hello", 5);

    argus_row_cache_free(&cache);
}

/* ARGUS_BINARY_RAW is for the backends that already hand over bytes: it
 * marks the cell so a character target renders hex, and touches nothing
 * else. */
static void test_cache_mark_binary_raw(void **state)
{
    (void)state;
    argus_column_desc_t cols[2];
    memset(cols, 0, sizeof(cols));
    cols[0].sql_type = SQL_VARBINARY;
    cols[1].sql_type = SQL_VARCHAR;

    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));
    cache.num_cols = 2;
    cache.num_rows = 1;
    cache.capacity = 1;
    cache.rows = calloc(1, sizeof(argus_row_t));
    assert_non_null(cache.rows);
    cache.rows[0].cells = calloc(2, sizeof(argus_cell_t));
    assert_non_null(cache.rows[0].cells);
    cache.rows[0].cells[0].data = malloc(4);
    memcpy(cache.rows[0].cells[0].data, "\x00\xff\x10", 4);
    cache.rows[0].cells[0].data_len = 3;
    cache.rows[0].cells[1].data = strdup("plain");
    cache.rows[0].cells[1].data_len = 5;

    argus_cache_decode_binary(&cache, cols, 2, ARGUS_BINARY_RAW);

    assert_int_equal(cache.rows[0].cells[0].native_kind, ARGUS_NATIVE_BINARY);
    assert_int_equal(cache.rows[0].cells[0].data_len, 3);
    assert_memory_equal(cache.rows[0].cells[0].data, "\x00\xff\x10", 3);
    assert_int_equal(cache.rows[0].cells[1].native_kind, ARGUS_NATIVE_NONE);

    argus_row_cache_free(&cache);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_decode_hex),
        cmocka_unit_test(test_decode_base64),
        cmocka_unit_test(test_cache_decode_binary),
        cmocka_unit_test(test_cache_mark_binary_raw),
        cmocka_unit_test(test_binary_cell_targets),
        cmocka_unit_test(test_text_to_binary_is_bytes),
        cmocka_unit_test(test_binary_to_char_chunks),
        cmocka_unit_test(test_binary_chunks),
        cmocka_unit_test(test_char_chunks_keep_utf8_whole),
        cmocka_unit_test(test_wchar_chunks),
        cmocka_unit_test(test_wchar_whole_value),
        cmocka_unit_test(test_no_data_then_fixed_reread),
        cmocka_unit_test(test_integer_rules),
        cmocka_unit_test(test_native_integer_rules),
        cmocka_unit_test(test_float_rules),
        cmocka_unit_test(test_numeric_rules),
        cmocka_unit_test(test_datetime_rules),
        cmocka_unit_test(test_unknown_target_type),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
