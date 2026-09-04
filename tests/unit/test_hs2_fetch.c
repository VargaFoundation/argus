/* SPDX-License-Identifier: Apache-2.0 */
/*
 * test_hs2_fetch.c — HiveServer2 FetchResults handling shared by the Hive
 * and Impala backends, driven through a TCLIServiceClient whose input
 * protocol reads pre-serialised replies from a memory buffer (no server).
 *
 * Covers the two silent-truncation bugs of the columnar path: a row set
 * whose only populated column is BINARY used to count as zero rows, and an
 * empty batch flagged hasMoreRows=true (Impala's FETCH_ROWS_TIMEOUT_MS)
 * used to end the result. Also checks that a server error is surfaced with
 * its message instead of a bare -1.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <string.h>
#include <cmocka.h>

#include <thrift/c_glib/transport/thrift_memory_buffer.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>
#include <thrift/c_glib/thrift_application_exception.h>

#include "hive/hive_internal.h"
#include "impala/impala_internal.h"
#include "hs2_fetch.h"
#include "locale_helper.h"

int hive_fetch_results(argus_backend_conn_t conn, argus_backend_op_t op,
                       int max_rows, argus_row_cache_t *cache,
                       argus_column_desc_t *columns, int *num_cols);
bool hive_get_last_error(argus_backend_conn_t conn, char *buf, size_t buflen);
int impala_fetch_results(argus_backend_conn_t conn, argus_backend_op_t op,
                         int max_rows, argus_row_cache_t *cache,
                         argus_column_desc_t *columns, int *num_cols);

/* ── Row-set builders ─────────────────────────────────────────── */

static TColumn *binary_column(int n, int null_row)
{
    TColumn *tcol = g_object_new(TYPE_T_COLUMN, NULL);
    TBinaryColumn *bc = tcol->binaryVal;
    GByteArray *nulls = g_byte_array_new();
    guint8 zero[8] = {0};
    g_byte_array_append(nulls, zero, (guint)((n + 7) / 8));
    for (int r = 0; r < n; r++) {
        GByteArray *v = g_byte_array_new();
        guint8 bytes[3] = { (guint8)r, 0xff, 0x10 };
        g_byte_array_append(v, bytes, sizeof(bytes));
        g_ptr_array_add(bc->values, v);
        if (r == null_row) nulls->data[r / 8] |= (guint8)(1u << (r % 8));
    }
    g_object_set(bc, "nulls", nulls, NULL);
    g_byte_array_unref(nulls);
    tcol->__isset_binaryVal = TRUE;
    return tcol;
}

static TColumn *string_column(int n)
{
    TColumn *tcol = g_object_new(TYPE_T_COLUMN, NULL);
    for (int r = 0; r < n; r++)
        g_ptr_array_add(tcol->stringVal->values, g_strdup_printf("s%d", r));
    GByteArray *nulls = g_byte_array_new();
    g_object_set(tcol->stringVal, "nulls", nulls, NULL);   /* setter takes a ref */
    g_byte_array_unref(nulls);
    tcol->__isset_stringVal = TRUE;
    return tcol;
}

static TColumn *double_column(int n, double first)
{
    TColumn *tcol = g_object_new(TYPE_T_COLUMN, NULL);
    for (int r = 0; r < n; r++) {
        gdouble v = first + r;
        g_array_append_val(tcol->doubleVal->values, v);
    }
    GByteArray *nulls = g_byte_array_new();
    g_object_set(tcol->doubleVal, "nulls", nulls, NULL);
    g_byte_array_unref(nulls);
    tcol->__isset_doubleVal = TRUE;
    return tcol;
}

/* A column the server left entirely NULL: set, but with no values at all
 * (what GetTables does for TABLE_CAT). */
static TColumn *empty_string_column(void)
{
    return string_column(0);
}

static GPtrArray *columns(TColumn *first, ...)
{
    GPtrArray *cols = g_ptr_array_new_with_free_func(g_object_unref);
    va_list ap;
    va_start(ap, first);
    for (TColumn *c = first; c; c = va_arg(ap, TColumn *))
        g_ptr_array_add(cols, c);
    va_end(ap);
    return cols;
}

/* ── Parser-level tests ───────────────────────────────────────── */

static void test_row_count_counts_binary_column(void **state)
{
    (void)state;
    GPtrArray *cols = columns(binary_column(3, -1), NULL);
    assert_int_equal(argus_hs2_rowset_row_count(cols), 3);
    g_ptr_array_unref(cols);
}

static void test_row_count_is_longest_column(void **state)
{
    (void)state;
    GPtrArray *cols = columns(empty_string_column(), string_column(2),
                              binary_column(4, -1), NULL);
    assert_int_equal(argus_hs2_rowset_row_count(cols), 4);
    g_ptr_array_unref(cols);

    assert_int_equal(argus_hs2_rowset_row_count(NULL), 0);
    cols = columns(empty_string_column(), NULL);
    assert_int_equal(argus_hs2_rowset_row_count(cols), 0);
    g_ptr_array_unref(cols);
}

static void test_rowset_to_cache_binary_only(void **state)
{
    (void)state;
    GPtrArray *cols = columns(binary_column(3, 1), NULL);
    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));
    int num_cols = 0;

    assert_int_equal(argus_hs2_rowset_to_cache(cols, &cache, &num_cols), 0);
    assert_int_equal(num_cols, 1);
    assert_int_equal(cache.num_cols, 1);
    assert_int_equal(cache.num_rows, 3);
    /* The bytes themselves, not a hex rendering; the NUL after them is
     * the block layout's, not part of the value. */
    assert_int_equal(cache.rows[0].cells[0].native_kind, ARGUS_NATIVE_BINARY);
    assert_int_equal(cache.rows[0].cells[0].data_len, 3);
    assert_memory_equal(cache.rows[0].cells[0].data, "\x00\xff\x10", 3);
    assert_true(cache.rows[1].cells[0].is_null);
    assert_memory_equal(cache.rows[2].cells[0].data, "\x02\xff\x10", 3);

    argus_row_cache_clear(&cache);
    g_ptr_array_unref(cols);
}

/* DOUBLE columns are rendered to text by the driver: the text must use '.'
 * even when the host application switched the process to a comma-decimal
 * locale (Excel and Tableau do). */
static void test_rowset_to_cache_double_ignores_locale(void **state)
{
    (void)state;
    const char *locale = argus_test_use_comma_locale();
    if (!locale && argus_test_locale_required()) fail();
    if (!locale) skip();

    GPtrArray *cols = columns(double_column(2, 1.5), NULL);
    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));
    int num_cols = 0;

    int rc = argus_hs2_rowset_to_cache(cols, &cache, &num_cols);
    argus_test_restore_c_locale();
    assert_int_equal(rc, 0);
    assert_int_equal(cache.num_rows, 2);
    assert_string_equal(cache.rows[0].cells[0].data, "1.5");
    assert_int_equal(cache.rows[0].cells[0].data_len, 3);
    assert_string_equal(cache.rows[1].cells[0].data, "2.5");
    /* ...and the native value rides along for numeric targets. */
    assert_int_equal(cache.rows[0].cells[0].native_kind, ARGUS_NATIVE_F64);
    assert_true(cache.rows[1].cells[0].native.f64 == 2.5);

    argus_row_cache_clear(&cache);
    g_ptr_array_unref(cols);
}

/* The text of a DOUBLE reads back as the same double: 15 digits when they
 * round-trip, the 17 a double can need otherwise. */
static void test_rowset_to_cache_double_round_trips(void **state)
{
    (void)state;
    GPtrArray *cols = columns(double_column(1, 0.1 + 0.2), NULL);
    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));

    assert_int_equal(argus_hs2_rowset_to_cache(cols, &cache, NULL), 0);
    assert_string_equal(cache.rows[0].cells[0].data, "0.30000000000000004");
    argus_row_cache_clear(&cache);
    g_ptr_array_unref(cols);

    cols = columns(double_column(1, 0.1), NULL);
    memset(&cache, 0, sizeof(cache));
    assert_int_equal(argus_hs2_rowset_to_cache(cols, &cache, NULL), 0);
    assert_string_equal(cache.rows[0].cells[0].data, "0.1");
    argus_row_cache_clear(&cache);
    g_ptr_array_unref(cols);
}

static void test_rowset_to_cache_short_column_leaves_cells_empty(void **state)
{
    (void)state;
    GPtrArray *cols = columns(string_column(1), binary_column(2, -1), NULL);
    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));

    assert_int_equal(argus_hs2_rowset_to_cache(cols, &cache, NULL), 0);
    assert_int_equal(cache.num_cols, 2);
    assert_int_equal(cache.num_rows, 2);
    assert_string_equal(cache.rows[0].cells[0].data, "s0");
    assert_null(cache.rows[1].cells[0].data);
    assert_int_equal(cache.rows[1].cells[1].data_len, 3);
    assert_memory_equal(cache.rows[1].cells[1].data, "\x01\xff\x10", 3);

    argus_row_cache_clear(&cache);
    g_ptr_array_unref(cols);
}

static void test_rowset_to_cache_empty(void **state)
{
    (void)state;
    argus_row_cache_t cache;
    memset(&cache, 0, sizeof(cache));
    int num_cols = 7;

    assert_int_equal(argus_hs2_rowset_to_cache(NULL, &cache, &num_cols), 0);
    assert_int_equal(cache.num_rows, 0);
    assert_int_equal(num_cols, 7);       /* untouched: no columns seen */

    GPtrArray *cols = columns(empty_string_column(), NULL);
    assert_int_equal(argus_hs2_rowset_to_cache(cols, &cache, &num_cols), 0);
    assert_int_equal(cache.num_rows, 0);
    assert_int_equal(num_cols, 1);
    assert_null(cache.rows);
    g_ptr_array_unref(cols);
}

static void test_status_ok(void **state)
{
    (void)state;
    char err[64];

    assert_true(argus_hs2_status_ok(NULL, err, sizeof(err)));

    TStatus *st = g_object_new(TYPE_T_STATUS, NULL);
    g_object_set(st, "statusCode", T_STATUS_CODE_SUCCESS_WITH_INFO_STATUS, NULL);
    assert_true(argus_hs2_status_ok(st, err, sizeof(err)));

    g_object_set(st, "statusCode", T_STATUS_CODE_ERROR_STATUS,
                 "errorMessage", "Table not found", NULL);
    err[0] = '\0';
    assert_false(argus_hs2_status_ok(st, err, sizeof(err)));
    assert_string_equal(err, "Table not found");

    /* Long messages are truncated, not overrun. */
    char long_msg[300];
    memset(long_msg, 'x', sizeof(long_msg) - 1);
    long_msg[sizeof(long_msg) - 1] = '\0';
    g_object_set(st, "errorMessage", long_msg, NULL);
    assert_false(argus_hs2_status_ok(st, err, sizeof(err)));
    assert_int_equal(strlen(err), sizeof(err) - 1);
    g_object_unref(st);

    st = g_object_new(TYPE_T_STATUS, NULL);
    g_object_set(st, "statusCode", T_STATUS_CODE_INVALID_HANDLE_STATUS, NULL);
    assert_false(argus_hs2_status_ok(st, err, sizeof(err)));
    assert_string_equal(err, "Invalid operation handle");
    g_object_unref(st);
}

/* ── Fake HiveServer2: a client reading canned replies ────────── */

typedef struct {
    ThriftMemoryBuffer *in;    /* replies the "server" sends */
    ThriftMemoryBuffer *out;   /* requests the client writes, discarded */
    ThriftProtocol     *in_proto;
    ThriftProtocol     *out_proto;
    TCLIServiceIf      *client;
    TOperationHandle   *op_handle;
} fake_hs2_t;

static void fake_hs2_init(fake_hs2_t *f)
{
    f->in  = g_object_new(THRIFT_TYPE_MEMORY_BUFFER, "buf_size", (guint)(1u << 20), NULL);
    f->out = g_object_new(THRIFT_TYPE_MEMORY_BUFFER, "buf_size", (guint)(1u << 20), NULL);
    f->in_proto  = g_object_new(THRIFT_TYPE_BINARY_PROTOCOL, "transport", f->in, NULL);
    f->out_proto = g_object_new(THRIFT_TYPE_BINARY_PROTOCOL, "transport", f->out, NULL);
    f->client = g_object_new(TYPE_T_C_L_I_SERVICE_CLIENT,
                             "input_protocol", f->in_proto,
                             "output_protocol", f->out_proto,
                             NULL);
    f->op_handle = g_object_new(TYPE_T_OPERATION_HANDLE, NULL);
    GByteArray *id = g_byte_array_new();
    g_byte_array_append(id, (const guint8 *)"0123456789abcdef", 16);
    g_object_set(f->op_handle->operationId, "guid", id, "secret", id, NULL);
    g_byte_array_unref(id);
}

static void fake_hs2_free(fake_hs2_t *f)
{
    g_object_unref(f->op_handle);
    g_object_unref(f->client);
    g_object_unref(f->in_proto);
    g_object_unref(f->out_proto);
    g_object_unref(f->in);
    g_object_unref(f->out);
}

/* Queue one FetchResults reply, framed exactly as the generated
 * recv_fetch_results expects it: REPLY message, result struct, field 0. */
static void fake_hs2_reply(fake_hs2_t *f, TFetchResultsResp *resp)
{
    GError *err = NULL;
    ThriftProtocol *p = f->in_proto;
    assert_true(thrift_protocol_write_message_begin(p, "FetchResults", T_REPLY, 0, &err) >= 0);
    assert_true(thrift_protocol_write_struct_begin(p, "FetchResults_result", &err) >= 0);
    assert_true(thrift_protocol_write_field_begin(p, "success", T_STRUCT, 0, &err) >= 0);
    assert_true(thrift_struct_write(THRIFT_STRUCT(resp), p, &err) >= 0);
    assert_true(thrift_protocol_write_field_end(p, &err) >= 0);
    assert_true(thrift_protocol_write_field_stop(p, &err) >= 0);
    assert_true(thrift_protocol_write_struct_end(p, &err) >= 0);
    assert_true(thrift_protocol_write_message_end(p, &err) >= 0);
    assert_null(err);
    g_object_unref(resp);
}

static TFetchResultsResp *reply(TStatusCode code, gboolean has_more,
                                GPtrArray *cols)
{
    TFetchResultsResp *resp = g_object_new(TYPE_T_FETCH_RESULTS_RESP, NULL);
    g_object_set(resp->status, "statusCode", code, NULL);
    g_object_set(resp, "hasMoreRows", has_more, NULL);
    if (cols) {
        g_object_set(resp->results, "columns", cols, NULL);
        g_ptr_array_unref(cols);
    }
    resp->__isset_results = TRUE;
    return resp;
}

static TFetchResultsResp *error_reply(const char *message)
{
    TFetchResultsResp *resp = g_object_new(TYPE_T_FETCH_RESULTS_RESP, NULL);
    g_object_set(resp->status, "statusCode", T_STATUS_CODE_ERROR_STATUS,
                 "errorMessage", message, NULL);
    return resp;
}

/* Queue a Thrift application exception in place of a reply, which is what
 * a server answers to a malformed or unknown request. */
static void fake_hs2_exception(fake_hs2_t *f, const char *message)
{
    GError *err = NULL;
    ThriftProtocol *p = f->in_proto;
    ThriftApplicationException *x =
        g_object_new(THRIFT_TYPE_APPLICATION_EXCEPTION,
                     "type", THRIFT_APPLICATION_EXCEPTION_ERROR_INTERNAL_ERROR,
                     "message", message, NULL);
    assert_true(thrift_protocol_write_message_begin(p, "FetchResults", T_EXCEPTION, 0, &err) >= 0);
    assert_true(thrift_struct_write(THRIFT_STRUCT(x), p, &err) >= 0);
    assert_true(thrift_protocol_write_message_end(p, &err) >= 0);
    assert_null(err);
    g_object_unref(x);
}

static guint replies_left(fake_hs2_t *f) { return f->in->buf->len; }

/* ── Backend-level tests ──────────────────────────────────────── */

static void test_hive_retries_empty_batch_with_more_rows(void **state)
{
    (void)state;
    fake_hs2_t f; fake_hs2_init(&f);
    hive_conn_t conn; memset(&conn, 0, sizeof(conn));
    conn.client = f.client;
    hive_operation_t op; memset(&op, 0, sizeof(op));
    op.op_handle = f.op_handle;
    op.metadata_fetched = true;

    /* Two "not yet" batches, then the rows. Before the fix the first empty
     * batch ended the result set with zero rows. */
    fake_hs2_reply(&f, reply(T_STATUS_CODE_SUCCESS_STATUS, TRUE, NULL));
    fake_hs2_reply(&f, reply(T_STATUS_CODE_SUCCESS_STATUS, TRUE,
                             columns(empty_string_column(), NULL)));
    fake_hs2_reply(&f, reply(T_STATUS_CODE_SUCCESS_STATUS, FALSE,
                             columns(string_column(2), binary_column(2, -1), NULL)));

    argus_row_cache_t cache; memset(&cache, 0, sizeof(cache));
    int num_cols = 0;
    assert_int_equal(hive_fetch_results(&conn, &op, 100, &cache, NULL, &num_cols), 0);
    assert_int_equal(num_cols, 2);
    assert_int_equal(cache.num_rows, 2);
    assert_string_equal(cache.rows[1].cells[0].data, "s1");
    assert_memory_equal(cache.rows[1].cells[1].data, "\x01\xff\x10", 3);
    assert_int_equal(replies_left(&f), 0);      /* all three consumed */
    assert_false(cache.exhausted);              /* the ODBC layer decides */

    argus_row_cache_clear(&cache);
    fake_hs2_free(&f);
}

static void test_impala_short_batch_is_not_the_end(void **state)
{
    (void)state;
    fake_hs2_t f; fake_hs2_init(&f);
    impala_conn_t conn; memset(&conn, 0, sizeof(conn));
    conn.client = f.client;
    impala_operation_t op; memset(&op, 0, sizeof(op));
    op.op_handle = f.op_handle;
    op.metadata_fetched = true;

    /* 3 rows for a 100-row request with hasMoreRows=true: one batch per
     * call, never a retry, and the flag never marks the cache exhausted. */
    fake_hs2_reply(&f, reply(T_STATUS_CODE_SUCCESS_STATUS, TRUE,
                             columns(binary_column(3, -1), NULL)));
    fake_hs2_reply(&f, reply(T_STATUS_CODE_SUCCESS_STATUS, FALSE, NULL));

    argus_row_cache_t cache; memset(&cache, 0, sizeof(cache));
    assert_int_equal(impala_fetch_results(&conn, &op, 100, &cache, NULL, NULL), 0);
    assert_int_equal(cache.num_rows, 3);
    assert_false(cache.exhausted);
    assert_true(replies_left(&f) > 0);          /* second reply still queued */
    argus_row_cache_clear(&cache);

    /* The final empty batch: zero rows, which ends the result upstream. */
    assert_int_equal(impala_fetch_results(&conn, &op, 100, &cache, NULL, NULL), 0);
    assert_int_equal(cache.num_rows, 0);
    assert_int_equal(replies_left(&f), 0);

    fake_hs2_free(&f);
}

static void test_hive_server_error_is_reported(void **state)
{
    (void)state;
    fake_hs2_t f; fake_hs2_init(&f);
    hive_conn_t conn; memset(&conn, 0, sizeof(conn));
    conn.client = f.client;
    hive_operation_t op; memset(&op, 0, sizeof(op));
    op.op_handle = f.op_handle;
    op.metadata_fetched = true;

    fake_hs2_reply(&f, error_reply("Error while processing statement: boom"));

    argus_row_cache_t cache; memset(&cache, 0, sizeof(cache));
    assert_int_equal(hive_fetch_results(&conn, &op, 100, &cache, NULL, NULL), -1);
    assert_int_equal(cache.num_rows, 0);

    char msg[512];
    assert_true(hive_get_last_error(&conn, msg, sizeof(msg)));
    assert_string_equal(msg, "Error while processing statement: boom");

    /* A Thrift-level failure (application exception) is reported too. */
    fake_hs2_exception(&f, "Internal error processing FetchResults");
    assert_int_equal(hive_fetch_results(&conn, &op, 100, &cache, NULL, NULL), -1);
    assert_true(hive_get_last_error(&conn, msg, sizeof(msg)));
    assert_memory_equal(msg, "FetchResults failed: ", 21);
    assert_non_null(strstr(msg, "Internal error processing FetchResults"));
    assert_int_equal(replies_left(&f), 0);

    fake_hs2_free(&f);
}


/*
 * HiveServer2 and Impala both fill TStatus.sqlState, and the driver read
 * only the message, so every server failure reached the application as
 * HY000 -- the one part of a diagnostic a BI tool can branch on.
 */
static void test_status_sqlstate(void **state)
{
    (void)state;
    char st6[6];

    /* No status at all, and a status with no state: HY000. */
    argus_hs2_status_sqlstate(NULL, st6);
    assert_string_equal(st6, "HY000");

    TStatus *st = g_object_new(TYPE_T_STATUS, NULL);
    g_object_set(st, "statusCode", T_STATUS_CODE_ERROR_STATUS, NULL);
    argus_hs2_status_sqlstate(st, st6);
    assert_string_equal(st6, "HY000");

    /* The state the server named comes through unchanged. */
    g_object_set(st, "sqlState", "42S02", NULL);
    argus_hs2_status_sqlstate(st, st6);
    assert_string_equal(st6, "42S02");

    /* "no error" and anything that is not five characters are not states. */
    g_object_set(st, "sqlState", "00000", NULL);
    argus_hs2_status_sqlstate(st, st6);
    assert_string_equal(st6, "HY000");

    g_object_set(st, "sqlState", "42", NULL);
    argus_hs2_status_sqlstate(st, st6);
    assert_string_equal(st6, "HY000");

    g_object_unref(st);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_row_count_counts_binary_column),
        cmocka_unit_test(test_row_count_is_longest_column),
        cmocka_unit_test(test_rowset_to_cache_binary_only),
        cmocka_unit_test(test_rowset_to_cache_double_ignores_locale),
        cmocka_unit_test(test_rowset_to_cache_double_round_trips),
        cmocka_unit_test(test_rowset_to_cache_short_column_leaves_cells_empty),
        cmocka_unit_test(test_rowset_to_cache_empty),
        cmocka_unit_test(test_status_ok),
        cmocka_unit_test(test_status_sqlstate),
        cmocka_unit_test(test_hive_retries_empty_batch_with_more_rows),
        cmocka_unit_test(test_impala_short_batch_is_not_the_end),
        cmocka_unit_test(test_hive_server_error_is_reported),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
