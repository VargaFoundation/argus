/* SPDX-License-Identifier: Apache-2.0 */
/*
 * test_thrift_sasl.c — SASL negotiation framing and the libthrift size
 * limits, driven through a ThriftMemoryBuffer instead of a server.
 *
 * A memory buffer is a FIFO: reads consume from the front, writes append
 * at the back. Queue the server's replies first and the client's own
 * messages land behind them, so once the handshake returns the buffer
 * holds exactly what the client put on the wire.
 *
 * Covers the pre-authentication frame cap (the frame length is the peer's
 * word, read in clear text before any credential is checked), the bounded
 * copy of a server-supplied rejection message, and the frame/message caps
 * attached to the framed transport used after SASL.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <string.h>
#include <cmocka.h>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

#include <thrift/c_glib/transport/thrift_memory_buffer.h>
#include <thrift/c_glib/transport/thrift_framed_transport.h>

#include "thrift_sasl.h"
#include "thrift_limits.h"

/* ── Wire helpers ─────────────────────────────────────────────── */

static ThriftTransport *wire_new(guint capacity)
{
    return g_object_new(THRIFT_TYPE_MEMORY_BUFFER,
                        "buf_size", capacity, NULL);
}

static GByteArray *wire_bytes(ThriftTransport *t)
{
    return THRIFT_MEMORY_BUFFER(t)->buf;
}

static void queue_bytes(ThriftTransport *t, const void *p, guint32 len)
{
    GError *err = NULL;
    assert_true(thrift_transport_write(t, (gpointer)p, len, &err));
    assert_null(err);
}

/* [status][big-endian length] with no payload behind it */
static void queue_header(ThriftTransport *t, guint8 status, guint32 len)
{
    guint32 net = htonl(len);
    queue_bytes(t, &status, 1);
    queue_bytes(t, &net, 4);
}

static void queue_reply(ThriftTransport *t, guint8 status,
                        const void *payload, guint32 len)
{
    queue_header(t, status, len);
    if (len > 0) queue_bytes(t, payload, len);
}

static void assert_wire_equals(ThriftTransport *t, const void *expected,
                               size_t len)
{
    GByteArray *b = wire_bytes(t);
    assert_int_equal(b->len, len);
    assert_memory_equal(b->data, expected, len);
}

/* ── PLAIN handshake ──────────────────────────────────────────── */

static void test_plain_success_sends_start_and_initial_response(void **state)
{
    (void)state;
    ThriftTransport *t = wire_new(4096);
    queue_header(t, TSASL_COMPLETE, 0);

    char err[512] = "";
    assert_int_equal(argus_thrift_sasl_handshake_plain(t, "user", "secret",
                                                       err, sizeof(err)), 0);

    /* START + "PLAIN", then OK + "\0user\0secret" — and nothing else. */
    static const char expected[] =
        "\x01" "\x00\x00\x00\x05" "PLAIN"
        "\x02" "\x00\x00\x00\x0c" "\x00" "user" "\x00" "secret";
    assert_wire_equals(t, expected, sizeof(expected) - 1);
    g_object_unref(t);
}

static void test_plain_rejection_reports_server_message(void **state)
{
    (void)state;
    ThriftTransport *t = wire_new(4096);
    static const char why[] = "Error validating the login";
    queue_reply(t, TSASL_ERROR, why, sizeof(why) - 1);

    char err[512] = "";
    assert_int_equal(argus_thrift_sasl_handshake_plain(t, "u", "p",
                                                       err, sizeof(err)), -1);
    assert_string_equal(err,
        "SASL handshake rejected (status=4): Error validating the login");
    g_object_unref(t);
}

/* A rejection message longer than the caller's buffer used to be memcpy'd
 * into a 512-byte stack array with a bound derived from errmsg_size - 60,
 * which underflows for small buffers. Both sizes must now stay in bounds. */
static void test_plain_rejection_excerpt_is_bounded(void **state)
{
    (void)state;
    const guint32 huge = 5000;
    char *why = g_malloc(huge);
    memset(why, 'x', huge);

    static const size_t sizes[] = { 512, 64, 40, 8 };
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        ThriftTransport *t = wire_new(1 << 16);
        queue_reply(t, TSASL_BAD, why, huge);

        char err[512];
        memset(err, 0x7f, sizeof(err));
        assert_int_equal(argus_thrift_sasl_handshake_plain(t, "u", "p",
                                                           err, sizes[i]), -1);
        /* NUL-terminated inside the declared size, untouched beyond it */
        assert_true(strnlen(err, sizes[i]) < sizes[i]);
        for (size_t k = sizes[i]; k < sizeof(err); k++)
            assert_int_equal((unsigned char)err[k], 0x7f);
        if (sizes[i] == 512) {
            assert_true(g_str_has_prefix(err,
                        "SASL handshake rejected (status=3): xxxx"));
            assert_int_equal(strlen(err),
                             strlen("SASL handshake rejected (status=3): ")
                             + ARGUS_THRIFT_SASL_ERR_EXCERPT);
        }
        g_object_unref(t);
    }
    g_free(why);
}

/* ── Frame cap ────────────────────────────────────────────────── */

static void test_oversized_sasl_frame_is_refused_before_reading(void **state)
{
    (void)state;
    static const guint32 lengths[] = { 0xFFFFFFFFu, 0x7FFFFFFFu,
                                       ARGUS_THRIFT_SASL_MAX_FRAME + 1 };
    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
        ThriftTransport *t = wire_new(4096);
        queue_header(t, TSASL_OK, lengths[i]);

        char err[512] = "";
        assert_int_equal(argus_thrift_sasl_handshake_plain(t, "u", "p",
                                                           err, sizeof(err)), -1);
        assert_non_null(strstr(err, "SASL frame"));
        assert_non_null(strstr(err, "limit"));

        /* Only the client's own two messages remain: the announced payload
         * was never read (there was none to read, and it never asked). */
        static const char expected[] =
            "\x01" "\x00\x00\x00\x05" "PLAIN"
            "\x02" "\x00\x00\x00\x04" "\x00" "u" "\x00" "p";
        assert_wire_equals(t, expected, sizeof(expected) - 1);
        g_object_unref(t);
    }
}

static void test_sasl_frame_at_the_cap_is_still_read(void **state)
{
    (void)state;
    ThriftTransport *t = wire_new(ARGUS_THRIFT_SASL_MAX_FRAME + 4096);
    char *why = g_malloc(ARGUS_THRIFT_SASL_MAX_FRAME);
    memset(why, 'y', ARGUS_THRIFT_SASL_MAX_FRAME);
    queue_reply(t, TSASL_ERROR, why, ARGUS_THRIFT_SASL_MAX_FRAME);
    g_free(why);

    char err[512] = "";
    assert_int_equal(argus_thrift_sasl_handshake_plain(t, "u", "p",
                                                       err, sizeof(err)), -1);
    /* The frame was accepted and consumed; the status is what failed. */
    assert_true(g_str_has_prefix(err, "SASL handshake rejected (status=4): yyyy"));
    assert_int_equal(wire_bytes(t)->len, 10 + 9);   /* START + OK only */
    g_object_unref(t);
}

/* ── libthrift limits on the post-SASL transport ──────────────── */

static void test_configuration_carries_the_pinned_limits(void **state)
{
    (void)state;
    ThriftConfiguration *c = argus_thrift_configuration_new();
    assert_int_equal(c->maxFrameSize_, ARGUS_THRIFT_MAX_FRAME_SIZE);
    assert_int_equal(c->maxMessageSize_, ARGUS_THRIFT_MAX_MESSAGE_SIZE);
    assert_int_equal(c->recursionLimit_, ARGUS_THRIFT_RECURSION_LIMIT);
    g_object_unref(c);
}

static void test_framed_transport_refuses_oversized_frame(void **state)
{
    (void)state;
    ThriftTransport *inner = wire_new(4096);
    guint32 net = htonl((guint32)ARGUS_THRIFT_MAX_FRAME_SIZE + 1);
    queue_bytes(inner, &net, 4);

    ThriftTransport *framed = argus_thrift_framed_transport_new(inner);
    assert_int_equal(THRIFT_FRAMED_TRANSPORT(framed)->max_frame_size,
                     ARGUS_THRIFT_MAX_FRAME_SIZE);

    guint8 buf[4];
    GError *err = NULL;
    assert_int_equal(thrift_transport_read(framed, buf, sizeof(buf), &err), -1);
    assert_non_null(err);
    assert_true(err->domain == THRIFT_TRANSPORT_ERROR);
    assert_int_equal(err->code, THRIFT_TRANSPORT_ERROR_MAX_MESSAGE_SIZE_REACHED);
    g_error_free(err);

    g_object_unref(framed);
    g_object_unref(inner);
}

/* A frame above libthrift's own 16 MB default but below the driver's cap
 * must go through: the point of pinning the limit is a wide FetchResults
 * batch, not a smaller ceiling than the library ships with. */
static void test_framed_transport_reads_frame_above_library_default(void **state)
{
    (void)state;
    const guint32 frame = 20u * 1024 * 1024;
    ThriftTransport *inner = wire_new(frame + 4096);
    guint32 net = htonl(frame);
    queue_bytes(inner, &net, 4);
    guint8 *payload = g_malloc(frame);
    for (guint32 i = 0; i < frame; i++) payload[i] = (guint8)(i * 7);
    queue_bytes(inner, payload, frame);

    ThriftTransport *framed = argus_thrift_framed_transport_new(inner);
    guint8 head[8];
    GError *err = NULL;
    assert_int_equal(thrift_transport_read_all(framed, head, sizeof(head), &err),
                     (gint32)sizeof(head));
    assert_null(err);
    assert_memory_equal(head, payload, sizeof(head));

    g_free(payload);
    g_object_unref(framed);
    g_object_unref(inner);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_plain_success_sends_start_and_initial_response),
        cmocka_unit_test(test_plain_rejection_reports_server_message),
        cmocka_unit_test(test_plain_rejection_excerpt_is_bounded),
        cmocka_unit_test(test_oversized_sasl_frame_is_refused_before_reading),
        cmocka_unit_test(test_sasl_frame_at_the_cap_is_still_read),
        cmocka_unit_test(test_configuration_carries_the_pinned_limits),
        cmocka_unit_test(test_framed_transport_refuses_oversized_frame),
        cmocka_unit_test(test_framed_transport_reads_frame_above_library_default),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
