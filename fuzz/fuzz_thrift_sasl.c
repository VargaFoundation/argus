/* SPDX-License-Identifier: Apache-2.0 */
/*
 * fuzz_thrift_sasl.c — libFuzzer harness for the SASL frame reader
 * (src/backend/thrift_sasl.c), the code that parses what a HiveServer2 or
 * Impala daemon answers during the PLAIN handshake: [status][length]
 * [payload] frames, with a length the peer chose. It is the one parser in
 * the driver that reads bytes from an unauthenticated peer before any
 * session exists.
 *
 * The fuzz input is the server's side of the conversation, served from a
 * memory buffer. ThriftMemoryBuffer alone will not do: its read returns 0
 * once the buffer is spent and thrift's read_all loops on 0 forever, so a
 * frame cut short -- the most ordinary mutation there is -- hung the
 * harness for libFuzzer's whole per-input timeout. The driver's own socket
 * transport (thrift_gio_transport.c) turns EOF into an error for exactly
 * that reason; the small subclass below does the same for the buffer, so
 * the reader sees what it would see from a peer that closed mid-frame.
 *
 * Build: CC=clang cmake -B build-fuzz -DENABLE_FUZZING=ON && cmake --build build-fuzz
 * Run:   ./build-fuzz/fuzz/fuzz_thrift_sasl fuzz/corpus/thrift_sasl -max_total_time=60
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <glib-object.h>
#include <thrift/c_glib/transport/thrift_transport.h>
#include <thrift/c_glib/transport/thrift_memory_buffer.h>

#include "thrift_sasl.h"

/* ── A memory buffer whose end is an EOF, not a zero-byte read ──── */

/* Classic boilerplate rather than G_DECLARE_FINAL_TYPE: the latter wants
 * the parent's autoptr cleanup, which thrift's types do not declare. */
typedef struct { ThriftMemoryBuffer parent; }           FuzzWire;
typedef struct { ThriftMemoryBufferClass parent_class; } FuzzWireClass;
#define FUZZ_TYPE_WIRE (fuzz_wire_get_type())
GType fuzz_wire_get_type(void);
G_DEFINE_TYPE(FuzzWire, fuzz_wire, THRIFT_TYPE_MEMORY_BUFFER)

static gint32 fuzz_wire_read(ThriftTransport *t, gpointer buf, guint32 len,
                             GError **error)
{
    ThriftTransportClass *parent =
        THRIFT_TRANSPORT_CLASS(fuzz_wire_parent_class);
    gint32 n = parent->read(t, buf, len, error);
    if (n == 0 && len > 0) {
        g_set_error(error, THRIFT_TRANSPORT_ERROR,
                    THRIFT_TRANSPORT_ERROR_RECEIVE, "peer closed the wire");
        return -1;
    }
    return n;
}

/* read_all through OUR read, so the EOF above ends the loop. */
static gint32 fuzz_wire_read_all(ThriftTransport *t, gpointer buf,
                                 guint32 len, GError **error)
{
    guint32 got = 0;
    while (got < len) {
        gint32 n = fuzz_wire_read(t, (guint8 *)buf + got, len - got, error);
        if (n < 0) return -1;
        got += (guint32)n;
    }
    return (gint32)got;
}

static void fuzz_wire_class_init(FuzzWireClass *klass)
{
    ThriftTransportClass *ttc = THRIFT_TRANSPORT_CLASS(klass);
    ttc->read = fuzz_wire_read;
    ttc->read_all = fuzz_wire_read_all;
}

static void fuzz_wire_init(FuzzWire *self)
{
    (void)self;
}

/* ── The harness ──────────────────────────────────────────────── */

/* One wire for the whole run, emptied between inputs. Finalising a
 * ThriftMemoryBuffer per input leaves a small allocation behind inside
 * GLib's array teardown, and libFuzzer's leak check would stop the run on
 * that -- a leak in the harness's furniture, not in the reader under test.
 * A single live object is reachable at exit and reported by nobody. */
static ThriftTransport *g_wire;

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16) return 0;

    if (!g_wire)
        g_wire = g_object_new(FUZZ_TYPE_WIRE, "buf_size", (guint)(1 << 17), NULL);
    GByteArray *buf = THRIFT_MEMORY_BUFFER(g_wire)->buf;
    g_byte_array_set_size(buf, 0);

    /* The input is the server's side of the conversation; what the
     * handshake writes lands behind it and is read back as further
     * "replies" once the input is spent -- one more shape of garbage --
     * and then the wire ends. */
    if (size) g_byte_array_append(buf, data, (guint)size);

    char errmsg[512];
    (void)argus_thrift_sasl_handshake_plain(g_wire, "fuzz", "fuzz",
                                            errmsg, sizeof(errmsg));
    return 0;
}
