/* SPDX-License-Identifier: Apache-2.0 */
/*
 * fuzz_thrift_sasl.c — libFuzzer harness for the SASL frame reader
 * (src/backend/thrift_sasl.c), the code that parses what a HiveServer2 or
 * Impala daemon answers during the PLAIN handshake: [status][length]
 * [payload] frames, with a length the peer chose. It is the one parser in
 * the driver that reads bytes from an unauthenticated peer before any
 * session exists, and the unit test drives it through the same
 * ThriftMemoryBuffer used here. The fuzz input is the server's side of the
 * conversation: the handshake's own writes land behind it in the FIFO and
 * are read back as further "replies" once the input is spent, which is one
 * more shape of garbage.
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

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16) return 0;

    /* Room for the reply, plus what the handshake writes behind it. */
    ThriftTransport *t = g_object_new(THRIFT_TYPE_MEMORY_BUFFER,
                                      "buf_size", (guint)(size + 4096), NULL);
    if (size) {
        GError *err = NULL;
        if (!thrift_transport_write(t, (gpointer)data, (guint32)size, &err)) {
            if (err) g_error_free(err);
            g_object_unref(t);
            return 0;
        }
    }

    char errmsg[512];
    (void)argus_thrift_sasl_handshake_plain(t, "fuzz", "fuzz",
                                            errmsg, sizeof(errmsg));
    g_object_unref(t);
    return 0;
}
