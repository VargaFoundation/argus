/* SPDX-License-Identifier: Apache-2.0 */
/*
 * fuzz_escape.c — libFuzzer harness for the ODBC escape-sequence translator
 * (src/odbc/escape.c), the hand-written parser every {fn}-generating BI tool
 * exercises with application-controlled SQL. Runs the input through every
 * registered dialect so dialect-specific template expansion is covered too.
 *
 * Build: CC=clang cmake -B build-fuzz -DENABLE_FUZZING=ON && cmake --build build-fuzz
 * Run:   ./build-fuzz/fuzz/fuzz_escape fuzz/corpus/escape -max_total_time=60
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#include "argus/dialect.h"
#include "argus/error.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16) return 0;   /* keep iterations fast */

    char *sql = malloc(size + 1);
    if (!sql) return 0;
    memcpy(sql, data, size);
    sql[size] = '\0';

    for (size_t i = 0; i < argus_dialect_count(); i++) {
        const argus_dialect_t *d = argus_dialect_at(i);
        argus_diag_t diag = {0};
        char *out = NULL;
        argus_escape_result_t r = argus_escape_translate(d, sql, &out, &diag);
        if (r == ARGUS_ESCAPE_OK) g_free(out);
        argus_diag_dispose(&diag);
    }

    free(sql);
    return 0;
}
