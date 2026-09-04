/* SPDX-License-Identifier: Apache-2.0 */
/*
 * fuzz_connstr.c — libFuzzer harness for the connection-string parser
 * (argus_conn_params_parse), which consumes fully attacker-controlled input:
 * a DSN-less connection string arrives verbatim from the application.
 *
 * Build: CC=clang cmake -B build-fuzz -DENABLE_FUZZING=ON && cmake --build build-fuzz
 * Run:   ./build-fuzz/fuzz/fuzz_connstr fuzz/corpus/connstr -max_total_time=60
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "argus/types.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16) return 0;

    char *s = malloc(size + 1);
    if (!s) return 0;
    memcpy(s, data, size);
    s[size] = '\0';

    argus_conn_params_t params;
    argus_conn_params_init(&params);
    if (argus_conn_params_parse(&params, s) == 0) {
        /* Exercise the lookup path with keys the driver actually asks for. */
        (void)argus_conn_params_get(&params, "HOST");
        (void)argus_conn_params_get(&params, "BACKEND");
        (void)argus_conn_params_get(&params, "PWD");
    }
    argus_conn_params_free(&params);

    free(s);
    return 0;
}
