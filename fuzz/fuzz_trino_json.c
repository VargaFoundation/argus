/*
 * fuzz_trino_json.c — libFuzzer harness for the hand-written JSON scanner the
 * Trino backend uses on the fetch fast path.
 *
 * This is the one parser in the driver that walks raw network bytes with
 * pointer arithmetic and no library between it and the server: building a
 * json-glib DOM for every result page costs about half of fetch time, so the
 * `data` array is scanned straight into the row cache. Everything it reads —
 * string escapes, nesting depth, number syntax, where the array ends — comes
 * from whatever answered on the coordinator's port.
 *
 * Build: CC=clang cmake -B build-fuzz -DENABLE_FUZZING=ON && cmake --build build-fuzz
 * Run:   ./build-fuzz/fuzz/fuzz_trino_json fuzz/corpus/trino_json -max_total_time=90
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "argus/types.h"
#include "trino_internal.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16) return 0;

    char *s = malloc(size + 1);
    if (!s) return 0;
    memcpy(s, data, size);
    s[size] = '\0';

    /* The envelope path: find `data` in what the server sent, then scan it.
     * A column count the response does not agree with is exactly the case
     * that matters, so it is driven from the input rather than fixed. */
    const char *vs = NULL, *ve = NULL;
    if (trino_sj_find_member(s, size, "data", &vs, &ve) == 0 && vs && ve) {
        argus_row_cache_t cache;
        memset(&cache, 0, sizeof(cache));
        int ncols = (size ? (int)(data[0] & 0x07) : 0) + 1;   /* 1..8 */
        (void)trino_sj_scan_data(vs, ve, &cache, ncols);
        argus_row_cache_free(&cache);
    }

    /* And the scanner on its own, over the whole input: a response whose
     * `data` member the locator rejects must not be the only way in. */
    {
        argus_row_cache_t cache;
        memset(&cache, 0, sizeof(cache));
        (void)trino_sj_scan_data(s, s + size, &cache, 2);
        argus_row_cache_free(&cache);
    }

    free(s);
    return 0;
}
