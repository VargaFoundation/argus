/* SPDX-License-Identifier: Apache-2.0 */
/*
 * fuzz_kudu_sql.c — libFuzzer harness for the Kudu SQL parser
 * (src/backend/kudu/kudu_sql_parser.c), a hand-written recogniser for the
 * SELECT subset the Kudu backend can push down: column lists, WHERE
 * predicates with IN lists and quoted literals, comments, LIMIT. It reads
 * application-controlled SQL, so every character position is an input.
 * The parser is plain C with no dependency on the Kudu client, and is
 * compiled on every platform, so this runs everywhere the other harnesses
 * do.
 *
 * Build: CC=clang cmake -B build-fuzz -DENABLE_FUZZING=ON && cmake --build build-fuzz
 * Run:   ./build-fuzz/fuzz/fuzz_kudu_sql fuzz/corpus/kudu_sql -max_total_time=60
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "kudu_sql_parser.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16) return 0;   /* keep iterations fast */

    char *sql = malloc(size + 1);
    if (!sql) return 0;
    memcpy(sql, data, size);
    sql[size] = '\0';

    kudu_parsed_query_t q;
    memset(&q, 0, sizeof(q));
    const char *err = NULL;
    if (kudu_sql_parse(sql, &q, &err) == 0)
        kudu_parsed_query_free(&q);

    free(sql);
    return 0;
}
