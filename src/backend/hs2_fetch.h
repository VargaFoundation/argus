/*
 * hs2_fetch.h — shared columnar TRowSet parsing for the HiveServer2 family
 * (Hive, Impala; Spark Thrift Server and Flink SQL Gateway through the hive
 * backend). Replaces the two near-identical 250-line parse_column_values
 * copies, and builds SINGLE-BLOCK rows (argus_row_alloc_block): one
 * allocation per row instead of one per cell — the hot path for every large
 * HS2 extract.
 */
#ifndef ARGUS_HS2_FETCH_H
#define ARGUS_HS2_FETCH_H

#include <glib.h>
#include "argus/types.h"

/*
 * Parse a TRowSet's TColumn array into cache->rows[0..num_rows).
 * `tcolumns` is the GPtrArray of TColumn GObjects; cache->rows must already
 * be a calloc'd array of num_rows argus_row_t (cells NULL). Fills
 * cache->rows[r].cells as one block per row. Returns 0 or -1 on allocation
 * failure. Cells in rows a short column does not cover stay zeroed, exactly
 * like the previous per-cell parser.
 */
int argus_hs2_parse_rowset(GPtrArray *tcolumns, int ncols, int num_rows,
                           argus_row_cache_t *cache);

#endif /* ARGUS_HS2_FETCH_H */
