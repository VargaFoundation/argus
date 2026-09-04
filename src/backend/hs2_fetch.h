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
#include <stdbool.h>
#include <stddef.h>
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

/*
 * Number of rows carried by a TRowSet's TColumn array: the longest populated
 * `values` array over every column, all seven union members plus binaryVal
 * probed. One column is not enough (a NULL-only column such as GetTables'
 * TABLE_CAT ships an empty values array) and skipping binaryVal made a row
 * set whose only populated column is BINARY count as zero rows, which the
 * ODBC layer takes as end of result.
 */
int argus_hs2_rowset_row_count(GPtrArray *tcolumns);

/*
 * Turn one FetchResults TRowSet (its `columns` array, NULL when the server
 * sent none) into cache->rows: sets cache->num_cols / *num_cols from the
 * column count, then num_rows/capacity/rows through argus_hs2_parse_rowset.
 * A missing, column-less or entirely empty row set yields zero rows.
 * Returns 0, or -1 on allocation failure.
 *
 * End-of-result is NOT decided here. The ODBC layer keeps calling
 * fetch_results until a batch comes back empty, and that is the only safe
 * signal for the HS2 family: HiveServer2 (and Spark Thrift Server) hard-code
 * hasMoreRows=false on every reply, and a short batch means nothing — Hive
 * caps maxRows at hive.server2.thrift.resultset.max.fetch.size and Impala
 * returns whatever row batches are ready. The one thing hasMoreRows is good
 * for is the opposite case, an EMPTY batch with hasMoreRows=true (Impala's
 * FETCH_ROWS_TIMEOUT_MS, Flink's gateway before the job produces rows), and
 * the backends handle that by asking again.
 */
int argus_hs2_rowset_to_cache(GPtrArray *tcolumns, argus_row_cache_t *cache,
                              int *num_cols);

/*
 * Inspect a TStatus. Returns true for SUCCESS / SUCCESS_WITH_INFO. Otherwise
 * copies the server's errorMessage (or a generic text) into errbuf, NUL
 * terminated, and returns false. `status` may be NULL (treated as success:
 * the reply carried no status at all).
 */
typedef struct _TStatus TStatus;
/* The SQLSTATE a TStatus names, or "HY000". */
void argus_hs2_status_sqlstate(TStatus *status, char out[6]);

bool argus_hs2_status_ok(TStatus *status, char *errbuf, size_t errlen);

#endif /* ARGUS_HS2_FETCH_H */
