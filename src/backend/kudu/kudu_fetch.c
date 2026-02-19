#include "kudu_internal.h"
#include <stdlib.h>
#include <string.h>

/* ── Create/free operation handles ────────────────────────────── */

kudu_operation_t *kudu_operation_new(void)
{
    kudu_operation_t *op = calloc(1, sizeof(kudu_operation_t));
    return op;
}

void kudu_operation_free(kudu_operation_t *op)
{
    if (!op) return;

    free(op->columns);

    if (op->synthetic_cache) {
        argus_row_cache_free(op->synthetic_cache);
        free(op->synthetic_cache);
    }

    free(op);
}

/* ── FetchResults from Kudu scanner or synthetic cache ────────── */

int kudu_fetch_results(argus_backend_conn_t raw_conn,
                       argus_backend_op_t raw_op,
                       int max_rows,
                       argus_row_cache_t *cache,
                       argus_column_desc_t *columns,
                       int *num_cols)
{
    (void)raw_conn;
    kudu_operation_t *op = (kudu_operation_t *)raw_op;
    if (!op) return -1;

    /* Return metadata if available */
    if (op->metadata_fetched && columns && num_cols) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
    }

    /* Handle synthetic result sets (from metadata queries) */
    if (op->is_synthetic && op->synthetic_cache) {
        argus_row_cache_t *sc = op->synthetic_cache;
        if (sc->current_row >= sc->num_rows) {
            cache->num_rows = 0;
            cache->exhausted = true;
            return 0;
        }

        /* Copy remaining rows */
        size_t remaining = sc->num_rows - sc->current_row;
        size_t to_copy = remaining;
        if (max_rows > 0 && (size_t)max_rows < to_copy)
            to_copy = (size_t)max_rows;

        cache->rows = calloc(to_copy, sizeof(argus_row_t));
        if (!cache->rows) return -1;
        cache->num_rows = to_copy;
        cache->capacity = to_copy;
        cache->num_cols = sc->num_cols;

        for (size_t i = 0; i < to_copy; i++) {
            size_t src_row = sc->current_row + i;
            cache->rows[i].cells = calloc((size_t)sc->num_cols,
                                          sizeof(argus_cell_t));
            if (!cache->rows[i].cells) return -1;

            for (int c = 0; c < sc->num_cols; c++) {
                argus_cell_t *src = &sc->rows[src_row].cells[c];
                argus_cell_t *dst = &cache->rows[i].cells[c];
                dst->is_null = src->is_null;
                if (!src->is_null && src->data) {
                    dst->data = strdup(src->data);
                    dst->data_len = src->data_len;
                }
            }
        }

        sc->current_row += to_copy;
        if (sc->current_row >= sc->num_rows)
            cache->exhausted = true;

        return 0;
    }

    /* Fetch from Kudu scanner (C++ implementation) */
    if (op->finished) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    return kudu_cpp_fetch_batch(op, cache, max_rows);
}

/* ── Get result set metadata ──────────────────────────────────── */

int kudu_get_result_metadata(argus_backend_conn_t raw_conn,
                              argus_backend_op_t raw_op,
                              argus_column_desc_t *columns,
                              int *num_cols)
{
    (void)raw_conn;
    kudu_operation_t *op = (kudu_operation_t *)raw_op;
    if (!op) return -1;

    if (op->metadata_fetched && op->columns && op->num_cols > 0) {
        if (columns && num_cols) {
            memcpy(columns, op->columns,
                   (size_t)op->num_cols * sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        }
        return 0;
    }

    return -1;
}
