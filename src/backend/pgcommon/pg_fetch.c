/* SPDX-License-Identifier: Apache-2.0 */
#include "pg_common.h"
#include "argus/log.h"
#include "argus/numtext.h"
#include <stdlib.h>
#include <string.h>

/*
 * Result metadata and the row loop.
 *
 * Rows are pulled in bounded chunks (see pg_query.c) and copied into the
 * driver's row cache. Numeric columns also carry a native int64/double
 * alongside the text, using the cache's existing fast path
 * (include/argus/types.h) so a bound SQL_C_DOUBLE never pays for a
 * double → text → double round trip.
 */

void pg_describe_result(pg_op_t *op, PGresult *res)
{
    if (!op || !res) return;

    int n = PQnfields(res);
    if (n > ARGUS_MAX_COLUMNS) n = ARGUS_MAX_COLUMNS;

    free(op->columns);
    op->columns = calloc((size_t)(n > 0 ? n : 1), sizeof(argus_column_desc_t));
    if (!op->columns) {
        op->num_cols = 0;
        return;
    }

    for (int i = 0; i < n; i++) {
        argus_column_desc_t *col = &op->columns[i];
        Oid oid = PQftype(res, i);
        int mod = PQfmod(res, i);

        /* A domain column reports the domain's OID; resolve it to the type it
         * is built on, carrying the domain's declared length or precision. */
        pg_resolve_oid(op->conn, &oid, &mod);

        const char *name = PQfname(res, i);
        if (name) {
            strncpy((char *)col->name, name, ARGUS_MAX_COLUMN_NAME - 1);
            col->name_len = (SQLSMALLINT)strlen((char *)col->name);
        }

        col->sql_type       = pg_oid_to_sql_type(oid, mod);
        col->column_size    = pg_column_size(oid, mod);
        col->decimal_digits = pg_decimal_digits(oid, mod);

        /*
         * The wire protocol's RowDescription carries no nullability, and a
         * per-column pg_attribute lookup would cost a round trip on every
         * query. SQL_NULLABLE_UNKNOWN is the honest answer here; SQLColumns,
         * which does read the catalog, reports the real constraint.
         */
        col->nullable = SQL_NULLABLE_UNKNOWN;
    }

    op->num_cols = n;
    op->metadata_fetched = true;
}

int pg_get_result_metadata(argus_backend_conn_t raw_conn,
                           argus_backend_op_t raw_op,
                           argus_column_desc_t *columns, int *num_cols)
{
    (void)raw_conn;
    pg_op_t *op = (pg_op_t *)raw_op;
    if (!op || !columns || !num_cols) return -1;

    if (!op->metadata_fetched || op->num_cols <= 0) {
        *num_cols = 0;
        return 0;
    }

    memcpy(columns, op->columns,
           (size_t)op->num_cols * sizeof(argus_column_desc_t));
    *num_cols = op->num_cols;
    return 0;
}

/* Pull the next PGresult of the stream into op->pending.
 * Returns 1 when rows are available, 0 at end of stream, -1 on error. */
static int pull_next_chunk(pg_conn_t *conn, pg_op_t *op)
{
    if (op->pending) {
        PQclear(op->pending);
        op->pending = NULL;
        op->pending_row = 0;
    }
    if (!op->streaming || op->drained) {
        op->drained = true;
        return 0;
    }

    PGresult *res = PQgetResult(conn->pg);
    if (!res) {
        op->drained = true;
        return 0;
    }

    switch (PQresultStatus(res)) {
    case PGRES_SINGLE_TUPLE:
#ifdef LIBPQ_HAS_CHUNK_MODE
    case PGRES_TUPLES_CHUNK:
#endif
        op->pending = res;
        op->pending_row = 0;
        return PQntuples(res) > 0 ? 1 : pull_next_chunk(conn, op);

    case PGRES_TUPLES_OK:
        /* Terminates the row stream; the trailing NULL still has to be read
         * so the connection is ready for the next statement. */
        PQclear(res);
        op->drained = true;
        while ((res = PQgetResult(conn->pg)) != NULL) PQclear(res);
        return 0;

    case PGRES_COMMAND_OK:
    case PGRES_EMPTY_QUERY:
        PQclear(res);
        op->drained = true;
        while ((res = PQgetResult(conn->pg)) != NULL) PQclear(res);
        return 0;

    default:
        pg_fail(conn, res, "HY000");
        PQclear(res);
        op->drained = true;
        while ((res = PQgetResult(conn->pg)) != NULL) PQclear(res);
        return -1;
    }
}

/* Copy one PGresult row into the cache. Returns 0, or -1 on allocation
 * failure. */
static int copy_row(const pg_conn_t *conn, PGresult *res, int row, int ncols,
                    argus_cell_t *cells)
{
    for (int c = 0; c < ncols; c++) {
        argus_cell_t *cell = &cells[c];

        if (PQgetisnull(res, row, c)) {
            cell->is_null = true;
            continue;
        }

        const char *val = PQgetvalue(res, row, c);
        size_t len = (size_t)PQgetlength(res, row, c);
        Oid oid = PQftype(res, c);
        int mod = -1;
        /* Resolve here too: a domain over integer must take the same native
         * fast path an integer column takes, or the two disagree. */
        pg_resolve_oid(conn, &oid, &mod);

        /*
         * PostgreSQL renders booleans as 't'/'f'. ODBC's SQL_BIT is 1/0, and
         * an application binding SQL_C_CHAR is entitled to read "1" — so the
         * text form is normalised here rather than leaving the two
         * representations to disagree with the native value below.
         */
        if (oid == PG_OID_BOOL) {
            bool t = (len > 0 && (val[0] == 't' || val[0] == 'T' || val[0] == '1'));
            cell->data = malloc(2);
            if (!cell->data) return -1;
            cell->data[0] = t ? '1' : '0';
            cell->data[1] = '\0';
            cell->data_len = 1;
            cell->native_kind = ARGUS_NATIVE_I64;
            cell->native.i64 = t ? 1 : 0;
            continue;
        }

        cell->data = malloc(len + 1);
        if (!cell->data) return -1;
        memcpy(cell->data, val, len);
        cell->data[len] = '\0';
        cell->data_len = len;

        /*
         * bytea comes over as text in whatever `bytea_output` says. The
         * default since 9.0 is the hex form ("\x00ff"), which decodes in
         * place to the bytes the application asked for; a server still set to
         * `escape` sends octal escapes, which the decoder rejects and the
         * cell keeps its text, as before.
         */
        if (oid == PG_OID_BYTEA) {
            argus_cell_decode_hex(cell);
            continue;
        }

        uint8_t kind = pg_native_kind(oid);
        if (kind == ARGUS_NATIVE_I64) {
            cell->native_kind = kind;
            cell->native.i64 = strtoll(cell->data, NULL, 10);
        } else if (kind == ARGUS_NATIVE_F64) {
            /* NaN and ±Infinity are spelled out by PostgreSQL and strtod
             * parses both, so no special case is needed. */
            cell->native_kind = kind;
            cell->native.f64 = argus_strtod(cell->data, NULL);
        }
    }
    return 0;
}

int pg_fetch_results(argus_backend_conn_t raw_conn, argus_backend_op_t raw_op,
                     int max_rows, argus_row_cache_t *cache,
                     argus_column_desc_t *columns, int *num_cols)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    pg_op_t *op = (pg_op_t *)raw_op;
    if (!conn || !op || !cache) return -1;

    if (columns && num_cols) {
        if (op->metadata_fetched && op->columns && op->num_cols > 0) {
            memcpy(columns, op->columns,
                   (size_t)op->num_cols * sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        } else {
            *num_cols = 0;
        }
    }

    /* DML/DDL, or a statement with no result set at all. */
    if (op->num_cols <= 0) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    int ncols = op->num_cols;
    int batch = (max_rows > 0) ? max_rows : conn->fetch_batch;
    if (batch <= 0) batch = ARGUS_DEFAULT_BATCH_SIZE;

    /* The cache owns its row array and argus_row_cache_clear() released the
     * previous one, so this is an allocation rather than a leak — the same
     * shape every other backend uses. */
    free(cache->rows);
    cache->rows = calloc((size_t)batch, sizeof(argus_row_t));
    if (!cache->rows) {
        cache->capacity = 0;
        return -1;
    }
    cache->capacity = (size_t)batch;
    cache->num_cols = ncols;

    size_t filled = 0;
    while (filled < (size_t)batch) {
        if (!op->pending || op->pending_row >= PQntuples(op->pending)) {
            int rc = pull_next_chunk(conn, op);
            if (rc < 0) return -1;
            if (rc == 0) break;          /* end of stream */
        }

        cache->rows[filled].cells = calloc((size_t)ncols, sizeof(argus_cell_t));
        if (!cache->rows[filled].cells) return -1;

        if (copy_row(conn, op->pending, op->pending_row, ncols,
                     cache->rows[filled].cells) != 0)
            return -1;

        op->pending_row++;
        filled++;
    }

    cache->num_rows = filled;
    if (filled < (size_t)batch) cache->exhausted = true;

    return 0;
}
