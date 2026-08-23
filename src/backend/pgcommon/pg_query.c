#include "pg_common.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>

/*
 * Statement execution.
 *
 * libpq is one statement at a time per connection, so the operation model is
 * simple, but two things are deliberately not the naive version:
 *
 *  - Rows are streamed, not buffered. PQexec materialises the entire result
 *    set in client memory before the first row is visible, which is what makes
 *    a multi-million-row extract through psqlODBC's default settings hurt.
 *    Here the query is sent with PQsendQuery and read back in bounded chunks,
 *    so memory is a function of FetchBufferSize rather than of the answer.
 *
 *  - Cancellation is real. PQcancel opens its own connection to the server and
 *    is safe to call from another thread, which is exactly what SQLCancel
 *    needs; the statement stops server-side rather than the driver pretending.
 *
 * execute() still waits for the *first* result before returning, because the
 * ODBC layer calls get_result_metadata() immediately afterwards and treats a
 * non-empty get_last_error() as a failed statement (src/odbc/execute.c:611,
 * :647). Blocking there is where a query error naturally surfaces. Only the
 * rows are streamed.
 */

/* Catalogue and internal queries: small result sets where streaming would only
 * add round trips. */
static int pg_run(pg_conn_t *conn, const char *query, bool streaming,
                  argus_backend_op_t *out_op);

static void op_free(pg_op_t *op)
{
    if (!op) return;
    if (op->pending) PQclear(op->pending);
    free(op->columns);
    free(op);
}

/* Read and discard everything left on the wire so the connection is reusable.
 * A connection left mid-result is unusable for the next statement, and the
 * pool would hand it out anyway. */
static void drain_connection(pg_conn_t *conn)
{
    if (!conn || !conn->pg) return;
    PGresult *r;
    while ((r = PQgetResult(conn->pg)) != NULL) {
        /* A COPY still in progress has to be ended before results resume. */
        if (PQresultStatus(r) == PGRES_COPY_OUT) {
            char *buf = NULL;
            while (PQgetCopyData(conn->pg, &buf, 0) > 0)
                if (buf) { PQfreemem(buf); buf = NULL; }
            if (buf) PQfreemem(buf);
        }
        PQclear(r);
    }
}

int pg_execute(argus_backend_conn_t raw_conn, const char *query,
               argus_backend_op_t *out_op)
{
    return pg_run((pg_conn_t *)raw_conn, query, true, out_op);
}

int pg_execute_buffered(argus_backend_conn_t raw_conn, const char *query,
                        argus_backend_op_t *out_op)
{
    return pg_run((pg_conn_t *)raw_conn, query, false, out_op);
}

static int pg_run(pg_conn_t *conn, const char *query, bool streaming,
                  argus_backend_op_t *out_op)
{
    if (!conn || !conn->pg || !query || !out_op) return -1;

    /* The ODBC layer reads get_last_error() after a *successful* execute and
     * fails the statement if it finds anything, so a stale message from the
     * previous statement would sink this one. Clear it first. */
    conn->last_error[0] = '\0';
    conn->last_sqlstate[0] = '\0';

    /* Anything left over from an abandoned statement (SQLCloseCursor on a
     * partially-read result, say) must go before a new one is sent. */
    if (PQtransactionStatus(conn->pg) == PQTRANS_ACTIVE)
        drain_connection(conn);

    pg_op_t *op = calloc(1, sizeof(*op));
    if (!op) return -1;
    op->conn = conn;
    op->streaming = streaming;

    PGresult *res = NULL;

    if (streaming) {
        if (!PQsendQuery(conn->pg, query)) {
            pg_fail(conn, NULL, "HY000");
            free(op);
            return -1;
        }
        /*
         * Bounded reads. libpq 17 hands back whole batches (one PGresult per
         * FetchBufferSize rows); before that, single-row mode is the only
         * bounded option and costs one PGresult per row. Both keep memory
         * flat; the batched form is simply cheaper, and the COPY BINARY path
         * (pg_copy.c) is what closes the gap on older libpq.
         */
#ifdef LIBPQ_HAS_CHUNK_MODE
        if (!PQsetChunkedRowsMode(conn->pg, conn->fetch_batch))
            PQsetSingleRowMode(conn->pg);
#else
        PQsetSingleRowMode(conn->pg);
#endif
        res = PQgetResult(conn->pg);
    } else {
        res = PQexec(conn->pg, query);
    }

    if (!res) {
        pg_fail(conn, NULL, "HY000");
        free(op);
        return -1;
    }

    switch (PQresultStatus(res)) {
    case PGRES_SINGLE_TUPLE:
#ifdef LIBPQ_HAS_CHUNK_MODE
    case PGRES_TUPLES_CHUNK:
#endif
        pg_describe_result(op, res);
        op->pending = res;
        op->pending_row = 0;
        break;

    case PGRES_TUPLES_OK:
        pg_describe_result(op, res);
        if (streaming) {
            /* In streaming mode TUPLES_OK carries no rows: it terminates the
             * stream, so this is an empty result set. */
            PQclear(res);
            op->drained = true;
            drain_connection(conn);
        } else {
            op->pending = res;
            op->pending_row = 0;
            op->drained = true;
        }
        break;

    case PGRES_COMMAND_OK: {
        const char *n = PQcmdTuples(res);
        op->affected_rows = (n && *n) ? atoll(n) : 0;
        op->num_cols = 0;
        op->metadata_fetched = true;
        op->drained = true;
        PQclear(res);
        if (streaming) drain_connection(conn);
        break;
    }

    case PGRES_EMPTY_QUERY:
        op->num_cols = 0;
        op->metadata_fetched = true;
        op->drained = true;
        PQclear(res);
        if (streaming) drain_connection(conn);
        break;

    default:
        pg_fail(conn, res, "HY000");
        PQclear(res);
        drain_connection(conn);
        free(op);
        return -1;
    }

    *out_op = op;
    return 0;
}

int pg_get_operation_status(argus_backend_conn_t raw_conn,
                            argus_backend_op_t raw_op, bool *finished)
{
    (void)raw_conn;
    (void)raw_op;
    /* execute() already waited for the first result, so by the time the ODBC
     * layer can ask, the statement has started producing. */
    if (finished) *finished = true;
    return 0;
}

void pg_close_operation(argus_backend_conn_t raw_conn, argus_backend_op_t raw_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    pg_op_t *op = (pg_op_t *)raw_op;
    if (!op) return;

    /* Abandoning a half-read stream leaves rows on the wire; the connection is
     * unusable until they are consumed, and the pool will hand it out. */
    if (conn && op->streaming && !op->drained)
        drain_connection(conn);

    op_free(op);
}

int pg_cancel(argus_backend_conn_t raw_conn, argus_backend_op_t raw_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)raw_op;
    if (!conn || !conn->pg) return -1;

    /*
     * PQgetCancel/PQcancel is the thread-safe pair: it snapshots what is
     * needed to reach the server and then opens its own connection, so it can
     * be called while another thread is blocked in PQgetResult. That is what
     * makes SQLCancel a real cancellation here rather than a no-op.
     */
    PGcancel *c = PQgetCancel(conn->pg);
    if (!c) return -1;

    char errbuf[256] = {0};
    int ok = PQcancel(c, errbuf, (int)sizeof(errbuf));
    PQfreeCancel(c);

    if (!ok) {
        ARGUS_LOG_WARN("PostgreSQL: cancel request failed: %s", errbuf);
        return -1;
    }
    ARGUS_LOG_DEBUG("PostgreSQL: cancel request sent");
    return 0;
}
