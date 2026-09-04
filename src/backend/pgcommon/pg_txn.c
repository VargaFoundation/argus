/* SPDX-License-Identifier: Apache-2.0 */
#include "pg_common.h"
#include "argus/log.h"
#include <string.h>

/*
 * Transactions.
 *
 * The PostgreSQL family is the first Argus backend with real ones, so this is
 * also the first place the driver has to mean what SQL_TXN_CAPABLE says.
 *
 * The model is ODBC's, not PostgreSQL's. ODBC has no BEGIN: turning
 * SQL_ATTR_AUTOCOMMIT off means every statement from then on runs inside a
 * transaction, and SQLEndTran ends the current one and implicitly starts the
 * next. PostgreSQL's own model is the opposite — it is in autocommit unless a
 * BEGIN is issued — so the driver opens transactions lazily: autocommit off
 * records the intent, and the BEGIN is sent when it is needed.
 *
 * Sending BEGIN eagerly at SQLSetConnectAttr time would hold an idle
 * transaction open from the moment a BI tool sets the attribute until it runs
 * its first query, which on PostgreSQL blocks VACUUM and pins the xmin
 * horizon. Idle-in-transaction is the failure mode every PostgreSQL operator
 * knows by name; a driver should not manufacture it.
 */

/* Send a statement that produces no result set, recording failure. */
static int pg_simple(pg_conn_t *conn, const char *sql)
{
    if (!conn || !conn->pg) return -1;

    PGresult *res = PQexec(conn->pg, sql);
    if (!res) return pg_fail(conn, NULL, "HY000");

    ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        pg_fail(conn, res, "HY000");
        PQclear(res);
        return -1;
    }
    PQclear(res);
    return 0;
}

/* Open a transaction if the connection wants one and is not already in one.
 * Called from the execute path, so the BEGIN rides along with the statement
 * the application actually asked for. */
int pg_txn_begin_if_needed(pg_conn_t *conn)
{
    if (!conn || !conn->pg || conn->autocommit) return 0;
    if (PQtransactionStatus(conn->pg) != PQTRANS_IDLE) return 0;

    if (conn->isolation_sql[0]) {
        char sql[128];
        snprintf(sql, sizeof(sql), "BEGIN ISOLATION LEVEL %s",
                 conn->isolation_sql);
        return pg_simple(conn, sql);
    }
    return pg_simple(conn, "BEGIN");
}

int pg_set_autocommit(argus_backend_conn_t raw_conn, bool on)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg) return -1;

    if (conn->autocommit == on) return 0;

    /*
     * ODBC: switching autocommit ON commits whatever is open. Silently
     * discarding it would lose the application's work with no diagnostic.
     */
    if (on && PQtransactionStatus(conn->pg) != PQTRANS_IDLE) {
        if (pg_simple(conn, "COMMIT") != 0) return -1;
    }

    conn->autocommit = on;
    ARGUS_LOG_DEBUG("PostgreSQL: autocommit %s", on ? "ON" : "OFF");
    return 0;
}

int pg_end_transaction(argus_backend_conn_t raw_conn, bool commit)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg) return -1;

    PGTransactionStatusType st = PQtransactionStatus(conn->pg);

    /*
     * Nothing open. ODBC says SQLEndTran on a connection with no active
     * transaction succeeds, and it is a normal thing for an application to
     * do — a commit after a read-only batch, say.
     */
    if (st == PQTRANS_IDLE) return 0;

    /*
     * A failed statement leaves PostgreSQL in an aborted transaction where
     * every statement but ROLLBACK errors with 25P02. COMMIT there is not an
     * error: PostgreSQL treats it as a rollback and says so. Letting it
     * through keeps the connection usable, which is what the application
     * needs next.
     */
    if (st == PQTRANS_INERROR && commit)
        ARGUS_LOG_WARN("PostgreSQL: COMMIT on an aborted transaction — "
                       "the server will roll it back");

    return pg_simple(conn, commit ? "COMMIT" : "ROLLBACK");
}

/*
 * ODBC's isolation constants onto PostgreSQL's.
 *
 * READ UNCOMMITTED is accepted and mapped to READ COMMITTED because that is
 * precisely what PostgreSQL does with it — there is no dirty-read mode. It is
 * not advertised in SQL_TXN_ISOLATION_OPTION for the same reason: accepting a
 * request is fine, claiming the semantics is not.
 */
int pg_set_isolation(argus_backend_conn_t raw_conn, SQLUINTEGER level)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg) return -1;

    const char *pg_level;
    switch (level) {
    case SQL_TXN_READ_UNCOMMITTED:
    case SQL_TXN_READ_COMMITTED:  pg_level = "READ COMMITTED";  break;
    case SQL_TXN_REPEATABLE_READ: pg_level = "REPEATABLE READ"; break;
    case SQL_TXN_SERIALIZABLE:    pg_level = "SERIALIZABLE";    break;
    default:                      return -1;
    }

    snprintf(conn->isolation_sql, sizeof(conn->isolation_sql), "%s", pg_level);

    /*
     * PostgreSQL cannot change the isolation level of a transaction that has
     * already run a statement, so the session default is set instead: it
     * applies to this transaction if nothing has run yet, and to every
     * transaction after it either way.
     */
    char sql[128];
    snprintf(sql, sizeof(sql),
             "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s",
             pg_level);
    return pg_simple(conn, sql);
}

/*
 * Make a pooled connection safe to hand to the next borrower.
 *
 * This is the failure mode that makes transaction support dangerous rather
 * than merely incomplete: a connection parked mid-transaction holds locks and
 * an open snapshot, and the next application to borrow it inherits both —
 * including, on PostgreSQL, an aborted transaction in which every statement
 * fails with 25P02. Session state is the same problem more quietly: a
 * search_path, a SET, a temporary table or a prepared statement left behind
 * changes the meaning of the next borrower's SQL.
 *
 * DISCARD ALL is PostgreSQL's own answer to exactly this and is what every
 * connection pooler issues.
 */
bool pg_reset_session(argus_backend_conn_t raw_conn)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg) return false;

    if (PQstatus(conn->pg) != CONNECTION_OK) return false;

    /* A statement still streaming would make everything below fail. */
    if (PQtransactionStatus(conn->pg) == PQTRANS_ACTIVE) return false;

    if (PQtransactionStatus(conn->pg) != PQTRANS_IDLE) {
        ARGUS_LOG_DEBUG("PostgreSQL: rolling back an open transaction "
                        "before returning the connection to the pool");
        if (pg_simple(conn, "ROLLBACK") != 0) return false;
    }

    if (pg_simple(conn, "DISCARD ALL") != 0) return false;

    conn->autocommit = true;
    conn->isolation_sql[0] = '\0';
    conn->last_error[0] = '\0';
    conn->last_sqlstate[0] = '\0';
    return true;
}
