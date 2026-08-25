#include "pg_common.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdio.h>
#include <string.h>

/*
 * Error mapping.
 *
 * PostgreSQL hands the client a five-character SQLSTATE in every error
 * response (PG_DIAG_SQLSTATE), and those codes are the same SQL standard codes
 * ODBC uses. So the driver passes the server's code straight through instead
 * of collapsing everything to HY000 the way the other backends have to —
 * "42P01 relation does not exist" reaches the application as 42P01, and a BI
 * tool that branches on SQLSTATE gets the truth.
 *
 * The one adjustment: ODBC 3 renamed a handful of ODBC 2 states, and a couple
 * of PostgreSQL classes have no ODBC equivalent at all. Those are remapped
 * below; everything else is verbatim.
 */

/* PostgreSQL states that must not reach the application as-is. */
static const char *remap_sqlstate(const char *pg_state)
{
    if (!pg_state || strlen(pg_state) != 5) return NULL;

    /* Query cancelled at the user's request: ODBC has a dedicated state, and
     * it must be tested before the class-57 rule below swallows it. */
    if (strcmp(pg_state, "57014") == 0) return "HY008";

    /* The rest of class 57 (operator intervention) and class 58 (system error)
     * have no ODBC counterpart; from the application's side they are
     * connection-level failures. */
    if (strncmp(pg_state, "57", 2) == 0 || strncmp(pg_state, "58", 2) == 0)
        return "08S01";      /* communication link failure */

    /* Class 53 (insufficient resources) maps to ODBC's general error; leaving
     * it verbatim would make an app think it was a constraint violation. */
    if (strncmp(pg_state, "53", 2) == 0) return "HY000";

    return pg_state;
}

int pg_fail(pg_conn_t *conn, PGresult *res, const char *fallback_sqlstate)
{
    if (!conn) return -1;

    const char *state = NULL;
    const char *primary = NULL;
    const char *detail = NULL;
    const char *hint = NULL;

    if (res) {
        state   = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        detail  = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        hint    = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
    }
    if (!primary || !*primary) {
        primary = PQerrorMessage(conn->pg);
        /* PQerrorMessage keeps the trailing newline; drop it so the message
         * reads correctly inside an ODBC diagnostic record. */
    }

    const char *mapped = remap_sqlstate(state);
    snprintf(conn->last_sqlstate, sizeof(conn->last_sqlstate), "%s",
             mapped ? mapped : (fallback_sqlstate ? fallback_sqlstate : "HY000"));

    int n = snprintf(conn->last_error, sizeof(conn->last_error), "%s",
                     (primary && *primary) ? primary : "unknown server error");
    if (n > 0 && (size_t)n < sizeof(conn->last_error)) {
        /* Trim the newline PQerrorMessage appends. */
        size_t len = strlen(conn->last_error);
        while (len > 0 && (conn->last_error[len - 1] == '\n' ||
                           conn->last_error[len - 1] == '\r'))
            conn->last_error[--len] = '\0';

        if (detail && *detail)
            snprintf(conn->last_error + len, sizeof(conn->last_error) - len,
                     " (%s)", detail);
        else if (hint && *hint)
            snprintf(conn->last_error + len, sizeof(conn->last_error) - len,
                     " (hint: %s)", hint);
    }

    ARGUS_LOG_ERROR("PostgreSQL error %s: %s",
                    conn->last_sqlstate, conn->last_error);
    return -1;
}

void pg_push_diag(argus_dbc_t *dbc, pg_conn_t *conn, const char *fallback_sqlstate)
{
    if (!dbc) return;

    const char *state = (conn && conn->last_sqlstate[0])
                        ? conn->last_sqlstate
                        : (fallback_sqlstate ? fallback_sqlstate : "HY000");
    const char *text = (conn && conn->last_error[0])
                       ? conn->last_error : "connection failed";
    char msg[ARGUS_MAX_MESSAGE_LEN];
    /* The prefix plus a maximum-length server message would overflow the
     * diagnostic record, so the message is explicitly truncated to what fits
     * rather than left to snprintf (and to a compiler warning). */
    snprintf(msg, sizeof(msg), "[Argus][PostgreSQL] %.*s",
             (int)(sizeof(msg) - sizeof("[Argus][PostgreSQL] ")), text);
    argus_set_error(&dbc->diag, state, msg, 0);
}

/*
 * The same message, plus the SQLSTATE PostgreSQL itself produced.
 *
 * The ODBC layer's older path can only report HY000, which throws away the one
 * piece of a diagnostic an application can branch on: a BI tool distinguishing
 * "relation does not exist" (42P01) from "permission denied" (42501) from
 * "serialization failure" (40001) has to read the SQLSTATE, because the
 * message text is localised and not a contract.
 */
bool pg_get_last_error_ex(argus_backend_conn_t raw_conn, char sqlstate[6],
                          char *buf, size_t buflen)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn) return false;

    if (!pg_get_last_error(raw_conn, buf, buflen)) return false;

    if (sqlstate) {
        snprintf(sqlstate, 6, "%s",
                 conn->last_sqlstate[0] ? conn->last_sqlstate : "HY000");
    }
    return true;
}

bool pg_get_last_error(argus_backend_conn_t raw_conn, char *buf, size_t buflen)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || buflen == 0) return false;

    const char *msg = conn->last_error[0] ? conn->last_error : NULL;
    if (!msg && conn->pg) {
        const char *e = PQerrorMessage(conn->pg);
        if (e && *e) msg = e;
    }
    if (!msg) return false;

    snprintf(buf, buflen, "%s", msg);
    size_t len = strlen(buf);
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r'))
        buf[--len] = '\0';
    return buf[0] != '\0';
}
