/* SPDX-License-Identifier: Apache-2.0 */
#include "pg_common.h"
#include "argus/compat.h"
#include "argus/log.h"
#include <string.h>

/*
 * Parameter metadata for SQLDescribeParam.
 *
 * This is metadata only. Execution still renders bound parameters as SQL
 * literals in src/odbc/execute.c, exactly as it does for every other backend —
 * nothing about how a statement runs changes here. What changes is that the
 * driver can answer "what type is parameter 2?" with the server's answer
 * instead of the SQL_VARCHAR/255 guess it has to give elsewhere, which is why
 * only the PostgreSQL family reports SQL_DESCRIBE_PARAMETER = "Y".
 *
 * The mechanism is a server-side Parse and Describe with no Execute:
 * PQprepare followed by PQdescribePrepared gives the resolved parameter OIDs,
 * and the statement is deallocated immediately. Nothing is left behind.
 */

/*
 * ODBC writes parameter markers as `?`; PostgreSQL wants `$1..$n`.
 *
 * The rewrite has to skip everything that can contain a question mark without
 * meaning a parameter: single-quoted strings (with '' escapes),
 * double-quoted identifiers, dollar-quoted bodies ($tag$…$tag$, which is how
 * every function body is written), line comments and block comments (which
 * nest in PostgreSQL).
 *
 * One case it cannot get right, and neither can psqlODBC: PostgreSQL's jsonb
 * existence operators are spelled `?`, `?|` and `?&`, so `WHERE j ? 'k'` looks
 * exactly like a parameter marker. The two-character forms are recognised and
 * left alone; the bare `?` is genuinely ambiguous. Since this path is metadata
 * only, the cost of guessing wrong is a Parse that fails and a fall back to
 * the generic answer — not a wrong query. jsonb_exists(j, 'k') is the
 * unambiguous spelling for anyone who hits it.
 */
static char *rewrite_markers(const char *sql, int *out_count)
{
    if (!sql) return NULL;

    GString *out = g_string_sized_new(strlen(sql) + 16);
    int n = 0;
    const char *p = sql;

    while (*p) {
        /* Single-quoted literal. */
        if (*p == '\'') {
            g_string_append_c(out, *p++);
            while (*p) {
                if (*p == '\'' && p[1] == '\'') {
                    g_string_append_len(out, p, 2);
                    p += 2;
                    continue;
                }
                g_string_append_c(out, *p);
                if (*p++ == '\'') break;
            }
            continue;
        }

        /* Quoted identifier. */
        if (*p == '"') {
            g_string_append_c(out, *p++);
            while (*p) {
                if (*p == '"' && p[1] == '"') {
                    g_string_append_len(out, p, 2);
                    p += 2;
                    continue;
                }
                g_string_append_c(out, *p);
                if (*p++ == '"') break;
            }
            continue;
        }

        /* Dollar-quoted string: $tag$ ... $tag$ (the tag may be empty). */
        if (*p == '$') {
            const char *q = p + 1;
            while (*q && (g_ascii_isalnum(*q) || *q == '_')) q++;
            if (*q == '$') {
                size_t taglen = (size_t)(q - p) + 1;      /* includes both $ */
                g_string_append_len(out, p, taglen);
                const char *body = p + taglen;
                /* Find the matching closing tag. */
                char *tag = g_strndup(p, taglen);
                const char *end = strstr(body, tag);
                if (end) {
                    g_string_append_len(out, body, (end - body) + taglen);
                    p = end + taglen;
                } else {
                    g_string_append(out, body);
                    p = body + strlen(body);
                }
                g_free(tag);
                continue;
            }
            g_string_append_c(out, *p++);
            continue;
        }

        /* Line comment. */
        if (p[0] == '-' && p[1] == '-') {
            while (*p && *p != '\n') g_string_append_c(out, *p++);
            continue;
        }

        /* Block comment; PostgreSQL nests them. */
        if (p[0] == '/' && p[1] == '*') {
            int depth = 0;
            while (*p) {
                if (p[0] == '/' && p[1] == '*') {
                    depth++;
                    g_string_append_len(out, p, 2);
                    p += 2;
                    continue;
                }
                if (p[0] == '*' && p[1] == '/') {
                    depth--;
                    g_string_append_len(out, p, 2);
                    p += 2;
                    if (depth == 0) break;
                    continue;
                }
                g_string_append_c(out, *p++);
            }
            continue;
        }

        if (*p == '?') {
            /* jsonb ?| and ?& are operators, not markers. */
            if (p[1] == '|' || p[1] == '&') {
                g_string_append_len(out, p, 2);
                p += 2;
                continue;
            }
            g_string_append_printf(out, "$%d", ++n);
            p++;
            continue;
        }

        g_string_append_c(out, *p++);
    }

    if (out_count) *out_count = n;
    return g_string_free(out, FALSE);
}

int pg_describe_params(argus_backend_conn_t raw_conn, const char *query,
                       argus_column_desc_t *params, int *num_params)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg || !query || !params || !num_params) return -1;

    int marker_count = 0;
    char *rewritten = rewrite_markers(query, &marker_count);
    if (!rewritten) return -1;

    if (marker_count == 0) {
        g_free(rewritten);
        *num_params = 0;
        return 0;
    }

    /*
     * The unnamed prepared statement. It is replaced by the next Parse and
     * discarded at the end of the transaction, so there is nothing to clean up
     * and no name to collide with.
     */
    PGresult *res = PQprepare(conn->pg, "", rewritten, 0, NULL);
    g_free(rewritten);

    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        /* Not an error the application should see: the caller falls back to
         * the generic description. A jsonb `?` operator lands here. */
        if (res) {
            ARGUS_LOG_DEBUG("PostgreSQL: parameter describe declined (%s)",
                            PQresultErrorMessage(res));
            PQclear(res);
        }
        /* Clear the sticky error so the next statement is not blamed for it. */
        conn->last_error[0] = '\0';
        conn->last_sqlstate[0] = '\0';
        return -1;
    }
    PQclear(res);

    res = PQdescribePrepared(conn->pg, "");
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        if (res) PQclear(res);
        conn->last_error[0] = '\0';
        conn->last_sqlstate[0] = '\0';
        return -1;
    }

    int n = PQnparams(res);
    if (n > ARGUS_MAX_PARAMS) n = ARGUS_MAX_PARAMS;

    for (int i = 0; i < n; i++) {
        Oid oid = PQparamtype(res, i);
        argus_column_desc_t *p = &params[i];
        memset(p, 0, sizeof(*p));
        p->sql_type       = pg_oid_to_sql_type(oid, -1);
        /*
         * Character parameters are SQL_VARCHAR, never SQL_LONGVARCHAR.
         *
         * Two things conspire here. A parameter carries no type modifier, so
         * pg_oid_to_sql_type's "varchar with no declared length is unbounded"
         * rule — right for a result column, where it drives buffer sizing —
         * would fire on every string parameter. And PostgreSQL resolves
         * `varchar_column = $1` through the text operator, so the type it
         * infers is `text` even when the column is varchar(50).
         *
         * Reporting SQL_LONGVARCHAR for an input parameter tells the
         * application it must stream the value with SQLPutData. The
         * long/short distinction is about how a driver hands data *out*; an
         * application already holds the parameter it is binding.
         */
        if (oid == PG_OID_VARCHAR || oid == PG_OID_TEXT || oid == PG_OID_BPCHAR)
            p->sql_type = SQL_VARCHAR;
        p->column_size    = pg_column_size(oid, -1);
        p->decimal_digits = pg_decimal_digits(oid, -1);
        /* A parameter's nullability is not knowable from a Describe: the
         * server reports the type it inferred, not whether the expression it
         * feeds accepts NULL. */
        p->nullable       = SQL_NULLABLE;
    }
    PQclear(res);

    *num_params = n;
    return 0;
}
