/* SPDX-License-Identifier: Apache-2.0 */
#include "pg_common.h"
#include "argus/compat.h"
#include <string.h>

/*
 * Helpers for building catalog SQL.
 *
 * Every filter these functions take comes from the application through
 * SQLTables/SQLColumns and friends, so none of it is interpolated by hand.
 * libpq's PQescapeLiteral quotes against the *connection's* current
 * client_encoding and standard_conforming_strings setting, which is the only
 * way to get this right for multibyte encodings — a hand-rolled quote-doubler
 * gets it wrong on exactly the inputs an attacker chooses.
 */

void pg_append_literal(GString *sql, PGconn *pg, const char *value)
{
    if (!sql) return;
    if (!value) { g_string_append(sql, "NULL"); return; }

    char *quoted = PQescapeLiteral(pg, value, strlen(value));
    if (quoted) {
        g_string_append(sql, quoted);
        PQfreemem(quoted);
    } else {
        /* Only on OOM or a broken connection; an empty literal can never
         * match, which is the safe direction for a catalog filter. */
        g_string_append(sql, "''");
    }
}

void pg_append_pattern(GString *sql, PGconn *pg, const char *col,
                       const char *pattern)
{
    if (!sql || !col || !pattern || !*pattern) return;

    /* ODBC search patterns use % and _ , which are LIKE's own wildcards, so a
     * pattern is passed through unchanged. When it has no wildcard, equality
     * lets the planner use the catalog indexes. */
    bool has_wildcard = strchr(pattern, '%') || strchr(pattern, '_');

    g_string_append_printf(sql, " AND %s %s ", col, has_wildcard ? "LIKE" : "=");
    pg_append_literal(sql, pg, pattern);
}

/*
 * ODBC table types → pg_class.relkind.
 *
 * The incoming list is comma-separated and each entry may be single-quoted
 * ("'TABLE','VIEW'"), which is the shape SQLTables is specified to accept and
 * the shape Excel and Tableau actually send.
 *
 *   TABLE              → r (ordinary), p (partitioned root)
 *   VIEW               → v
 *   MATERIALIZED VIEW  → m
 *   FOREIGN TABLE      → f
 *   SYSTEM TABLE       → r restricted to the system schemas, handled by the
 *                        caller's schema filter rather than by relkind
 */
void pg_append_relkinds(GString *sql, const char *col, const char *table_types)
{
    if (!sql || !col) return;

    /* Default: everything a BI tool can read from. */
    static const char *all = "'r','p','v','m','f'";

    if (!table_types || !*table_types || strcmp(table_types, "%") == 0) {
        g_string_append_printf(sql, " AND %s IN (%s)", col, all);
        return;
    }

    char tmp[512];
    strncpy(tmp, table_types, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    GString *kinds = g_string_new(NULL);
    char *save = NULL;
    for (char *tok = strtok_r(tmp, ",", &save); tok;
         tok = strtok_r(NULL, ",", &save)) {
        while (*tok == ' ' || *tok == '\'' || *tok == '"') tok++;
        char *end = tok + strlen(tok);
        while (end > tok && (end[-1] == ' ' || end[-1] == '\'' || end[-1] == '"'))
            *--end = '\0';
        if (!*tok) continue;

        const char *mapped = NULL;
        if (strcasecmp(tok, "TABLE") == 0)                    mapped = "'r','p'";
        else if (strcasecmp(tok, "VIEW") == 0)                mapped = "'v'";
        else if (strcasecmp(tok, "MATERIALIZED VIEW") == 0)   mapped = "'m'";
        else if (strcasecmp(tok, "FOREIGN TABLE") == 0)       mapped = "'f'";
        else if (strcasecmp(tok, "SYSTEM TABLE") == 0)        mapped = "'r'";
        else if (strcmp(tok, "%") == 0)                       mapped = all;
        if (!mapped) continue;

        if (kinds->len > 0) g_string_append_c(kinds, ',');
        g_string_append(kinds, mapped);
    }

    /* An unrecognised list must return no rows, not every row: the application
     * asked for something specific and got it wrong, and inventing a match
     * would put the wrong objects in front of the user. */
    g_string_append_printf(sql, " AND %s IN (%s)", col,
                           kinds->len > 0 ? kinds->str : "''");
    g_string_free(kinds, TRUE);
}

/*
 * SQLColumns' DATA_TYPE column (column 5) must be the numeric ODBC type code,
 * so the mapping in pg_types.c has to exist a second time as SQL. Keeping the
 * two in step is what test_postgres_types checks.
 */
void pg_append_odbc_type_case(GString *sql, const char *oid_expr,
                              const char *typmod_expr)
{
    if (!sql || !oid_expr) return;

    g_string_append_printf(sql,
        "CASE %s "
        "WHEN 16 THEN -7 "                       /* bool      → SQL_BIT */
        "WHEN 17 THEN -4 "                       /* bytea     → SQL_LONGVARBINARY */
        "WHEN 18 THEN 1 "                        /* \"char\"  → SQL_CHAR */
        "WHEN 19 THEN 12 "                       /* name      → SQL_VARCHAR */
        "WHEN 20 THEN -5 "                       /* int8      → SQL_BIGINT */
        "WHEN 21 THEN 5 "                        /* int2      → SQL_SMALLINT */
        "WHEN 23 THEN 4 "                        /* int4      → SQL_INTEGER */
        "WHEN 26 THEN 4 "                        /* oid       → SQL_INTEGER */
        "WHEN 700 THEN 7 "                       /* float4    → SQL_REAL */
        "WHEN 701 THEN 8 "                       /* float8    → SQL_DOUBLE */
        "WHEN 1042 THEN 1 "                      /* bpchar    → SQL_CHAR */
        "WHEN 1082 THEN 91 "                     /* date      → SQL_TYPE_DATE */
        "WHEN 1083 THEN 92 "                     /* time      → SQL_TYPE_TIME */
        "WHEN 1266 THEN 92 "                     /* timetz    → SQL_TYPE_TIME */
        "WHEN 1114 THEN 93 "                     /* timestamp → SQL_TYPE_TIMESTAMP */
        "WHEN 1184 THEN 93 "                     /* timestamptz */
        "WHEN 1186 THEN 12 "                     /* interval  → SQL_VARCHAR */
        "WHEN 790 THEN 12 "                      /* money     → SQL_VARCHAR */
        "WHEN 1560 THEN 12 WHEN 1562 THEN 12 "   /* bit, varbit */
        "WHEN 1700 THEN 2 "                      /* numeric   → SQL_NUMERIC */
        "WHEN 2950 THEN -11 "                    /* uuid      → SQL_GUID */
        /* varchar with a declared length is SQL_VARCHAR, otherwise it is an
         * unbounded long type — the same split pg_oid_to_sql_type makes. */
        "WHEN 1043 THEN (CASE WHEN %s > 4 THEN 12 ELSE -1 END) "
        "ELSE -1 END",                           /* text, json, arrays, … */
        oid_expr, typmod_expr ? typmod_expr : "-1");
}
