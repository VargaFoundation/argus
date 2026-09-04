/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ARGUS_CAPS_H
#define ARGUS_CAPS_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>

/*
 * Per-backend SQLGetInfo capabilities.
 *
 * SQLGetInfo answers a contract, and several of its answers are properties of
 * the *engine*, not of the driver: whether it has transactions, what it calls a
 * schema, how long an identifier may be. Those used to be constants in
 * info.c — correct for ten analytics engines that genuinely have no
 * transactions and no schema level below the database, and wrong the moment a
 * PostgreSQL-family backend appeared.
 *
 * The rule that makes the change safe: **every zero or NULL field means the
 * value info.c returned before this struct existed.** A backend that declares
 * no capabilities (`caps == NULL`), or declares some and leaves others unset,
 * is answered exactly as it was. Two of the defaults fall out for free —
 * SQL_TC_NONE and SQL_OSC_MINIMUM are both 0 — which is asserted in caps.c
 * rather than assumed, and pinned for every registered backend in
 * tests/unit/test_backend_caps.c.
 *
 * SQL_PROCEDURES is deliberately *not* here. It promises both that the engine
 * has procedures and that the driver accepts ODBC's {call ...} syntax, so it is
 * derived from the dialect's call template — the only thing that can make the
 * second half true — rather than from a flag that could disagree with it.
 *
 * This is a data table, not behaviour: only info.c reads it.
 */
typedef struct argus_backend_caps {
    /* SQL_DBMS_NAME. NULL → the backend's own name (what the strcmp chain in
     * info.c fell through to). */
    const char    *dbms_name;

    const char    *catalog_term;         /* NULL → "catalog"   */
    const char    *schema_term;          /* NULL → "database"  */
    const char    *procedure_term;       /* NULL → "procedure" */

    /* SQL_MAX_IDENTIFIER_LEN. 0 → 128. PostgreSQL truncates at 63. */
    SQLUSMALLINT   max_identifier_len;

    /* SQL_TXN_CAPABLE. 0 == SQL_TC_NONE, which is the legacy answer. */
    SQLUSMALLINT   txn_capable;
    SQLUINTEGER    txn_isolation_options; /* 0 → none advertised */
    SQLUINTEGER    default_txn_isolation; /* 0 → none advertised */

    /* SQL_ODBC_SQL_CONFORMANCE. 0 == SQL_OSC_MINIMUM, the legacy answer. */
    SQLUSMALLINT   odbc_sql_conformance;

    bool           describe_parameter;   /* false → SQL_DESCRIBE_PARAMETER = "N" */

    /*
     * SQL_IDENTIFIER_CASE: how the engine stores an unquoted identifier.
     * 0 → SQL_IC_LOWER, which is what info.c answered for every backend,
     * true or not: Phoenix folds to upper, and BigQuery, Druid, Pinot and
     * Kudu are case-sensitive. A tool that lower-cases a name because the
     * driver said the engine does looks for a table that is not there.
     */
    SQLUSMALLINT   identifier_case;

    /*
     * SQL_KEYWORDS: the engine's reserved words that are NOT in SQL-92,
     * comma-separated, no spaces. NULL → the Hive list, which every backend
     * used to be handed. An application quotes what this names, so a wrong
     * list means it either quotes the wrong words or leaves a real keyword
     * bare.
     */
    const char    *keywords;
} argus_backend_caps_t;

typedef struct argus_dbc argus_dbc_t;

/* The connection's capabilities, or the all-zero legacy descriptor. Never
 * NULL, so callers need no branch. */
const argus_backend_caps_t *argus_caps_for(const argus_dbc_t *dbc);

/* Defaulting helpers, so info.c reads as "value or legacy answer". */
const char  *argus_caps_str(const char *value, const char *dflt);
SQLUSMALLINT argus_caps_u16(SQLUSMALLINT value, SQLUSMALLINT dflt);

#endif /* ARGUS_CAPS_H */
