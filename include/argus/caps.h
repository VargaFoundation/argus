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

    bool           procedures;           /* false → SQL_PROCEDURES        = "N" */
    bool           describe_parameter;   /* false → SQL_DESCRIBE_PARAMETER = "N" */
} argus_backend_caps_t;

typedef struct argus_dbc argus_dbc_t;

/* The connection's capabilities, or the all-zero legacy descriptor. Never
 * NULL, so callers need no branch. */
const argus_backend_caps_t *argus_caps_for(const argus_dbc_t *dbc);

/* Defaulting helpers, so info.c reads as "value or legacy answer". */
const char  *argus_caps_str(const char *value, const char *dflt);
SQLUSMALLINT argus_caps_u16(SQLUSMALLINT value, SQLUSMALLINT dflt);

#endif /* ARGUS_CAPS_H */
