#include "argus/handle.h"
#include "argus/odbc_api.h"
#include "argus/dialect.h"
#include "argus/log.h"
#include <string.h>
#include <stdio.h>

/* Normally injected by the build (see src/CMakeLists.txt). */
#ifndef ARGUS_DRIVER_NAME
#define ARGUS_DRIVER_NAME "libargus_odbc.so"
#endif
#ifndef ARGUS_VERSION_MAJOR
#define ARGUS_VERSION_MAJOR 0
#endif
#ifndef ARGUS_VERSION_MINOR
#define ARGUS_VERSION_MINOR 0
#endif
#ifndef ARGUS_VERSION_PATCH
#define ARGUS_VERSION_PATCH 0
#endif

/* Fallback defines for ODBC macros not present in all implementations */
#ifndef SQL_DL_SQL92_DATE
#define SQL_DL_SQL92_DATE       0x00000001L
#endif
#ifndef SQL_DL_SQL92_TIMESTAMP
#define SQL_DL_SQL92_TIMESTAMP  0x00000010L
#endif
#ifndef SQL_IS_INSERT_LITERALS
#define SQL_IS_INSERT_LITERALS  0x00000001L
#endif
#ifndef SQL_IS_SELECT_INTO
#define SQL_IS_SELECT_INTO      0x00000004L
#endif

extern SQLSMALLINT argus_copy_string(const char *src,
                                      SQLCHAR *dst, SQLSMALLINT dst_len);

/* ── Helper: set string info ─────────────────────────────────── */

static SQLRETURN set_string_info(const char *value,
                                  SQLPOINTER info_value,
                                  SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length)
{
    SQLSMALLINT len = argus_copy_string(value,
                                         (SQLCHAR *)info_value,
                                         buffer_length);
    if (string_length) *string_length = len;
    return SQL_SUCCESS;
}

static SQLRETURN set_usmallint_info(SQLUSMALLINT value,
                                     SQLPOINTER info_value,
                                     SQLSMALLINT *string_length)
{
    if (info_value)
        *(SQLUSMALLINT *)info_value = value;
    if (string_length)
        *string_length = sizeof(SQLUSMALLINT);
    return SQL_SUCCESS;
}

static SQLRETURN set_uinteger_info(SQLUINTEGER value,
                                    SQLPOINTER info_value,
                                    SQLSMALLINT *string_length)
{
    if (info_value)
        *(SQLUINTEGER *)info_value = value;
    if (string_length)
        *string_length = sizeof(SQLUINTEGER);
    return SQL_SUCCESS;
}

/* The identifier quote character is dialect-specific. Power BI's Power Query
 * reads it from SQLGetInfo and generates folded SQL with it, so a wrong value
 * makes every generated query fail on the server (e.g. Trino rejects backtick).
 * The per-backend values live in the dialect table (src/odbc/dialect.c). */
static const char *backend_quote_char(const argus_dbc_t *dbc)
{
    return argus_dialect_for(dbc)->quote_char;
}

/* ── ODBC API: SQLGetInfo ────────────────────────────────────── */

SQLRETURN SQL_API SQLGetInfo(
    SQLHDBC      ConnectionHandle,
    SQLUSMALLINT InfoType,
    SQLPOINTER   InfoValue,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&dbc->diag);

    switch (InfoType) {
    /* ── Driver/data source info ─────────────────────────────── */
    case SQL_DRIVER_NAME:
        return set_string_info(ARGUS_DRIVER_NAME, InfoValue, BufferLength, StringLength);
    case SQL_DRIVER_VER: {
        /* ODBC fixes the format at "##.##.####"; the numbers come from the
         * project version so this cannot drift from the shipped build. */
        char ver[16];
        snprintf(ver, sizeof(ver), "%02d.%02d.%04d",
                 ARGUS_VERSION_MAJOR, ARGUS_VERSION_MINOR, ARGUS_VERSION_PATCH);
        return set_string_info(ver, InfoValue, BufferLength, StringLength);
    }
    case SQL_DRIVER_ODBC_VER:
        return set_string_info("03.80", InfoValue, BufferLength, StringLength);
    case SQL_ODBC_VER:
        return set_string_info("03.80", InfoValue, BufferLength, StringLength);
    case SQL_DATA_SOURCE_NAME:
        return set_string_info("Argus", InfoValue, BufferLength, StringLength);
    case SQL_SERVER_NAME:
        return set_string_info(dbc->host ? dbc->host : "", InfoValue, BufferLength, StringLength);
    case SQL_DATABASE_NAME:
        return set_string_info(dbc->database ? dbc->database : "default",
                               InfoValue, BufferLength, StringLength);
    case SQL_DBMS_NAME: {
        /* Return backend-specific name */
        const char *dbms_name = "Unknown";
        if (dbc->backend) {
            if (strcmp(dbc->backend->name, "hive") == 0) {
                dbms_name = "Apache Hive";
            } else if (strcmp(dbc->backend->name, "impala") == 0) {
                dbms_name = "Apache Impala";
            } else if (strcmp(dbc->backend->name, "trino") == 0) {
                dbms_name = "Trino";
            } else {
                dbms_name = dbc->backend->name;
            }
        }
        return set_string_info(dbms_name, InfoValue, BufferLength, StringLength);
    }
    /*
     * The real server version when the backend can report one. ODBC fixes the
     * shape at "##.##.####" optionally followed by the vendor's own string
     * ("04.01.0000 Rdb 4.1"), so the raw value is both parsed and appended —
     * Trino's "467" has no minor/release part, and a tool that wants the truth
     * reads the suffix.
     *
     * A backend with no get_server_version hook reports 00.00.0000 (unknown)
     * rather than the invented "04.00.0000" this used to return: BI tools gate
     * features on this value, so a plausible lie is worse than an obvious gap.
     */
    case SQL_DBMS_VER: {
        char raw[128] = {0};
        if (dbc->connected && dbc->backend && dbc->backend->get_server_version &&
            dbc->backend->get_server_version(dbc->backend_conn, raw, sizeof(raw)) &&
            raw[0]) {
            unsigned major = 0, minor = 0, release = 0;
            sscanf(raw, "%u.%u.%u", &major, &minor, &release);

            char ver[192];
            snprintf(ver, sizeof(ver), "%02u.%02u.%04u %s",
                     major, minor, release, raw);
            return set_string_info(ver, InfoValue, BufferLength, StringLength);
        }
        return set_string_info("00.00.0000", InfoValue, BufferLength, StringLength);
    }

    /* ── SQL conformance ─────────────────────────────────────── */
    case SQL_ODBC_API_CONFORMANCE:
        return set_usmallint_info(SQL_OAC_LEVEL1, InfoValue, StringLength);
    case SQL_ODBC_SQL_CONFORMANCE:
        return set_usmallint_info(SQL_OSC_MINIMUM, InfoValue, StringLength);

    /* ── Supported features ──────────────────────────────────── */
    case SQL_GETDATA_EXTENSIONS:
        return set_uinteger_info(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER,
                                  InfoValue, StringLength);
    case SQL_CURSOR_COMMIT_BEHAVIOR:
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
        return set_usmallint_info(SQL_CB_CLOSE, InfoValue, StringLength);

    /* ── Identifier info ─────────────────────────────────────── */
    case SQL_IDENTIFIER_QUOTE_CHAR:
        return set_string_info(backend_quote_char(dbc), InfoValue,
                               BufferLength, StringLength);
    case SQL_CATALOG_NAME_SEPARATOR:
        return set_string_info(".", InfoValue, BufferLength, StringLength);
    case SQL_CATALOG_TERM:
        return set_string_info("catalog", InfoValue, BufferLength, StringLength);
    case SQL_SCHEMA_TERM:
        return set_string_info("database", InfoValue, BufferLength, StringLength);
    case SQL_TABLE_TERM:
        return set_string_info("table", InfoValue, BufferLength, StringLength);
    case SQL_PROCEDURE_TERM:
        return set_string_info("procedure", InfoValue, BufferLength, StringLength);
    case SQL_CATALOG_LOCATION:
        return set_usmallint_info(SQL_CL_START, InfoValue, StringLength);

    /* ── Scalar functions ────────────────────────────────────────
     * Derived from the dialect's fn_map, never hard-coded: reporting a bit here
     * is a promise that {fn NAME(...)} will be translated by escape.c (ODBC
     * spec, "Escape Sequences in ODBC"). Deriving both from one table is what
     * keeps that promise true, and makes the answer backend-specific — Druid
     * and Hive do not have the same functions. */
    case SQL_STRING_FUNCTIONS:
        return set_uinteger_info(
            argus_dialect_fn_bitmap(argus_dialect_for(dbc), ARGUS_FN_GROUP_STRING),
            InfoValue, StringLength);

    case SQL_NUMERIC_FUNCTIONS:
        return set_uinteger_info(
            argus_dialect_fn_bitmap(argus_dialect_for(dbc), ARGUS_FN_GROUP_NUMERIC),
            InfoValue, StringLength);

    case SQL_SYSTEM_FUNCTIONS:
        return set_uinteger_info(
            argus_dialect_fn_bitmap(argus_dialect_for(dbc), ARGUS_FN_GROUP_SYSTEM),
            InfoValue, StringLength);

    case SQL_TIMEDATE_FUNCTIONS:
        return set_uinteger_info(
            argus_dialect_fn_bitmap(argus_dialect_for(dbc), ARGUS_FN_GROUP_TIMEDATE),
            InfoValue, StringLength);

    /* ── SQL syntax support ──────────────────────────────────── */
    case SQL_SEARCH_PATTERN_ESCAPE:
        return set_string_info("\\", InfoValue, BufferLength, StringLength);
    case SQL_LIKE_ESCAPE_CLAUSE:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);
    case SQL_SPECIAL_CHARACTERS:
        return set_string_info("_%", InfoValue, BufferLength, StringLength);
    case SQL_MAX_IDENTIFIER_LEN:
        return set_usmallint_info(128, InfoValue, StringLength);
    case SQL_MAX_TABLE_NAME_LEN:
    case SQL_MAX_SCHEMA_NAME_LEN:
    case SQL_MAX_CATALOG_NAME_LEN:
        return set_usmallint_info(128, InfoValue, StringLength);
    case SQL_MAX_COLUMN_NAME_LEN:
        return set_usmallint_info(256, InfoValue, StringLength);

    /* ── Cursor capabilities ─────────────────────────────────────
     * SQLFetchScroll implements a real static cursor over a materialised copy
     * of the result set (FIRST/LAST/PRIOR/ABSOLUTE/RELATIVE), and
     * SQLSetStmtAttr accepts SQL_CURSOR_STATIC. Reporting forward-only here
     * meant every BI tool that asks before it scrolls concluded it could not,
     * and fell back to refetching. Keyset and dynamic stay at zero: they are
     * downgraded to static, so claiming them would be a lie of a different
     * kind. */
    case SQL_SCROLL_OPTIONS:
        return set_uinteger_info(SQL_SO_FORWARD_ONLY | SQL_SO_STATIC,
                                 InfoValue, StringLength);
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        return set_uinteger_info(SQL_CA1_NEXT, InfoValue, StringLength);
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
        return set_uinteger_info(SQL_CA2_READ_ONLY_CONCURRENCY,
                                 InfoValue, StringLength);
    /* SQL_CA1_ABSOLUTE covers FETCH_FIRST/LAST/ABSOLUTE, SQL_CA1_RELATIVE
     * covers FETCH_PRIOR/RELATIVE — all handled in fetch.c's scroll cache.
     * SQLSetPos implements SQL_POSITION and SQL_REFRESH only. */
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
        return set_uinteger_info(
            SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE |
            SQL_CA1_POS_POSITION | SQL_CA1_POS_REFRESH | SQL_CA1_LOCK_NO_CHANGE,
            InfoValue, StringLength);
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
        return set_uinteger_info(SQL_CA2_READ_ONLY_CONCURRENCY,
                                 InfoValue, StringLength);
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
    case SQL_KEYSET_CURSOR_ATTRIBUTES1:
    case SQL_KEYSET_CURSOR_ATTRIBUTES2:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_CURSOR_SENSITIVITY:
        return set_uinteger_info(SQL_UNSPECIFIED, InfoValue, StringLength);

    /* ── Transaction support ─────────────────────────────────── */
    case SQL_TXN_CAPABLE:
        return set_usmallint_info(SQL_TC_NONE, InfoValue, StringLength);
    case SQL_TXN_ISOLATION_OPTION:
    case SQL_DEFAULT_TXN_ISOLATION:
        return set_uinteger_info(0, InfoValue, StringLength);

    /* ── Supported conversions ───────────────────────────────── */
    case SQL_CONVERT_FUNCTIONS:
        return set_uinteger_info(SQL_FN_CVT_CAST, InfoValue, StringLength);

    case SQL_CONVERT_BIGINT:
    case SQL_CONVERT_INTEGER:
    case SQL_CONVERT_SMALLINT:
    case SQL_CONVERT_TINYINT:
    case SQL_CONVERT_FLOAT:
    case SQL_CONVERT_DOUBLE:
    case SQL_CONVERT_CHAR:
    case SQL_CONVERT_VARCHAR:
    case SQL_CONVERT_LONGVARCHAR:
        return set_uinteger_info(
            SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
            SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_TINYINT |
            SQL_CVT_BIGINT | SQL_CVT_FLOAT | SQL_CVT_DOUBLE,
            InfoValue, StringLength);

    case SQL_CONVERT_BIT:
    case SQL_CONVERT_DATE:
    case SQL_CONVERT_TIME:
    case SQL_CONVERT_TIMESTAMP:
    case SQL_CONVERT_BINARY:
    case SQL_CONVERT_VARBINARY:
    case SQL_CONVERT_LONGVARBINARY:
    case SQL_CONVERT_DECIMAL:
    case SQL_CONVERT_NUMERIC:
    case SQL_CONVERT_REAL:
        return set_uinteger_info(SQL_CVT_CHAR | SQL_CVT_VARCHAR,
                                  InfoValue, StringLength);

    /* ── Misc capabilities ───────────────────────────────────── */
    case SQL_COLUMN_ALIAS:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);
    case SQL_GROUP_BY:
        return set_usmallint_info(SQL_GB_GROUP_BY_CONTAINS_SELECT, InfoValue, StringLength);
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
        return set_string_info("N", InfoValue, BufferLength, StringLength);
    case SQL_EXPRESSIONS_IN_ORDERBY:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);
    case SQL_MULT_RESULT_SETS:
        return set_string_info("N", InfoValue, BufferLength, StringLength);
    case SQL_MULTIPLE_ACTIVE_TXN:
        return set_string_info("N", InfoValue, BufferLength, StringLength);
    /* Reporting outer-join capability commits the driver to accepting the
     * {oj ...} escape, so both answers come from the dialect. Backends whose
     * join surface we have not verified (Phoenix, Pinot, Druid, Flight SQL,
     * Kudu) report none rather than claim all of them. */
    case SQL_OUTER_JOINS:
        return set_string_info(argus_dialect_for(dbc)->supports_oj ? "Y" : "N",
                               InfoValue, BufferLength, StringLength);
    case SQL_OJ_CAPABILITIES:
        return set_uinteger_info(
            argus_dialect_for(dbc)->supports_oj
                ? (SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL |
                   SQL_OJ_NESTED | SQL_OJ_NOT_ORDERED |
                   SQL_OJ_INNER | SQL_OJ_ALL_COMPARISON_OPS)
                : 0,
            InfoValue, StringLength);
    case SQL_SUBQUERIES:
        return set_uinteger_info(
            SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
            SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED,
            InfoValue, StringLength);
    case SQL_UNION:
        return set_uinteger_info(SQL_U_UNION | SQL_U_UNION_ALL,
                                  InfoValue, StringLength);
    case SQL_MAX_COLUMNS_IN_SELECT:
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
    case SQL_MAX_COLUMNS_IN_TABLE:
    case SQL_MAX_TABLES_IN_SELECT:
        return set_usmallint_info(0, InfoValue, StringLength); /* 0 = no limit */

    case SQL_MAX_ROW_SIZE:
    case SQL_MAX_STATEMENT_LEN:
    case SQL_MAX_CHAR_LITERAL_LEN:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);

    case SQL_NEED_LONG_DATA_LEN:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_NULL_COLLATION:
        return set_usmallint_info(SQL_NC_END, InfoValue, StringLength);

    case SQL_CONCAT_NULL_BEHAVIOR:
        return set_usmallint_info(SQL_CB_NULL, InfoValue, StringLength);

    case SQL_QUOTED_IDENTIFIER_CASE:
        return set_usmallint_info(SQL_IC_SENSITIVE, InfoValue, StringLength);

    case SQL_IDENTIFIER_CASE:
        return set_usmallint_info(SQL_IC_LOWER, InfoValue, StringLength);

    case SQL_CORRELATION_NAME:
        return set_usmallint_info(SQL_CN_ANY, InfoValue, StringLength);

    case SQL_NON_NULLABLE_COLUMNS:
        return set_usmallint_info(SQL_NNC_NON_NULL, InfoValue, StringLength);

    case SQL_ALTER_TABLE:
        return set_uinteger_info(SQL_AT_ADD_COLUMN | SQL_AT_DROP_COLUMN,
                                  InfoValue, StringLength);

    case SQL_SQL_CONFORMANCE:
        return set_uinteger_info(SQL_SC_SQL92_ENTRY, InfoValue, StringLength);

    /* Level 1 interface conformance, matching the commercial Simba/Starburst
     * drivers for these same engines. Level 1 = Core + features 101-109: schema
     * qualification, scrollable cursors (SQLFetchScroll), SQLPrimaryKeys,
     * SQLBrowseConnect, SQLSetPos POSITION/REFRESH, SQLMoreResults, and real
     * descriptor handles (SQLAllocHandle SQL_HANDLE_DESC) — all present. The two
     * OLTP features Level 1 also lists, stored procedures (105) and transactions
     * with rollback (109), are absent from Trino/BigQuery/Hive/… and reported so
     * honestly via SQL_PROCEDURES="N" and SQL_TXN_CAPABLE=SQL_TC_NONE, exactly
     * as the commercial drivers do over the same engines. */
    case SQL_ODBC_INTERFACE_CONFORMANCE:
        return set_uinteger_info(SQL_OIC_LEVEL1, InfoValue, StringLength);

    case SQL_SQL92_PREDICATES:
        return set_uinteger_info(
            SQL_SP_COMPARISON | SQL_SP_EXISTS | SQL_SP_IN |
            SQL_SP_ISNOTNULL | SQL_SP_ISNULL | SQL_SP_LIKE |
            SQL_SP_BETWEEN,
            InfoValue, StringLength);

    case SQL_SQL92_VALUE_EXPRESSIONS:
        return set_uinteger_info(
            SQL_SVE_CASE | SQL_SVE_CAST | SQL_SVE_NULLIF | SQL_SVE_COALESCE,
            InfoValue, StringLength);

    case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
        return set_uinteger_info(
            SQL_SRJO_CROSS_JOIN | SQL_SRJO_INNER_JOIN |
            SQL_SRJO_LEFT_OUTER_JOIN | SQL_SRJO_RIGHT_OUTER_JOIN |
            SQL_SRJO_FULL_OUTER_JOIN,
            InfoValue, StringLength);

    case SQL_AGGREGATE_FUNCTIONS:
        return set_uinteger_info(
            SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
            SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM,
            InfoValue, StringLength);

    case SQL_CATALOG_USAGE:
    case SQL_SCHEMA_USAGE:
        return set_uinteger_info(
            SQL_CU_DML_STATEMENTS | SQL_CU_TABLE_DEFINITION,
            InfoValue, StringLength);

    case SQL_ACCESSIBLE_TABLES:
    case SQL_ACCESSIBLE_PROCEDURES:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_BATCH_SUPPORT:
    case SQL_BATCH_ROW_COUNT:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_PARAM_ARRAY_SELECTS:
        return set_uinteger_info(SQL_PAS_NO_SELECT, InfoValue, StringLength);

    /* Reported NONE, and this is deliberate. The statement-async scaffolding
     * exists (SQL_ATTR_ASYNC_ENABLE, execute.c async_poll returning
     * SQL_STILL_EXECUTING), but the backends' get_operation_status is *passive*:
     * trino_get_operation_status reports finished from the current op state and
     * never advances the query — Trino only progresses when its nextUri is
     * polled, which happens during fetch, not during async_poll. So an async
     * SQLExecDirect would return SQL_STILL_EXECUTING forever (proven by
     * tests/integration/test_async.c). Advertising SQL_AM_STATEMENT would make
     * BI tools hang. To advertise it honestly, get_operation_status must drive
     * the operation forward one step per poll; until then, NONE is the truth. */
    case SQL_ASYNC_MODE:
        return set_uinteger_info(SQL_AM_NONE, InfoValue, StringLength);

    case SQL_INFO_SCHEMA_VIEWS:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_KEYWORDS:
        return set_string_info(
            "LATERAL,MAP,REDUCE,TRANSFORM,TABLESAMPLE,CLUSTER,DISTRIBUTE,SORT",
            InfoValue, BufferLength, StringLength);

    case SQL_USER_NAME:
        return set_string_info(dbc->username ? dbc->username : "",
                               InfoValue, BufferLength, StringLength);

    case SQL_MAX_CONCURRENT_ACTIVITIES:
        return set_usmallint_info(0, InfoValue, StringLength);

    case SQL_MAX_DRIVER_CONNECTIONS:
        return set_usmallint_info(0, InfoValue, StringLength);

    case SQL_ROW_UPDATES:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_BOOKMARK_PERSISTENCE:
        return set_uinteger_info(0, InfoValue, StringLength);

    /* SQLDescribeParam can only guess (SQL_VARCHAR/255): none of these engines
     * is asked to prepare server-side, so there is no parameter metadata to
     * report. Saying "Y" here made applications trust that guess. */
    case SQL_DESCRIBE_PARAMETER:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_INTEGRITY:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_MAX_INDEX_SIZE:
        return set_uinteger_info(0, InfoValue, StringLength);

    /* The driver forwards whatever SQL it is given, and SQL_CREATE_TABLE /
     * SQL_DROP_TABLE / SQL_INSERT_STATEMENT below advertise DDL and DML. What
     * a given user may actually write is the server's decision, not ours. */
    case SQL_DATA_SOURCE_READ_ONLY:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    /* SQLSetPos implements these two; ADD/UPDATE/DELETE return HYC00. */
    case SQL_POS_OPERATIONS:
        return set_uinteger_info(SQL_POS_POSITION | SQL_POS_REFRESH,
                                 InfoValue, StringLength);

    case SQL_LOCK_TYPES:
        return set_uinteger_info(SQL_LCK_NO_CHANGE, InfoValue, StringLength);

    case SQL_POSITIONED_STATEMENTS:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_SQL92_STRING_FUNCTIONS:
        return set_uinteger_info(
            SQL_SSF_LOWER | SQL_SSF_UPPER | SQL_SSF_SUBSTRING |
            SQL_SSF_TRIM_BOTH | SQL_SSF_TRIM_LEADING | SQL_SSF_TRIM_TRAILING,
            InfoValue, StringLength);

    case SQL_SQL92_DATETIME_FUNCTIONS:
        return set_uinteger_info(SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIMESTAMP,
                                  InfoValue, StringLength);

    /* Reporting SQL-92 datetime literals means {d '...'} / {ts '...'} will be
     * rendered as DATE '...' / TIMESTAMP '...', so the answer follows the
     * dialect's literal style rather than being assumed for every backend. */
    case SQL_DATETIME_LITERALS:
        return set_uinteger_info(
            argus_dialect_for(dbc)->literals == ARGUS_LIT_ANSI
                ? (SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIMESTAMP)
                : 0,
            InfoValue, StringLength);

    case SQL_CATALOG_NAME:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);

    case SQL_COLLATION_SEQ:
        return set_string_info("", InfoValue, BufferLength, StringLength);

    case SQL_MAX_COLUMNS_IN_INDEX:
        return set_usmallint_info(0, InfoValue, StringLength);

    case SQL_CREATE_TABLE:
        return set_uinteger_info(SQL_CT_CREATE_TABLE, InfoValue, StringLength);

    case SQL_CREATE_VIEW:
        return set_uinteger_info(SQL_CV_CREATE_VIEW, InfoValue, StringLength);

    case SQL_DROP_TABLE:
        return set_uinteger_info(SQL_DT_DROP_TABLE, InfoValue, StringLength);

    case SQL_DROP_VIEW:
        return set_uinteger_info(SQL_DV_DROP_VIEW, InfoValue, StringLength);

    case SQL_INDEX_KEYWORDS:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_INSERT_STATEMENT:
        return set_uinteger_info(SQL_IS_INSERT_LITERALS | SQL_IS_SELECT_INTO,
                                  InfoValue, StringLength);

    /* ── Additional info types for BI tool compatibility ─────── */
    case SQL_PROCEDURES:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_MAX_BINARY_LITERAL_LEN:
        return set_uinteger_info(0, InfoValue, StringLength);

#ifdef SQL_TIMEDATE_ADD_INTERVALS
    case SQL_TIMEDATE_ADD_INTERVALS:
        return set_uinteger_info(0, InfoValue, StringLength);
#endif
#ifdef SQL_TIMEDATE_DIFF_INTERVALS
    case SQL_TIMEDATE_DIFF_INTERVALS:
        return set_uinteger_info(0, InfoValue, StringLength);
#endif

    /*
     * ODBC 2.x aliases (SQL_OWNER_TERM, SQL_QUALIFIER_TERM, etc.) are
     * #defined to the same numeric values as their ODBC 3.x counterparts
     * (SQL_SCHEMA_TERM, SQL_CATALOG_TERM, etc.), so they are already
     * handled by the cases above. No additional case labels needed.
     */

#ifdef SQL_ACTIVE_ENVIRONMENTS
    case SQL_ACTIVE_ENVIRONMENTS:
        return set_usmallint_info(0, InfoValue, StringLength);
#endif

    case SQL_CREATE_ASSERTION:
    case SQL_CREATE_CHARACTER_SET:
    case SQL_CREATE_COLLATION:
    case SQL_CREATE_DOMAIN:
    case SQL_CREATE_SCHEMA:
    case SQL_CREATE_TRANSLATION:
    case SQL_DROP_ASSERTION:
    case SQL_DROP_CHARACTER_SET:
    case SQL_DROP_COLLATION:
    case SQL_DROP_DOMAIN:
    case SQL_DROP_SCHEMA:
    case SQL_DROP_TRANSLATION:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_SQL92_GRANT:
    case SQL_SQL92_REVOKE:
        return set_uinteger_info(0, InfoValue, StringLength);

#ifdef SQL_STANDARD_CLI_CONFORMANCE
    case SQL_STANDARD_CLI_CONFORMANCE:
        return set_uinteger_info(0, InfoValue, StringLength);
#endif

#ifdef SQL_DDL_INDEX
    case SQL_DDL_INDEX:
        return set_uinteger_info(0, InfoValue, StringLength);
#endif

    default:
        /*
         * Deliberately permissive, and a documented deviation: ODBC reserves
         * HY096 for an InfoType that does not exist, but this branch cannot
         * tell those apart from the ones simply not handled above. For the
         * latter — the overwhelming majority — zero means "capability absent",
         * which is both the right answer and what the caller expects, so
         * erroring would break working applications to no end.
         *
         * The log line is the safety net: it makes the gap visible while
         * debugging a BI tool instead of hiding behind a silent zero.
         */
        ARGUS_LOG_DEBUG("SQLGetInfo: unhandled InfoType %u, reporting zero/empty",
                        (unsigned)InfoType);
        if (InfoValue && BufferLength > 0)
            memset(InfoValue, 0, (size_t)BufferLength);
        if (StringLength) *StringLength = 0;
        return SQL_SUCCESS;
    }
}

/* ── ODBC API: SQLGetFunctions ───────────────────────────────── */

SQLRETURN SQL_API SQLGetFunctions(
    SQLHDBC      ConnectionHandle,
    SQLUSMALLINT FunctionId,
    SQLUSMALLINT *Supported)
{
    argus_dbc_t *dbc = (argus_dbc_t *)ConnectionHandle;
    if (!argus_valid_dbc(dbc)) return SQL_INVALID_HANDLE;

    if (!Supported) return SQL_ERROR;

    if (FunctionId == SQL_API_ODBC3_ALL_FUNCTIONS) {
        /* Return bitmap for all ODBC 3.x functions */
        /* SQL_API_ODBC3_ALL_FUNCTIONS_SIZE is 250 words */
        memset(Supported, 0, SQL_API_ODBC3_ALL_FUNCTIONS_SIZE * sizeof(SQLUSMALLINT));

        /* Mark supported functions */
        #define SET_FUNC(id) Supported[(id) >> 4] |= (SQLUSMALLINT)(1 << ((id) & 0x000F))

        SET_FUNC(SQL_API_SQLALLOCHANDLE);
        SET_FUNC(SQL_API_SQLFREEHANDLE);
        SET_FUNC(SQL_API_SQLFREESTMT);
        SET_FUNC(SQL_API_SQLCONNECT);
        SET_FUNC(SQL_API_SQLDRIVERCONNECT);
        SET_FUNC(SQL_API_SQLDISCONNECT);
        SET_FUNC(SQL_API_SQLEXECDIRECT);
        SET_FUNC(SQL_API_SQLPREPARE);
        SET_FUNC(SQL_API_SQLEXECUTE);
        SET_FUNC(SQL_API_SQLFETCH);
        SET_FUNC(SQL_API_SQLFETCHSCROLL);
        SET_FUNC(SQL_API_SQLGETDATA);
        SET_FUNC(SQL_API_SQLBINDCOL);
        SET_FUNC(SQL_API_SQLNUMRESULTCOLS);
        SET_FUNC(SQL_API_SQLDESCRIBECOL);
        SET_FUNC(SQL_API_SQLCOLATTRIBUTE);
        SET_FUNC(SQL_API_SQLROWCOUNT);
        SET_FUNC(SQL_API_SQLTABLES);
        SET_FUNC(SQL_API_SQLCOLUMNS);
        SET_FUNC(SQL_API_SQLGETTYPEINFO);
        SET_FUNC(SQL_API_SQLSTATISTICS);
        SET_FUNC(SQL_API_SQLSPECIALCOLUMNS);
        SET_FUNC(SQL_API_SQLPRIMARYKEYS);
        SET_FUNC(SQL_API_SQLFOREIGNKEYS);
        SET_FUNC(SQL_API_SQLPROCEDURES);
        SET_FUNC(SQL_API_SQLPROCEDURECOLUMNS);
        SET_FUNC(SQL_API_SQLGETINFO);
        SET_FUNC(SQL_API_SQLGETFUNCTIONS);
        SET_FUNC(SQL_API_SQLGETDIAGREC);
        SET_FUNC(SQL_API_SQLGETDIAGFIELD);
        SET_FUNC(SQL_API_SQLSETENVATTR);
        SET_FUNC(SQL_API_SQLGETENVATTR);
        SET_FUNC(SQL_API_SQLSETCONNECTATTR);
        SET_FUNC(SQL_API_SQLGETCONNECTATTR);
        SET_FUNC(SQL_API_SQLSETSTMTATTR);
        SET_FUNC(SQL_API_SQLGETSTMTATTR);
        SET_FUNC(SQL_API_SQLCLOSECURSOR);
        SET_FUNC(SQL_API_SQLCANCEL);
        SET_FUNC(SQL_API_SQLENDTRAN);
        SET_FUNC(SQL_API_SQLNATIVESQL);
        SET_FUNC(SQL_API_SQLMORERESULTS);
        SET_FUNC(SQL_API_SQLNUMPARAMS);
        SET_FUNC(SQL_API_SQLBINDPARAMETER);
        SET_FUNC(SQL_API_SQLGETDESCFIELD);
        SET_FUNC(SQL_API_SQLSETDESCFIELD);
        SET_FUNC(SQL_API_SQLGETDESCREC);
        SET_FUNC(SQL_API_SQLTABLEPRIVILEGES);
        SET_FUNC(SQL_API_SQLCOLUMNPRIVILEGES);
        SET_FUNC(SQL_API_SQLBROWSECONNECT);
        SET_FUNC(SQL_API_SQLPARAMDATA);
        SET_FUNC(SQL_API_SQLPUTDATA);
        SET_FUNC(SQL_API_SQLGETCURSORNAME);
        SET_FUNC(SQL_API_SQLSETCURSORNAME);
        SET_FUNC(SQL_API_SQLERROR);
        SET_FUNC(SQL_API_SQLCOPYDESC);
        SET_FUNC(SQL_API_SQLSETDESCREC);
        SET_FUNC(SQL_API_SQLDESCRIBEPARAM);
        /* SQLSetPos is here because SQL_POSITION and SQL_REFRESH work;
         * SQL_POS_OPERATIONS says which. SQLBulkOperations is deliberately
         * absent — it is a pure HYC00 stub, and claiming it made applications
         * call it and fail. */
        SET_FUNC(SQL_API_SQLSETPOS);

        #undef SET_FUNC
        return SQL_SUCCESS;
    }

    if (FunctionId == SQL_API_ALL_FUNCTIONS) {
        /* ODBC 2.x bitmap (100 entries) */
        memset(Supported, 0, 100 * sizeof(SQLUSMALLINT));
        Supported[SQL_API_SQLALLOCENV]     = SQL_TRUE;
        Supported[SQL_API_SQLFREEENV]      = SQL_TRUE;
        Supported[SQL_API_SQLALLOCCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLFREECONNECT]  = SQL_TRUE;
        Supported[SQL_API_SQLALLOCSTMT]    = SQL_TRUE;
        Supported[SQL_API_SQLFREESTMT]     = SQL_TRUE;
        Supported[SQL_API_SQLCONNECT]      = SQL_TRUE;
        Supported[SQL_API_SQLDISCONNECT]   = SQL_TRUE;
        Supported[SQL_API_SQLEXECDIRECT]   = SQL_TRUE;
        Supported[SQL_API_SQLPREPARE]      = SQL_TRUE;
        Supported[SQL_API_SQLEXECUTE]      = SQL_TRUE;
        Supported[SQL_API_SQLFETCH]        = SQL_TRUE;
        Supported[SQL_API_SQLGETDATA]      = SQL_TRUE;
        Supported[SQL_API_SQLBINDCOL]      = SQL_TRUE;
        Supported[SQL_API_SQLNUMRESULTCOLS]= SQL_TRUE;
        Supported[SQL_API_SQLDESCRIBECOL]  = SQL_TRUE;
        Supported[SQL_API_SQLROWCOUNT]     = SQL_TRUE;
        Supported[SQL_API_SQLTABLES]       = SQL_TRUE;
        Supported[SQL_API_SQLCOLUMNS]      = SQL_TRUE;
        Supported[SQL_API_SQLGETTYPEINFO]  = SQL_TRUE;
        Supported[SQL_API_SQLSTATISTICS]   = SQL_TRUE;
        Supported[SQL_API_SQLGETINFO]      = SQL_TRUE;
        Supported[SQL_API_SQLGETFUNCTIONS] = SQL_TRUE;
        Supported[SQL_API_SQLERROR]        = SQL_TRUE;
        Supported[SQL_API_SQLSPECIALCOLUMNS] = SQL_TRUE;
        Supported[SQL_API_SQLPRIMARYKEYS]    = SQL_TRUE;
        Supported[SQL_API_SQLFOREIGNKEYS]    = SQL_TRUE;
        Supported[SQL_API_SQLPROCEDURES]     = SQL_TRUE;
        Supported[SQL_API_SQLPROCEDURECOLUMNS] = SQL_TRUE;
        Supported[SQL_API_SQLTABLEPRIVILEGES] = SQL_TRUE;
        Supported[SQL_API_SQLCOLUMNPRIVILEGES] = SQL_TRUE;
        Supported[SQL_API_SQLBROWSECONNECT]  = SQL_TRUE;
        Supported[SQL_API_SQLPARAMDATA]      = SQL_TRUE;
        Supported[SQL_API_SQLPUTDATA]        = SQL_TRUE;
        Supported[SQL_API_SQLGETCURSORNAME]  = SQL_TRUE;
        Supported[SQL_API_SQLSETCURSORNAME]  = SQL_TRUE;
        Supported[SQL_API_SQLSETPOS]         = SQL_TRUE;
        Supported[SQL_API_SQLDESCRIBEPARAM]  = SQL_TRUE;
        return SQL_SUCCESS;
    }

    /* Single function query */
    switch (FunctionId) {
#ifdef SQL_API_SQLCANCELHANDLE
    case SQL_API_SQLCANCELHANDLE:   /* ODBC 3.8 */
#endif
    case SQL_API_SQLALLOCHANDLE:
    case SQL_API_SQLFREEHANDLE:
    case SQL_API_SQLFREESTMT:
    case SQL_API_SQLCONNECT:
    case SQL_API_SQLDRIVERCONNECT:
    case SQL_API_SQLDISCONNECT:
    case SQL_API_SQLEXECDIRECT:
    case SQL_API_SQLPREPARE:
    case SQL_API_SQLEXECUTE:
    case SQL_API_SQLFETCH:
    case SQL_API_SQLFETCHSCROLL:
    case SQL_API_SQLGETDATA:
    case SQL_API_SQLBINDCOL:
    case SQL_API_SQLNUMRESULTCOLS:
    case SQL_API_SQLDESCRIBECOL:
    case SQL_API_SQLCOLATTRIBUTE:
    case SQL_API_SQLROWCOUNT:
    case SQL_API_SQLTABLES:
    case SQL_API_SQLCOLUMNS:
    case SQL_API_SQLGETTYPEINFO:
    case SQL_API_SQLSTATISTICS:
    case SQL_API_SQLSPECIALCOLUMNS:
    case SQL_API_SQLPRIMARYKEYS:
    case SQL_API_SQLFOREIGNKEYS:
    case SQL_API_SQLGETINFO:
    case SQL_API_SQLGETFUNCTIONS:
    case SQL_API_SQLGETDIAGREC:
    case SQL_API_SQLGETDIAGFIELD:
    case SQL_API_SQLSETENVATTR:
    case SQL_API_SQLGETENVATTR:
    case SQL_API_SQLSETCONNECTATTR:
    case SQL_API_SQLGETCONNECTATTR:
    case SQL_API_SQLSETSTMTATTR:
    case SQL_API_SQLGETSTMTATTR:
    case SQL_API_SQLCLOSECURSOR:
    case SQL_API_SQLCANCEL:
    case SQL_API_SQLENDTRAN:
    case SQL_API_SQLNATIVESQL:
    case SQL_API_SQLMORERESULTS:
    case SQL_API_SQLNUMPARAMS:
    case SQL_API_SQLBINDPARAMETER:
    case SQL_API_SQLGETDESCFIELD:
    case SQL_API_SQLSETDESCFIELD:
    case SQL_API_SQLGETDESCREC:
    case SQL_API_SQLTABLEPRIVILEGES:
    case SQL_API_SQLCOLUMNPRIVILEGES:
    case SQL_API_SQLPROCEDURES:
    case SQL_API_SQLPROCEDURECOLUMNS:
    case SQL_API_SQLBROWSECONNECT:
    case SQL_API_SQLPARAMDATA:
    case SQL_API_SQLPUTDATA:
    case SQL_API_SQLGETCURSORNAME:
    case SQL_API_SQLSETCURSORNAME:
    case SQL_API_SQLERROR:
    case SQL_API_SQLCOPYDESC:
    case SQL_API_SQLSETDESCREC:
    case SQL_API_SQLDESCRIBEPARAM:
    case SQL_API_SQLSETPOS:
        *Supported = SQL_TRUE;
        break;
    default:
        /* Includes SQL_API_SQLBULKOPERATIONS: a pure HYC00 stub, so SQL_FALSE
         * is the honest answer and keeps applications from calling it. */
        *Supported = SQL_FALSE;
        break;
    }

    return SQL_SUCCESS;
}
