#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <string.h>

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
        return set_string_info("libargus_odbc.so", InfoValue, BufferLength, StringLength);
    case SQL_DRIVER_VER:
        return set_string_info("00.01.0000", InfoValue, BufferLength, StringLength);
    case SQL_DRIVER_ODBC_VER:
        return set_string_info("03.80", InfoValue, BufferLength, StringLength);
    case SQL_ODBC_VER:
        return set_string_info("03.80", InfoValue, BufferLength, StringLength);
    case SQL_DATA_SOURCE_NAME:
        return set_string_info("Argus", InfoValue, BufferLength, StringLength);
    case SQL_SERVER_NAME:
        return set_string_info(dbc->host ? dbc->host : "", InfoValue, BufferLength, StringLength);
    case SQL_DATABASE_NAME:
    case SQL_CATALOG_NAME:
        return set_string_info(dbc->database ? dbc->database : "default",
                               InfoValue, BufferLength, StringLength);
    case SQL_DBMS_NAME:
        return set_string_info("Apache Hive", InfoValue, BufferLength, StringLength);
    case SQL_DBMS_VER:
        return set_string_info("04.00.0000", InfoValue, BufferLength, StringLength);

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
        return set_string_info("`", InfoValue, BufferLength, StringLength);
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

    /* ── String functions ────────────────────────────────────── */
    case SQL_STRING_FUNCTIONS:
        return set_uinteger_info(
            SQL_FN_STR_CONCAT | SQL_FN_STR_LENGTH | SQL_FN_STR_SUBSTRING |
            SQL_FN_STR_LTRIM | SQL_FN_STR_RTRIM | SQL_FN_STR_LCASE |
            SQL_FN_STR_UCASE | SQL_FN_STR_REPLACE,
            InfoValue, StringLength);

    case SQL_NUMERIC_FUNCTIONS:
        return set_uinteger_info(
            SQL_FN_NUM_ABS | SQL_FN_NUM_CEILING | SQL_FN_NUM_FLOOR |
            SQL_FN_NUM_MOD | SQL_FN_NUM_ROUND | SQL_FN_NUM_SQRT |
            SQL_FN_NUM_POWER | SQL_FN_NUM_LOG | SQL_FN_NUM_EXP,
            InfoValue, StringLength);

    case SQL_SYSTEM_FUNCTIONS:
        return set_uinteger_info(SQL_FN_SYS_IFNULL, InfoValue, StringLength);

    case SQL_TIMEDATE_FUNCTIONS:
        return set_uinteger_info(
            SQL_FN_TD_NOW | SQL_FN_TD_CURDATE | SQL_FN_TD_YEAR |
            SQL_FN_TD_MONTH | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_HOUR |
            SQL_FN_TD_MINUTE | SQL_FN_TD_SECOND,
            InfoValue, StringLength);

    /* ── SQL syntax support ──────────────────────────────────── */
    case SQL_SEARCH_PATTERN_ESCAPE:
        return set_string_info("\\", InfoValue, BufferLength, StringLength);
    case SQL_LIKE_ESCAPE_CLAUSE:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);
    case SQL_SPECIAL_CHARACTERS:
        return set_string_info("_", InfoValue, BufferLength, StringLength);
    case SQL_MAX_IDENTIFIER_LEN:
        return set_usmallint_info(128, InfoValue, StringLength);
    case SQL_MAX_TABLE_NAME_LEN:
    case SQL_MAX_COLUMN_NAME_LEN:
    case SQL_MAX_SCHEMA_NAME_LEN:
    case SQL_MAX_CATALOG_NAME_LEN:
        return set_usmallint_info(128, InfoValue, StringLength);

    /* ── Cursor capabilities ─────────────────────────────────── */
    case SQL_SCROLL_OPTIONS:
        return set_uinteger_info(SQL_SO_FORWARD_ONLY, InfoValue, StringLength);
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        return set_uinteger_info(SQL_CA1_NEXT, InfoValue, StringLength);
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
        return set_uinteger_info(0, InfoValue, StringLength);
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
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
        return set_usmallint_info(SQL_GB_GROUP_BY_EQUALS_SELECT, InfoValue, StringLength);
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
        return set_string_info("N", InfoValue, BufferLength, StringLength);
    case SQL_EXPRESSIONS_IN_ORDERBY:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);
    case SQL_MULT_RESULT_SETS:
        return set_string_info("N", InfoValue, BufferLength, StringLength);
    case SQL_MULTIPLE_ACTIVE_TXN:
        return set_string_info("N", InfoValue, BufferLength, StringLength);
    case SQL_OUTER_JOINS:
        return set_string_info("Y", InfoValue, BufferLength, StringLength);
    case SQL_OJ_CAPABILITIES:
        return set_uinteger_info(
            SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL |
            SQL_OJ_NESTED | SQL_OJ_NOT_ORDERED,
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
        return set_string_info("Y", InfoValue, BufferLength, StringLength);

    case SQL_BATCH_SUPPORT:
    case SQL_BATCH_ROW_COUNT:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_PARAM_ARRAY_SELECTS:
        return set_uinteger_info(SQL_PAS_NO_SELECT, InfoValue, StringLength);

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

    case SQL_DESCRIBE_PARAMETER:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_INTEGRITY:
        return set_string_info("N", InfoValue, BufferLength, StringLength);

    case SQL_MAX_INDEX_SIZE:
        return set_uinteger_info(0, InfoValue, StringLength);

    case SQL_NUMERIC_FUNCTIONS + 1000: /* placeholder to avoid empty default */
    default:
        /* Return empty/zero for unknown info types */
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
        return SQL_SUCCESS;
    }

    /* Single function query */
    switch (FunctionId) {
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
        *Supported = SQL_TRUE;
        break;
    default:
        *Supported = SQL_FALSE;
        break;
    }

    return SQL_SUCCESS;
}
