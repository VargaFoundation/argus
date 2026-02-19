#include "kudu_internal.h"
#include <string.h>

/* ── Map Kudu DataType integer ID -> ODBC SQL type ───────────── */

SQLSMALLINT kudu_type_to_sql_type(int kudu_type)
{
    switch (kudu_type) {
    case KUDU_TYPE_INT8:             return SQL_TINYINT;
    case KUDU_TYPE_INT16:            return SQL_SMALLINT;
    case KUDU_TYPE_INT32:            return SQL_INTEGER;
    case KUDU_TYPE_INT64:            return SQL_BIGINT;
    case KUDU_TYPE_FLOAT:            return SQL_REAL;
    case KUDU_TYPE_DOUBLE:           return SQL_DOUBLE;
    case KUDU_TYPE_BOOL:             return SQL_BIT;
    case KUDU_TYPE_STRING:           return SQL_VARCHAR;
    case KUDU_TYPE_BINARY:           return SQL_VARBINARY;
    case KUDU_TYPE_UNIXTIME_MICROS:  return SQL_TYPE_TIMESTAMP;
    case KUDU_TYPE_DECIMAL:          return SQL_DECIMAL;
    case KUDU_TYPE_VARCHAR:          return SQL_VARCHAR;
    case KUDU_TYPE_DATE:             return SQL_TYPE_DATE;
    default:                         return SQL_VARCHAR;
    }
}

/* ── Map Kudu type name string -> ODBC SQL type ──────────────── */

SQLSMALLINT kudu_type_name_to_sql_type(const char *type_name)
{
    if (!type_name) return SQL_VARCHAR;

    if (strcasecmp(type_name, "INT8") == 0 ||
        strcasecmp(type_name, "TINYINT") == 0)       return SQL_TINYINT;
    if (strcasecmp(type_name, "INT16") == 0 ||
        strcasecmp(type_name, "SMALLINT") == 0)      return SQL_SMALLINT;
    if (strcasecmp(type_name, "INT32") == 0 ||
        strcasecmp(type_name, "INT") == 0 ||
        strcasecmp(type_name, "INTEGER") == 0)       return SQL_INTEGER;
    if (strcasecmp(type_name, "INT64") == 0 ||
        strcasecmp(type_name, "BIGINT") == 0)        return SQL_BIGINT;
    if (strcasecmp(type_name, "FLOAT") == 0)         return SQL_REAL;
    if (strcasecmp(type_name, "DOUBLE") == 0)        return SQL_DOUBLE;
    if (strcasecmp(type_name, "BOOL") == 0 ||
        strcasecmp(type_name, "BOOLEAN") == 0)       return SQL_BIT;
    if (strcasecmp(type_name, "STRING") == 0)        return SQL_VARCHAR;
    if (strcasecmp(type_name, "BINARY") == 0)        return SQL_VARBINARY;
    if (strcasecmp(type_name, "UNIXTIME_MICROS") == 0 ||
        strcasecmp(type_name, "TIMESTAMP") == 0)     return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(type_name, "DECIMAL") == 0)       return SQL_DECIMAL;
    if (strcasecmp(type_name, "VARCHAR") == 0)       return SQL_VARCHAR;
    if (strcasecmp(type_name, "DATE") == 0)          return SQL_TYPE_DATE;

    return SQL_VARCHAR;  /* Default fallback */
}

/* ── Column size (precision/display width) for SQL type ──────── */

SQLULEN kudu_type_column_size(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_BIT:            return 1;
    case SQL_TINYINT:        return 3;
    case SQL_SMALLINT:       return 5;
    case SQL_INTEGER:        return 10;
    case SQL_BIGINT:         return 19;
    case SQL_REAL:           return 7;
    case SQL_FLOAT:          return 15;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 38;
    case SQL_CHAR:           return 255;
    case SQL_VARCHAR:        return 65535;
    case SQL_LONGVARCHAR:    return 2147483647;
    case SQL_VARBINARY:      return 65535;
    case SQL_BINARY:         return 65535;
    case SQL_TYPE_DATE:      return 10;    /* YYYY-MM-DD */
    case SQL_TYPE_TIMESTAMP: return 29;    /* YYYY-MM-DD HH:MM:SS.ffffff */
    default:                 return 65535;
    }
}

/* ── Decimal digits (scale) for SQL type ─────────────────────── */

SQLSMALLINT kudu_type_decimal_digits(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_REAL:           return 7;
    case SQL_FLOAT:          return 15;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 18;
    case SQL_TYPE_TIMESTAMP: return 6;   /* Kudu uses microseconds */
    default:                 return 0;
    }
}

/* ── Convert Kudu type ID to type name string ────────────────── */

const char *kudu_type_id_to_name(int kudu_type)
{
    switch (kudu_type) {
    case KUDU_TYPE_INT8:             return "INT8";
    case KUDU_TYPE_INT16:            return "INT16";
    case KUDU_TYPE_INT32:            return "INT32";
    case KUDU_TYPE_INT64:            return "INT64";
    case KUDU_TYPE_FLOAT:            return "FLOAT";
    case KUDU_TYPE_DOUBLE:           return "DOUBLE";
    case KUDU_TYPE_BOOL:             return "BOOL";
    case KUDU_TYPE_STRING:           return "STRING";
    case KUDU_TYPE_BINARY:           return "BINARY";
    case KUDU_TYPE_UNIXTIME_MICROS:  return "UNIXTIME_MICROS";
    case KUDU_TYPE_DECIMAL:          return "DECIMAL";
    case KUDU_TYPE_VARCHAR:          return "VARCHAR";
    case KUDU_TYPE_DATE:             return "DATE";
    default:                         return "UNKNOWN";
    }
}
