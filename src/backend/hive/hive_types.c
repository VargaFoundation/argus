#include "hive_internal.h"
#include <string.h>
#include <strings.h>

/* ── Map Hive type name string -> ODBC SQL type ──────────────── */

SQLSMALLINT hive_type_to_sql_type(const char *hive_type)
{
    if (!hive_type) return SQL_VARCHAR;

    if (strcasecmp(hive_type, "BOOLEAN") == 0)    return SQL_BIT;
    if (strcasecmp(hive_type, "TINYINT") == 0)    return SQL_TINYINT;
    if (strcasecmp(hive_type, "SMALLINT") == 0)   return SQL_SMALLINT;
    if (strcasecmp(hive_type, "INT") == 0)         return SQL_INTEGER;
    if (strcasecmp(hive_type, "INTEGER") == 0)     return SQL_INTEGER;
    if (strcasecmp(hive_type, "BIGINT") == 0)      return SQL_BIGINT;
    if (strcasecmp(hive_type, "FLOAT") == 0)       return SQL_FLOAT;
    if (strcasecmp(hive_type, "DOUBLE") == 0)      return SQL_DOUBLE;
    if (strcasecmp(hive_type, "STRING") == 0)      return SQL_VARCHAR;
    if (strcasecmp(hive_type, "VARCHAR") == 0)     return SQL_VARCHAR;
    if (strcasecmp(hive_type, "CHAR") == 0)        return SQL_CHAR;
    if (strcasecmp(hive_type, "TIMESTAMP") == 0)   return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(hive_type, "DATE") == 0)        return SQL_TYPE_DATE;
    if (strcasecmp(hive_type, "BINARY") == 0)      return SQL_BINARY;
    if (strcasecmp(hive_type, "DECIMAL") == 0)     return SQL_DECIMAL;
    if (strcasecmp(hive_type, "INTERVAL_YEAR_MONTH") == 0) return SQL_VARCHAR;
    if (strcasecmp(hive_type, "INTERVAL_DAY_TIME") == 0)   return SQL_VARCHAR;

    /* Complex types (ARRAY, MAP, STRUCT, UNIONTYPE) -> VARCHAR */
    if (strncasecmp(hive_type, "ARRAY", 5) == 0)   return SQL_VARCHAR;
    if (strncasecmp(hive_type, "MAP", 3) == 0)     return SQL_VARCHAR;
    if (strncasecmp(hive_type, "STRUCT", 6) == 0)  return SQL_VARCHAR;
    if (strncasecmp(hive_type, "UNION", 5) == 0)   return SQL_VARCHAR;

    return SQL_VARCHAR;  /* Default fallback */
}

/* ── Column size (precision/display width) for SQL type ──────── */

SQLULEN hive_type_column_size(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_BIT:            return 1;
    case SQL_TINYINT:        return 3;
    case SQL_SMALLINT:       return 5;
    case SQL_INTEGER:        return 10;
    case SQL_BIGINT:         return 19;
    case SQL_FLOAT:          return 7;
    case SQL_REAL:           return 7;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 38;
    case SQL_CHAR:           return 255;
    case SQL_VARCHAR:        return 65535;
    case SQL_LONGVARCHAR:    return 2147483647;
    case SQL_BINARY:         return 65535;
    case SQL_TYPE_DATE:      return 10;    /* YYYY-MM-DD */
    case SQL_TYPE_TIMESTAMP: return 29;    /* YYYY-MM-DD HH:MM:SS.fffffffff */
    default:                 return 65535;
    }
}

/* ── Decimal digits (scale) for SQL type ─────────────────────── */

SQLSMALLINT hive_type_decimal_digits(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_FLOAT:          return 7;
    case SQL_REAL:           return 7;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 18;
    case SQL_TYPE_TIMESTAMP: return 9;
    default:                 return 0;
    }
}
