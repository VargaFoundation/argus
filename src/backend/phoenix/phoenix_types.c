#include "phoenix_internal.h"
#include "argus/compat.h"
#include <string.h>

/* ── Map Phoenix/HBase type name string -> ODBC SQL type ─────── */

SQLSMALLINT phoenix_type_to_sql_type(const char *phoenix_type)
{
    if (!phoenix_type) return SQL_VARCHAR;

    /* Exact matches (Avatica reports Java SQL type names) */
    if (strcasecmp(phoenix_type, "BOOLEAN") == 0)       return SQL_BIT;
    if (strcasecmp(phoenix_type, "TINYINT") == 0)       return SQL_TINYINT;
    if (strcasecmp(phoenix_type, "SMALLINT") == 0)      return SQL_SMALLINT;
    if (strcasecmp(phoenix_type, "INTEGER") == 0)       return SQL_INTEGER;
    if (strcasecmp(phoenix_type, "INT") == 0)           return SQL_INTEGER;
    if (strcasecmp(phoenix_type, "BIGINT") == 0)        return SQL_BIGINT;
    if (strcasecmp(phoenix_type, "FLOAT") == 0)         return SQL_REAL;
    if (strcasecmp(phoenix_type, "REAL") == 0)          return SQL_REAL;
    if (strcasecmp(phoenix_type, "DOUBLE") == 0)        return SQL_DOUBLE;
    if (strcasecmp(phoenix_type, "VARCHAR") == 0)       return SQL_VARCHAR;
    if (strcasecmp(phoenix_type, "CHAR") == 0)          return SQL_CHAR;
    if (strcasecmp(phoenix_type, "CHARACTER") == 0)     return SQL_CHAR;
    if (strcasecmp(phoenix_type, "VARBINARY") == 0)     return SQL_VARBINARY;
    if (strcasecmp(phoenix_type, "BINARY") == 0)        return SQL_BINARY;
    if (strcasecmp(phoenix_type, "DATE") == 0)          return SQL_TYPE_DATE;
    if (strcasecmp(phoenix_type, "TIMESTAMP") == 0)     return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(phoenix_type, "TIME") == 0)          return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(phoenix_type, "DECIMAL") == 0)       return SQL_DECIMAL;
    if (strcasecmp(phoenix_type, "NUMERIC") == 0)       return SQL_DECIMAL;

    /* Phoenix-specific types */
    if (strcasecmp(phoenix_type, "UNSIGNED_TINYINT") == 0)  return SQL_TINYINT;
    if (strcasecmp(phoenix_type, "UNSIGNED_SMALLINT") == 0) return SQL_SMALLINT;
    if (strcasecmp(phoenix_type, "UNSIGNED_INT") == 0)      return SQL_INTEGER;
    if (strcasecmp(phoenix_type, "UNSIGNED_LONG") == 0)     return SQL_BIGINT;
    if (strcasecmp(phoenix_type, "UNSIGNED_FLOAT") == 0)    return SQL_REAL;
    if (strcasecmp(phoenix_type, "UNSIGNED_DOUBLE") == 0)   return SQL_DOUBLE;
    if (strcasecmp(phoenix_type, "UNSIGNED_DATE") == 0)     return SQL_TYPE_DATE;
    if (strcasecmp(phoenix_type, "UNSIGNED_TIME") == 0)     return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(phoenix_type, "UNSIGNED_TIMESTAMP") == 0) return SQL_TYPE_TIMESTAMP;

    /* Avatica may report Java SQL type IDs as strings */
    if (strcasecmp(phoenix_type, "ARRAY") == 0)         return SQL_VARCHAR;

    /* Parameterized types */
    if (strncasecmp(phoenix_type, "VARCHAR", 7) == 0)    return SQL_VARCHAR;
    if (strncasecmp(phoenix_type, "CHAR", 4) == 0)       return SQL_CHAR;
    if (strncasecmp(phoenix_type, "DECIMAL", 7) == 0)    return SQL_DECIMAL;
    if (strncasecmp(phoenix_type, "NUMERIC", 7) == 0)    return SQL_DECIMAL;
    if (strncasecmp(phoenix_type, "TIMESTAMP", 9) == 0)  return SQL_TYPE_TIMESTAMP;
    if (strncasecmp(phoenix_type, "VARBINARY", 9) == 0)  return SQL_VARBINARY;
    if (strncasecmp(phoenix_type, "BINARY", 6) == 0)     return SQL_BINARY;

    return SQL_VARCHAR;  /* Default fallback */
}

/* ── Column size (precision/display width) for SQL type ──────── */

SQLULEN phoenix_type_column_size(SQLSMALLINT sql_type)
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
    case SQL_TYPE_TIMESTAMP: return 29;    /* YYYY-MM-DD HH:MM:SS.fffffffff */
    default:                 return 65535;
    }
}

/* ── Decimal digits (scale) for SQL type ─────────────────────── */

SQLSMALLINT phoenix_type_decimal_digits(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_REAL:           return 7;
    case SQL_FLOAT:          return 15;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 18;
    case SQL_TYPE_TIMESTAMP: return 9;
    default:                 return 0;
    }
}
