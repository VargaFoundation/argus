#include "impala_internal.h"
#include "argus/compat.h"
#include <string.h>

/* ── Map Impala type name string -> ODBC SQL type ────────────── */

SQLSMALLINT impala_type_to_sql_type(const char *impala_type)
{
    if (!impala_type) return SQL_VARCHAR;

    if (strcasecmp(impala_type, "BOOLEAN") == 0)    return SQL_BIT;
    if (strcasecmp(impala_type, "TINYINT") == 0)    return SQL_TINYINT;
    if (strcasecmp(impala_type, "SMALLINT") == 0)   return SQL_SMALLINT;
    if (strcasecmp(impala_type, "INT") == 0)         return SQL_INTEGER;
    if (strcasecmp(impala_type, "INTEGER") == 0)     return SQL_INTEGER;
    if (strcasecmp(impala_type, "BIGINT") == 0)      return SQL_BIGINT;
    if (strcasecmp(impala_type, "FLOAT") == 0)       return SQL_FLOAT;
    if (strcasecmp(impala_type, "DOUBLE") == 0)      return SQL_DOUBLE;
    if (strcasecmp(impala_type, "REAL") == 0)        return SQL_DOUBLE;
    if (strcasecmp(impala_type, "STRING") == 0)      return SQL_VARCHAR;
    if (strcasecmp(impala_type, "VARCHAR") == 0)     return SQL_VARCHAR;
    if (strcasecmp(impala_type, "CHAR") == 0)        return SQL_CHAR;
    if (strcasecmp(impala_type, "TIMESTAMP") == 0)   return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(impala_type, "DATE") == 0)        return SQL_TYPE_DATE;
    if (strcasecmp(impala_type, "BINARY") == 0)      return SQL_BINARY;
    if (strcasecmp(impala_type, "DECIMAL") == 0)     return SQL_DECIMAL;

    /* Complex types -> VARCHAR */
    if (strncasecmp(impala_type, "ARRAY", 5) == 0)   return SQL_VARCHAR;
    if (strncasecmp(impala_type, "MAP", 3) == 0)     return SQL_VARCHAR;
    if (strncasecmp(impala_type, "STRUCT", 6) == 0)  return SQL_VARCHAR;

    return SQL_VARCHAR;  /* Default fallback */
}

/* ── Column size (precision/display width) for SQL type ──────── */

SQLULEN impala_type_column_size(SQLSMALLINT sql_type)
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

SQLSMALLINT impala_type_decimal_digits(SQLSMALLINT sql_type)
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
