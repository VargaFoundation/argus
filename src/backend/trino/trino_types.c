#include "trino_internal.h"
#include "argus/compat.h"
#include <string.h>

/* ── Map Trino type name string -> ODBC SQL type ─────────────── */

SQLSMALLINT trino_type_to_sql_type(const char *trino_type)
{
    if (!trino_type) return SQL_VARCHAR;

    if (strcasecmp(trino_type, "boolean") == 0)      return SQL_BIT;
    if (strcasecmp(trino_type, "tinyint") == 0)      return SQL_TINYINT;
    if (strcasecmp(trino_type, "smallint") == 0)     return SQL_SMALLINT;
    if (strcasecmp(trino_type, "integer") == 0)      return SQL_INTEGER;
    if (strcasecmp(trino_type, "int") == 0)          return SQL_INTEGER;
    if (strcasecmp(trino_type, "bigint") == 0)       return SQL_BIGINT;
    if (strcasecmp(trino_type, "real") == 0)         return SQL_REAL;
    if (strcasecmp(trino_type, "double") == 0)       return SQL_DOUBLE;
    if (strcasecmp(trino_type, "varchar") == 0)      return SQL_VARCHAR;
    if (strcasecmp(trino_type, "char") == 0)         return SQL_CHAR;
    if (strcasecmp(trino_type, "varbinary") == 0)    return SQL_VARBINARY;
    if (strcasecmp(trino_type, "date") == 0)         return SQL_TYPE_DATE;
    if (strcasecmp(trino_type, "timestamp") == 0)    return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(trino_type, "time") == 0)         return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(trino_type, "decimal") == 0)      return SQL_DECIMAL;
    if (strcasecmp(trino_type, "json") == 0)         return SQL_VARCHAR;
    if (strcasecmp(trino_type, "uuid") == 0)         return SQL_VARCHAR;
    if (strcasecmp(trino_type, "ipaddress") == 0)    return SQL_VARCHAR;

    /* Parameterized types: varchar(n), char(n), decimal(p,s), timestamp(p) */
    if (strncasecmp(trino_type, "varchar", 7) == 0)   return SQL_VARCHAR;
    if (strncasecmp(trino_type, "char", 4) == 0)      return SQL_CHAR;
    if (strncasecmp(trino_type, "decimal", 7) == 0)   return SQL_DECIMAL;
    if (strncasecmp(trino_type, "timestamp", 9) == 0) return SQL_TYPE_TIMESTAMP;
    if (strncasecmp(trino_type, "time", 4) == 0)      return SQL_TYPE_TIMESTAMP;
    if (strncasecmp(trino_type, "varbinary", 9) == 0) return SQL_VARBINARY;

    /* Complex types -> VARCHAR */
    if (strncasecmp(trino_type, "array", 5) == 0)  return SQL_VARCHAR;
    if (strncasecmp(trino_type, "map", 3) == 0)    return SQL_VARCHAR;
    if (strncasecmp(trino_type, "row", 3) == 0)    return SQL_VARCHAR;

    return SQL_VARCHAR;  /* Default fallback */
}

/* ── Column size (precision/display width) for SQL type ──────── */

SQLULEN trino_type_column_size(SQLSMALLINT sql_type)
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

SQLSMALLINT trino_type_decimal_digits(SQLSMALLINT sql_type)
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
