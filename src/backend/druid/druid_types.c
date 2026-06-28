#include "druid_internal.h"
#include <string.h>

/* ── Druid SQL type name → ODBC SQL type ─────────────────────── */

SQLSMALLINT druid_type_to_sql_type(const char *t)
{
    if (!t) return SQL_VARCHAR;

    if (strcmp(t, "BOOLEAN") == 0)   return SQL_BIT;
    if (strcmp(t, "TINYINT") == 0)   return SQL_TINYINT;
    if (strcmp(t, "SMALLINT") == 0)  return SQL_SMALLINT;
    if (strcmp(t, "INTEGER") == 0)   return SQL_INTEGER;
    if (strcmp(t, "BIGINT") == 0)    return SQL_BIGINT;
    if (strcmp(t, "FLOAT") == 0)     return SQL_REAL;
    if (strcmp(t, "REAL") == 0)      return SQL_REAL;
    if (strcmp(t, "DOUBLE") == 0)    return SQL_DOUBLE;
    if (strcmp(t, "DECIMAL") == 0)   return SQL_DECIMAL;
    if (strcmp(t, "TIMESTAMP") == 0) return SQL_TYPE_TIMESTAMP;
    if (strcmp(t, "DATE") == 0)      return SQL_TYPE_DATE;
    if (strcmp(t, "CHAR") == 0)      return SQL_CHAR;
    if (strncmp(t, "VARCHAR", 7) == 0) return SQL_VARCHAR;

    /* complex / array / other → text */
    return SQL_VARCHAR;
}

SQLULEN druid_type_column_size(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_BIT:            return 1;
    case SQL_TINYINT:        return 3;
    case SQL_SMALLINT:       return 5;
    case SQL_INTEGER:        return 10;
    case SQL_BIGINT:         return 19;
    case SQL_REAL:           return 7;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 38;
    case SQL_TYPE_DATE:      return 10;
    case SQL_TYPE_TIMESTAMP: return 23;
    default:                 return 255;
    }
}
