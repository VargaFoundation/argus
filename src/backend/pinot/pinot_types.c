#include "pinot_internal.h"
#include <string.h>

/* ── Pinot columnDataType string → ODBC SQL type ─────────────── */

SQLSMALLINT pinot_type_to_sql_type(const char *t)
{
    if (!t) return SQL_VARCHAR;

    if (strcmp(t, "INT") == 0)         return SQL_INTEGER;
    if (strcmp(t, "LONG") == 0)        return SQL_BIGINT;
    if (strcmp(t, "FLOAT") == 0)       return SQL_REAL;
    if (strcmp(t, "DOUBLE") == 0)      return SQL_DOUBLE;
    if (strcmp(t, "BIG_DECIMAL") == 0) return SQL_DECIMAL;
    if (strcmp(t, "BOOLEAN") == 0)     return SQL_BIT;
    if (strcmp(t, "TIMESTAMP") == 0)   return SQL_TYPE_TIMESTAMP;
    if (strcmp(t, "STRING") == 0)      return SQL_VARCHAR;
    if (strcmp(t, "JSON") == 0)        return SQL_LONGVARCHAR;
    if (strcmp(t, "BYTES") == 0)       return SQL_VARBINARY;

    /* Multi-value columns (INT_ARRAY, STRING_ARRAY, ...) are surfaced as their
     * JSON text form. */
    return SQL_VARCHAR;
}

SQLULEN pinot_type_column_size(SQLSMALLINT sql_type)
{
    switch (sql_type) {
    case SQL_BIT:            return 1;
    case SQL_INTEGER:        return 10;
    case SQL_BIGINT:         return 19;
    case SQL_REAL:           return 7;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return 38;
    case SQL_TYPE_TIMESTAMP: return 19;
    default:                 return 255;
    }
}
