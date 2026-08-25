/*
 * hs2_types.c — shared HiveServer2-family type mapping (see hs2_types.h).
 * The union of what Hive and Impala each mapped before the merge; entries a
 * given engine never emits are harmless.
 */
#include "hs2_types.h"

#include <string.h>
#include <strings.h>

SQLSMALLINT argus_hs2_type_to_sql_type(const char *type_name)
{
    if (!type_name) return SQL_VARCHAR;

    if (strcasecmp(type_name, "BOOLEAN") == 0)    return SQL_BIT;
    if (strcasecmp(type_name, "TINYINT") == 0)    return SQL_TINYINT;
    if (strcasecmp(type_name, "SMALLINT") == 0)   return SQL_SMALLINT;
    if (strcasecmp(type_name, "INT") == 0)         return SQL_INTEGER;
    if (strcasecmp(type_name, "INTEGER") == 0)     return SQL_INTEGER;
    if (strcasecmp(type_name, "BIGINT") == 0)      return SQL_BIGINT;
    if (strcasecmp(type_name, "FLOAT") == 0)       return SQL_FLOAT;
    if (strcasecmp(type_name, "DOUBLE") == 0)      return SQL_DOUBLE;
    if (strcasecmp(type_name, "REAL") == 0)        return SQL_DOUBLE;   /* Impala */
    if (strcasecmp(type_name, "STRING") == 0)      return SQL_VARCHAR;
    if (strcasecmp(type_name, "VARCHAR") == 0)     return SQL_VARCHAR;
    if (strcasecmp(type_name, "CHAR") == 0)        return SQL_CHAR;
    if (strcasecmp(type_name, "TIMESTAMP") == 0)   return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(type_name, "DATE") == 0)        return SQL_TYPE_DATE;
    if (strcasecmp(type_name, "BINARY") == 0)      return SQL_BINARY;
    if (strcasecmp(type_name, "DECIMAL") == 0)     return SQL_DECIMAL;
    if (strcasecmp(type_name, "INTERVAL_YEAR_MONTH") == 0) return SQL_VARCHAR;  /* Hive */
    if (strcasecmp(type_name, "INTERVAL_DAY_TIME") == 0)   return SQL_VARCHAR;  /* Hive */

    /* Complex types (ARRAY, MAP, STRUCT, UNIONTYPE) -> VARCHAR */
    if (strncasecmp(type_name, "ARRAY", 5) == 0)   return SQL_VARCHAR;
    if (strncasecmp(type_name, "MAP", 3) == 0)     return SQL_VARCHAR;
    if (strncasecmp(type_name, "STRUCT", 6) == 0)  return SQL_VARCHAR;
    if (strncasecmp(type_name, "UNION", 5) == 0)   return SQL_VARCHAR;

    return SQL_VARCHAR;
}

SQLULEN argus_hs2_type_column_size(SQLSMALLINT sql_type)
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

SQLSMALLINT argus_hs2_type_decimal_digits(SQLSMALLINT sql_type)
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
