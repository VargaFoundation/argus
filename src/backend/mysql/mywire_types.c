#include "mywire_internal.h"

/* ── Map a MySQL field type to an ODBC SQL type ──────────────── */

SQLSMALLINT mywire_field_to_sql_type(enum enum_field_types type,
                                     unsigned int flags)
{
    switch (type) {
    case MYSQL_TYPE_TINY:        return SQL_TINYINT;
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_YEAR:        return SQL_SMALLINT;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:        return SQL_INTEGER;
    case MYSQL_TYPE_LONGLONG:    return SQL_BIGINT;
    case MYSQL_TYPE_FLOAT:       return SQL_REAL;
    case MYSQL_TYPE_DOUBLE:      return SQL_DOUBLE;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:  return SQL_DECIMAL;
    case MYSQL_TYPE_BIT:         return SQL_BIT;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:     return SQL_TYPE_DATE;
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:       return SQL_TYPE_TIME;
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:  return SQL_TYPE_TIMESTAMP;
    case MYSQL_TYPE_STRING:
        return (flags & BINARY_FLAG) ? SQL_BINARY : SQL_CHAR;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR:
        return (flags & BINARY_FLAG) ? SQL_VARBINARY : SQL_VARCHAR;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
        return (flags & BINARY_FLAG) ? SQL_LONGVARBINARY : SQL_LONGVARCHAR;
    case MYSQL_TYPE_GEOMETRY:    return SQL_LONGVARBINARY;
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:         return SQL_VARCHAR;
    case MYSQL_TYPE_NULL:        return SQL_VARCHAR;
    default:                     return SQL_VARCHAR;
    }
}

/* ── Column display size for an ODBC SQL type ────────────────── */

SQLULEN mywire_column_size(SQLSMALLINT sql_type, unsigned long field_length)
{
    switch (sql_type) {
    case SQL_BIT:            return 1;
    case SQL_TINYINT:        return 3;
    case SQL_SMALLINT:       return 5;
    case SQL_INTEGER:        return 10;
    case SQL_BIGINT:         return 19;
    case SQL_REAL:           return 7;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL:        return field_length > 0 ? (SQLULEN)field_length : 38;
    case SQL_TYPE_DATE:      return 10;
    case SQL_TYPE_TIME:      return 8;
    case SQL_TYPE_TIMESTAMP: return 19;
    default:                 return field_length > 0 ? (SQLULEN)field_length : 255;
    }
}

/* ── Decimal digits (scale) for an ODBC SQL type ─────────────── */

SQLSMALLINT mywire_decimal_digits(SQLSMALLINT sql_type, unsigned int decimals)
{
    switch (sql_type) {
    case SQL_DECIMAL:        return (SQLSMALLINT)decimals;
    case SQL_TYPE_TIME:
    case SQL_TYPE_TIMESTAMP: return decimals <= 6 ? (SQLSMALLINT)decimals : 6;
    default:                 return 0;
    }
}
