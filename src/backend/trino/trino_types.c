/* SPDX-License-Identifier: Apache-2.0 */
#include "trino_internal.h"
#include "argus/compat.h"
#include <string.h>
#include <stdlib.h>

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
    if (strcasecmp(trino_type, "time") == 0)         return SQL_TYPE_TIME;
    if (strcasecmp(trino_type, "decimal") == 0)      return SQL_DECIMAL;
    if (strcasecmp(trino_type, "json") == 0)         return SQL_VARCHAR;
    if (strcasecmp(trino_type, "uuid") == 0)         return SQL_GUID;
    if (strcasecmp(trino_type, "ipaddress") == 0)    return SQL_VARCHAR;

    /* Parameterized types: varchar(n), char(n), decimal(p,s), timestamp(p) */
    if (strncasecmp(trino_type, "varchar", 7) == 0)   return SQL_VARCHAR;
    if (strncasecmp(trino_type, "char", 4) == 0)      return SQL_CHAR;
    if (strncasecmp(trino_type, "decimal", 7) == 0)   return SQL_DECIMAL;
    if (strncasecmp(trino_type, "timestamp", 9) == 0) return SQL_TYPE_TIMESTAMP;
    if (strncasecmp(trino_type, "time", 4) == 0)      return SQL_TYPE_TIME;
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
    case SQL_TYPE_TIME:      return 8;     /* HH:MM:SS */
    case SQL_TYPE_TIMESTAMP: return 29;    /* YYYY-MM-DD HH:MM:SS.fffffffff */
    case SQL_GUID:           return 36;    /* xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx */
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
    case SQL_TYPE_TIME:      return 3;
    case SQL_TYPE_TIMESTAMP: return 9;
    default:                 return 0;
    }
}

/* ── Parameters carried by the type name ─────────────────────── */

/* Trino names a column's type in full — decimal(18,4), varchar(20), char(3),
 * timestamp(6) — so the precision and scale are there to be read instead of
 * reported as the family maximums (38/18, 65535), which is what a BI tool
 * sizes its columns from. A bare name leaves both as the caller set them.
 * The parameters of the timestamp family are its fractional-second digits. */
void trino_type_apply_params(const char *trino_type, SQLULEN *column_size,
                             SQLSMALLINT *decimal_digits)
{
    if (!trino_type) return;
    const char *open = strchr(trino_type, '(');
    if (!open) return;

    char *end = NULL;
    long first = strtol(open + 1, &end, 10);
    if (end == open + 1 || first < 0) return;         /* e.g. array(varchar) */

    long second = -1;
    if (end && *end == ',') {
        char *e2 = NULL;
        long v = strtol(end + 1, &e2, 10);
        if (e2 != end + 1 && v >= 0) second = v;
    }

    if (strncasecmp(trino_type, "decimal", 7) == 0) {
        if (column_size) *column_size = (SQLULEN)first;
        if (decimal_digits) *decimal_digits = (SQLSMALLINT)(second < 0 ? 0 : second);
        return;
    }
    if (strncasecmp(trino_type, "varchar", 7) == 0 ||
        strncasecmp(trino_type, "char", 4) == 0) {
        if (column_size) *column_size = (SQLULEN)first;
        return;
    }
    if (strncasecmp(trino_type, "timestamp", 9) == 0) {
        /* "YYYY-MM-DD HH:MM:SS" plus the '.' and the fractional digits. */
        if (decimal_digits) *decimal_digits = (SQLSMALLINT)first;
        if (column_size) *column_size = (SQLULEN)(first > 0 ? 20 + first : 19);
        return;
    }
    if (strncasecmp(trino_type, "time", 4) == 0) {
        /* "HH:MM:SS" plus the '.' and the fractional digits. */
        if (decimal_digits) *decimal_digits = (SQLSMALLINT)first;
        if (column_size) *column_size = (SQLULEN)(first > 0 ? 9 + first : 8);
    }
}
