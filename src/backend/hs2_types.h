/*
 * hs2_types.h — shared type mapping for the HiveServer2 protocol family
 * (Hive, Impala; also Spark Thrift Server and Flink SQL Gateway through the
 * hive backend). hive_types.c and impala_types.c were byte-for-byte
 * near-copies drifting independently (Impala had gained REAL, Hive had
 * INTERVAL_*); this is the single superset both now delegate to.
 */
#ifndef ARGUS_HS2_TYPES_H
#define ARGUS_HS2_TYPES_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include "argus/types.h"

/* Engine type name (e.g. "BIGINT", "ARRAY<...>") -> ODBC SQL type. */
SQLSMALLINT argus_hs2_type_to_sql_type(const char *type_name);

/* Column size (precision / display width) for an ODBC SQL type. */
SQLULEN argus_hs2_type_column_size(SQLSMALLINT sql_type);

/* Decimal digits (scale) for an ODBC SQL type. */
SQLSMALLINT argus_hs2_type_decimal_digits(SQLSMALLINT sql_type);

/* Fill `col` from one TColumnDesc of a GetResultSetMetadata reply: the
 * name, the ODBC type of the primitive type id, and the size and scale the
 * type qualifiers carry — DECIMAL(p,s) is reported as p/s and CHAR(n) /
 * VARCHAR(n) as n, instead of the family-wide maximums. A zoned timestamp
 * (Hive's TIMESTAMP WITH LOCAL TIME ZONE) is described as SQL_VARCHAR so the
 * zone the engine prints is not silently dropped; SQL_C_TYPE_TIMESTAMP still
 * converts it. */
struct _TColumnDesc;
void argus_hs2_describe_column(struct _TColumnDesc *cd, argus_column_desc_t *col);

#endif /* ARGUS_HS2_TYPES_H */
