/* SPDX-License-Identifier: Apache-2.0 */
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
#include <stdbool.h>
#include <stddef.h>

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

/* The server's version string, by GetInfo(CLI_DBMS_VER) on the open
 * session -- what backs SQLGetInfo(SQL_DBMS_VER). HiveServer2 answers
 * "3.1.3"; Impala answers "impalad version 4.4.0-RELEASE RELEASE (build
 * ...)". False when the server does not answer or answers nothing, so the
 * ODBC layer keeps reporting "unknown" rather than a version this driver
 * made up. One round trip: callers cache the result per connection. */
struct _TCLIServiceIf;
struct _TSessionHandle;
bool argus_hs2_dbms_version(struct _TCLIServiceIf *client,
                            struct _TSessionHandle *session,
                            char *out, size_t outlen);

#endif /* ARGUS_HS2_TYPES_H */
