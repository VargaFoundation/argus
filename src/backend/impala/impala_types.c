/*
 * impala_types.c — Impala type mapping, delegating to the shared HiveServer2
 * family mapper (../hs2_types.c). Kept as a thin wrapper so the backend's
 * public symbol names stay stable.
 */
#include "impala_internal.h"
#include "../hs2_types.h"

SQLSMALLINT impala_type_to_sql_type(const char *impala_type)
{
    return argus_hs2_type_to_sql_type(impala_type);
}

SQLULEN impala_type_column_size(SQLSMALLINT sql_type)
{
    return argus_hs2_type_column_size(sql_type);
}

SQLSMALLINT impala_type_decimal_digits(SQLSMALLINT sql_type)
{
    return argus_hs2_type_decimal_digits(sql_type);
}
