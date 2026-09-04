/* SPDX-License-Identifier: Apache-2.0 */
/*
 * hive_types.c — Hive type mapping, delegating to the shared HiveServer2
 * family mapper (../hs2_types.c). Kept as a thin wrapper so the backend's
 * public symbol names stay stable.
 */
#include "hive_internal.h"
#include "../hs2_types.h"

SQLSMALLINT hive_type_to_sql_type(const char *hive_type)
{
    return argus_hs2_type_to_sql_type(hive_type);
}

SQLULEN hive_type_column_size(SQLSMALLINT sql_type)
{
    return argus_hs2_type_column_size(sql_type);
}

SQLSMALLINT hive_type_decimal_digits(SQLSMALLINT sql_type)
{
    return argus_hs2_type_decimal_digits(sql_type);
}
