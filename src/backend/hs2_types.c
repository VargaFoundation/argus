/* SPDX-License-Identifier: Apache-2.0 */
/*
 * hs2_types.c — shared HiveServer2-family type mapping (see hs2_types.h).
 * The union of what Hive and Impala each mapped before the merge; entries a
 * given engine never emits are harmless.
 */
#include "hs2_types.h"

#include <string.h>
#include <strings.h>

#include <glib-object.h>
#include "gen-c_glib/t_c_l_i_service_types.h"
#include "gen-c_glib/t_c_l_i_service.h"

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

/* ── Result-set metadata ─────────────────────────────────────── */

static SQLSMALLINT type_id_to_sql_type(TTypeId id)
{
    switch (id) {
    case T_TYPE_ID_BOOLEAN_TYPE:        return SQL_BIT;
    case T_TYPE_ID_TINYINT_TYPE:        return SQL_TINYINT;
    case T_TYPE_ID_SMALLINT_TYPE:       return SQL_SMALLINT;
    case T_TYPE_ID_INT_TYPE:            return SQL_INTEGER;
    case T_TYPE_ID_BIGINT_TYPE:         return SQL_BIGINT;
    case T_TYPE_ID_FLOAT_TYPE:          return SQL_FLOAT;
    case T_TYPE_ID_DOUBLE_TYPE:         return SQL_DOUBLE;
    case T_TYPE_ID_TIMESTAMP_TYPE:      return SQL_TYPE_TIMESTAMP;
    case T_TYPE_ID_BINARY_TYPE:         return SQL_BINARY;
    case T_TYPE_ID_DECIMAL_TYPE:        return SQL_DECIMAL;
    case T_TYPE_ID_DATE_TYPE:           return SQL_TYPE_DATE;
    case T_TYPE_ID_CHAR_TYPE:           return SQL_CHAR;
    case T_TYPE_ID_STRING_TYPE:
    case T_TYPE_ID_VARCHAR_TYPE:
    case T_TYPE_ID_TIMESTAMPLOCALTZ_TYPE:
    case T_TYPE_ID_INTERVAL_YEAR_MONTH_TYPE:
    case T_TYPE_ID_INTERVAL_DAY_TIME_TYPE:
    default:                            /* ARRAY, MAP, STRUCT, UNION, NULL */
        return SQL_VARCHAR;
    }
}

/* TCLIService's PRECISION / SCALE / CHARACTER_MAXIMUM_LENGTH qualifiers. */
static bool qualifier_i32(TTypeQualifiers *q, const char *key, gint32 *out)
{
    if (!q || !q->qualifiers) return false;
    TTypeQualifierValue *v = g_hash_table_lookup(q->qualifiers, key);
    if (!v || !v->__isset_i32Value) return false;
    *out = v->i32Value;
    return true;
}

void argus_hs2_describe_column(TColumnDesc *cd, argus_column_desc_t *col)
{
    memset(col, 0, sizeof(*col));
    if (cd->columnName) {
        strncpy((char *)col->name, cd->columnName, ARGUS_MAX_COLUMN_NAME - 1);
        col->name_len = (SQLSMALLINT)strlen((char *)col->name);
    }

    TTypeId type_id = T_TYPE_ID_STRING_TYPE;
    TTypeQualifiers *q = NULL;
    GPtrArray *types = cd->typeDesc ? cd->typeDesc->types : NULL;
    if (types && types->len > 0) {
        TTypeEntry *te = g_ptr_array_index(types, 0);
        if (te && te->__isset_primitiveEntry && te->primitiveEntry) {
            type_id = te->primitiveEntry->type;
            if (te->primitiveEntry->__isset_typeQualifiers)
                q = te->primitiveEntry->typeQualifiers;
        }
    }

    col->sql_type       = type_id_to_sql_type(type_id);
    col->column_size    = argus_hs2_type_column_size(col->sql_type);
    col->decimal_digits = argus_hs2_type_decimal_digits(col->sql_type);
    col->nullable       = SQL_NULLABLE_UNKNOWN;

    gint32 n;
    switch (type_id) {
    case T_TYPE_ID_DECIMAL_TYPE:
        if (qualifier_i32(q, "precision", &n) && n > 0)
            col->column_size = (SQLULEN)n;
        if (qualifier_i32(q, "scale", &n) && n >= 0)
            col->decimal_digits = (SQLSMALLINT)n;
        break;
    case T_TYPE_ID_CHAR_TYPE:
    case T_TYPE_ID_VARCHAR_TYPE:
        if (qualifier_i32(q, "characterMaximumLength", &n) && n > 0)
            col->column_size = (SQLULEN)n;
        break;
    default:
        break;
    }
}

bool argus_hs2_dbms_version(struct _TCLIServiceIf *client,
                            struct _TSessionHandle *session,
                            char *out, size_t outlen)
{
    if (!client || !session || !out || outlen == 0) return false;
    out[0] = '\0';

    TGetInfoReq *req = g_object_new(TYPE_T_GET_INFO_REQ,
                                    "sessionHandle", session,
                                    "infoType", T_GET_INFO_TYPE_CLI_DBMS_VER,
                                    NULL);
    /* The c_glib client reads the reply INTO the object the caller passes;
     * a NULL here is what thrift_struct_read's THRIFT_IS_STRUCT assertion
     * complains about, as the CI's HiveServer2 promptly showed. */
    TGetInfoResp *resp = g_object_new(TYPE_T_GET_INFO_RESP, NULL);
    GError *error = NULL;
    gboolean ok = t_c_l_i_service_client_get_info(client, &resp, req, &error);
    g_object_unref(req);
    if (error) g_error_free(error);
    if (!ok || !resp) {
        if (resp) g_object_unref(resp);
        return false;
    }

    bool have = false;
    TStatus *status = NULL;
    g_object_get(resp, "status", &status, NULL);
    TStatusCode code = T_STATUS_CODE_ERROR_STATUS;
    if (status) {
        g_object_get(status, "statusCode", &code, NULL);
        g_object_unref(status);
    }
    if (code == T_STATUS_CODE_SUCCESS_STATUS ||
        code == T_STATUS_CODE_SUCCESS_WITH_INFO_STATUS) {
        TGetInfoValue *val = NULL;
        g_object_get(resp, "infoValue", &val, NULL);
        if (val) {
            gchar *sv = NULL;
            g_object_get(val, "stringValue", &sv, NULL);
            if (sv && *sv) {
                g_strlcpy(out, sv, outlen);
                have = true;
            }
            g_free(sv);
            g_object_unref(val);
        }
    }
    g_object_unref(resp);
    return have;
}
