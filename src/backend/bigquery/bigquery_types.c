#include "bigquery_internal.h"
#include "argus/compat.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

/*
 * BigQuery standard-SQL type names to ODBC SQL types.
 * The REST API reports legacy spellings (INTEGER, FLOAT, BOOLEAN) as well
 * as standard ones (INT64, FLOAT64, BOOL); both are accepted.
 */
SQLSMALLINT bq_type_to_sql_type(const char *bq_type)
{
    if (!bq_type) return SQL_VARCHAR;

    if (strcasecmp(bq_type, "INTEGER") == 0 ||
        strcasecmp(bq_type, "INT64") == 0)
        return SQL_BIGINT;
    if (strcasecmp(bq_type, "FLOAT") == 0 ||
        strcasecmp(bq_type, "FLOAT64") == 0)
        return SQL_DOUBLE;
    if (strcasecmp(bq_type, "BOOLEAN") == 0 ||
        strcasecmp(bq_type, "BOOL") == 0)
        return SQL_BIT;
    if (strcasecmp(bq_type, "NUMERIC") == 0 ||
        strcasecmp(bq_type, "BIGNUMERIC") == 0)
        return SQL_DECIMAL;
    if (strcasecmp(bq_type, "TIMESTAMP") == 0 ||
        strcasecmp(bq_type, "DATETIME") == 0)
        return SQL_TYPE_TIMESTAMP;
    if (strcasecmp(bq_type, "DATE") == 0)
        return SQL_TYPE_DATE;
    if (strcasecmp(bq_type, "TIME") == 0)
        return SQL_TYPE_TIME;
    /* STRING, BYTES (base64 text), GEOGRAPHY, JSON, INTERVAL, RANGE,
     * RECORD/STRUCT (serialized JSON), REPEATED fields */
    return SQL_VARCHAR;
}

SQLULEN bq_type_column_size(const char *bq_type)
{
    switch (bq_type_to_sql_type(bq_type)) {
    case SQL_BIGINT:          return 19;
    case SQL_DOUBLE:          return 15;
    case SQL_BIT:             return 1;
    case SQL_TYPE_DATE:       return 10;
    case SQL_TYPE_TIME:       return 15;   /* HH:MM:SS.ffffff */
    case SQL_TYPE_TIMESTAMP:  return 26;   /* YYYY-MM-DD HH:MM:SS.ffffff */
    case SQL_DECIMAL:
        return (bq_type && strcasecmp(bq_type, "BIGNUMERIC") == 0) ? 76 : 38;
    default:                  return 65535; /* STRING et al. have no fixed max */
    }
}

SQLSMALLINT bq_type_decimal_digits(const char *bq_type)
{
    if (!bq_type) return 0;
    if (strcasecmp(bq_type, "NUMERIC") == 0)    return 9;
    if (strcasecmp(bq_type, "BIGNUMERIC") == 0) return 38;
    if (strcasecmp(bq_type, "TIMESTAMP") == 0 ||
        strcasecmp(bq_type, "DATETIME") == 0 ||
        strcasecmp(bq_type, "TIME") == 0)       return 6;
    return 0;
}

/* ── Cell conversion ─────────────────────────────────────────── */

/* The REST API returns TIMESTAMP as fractional epoch seconds in a string
 * (possibly scientific notation, e.g. "1.7180000005E9"). Format it as an
 * ODBC timestamp literal in UTC. */
static char *bq_epoch_to_timestamp(const char *value)
{
    double sec = g_ascii_strtod(value, NULL);
    gint64 whole = (gint64)sec;
    int micros = (int)((sec - (double)whole) * 1e6 + 0.5);
    if (micros >= 1000000) { whole++; micros -= 1000000; }
    if (micros < 0) { whole--; micros += 1000000; }

    GDateTime *dt = g_date_time_new_from_unix_utc(whole);
    if (!dt) return strdup(value);

    char *out;
    if (micros > 0)
        out = g_strdup_printf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                              g_date_time_get_year(dt),
                              g_date_time_get_month(dt),
                              g_date_time_get_day_of_month(dt),
                              g_date_time_get_hour(dt),
                              g_date_time_get_minute(dt),
                              g_date_time_get_second(dt),
                              micros);
    else
        out = g_strdup_printf("%04d-%02d-%02d %02d:%02d:%02d",
                              g_date_time_get_year(dt),
                              g_date_time_get_month(dt),
                              g_date_time_get_day_of_month(dt),
                              g_date_time_get_hour(dt),
                              g_date_time_get_minute(dt),
                              g_date_time_get_second(dt));
    g_date_time_unref(dt);

    /* hand back a plain malloc'd string, consistent with the row cache */
    char *copy = strdup(out);
    g_free(out);
    return copy;
}

void bq_fill_cell(argus_cell_t *cell, const char *bq_type, const char *value)
{
    if (!cell) return;
    if (!value) { cell->is_null = true; return; }

    SQLSMALLINT st = bq_type_to_sql_type(bq_type);

    switch (st) {
    case SQL_BIGINT: {
        char *end = NULL;
        long long v = strtoll(value, &end, 10);
        if (end && *end == '\0') {
            cell->native_kind = ARGUS_NATIVE_I64;
            cell->native.i64 = (int64_t)v;
            return;
        }
        break;
    }
    case SQL_DOUBLE: {
        char *end = NULL;
        double v = g_ascii_strtod(value, &end);
        if (end && *end == '\0') {
            cell->native_kind = ARGUS_NATIVE_F64;
            cell->native.f64 = v;
            return;
        }
        break;
    }
    case SQL_BIT:
        cell->data = strdup(strcasecmp(value, "true") == 0 ? "1" : "0");
        cell->data_len = 1;
        return;
    case SQL_TYPE_TIMESTAMP:
        if (bq_type && strcasecmp(bq_type, "TIMESTAMP") == 0) {
            cell->data = bq_epoch_to_timestamp(value);
            cell->data_len = cell->data ? strlen(cell->data) : 0;
            cell->is_null = (cell->data == NULL);
            return;
        }
        /* DATETIME arrives as "YYYY-MM-DDTHH:MM:SS[.ffffff]" */
        cell->data = strdup(value);
        if (cell->data) {
            char *t = strchr(cell->data, 'T');
            if (t) *t = ' ';
            cell->data_len = strlen(cell->data);
        } else {
            cell->is_null = true;
        }
        return;
    default:
        break;
    }

    cell->data = strdup(value);
    cell->data_len = cell->data ? strlen(cell->data) : 0;
    cell->is_null = (cell->data == NULL);
}
