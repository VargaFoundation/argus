/* SPDX-License-Identifier: Apache-2.0 */
#include "argus/handle.h"
#include "argus/dialect.h"
#include "argus/odbc_api.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Fallback defines for datetime subcodes */
#ifndef SQL_CODE_DATE
#define SQL_CODE_DATE       1
#endif
#ifndef SQL_CODE_TIMESTAMP
#define SQL_CODE_TIMESTAMP  3
#endif
#ifndef SQL_DATETIME
#define SQL_DATETIME        9
#endif


/* ── Helper: strip trailing spaces from identifier (metadata_id mode) ── */

static char *strip_trailing_spaces(const char *s)
{
    if (!s) return NULL;
    size_t len = strlen(s);
    while (len > 0 && s[len - 1] == ' ')
        len--;
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, s, len);
    out[len] = '\0';
    return out;
}

/* ── Helper: HY090 on a name length that is neither a count nor SQL_NTS ──
 * Checked before the first argus_str_dup_short, whose NULL for a bad length
 * would otherwise read here as "argument omitted" and turn the caller's bug
 * into an unfiltered catalog query. */
static SQLRETURN catalog_check_lengths(argus_stmt_t *stmt,
                                       const SQLSMALLINT *lens, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (!argus_odbc_len_valid(lens[i]))
            return argus_set_error(&stmt->diag, "HY090",
                                   "[Argus] Invalid string or buffer length", 0);
    }
    return SQL_SUCCESS;
}
#define CATALOG_CHECK_LENGTHS(stmt, ...) \
    catalog_check_lengths((stmt), (const SQLSMALLINT[]){ __VA_ARGS__ }, \
                          sizeof((const SQLSMALLINT[]){ __VA_ARGS__ }) / sizeof(SQLSMALLINT))

/* ── Helper: dup catalog arg, applying metadata_id rules if set ── */

/*
 * With SQL_ATTR_METADATA_ID set to SQL_TRUE the argument is an identifier,
 * not a search pattern, and ODBC gives a delimited one -- "Sales Data",
 * quotes included -- its own meaning: the delimiters come off, a doubled
 * quote inside stands for one, and the case between them is the identifier's
 * own. Only the trailing blanks were being stripped, so an application that
 * quoted an identifier searched for a name that still had the quote
 * characters in it, and found nothing.
 */
static char *catalog_unquote_identifier(char *raw, const char *quote)
{
    char q = (quote && quote[0]) ? quote[0] : '"';
    size_t n = strlen(raw);
    if (n < 2 || raw[0] != q || raw[n - 1] != q) return raw;

    char *out = malloc(n - 1);
    if (!out) return raw;
    size_t j = 0;
    for (size_t i = 1; i < n - 1; i++) {
        if (raw[i] == q && raw[i + 1] == q) i++;   /* "" stands for one quote */
        out[j++] = raw[i];
    }
    out[j] = '\0';
    free(raw);
    return out;
}

static char *catalog_arg_dup(const SQLCHAR *str, SQLSMALLINT len,
                             SQLULEN metadata_id, const char *quote)
{
    char *raw = argus_str_dup_short(str, len);
    if (raw && metadata_id == SQL_TRUE) {
        char *stripped = strip_trailing_spaces(raw);
        free(raw);
        if (!stripped) return NULL;
        char *ident = catalog_unquote_identifier(stripped, quote);
        if (!ident) return NULL;
        /*
         * An identifier, not a pattern: `%` and `_` in it stand for
         * themselves. They were left as LIKE's wildcards, so asking for
         * `my_table` under SQL_ATTR_METADATA_ID also found `myXtable`.
         * Escaping them here is what the backends' ESCAPE clause acts on.
         */
        char *pat = argus_sql_escape_pattern(ident);
        free(ident);
        return pat;
    }
    return raw;
}

/*
 * ODBC: with SQL_ATTR_METADATA_ID set, an identifier argument may not be a
 * null pointer. It is HY009 -- not, as it was, an unfiltered catalog query
 * over every schema on the server.
 */
static SQLRETURN catalog_check_identifiers(argus_stmt_t *stmt,
                                           const SQLCHAR *const *args,
                                           size_t n)
{
    if (stmt->metadata_id != SQL_TRUE) return SQL_SUCCESS;
    for (size_t i = 0; i < n; i++)
        if (!args[i])
            return argus_set_error(&stmt->diag, "HY009",
                                   "[Argus] Invalid use of null pointer: with "
                                   "SQL_ATTR_METADATA_ID set, every identifier "
                                   "argument is required", 0);
    return SQL_SUCCESS;
}
#define CATALOG_CHECK_IDENTIFIERS(stmt, ...) \
    catalog_check_identifiers((stmt), (const SQLCHAR *const[]){ __VA_ARGS__ }, \
                              sizeof((const SQLCHAR *const[]){ __VA_ARGS__ }) / \
                              sizeof(const SQLCHAR *))

/* ── Helper: dispatch catalog operation and setup result set ─── */

static SQLRETURN catalog_dispatch(argus_stmt_t *stmt)
{
    stmt->executed = true;

    /* Get metadata for the catalog result set */
    if (stmt->dbc->backend->get_result_metadata) {
        int ncols = 0;
        argus_column_desc_t tmp_cols[64];
        int rc = stmt->dbc->backend->get_result_metadata(
            stmt->dbc->backend_conn, stmt->op,
            tmp_cols, &ncols);
        if (rc == 0 && ncols > 0) {
            if (argus_stmt_ensure_columns(stmt, ncols) == 0) {
                if (ncols <= 64) {
                    memcpy(stmt->columns, tmp_cols,
                           (size_t)ncols * sizeof(argus_column_desc_t));
                } else {
                    /* Re-query into the now-allocated buffer */
                    int ncols2 = 0;
                    stmt->dbc->backend->get_result_metadata(
                        stmt->dbc->backend_conn, stmt->op,
                        stmt->columns, &ncols2);
                    ncols = ncols2;
                }
                stmt->num_cols = ncols;
                stmt->metadata_fetched = true;
            }
        }
    }

    return SQL_SUCCESS;
}

/* True when the argument is present and exactly `want`. A NULL pointer is
 * "omitted" and never matches: ODBC distinguishes an absent argument (no
 * filter) from an empty one (no value), and SQLTables' enumeration forms are
 * defined in terms of the empty string. */
static bool argus_arg_is(const SQLCHAR *arg, SQLSMALLINT len, const char *want)
{
    if (!arg) return false;
    size_t n = (len == SQL_NTS) ? strlen((const char *)arg) : (size_t)len;
    return n == strlen(want) && memcmp(arg, want, n) == 0;
}

static SQLRETURN catalog_delegate(argus_stmt_t *stmt, const char *what);
static SQLRETURN catalog_failed(argus_stmt_t *stmt, const char *what);

/* ── ODBC API: SQLTables ─────────────────────────────────────── */

static SQLRETURN sqltables_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName,  SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,   SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,    SQLSMALLINT NameLength3,
    SQLCHAR   *TableType,    SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3,
                                                        NameLength4);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    TableName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->connected || !dbc->backend) {
        return argus_set_error(&stmt->diag, "08003",
                               "[Argus] Connection not open", 0);
    }

    if (!dbc->backend->get_tables) {
        return argus_set_not_implemented(&stmt->diag, "SQLTables");
    }

    /*
     * SQLTables' three enumeration forms.
     *
     * ODBC overloads SQLTables: "%" in exactly one argument with the others
     * given as *empty strings* (not NULL — an omitted argument means "no
     * filter", an empty one means "no value") asks for the list of catalogs or
     * of schemas rather than for tables. Power BI's hierarchical navigator and
     * Tableau's schema picker both open with one of these.
     *
     * Every backend has implemented get_catalogs and get_schemas since the
     * vtable was written, and nothing ever called them: the special cases fell
     * through to get_tables, so asking for the catalog list returned the table
     * list. Routing them is the whole fix.
     */
    {
        bool cat_pct = argus_arg_is(CatalogName, NameLength1, "%");
        bool sch_pct = argus_arg_is(SchemaName,  NameLength2, "%");
        bool cat_empty = argus_arg_is(CatalogName, NameLength1, "");
        bool sch_empty = argus_arg_is(SchemaName,  NameLength2, "");
        bool tab_empty = argus_arg_is(TableName,   NameLength3, "");

        if (cat_pct && sch_empty && tab_empty && dbc->backend->get_catalogs) {
            if (dbc->backend->get_catalogs(dbc->backend_conn, &stmt->op) != 0)
                return catalog_failed(stmt, "SQLTables (catalog list)");
            return catalog_delegate(stmt, "SQLTables (catalog list)");
        }

        if (sch_pct && cat_empty && tab_empty && dbc->backend->get_schemas) {
            if (dbc->backend->get_schemas(dbc->backend_conn, NULL, NULL,
                                          &stmt->op) != 0)
                return catalog_failed(stmt, "SQLTables (schema list)");
            return catalog_delegate(stmt, "SQLTables (schema list)");
        }
    }

    SQLULEN mid = stmt->metadata_id;
    const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
    char *catalog    = catalog_arg_dup(CatalogName, NameLength1, mid, qc);
    char *schema     = catalog_arg_dup(SchemaName, NameLength2, mid, qc);
    char *table_name = catalog_arg_dup(TableName, NameLength3, mid, qc);
    char *table_type = catalog_arg_dup(TableType, NameLength4, mid, qc);

    /* Check metadata cache first */
    if (argus_metadata_cache_lookup(dbc, stmt, "SQLTables",
                                     catalog, schema, table_name, table_type)) {
        free(catalog);
        free(schema);
        free(table_name);
        free(table_type);
        return SQL_SUCCESS;
    }

    int rc = dbc->backend->get_tables(
        dbc->backend_conn,
        catalog, schema, table_name, table_type,
        &stmt->op);

    if (rc != 0) {
        free(catalog);
        free(schema);
        free(table_name);
        free(table_type);
        if (stmt->diag.count == 0)
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Failed to get tables", 0);
        return SQL_ERROR;
    }

    SQLRETURN ret = catalog_dispatch(stmt);

    /* Fetch all rows eagerly for caching */
    if (ret == SQL_SUCCESS && dbc->backend->fetch_results) {
        int ncols = 0;
        dbc->backend->fetch_results(
            dbc->backend_conn, stmt->op, 10000,
            &stmt->row_cache, stmt->columns, &ncols);
        if (ncols > 0 && !stmt->metadata_fetched) {
            stmt->num_cols = ncols;
            stmt->metadata_fetched = true;
        }
        stmt->row_cache.exhausted = true;
        /* Rows were fetched eagerly into the cache; mark the fetch as started so
         * SQLFetch iterates the cache instead of re-fetching (which would clear
         * the cache and read the empty next batch). */
        stmt->fetch_started = true;
        /* Store in cache */
        argus_metadata_cache_store(dbc, stmt, "SQLTables",
                                    catalog, schema, table_name, table_type);
    }

    free(catalog);
    free(schema);
    free(table_name);
    free(table_type);

    return ret;
}

/* ── ODBC API: SQLColumns ────────────────────────────────────── */

static SQLRETURN sqlcolumns_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName,  SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,   SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,    SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,   SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3,
                                                        NameLength4);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    TableName,
                                                    ColumnName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->connected || !dbc->backend) {
        return argus_set_error(&stmt->diag, "08003",
                               "[Argus] Connection not open", 0);
    }

    if (!dbc->backend->get_columns) {
        return argus_set_not_implemented(&stmt->diag, "SQLColumns");
    }

    SQLULEN mid2 = stmt->metadata_id;
    const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
    char *catalog     = catalog_arg_dup(CatalogName, NameLength1, mid2, qc);
    char *schema      = catalog_arg_dup(SchemaName, NameLength2, mid2, qc);
    char *table_name  = catalog_arg_dup(TableName, NameLength3, mid2, qc);
    char *column_name = catalog_arg_dup(ColumnName, NameLength4, mid2, qc);

    /* Check metadata cache first */
    if (argus_metadata_cache_lookup(dbc, stmt, "SQLColumns",
                                     catalog, schema, table_name, column_name)) {
        free(catalog);
        free(schema);
        free(table_name);
        free(column_name);
        return SQL_SUCCESS;
    }

    int rc = dbc->backend->get_columns(
        dbc->backend_conn,
        catalog, schema, table_name, column_name,
        &stmt->op);

    if (rc != 0) {
        free(catalog);
        free(schema);
        free(table_name);
        free(column_name);
        if (stmt->diag.count == 0)
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Failed to get columns", 0);
        return SQL_ERROR;
    }

    SQLRETURN ret = catalog_dispatch(stmt);

    /* Fetch all rows eagerly for caching */
    if (ret == SQL_SUCCESS && dbc->backend->fetch_results) {
        int ncols = 0;
        dbc->backend->fetch_results(
            dbc->backend_conn, stmt->op, 10000,
            &stmt->row_cache, stmt->columns, &ncols);
        if (ncols > 0 && !stmt->metadata_fetched) {
            stmt->num_cols = ncols;
            stmt->metadata_fetched = true;
        }
        stmt->row_cache.exhausted = true;
        /* Rows were fetched eagerly into the cache; mark the fetch as started so
         * SQLFetch iterates the cache instead of re-fetching (which would clear
         * the cache and read the empty next batch). */
        stmt->fetch_started = true;
        /* Store in cache */
        argus_metadata_cache_store(dbc, stmt, "SQLColumns",
                                    catalog, schema, table_name, column_name);
    }

    free(catalog);
    free(schema);
    free(table_name);
    free(column_name);

    return ret;
}

/* ── Built-in type info for SQLGetTypeInfo fallback ──────────── */

typedef struct {
    const char  *type_name;
    SQLSMALLINT  data_type;
    SQLINTEGER   column_size;
    const char  *literal_prefix;
    const char  *literal_suffix;
    const char  *create_params;
    SQLSMALLINT  nullable;
    SQLSMALLINT  case_sensitive;
    SQLSMALLINT  searchable;
    SQLSMALLINT  unsigned_attr;
    SQLSMALLINT  fixed_prec_scale;
    SQLSMALLINT  auto_unique;
    const char  *local_type_name;
    SQLSMALLINT  min_scale;
    SQLSMALLINT  max_scale;
    SQLSMALLINT  sql_data_type;
    SQLSMALLINT  sql_datetime_sub;
    SQLINTEGER   num_prec_radix;
    SQLSMALLINT  interval_precision;
} builtin_type_info_t;

static const builtin_type_info_t builtin_types[] = {
    {"VARCHAR",   SQL_VARCHAR,        65535, "'",  "'",  "max length",
     SQL_NULLABLE, SQL_TRUE,  SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "VARCHAR",  0, 0, SQL_VARCHAR, 0, 0, 0},
    {"CHAR",      SQL_CHAR,           255,   "'",  "'",  "length",
     SQL_NULLABLE, SQL_TRUE,  SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "CHAR",     0, 0, SQL_CHAR, 0, 0, 0},
    {"INTEGER",   SQL_INTEGER,        10,    NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "INTEGER",  0, 0, SQL_INTEGER, 0, 10, 0},
    {"BIGINT",    SQL_BIGINT,         19,    NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "BIGINT",   0, 0, SQL_BIGINT, 0, 10, 0},
    {"SMALLINT",  SQL_SMALLINT,       5,     NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "SMALLINT", 0, 0, SQL_SMALLINT, 0, 10, 0},
    {"TINYINT",   SQL_TINYINT,        3,     NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "TINYINT",  0, 0, SQL_TINYINT, 0, 10, 0},
    {"FLOAT",     SQL_FLOAT,          15,    NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "FLOAT",    0, 0, SQL_FLOAT, 0, 2, 0},
    {"DOUBLE",    SQL_DOUBLE,         15,    NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "DOUBLE",   0, 0, SQL_DOUBLE, 0, 2, 0},
    {"REAL",      SQL_REAL,           7,     NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "REAL",     0, 0, SQL_REAL, 0, 2, 0},
    {"DECIMAL",   SQL_DECIMAL,        38,    NULL, NULL, "precision,scale",
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
     "DECIMAL",  0, 38, SQL_DECIMAL, 0, 10, 0},
    {"BOOLEAN",   SQL_BIT,            1,     NULL, NULL, NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "BOOLEAN",  0, 0, SQL_BIT, 0, 0, 0},
    {"DATE",      SQL_TYPE_DATE,      10,    "'",  "'",  NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "DATE",     0, 0, SQL_DATETIME, SQL_CODE_DATE, 0, 0},
    {"TIMESTAMP", SQL_TYPE_TIMESTAMP, 26,    "'",  "'",  NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "TIMESTAMP",0, 6, SQL_DATETIME, SQL_CODE_TIMESTAMP, 0, 0},
    {"BINARY",    SQL_BINARY,         65535, "X'", "'",  "max length",
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "BINARY",   0, 0, SQL_BINARY, 0, 0, 0},
    {"LONGVARCHAR", SQL_LONGVARCHAR,  2147483647, "'", "'", NULL,
     SQL_NULLABLE, SQL_TRUE,  SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "STRING",   0, 0, SQL_LONGVARCHAR, 0, 0, 0},
    {"GUID",      SQL_GUID,           36,    "'",  "'",  NULL,
     SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, -1, SQL_FALSE, -1,
     "UUID",     0, 0, SQL_GUID, 0, 0, 0},
};

#define BUILTIN_TYPE_COUNT (sizeof(builtin_types) / sizeof(builtin_types[0]))

static void setup_type_info_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } ti_cols[] = {
        {"TYPE_NAME",          SQL_VARCHAR,  128},
        {"DATA_TYPE",          SQL_SMALLINT, 5},
        {"COLUMN_SIZE",        SQL_INTEGER,  10},
        {"LITERAL_PREFIX",     SQL_VARCHAR,  128},
        {"LITERAL_SUFFIX",     SQL_VARCHAR,  128},
        {"CREATE_PARAMS",      SQL_VARCHAR,  128},
        {"NULLABLE",           SQL_SMALLINT, 5},
        {"CASE_SENSITIVE",     SQL_SMALLINT, 5},
        {"SEARCHABLE",         SQL_SMALLINT, 5},
        {"UNSIGNED_ATTRIBUTE", SQL_SMALLINT, 5},
        {"FIXED_PREC_SCALE",   SQL_SMALLINT, 5},
        {"AUTO_UNIQUE_VALUE",  SQL_SMALLINT, 5},
        {"LOCAL_TYPE_NAME",    SQL_VARCHAR,  128},
        {"MINIMUM_SCALE",      SQL_SMALLINT, 5},
        {"MAXIMUM_SCALE",      SQL_SMALLINT, 5},
        {"SQL_DATA_TYPE",      SQL_SMALLINT, 5},
        {"SQL_DATETIME_SUB",   SQL_SMALLINT, 5},
        {"NUM_PREC_RADIX",     SQL_INTEGER,  10},
        {"INTERVAL_PRECISION", SQL_SMALLINT, 5},
    };

    stmt->num_cols = 19;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 19; i++) {
        strncpy((char *)stmt->columns[i].name, ti_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(ti_cols[i].name);
        stmt->columns[i].sql_type = ti_cols[i].sql_type;
        stmt->columns[i].column_size = ti_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

static void add_type_row(argus_row_cache_t *cache, int row_idx,
                          const builtin_type_info_t *t)
{
    argus_row_t *row = &cache->rows[row_idx];
    row->cells = calloc(19, sizeof(argus_cell_t));
    if (!row->cells) return;

    /* Helper macro: set a string cell */
    #define SET_STR(idx, val) do { \
        if (val) { \
            row->cells[idx].data = strdup(val); \
            row->cells[idx].data_len = strlen(val); \
            row->cells[idx].is_null = false; \
        } else { \
            row->cells[idx].data = NULL; \
            row->cells[idx].data_len = 0; \
            row->cells[idx].is_null = true; \
        } \
    } while (0)

    /* Helper macro: set an integer cell */
    #define SET_INT(idx, val) do { \
        char buf[32]; \
        snprintf(buf, sizeof(buf), "%d", (int)(val)); \
        row->cells[idx].data = strdup(buf); \
        row->cells[idx].data_len = strlen(buf); \
        row->cells[idx].is_null = false; \
    } while (0)

    /* Helper macro: set nullable smallint (-1 means NULL) */
    #define SET_NSINT(idx, val) do { \
        if ((val) == -1) { \
            row->cells[idx].data = NULL; \
            row->cells[idx].data_len = 0; \
            row->cells[idx].is_null = true; \
        } else { \
            SET_INT(idx, val); \
        } \
    } while (0)

    SET_STR(0, t->type_name);           /* TYPE_NAME */
    SET_INT(1, t->data_type);            /* DATA_TYPE */
    SET_INT(2, t->column_size);          /* COLUMN_SIZE */
    SET_STR(3, t->literal_prefix);       /* LITERAL_PREFIX */
    SET_STR(4, t->literal_suffix);       /* LITERAL_SUFFIX */
    SET_STR(5, t->create_params);        /* CREATE_PARAMS */
    SET_INT(6, t->nullable);             /* NULLABLE */
    SET_INT(7, t->case_sensitive);        /* CASE_SENSITIVE */
    SET_INT(8, t->searchable);           /* SEARCHABLE */
    SET_NSINT(9, t->unsigned_attr);      /* UNSIGNED_ATTRIBUTE */
    SET_INT(10, t->fixed_prec_scale);     /* FIXED_PREC_SCALE */
    SET_NSINT(11, t->auto_unique);        /* AUTO_UNIQUE_VALUE */
    SET_STR(12, t->local_type_name);     /* LOCAL_TYPE_NAME */
    SET_INT(13, t->min_scale);            /* MINIMUM_SCALE */
    SET_INT(14, t->max_scale);            /* MAXIMUM_SCALE */
    SET_INT(15, t->sql_data_type);        /* SQL_DATA_TYPE */
    SET_INT(16, t->sql_datetime_sub);     /* SQL_DATETIME_SUB */
    if (t->num_prec_radix > 0)
        SET_INT(17, t->num_prec_radix);   /* NUM_PREC_RADIX */
    else {
        row->cells[17].data = NULL;
        row->cells[17].data_len = 0;
        row->cells[17].is_null = true;
    }
    SET_INT(18, t->interval_precision);   /* INTERVAL_PRECISION */

    #undef SET_STR
    #undef SET_INT
    #undef SET_NSINT
}

static SQLRETURN builtin_get_type_info(argus_stmt_t *stmt,
                                        SQLSMALLINT DataType)
{
    stmt->executed = true;
    setup_type_info_metadata(stmt);
    /* The rows are materialized here, not fetched from a backend op, so mark
     * the fetch as started: otherwise the first SQLFetch clears this cache
     * and pulls from a NULL op, dropping every type row. */
    stmt->fetch_started = true;

    /* Count matching types */
    size_t count = 0;
    for (size_t i = 0; i < BUILTIN_TYPE_COUNT; i++) {
        if (DataType == SQL_ALL_TYPES || builtin_types[i].data_type == DataType)
            count++;
    }

    if (count == 0) {
        stmt->row_cache.exhausted = true;
        return SQL_SUCCESS;
    }

    /* Allocate rows */
    stmt->row_cache.rows = calloc(count, sizeof(argus_row_t));
    if (!stmt->row_cache.rows) {
        stmt->row_cache.exhausted = true;
        return SQL_SUCCESS;
    }
    stmt->row_cache.num_rows = count;
    stmt->row_cache.num_cols = 19;
    stmt->row_cache.current_row = 0;
    stmt->row_cache.exhausted = true; /* all data in one batch */

    int row_idx = 0;
    for (size_t i = 0; i < BUILTIN_TYPE_COUNT; i++) {
        if (DataType == SQL_ALL_TYPES || builtin_types[i].data_type == DataType) {
            add_type_row(&stmt->row_cache, row_idx, &builtin_types[i]);
            row_idx++;
        }
    }

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLGetTypeInfo ────────────────────────────────── */

static SQLRETURN sqlgettypeinfo_impl(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT DataType)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (!dbc || !dbc->connected || !dbc->backend) {
        return argus_set_error(&stmt->diag, "08003",
                               "[Argus] Connection not open", 0);
    }

    /* If backend implements get_type_info, delegate */
    if (dbc->backend->get_type_info) {
        int rc = dbc->backend->get_type_info(
            dbc->backend_conn, DataType, &stmt->op);

        if (rc == 0)
            return catalog_dispatch(stmt);

        /* Backend failed — fall through to built-in */
        argus_diag_clear(&stmt->diag);
    }

    /* Built-in fallback with standard SQL types */
    return builtin_get_type_info(stmt, DataType);
}

/* ── Helper: setup standard SQLStatistics result metadata ────── */

static void setup_statistics_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } stats_cols[] = {
        {"TABLE_CAT",        SQL_VARCHAR, 128},
        {"TABLE_SCHEM",      SQL_VARCHAR, 128},
        {"TABLE_NAME",       SQL_VARCHAR, 128},
        {"NON_UNIQUE",       SQL_SMALLINT, 5},
        {"INDEX_QUALIFIER",  SQL_VARCHAR, 128},
        {"INDEX_NAME",       SQL_VARCHAR, 128},
        {"TYPE",             SQL_SMALLINT, 5},
        {"ORDINAL_POSITION", SQL_SMALLINT, 5},
        {"COLUMN_NAME",      SQL_VARCHAR, 128},
        {"ASC_OR_DESC",      SQL_CHAR, 1},
        {"CARDINALITY",      SQL_BIGINT, 20},
        {"PAGES",            SQL_BIGINT, 20},
        {"FILTER_CONDITION", SQL_VARCHAR, 128},
    };

    stmt->num_cols = 13;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 13; i++) {
        strncpy((char *)stmt->columns[i].name, stats_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(stats_cols[i].name);
        stmt->columns[i].sql_type = stats_cols[i].sql_type;
        stmt->columns[i].column_size = stats_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLStatistics ─────────────────────────────────── */

static SQLRETURN sqlstatistics_impl(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR     *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR     *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique,
    SQLUSMALLINT Reserved)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;

    /* If backend implements get_statistics, delegate */
    if (dbc && dbc->connected && dbc->backend &&
        dbc->backend->get_statistics) {
        char *catalog    = argus_str_dup_short(CatalogName, NameLength1);
        char *schema     = argus_str_dup_short(SchemaName,  NameLength2);
        char *table_name = argus_str_dup_short(TableName,   NameLength3);

        int rc = dbc->backend->get_statistics(
            dbc->backend_conn,
            catalog, schema, table_name,
            Unique, Reserved,
            &stmt->op);

        free(catalog);
        free(schema);
        free(table_name);

        if (rc != 0) {
            if (stmt->diag.count == 0)
                argus_set_error(&stmt->diag, "HY000",
                                "[Argus] Failed to get statistics", 0);
            return SQL_ERROR;
        }

        return catalog_dispatch(stmt);
    }

    /* Return empty result set with proper metadata */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_statistics_metadata(stmt);

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)Unique;      (void)Reserved;

    return SQL_SUCCESS;
}

/*
 * Shared tail for a catalog function that a backend implements.
 *
 * Mirrors what SQLTables does: pull the column metadata through, drain the
 * rows into the statement's cache, and mark the fetch started so SQLFetch
 * walks the cache instead of asking the backend for a second (empty) batch.
 * These result sets are small by construction — one row per constraint column,
 * per privilege, per procedure argument — so eager fetching costs nothing and
 * keeps every one of them on the same path.
 */
static SQLRETURN catalog_delegate(argus_stmt_t *stmt, const char *what)
{
    argus_dbc_t *dbc = stmt->dbc;

    SQLRETURN ret = catalog_dispatch(stmt);
    if (ret != SQL_SUCCESS) return ret;

    if (dbc->backend->fetch_results) {
        int ncols = 0;
        dbc->backend->fetch_results(dbc->backend_conn, stmt->op, 10000,
                                    &stmt->row_cache, stmt->columns, &ncols);
        if (ncols > 0 && !stmt->metadata_fetched) {
            stmt->num_cols = ncols;
            stmt->metadata_fetched = true;
        }
        stmt->row_cache.exhausted = true;
        stmt->fetch_started = true;
    }
    ARGUS_LOG_TRACE("%s: %zu row(s)", what, stmt->row_cache.num_rows);
    return SQL_SUCCESS;
}

/* The error path shared by the same set of functions. */
static SQLRETURN catalog_failed(argus_stmt_t *stmt, const char *what)
{
    if (stmt->diag.count == 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "[Argus] %s failed", what);
        argus_set_error(&stmt->diag, "HY000", msg, 0);
    }
    return SQL_ERROR;
}

/* ── Helper: setup standard SQLSpecialColumns result metadata ── */

static void setup_special_columns_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } sc_cols[] = {
        {"SCOPE",           SQL_SMALLINT, 5},
        {"COLUMN_NAME",     SQL_VARCHAR, 128},
        {"DATA_TYPE",       SQL_SMALLINT, 5},
        {"TYPE_NAME",       SQL_VARCHAR, 128},
        {"COLUMN_SIZE",     SQL_INTEGER, 10},
        {"BUFFER_LENGTH",   SQL_INTEGER, 10},
        {"DECIMAL_DIGITS",  SQL_SMALLINT, 5},
        {"PSEUDO_COLUMN",   SQL_SMALLINT, 5},
    };

    stmt->num_cols = 8;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 8; i++) {
        strncpy((char *)stmt->columns[i].name, sc_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(sc_cols[i].name);
        stmt->columns[i].sql_type = sc_cols[i].sql_type;
        stmt->columns[i].column_size = sc_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLSpecialColumns (empty result set) ──────────── */

static SQLRETURN sqlspecialcolumns_impl(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT IdentifierType,
    SQLCHAR     *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR     *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR     *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope,
    SQLUSMALLINT Nullable)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    TableName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (dbc && dbc->connected && dbc->backend && dbc->backend->get_special_columns) {
        SQLULEN mid = stmt->metadata_id;
        const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
        char *cat = catalog_arg_dup(CatalogName, NameLength1, mid, qc);
        char *sch = catalog_arg_dup(SchemaName, NameLength2, mid, qc);
        char *tab = catalog_arg_dup(TableName, NameLength3, mid, qc);

        int rc = dbc->backend->get_special_columns(dbc->backend_conn,
                                                   IdentifierType,
                                                   cat, sch, tab,
                                                   Scope, Nullable, &stmt->op);
        free(cat); free(sch); free(tab);

        if (rc != 0) return catalog_failed(stmt, "SQLSpecialColumns");
        return catalog_delegate(stmt, "SQLSpecialColumns");
    }

    (void)IdentifierType;
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)Scope;       (void)Nullable;

    /* No hook: an engine with no such objects, and the empty result set with
     * the right column shape is the correct answer. fetch_started matters —
     * on a connected statement without it, SQLFetch would go to the backend
     * with a NULL operation handle and fail instead of reporting
     * SQL_NO_DATA. */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_special_columns_metadata(stmt);

    return SQL_SUCCESS;
}

/* ── Helper: setup standard SQLPrimaryKeys result metadata ──── */

static void setup_primary_keys_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } pk_cols[] = {
        {"TABLE_CAT",  SQL_VARCHAR, 128},
        {"TABLE_SCHEM", SQL_VARCHAR, 128},
        {"TABLE_NAME",  SQL_VARCHAR, 128},
        {"COLUMN_NAME", SQL_VARCHAR, 128},
        {"KEY_SEQ",     SQL_SMALLINT, 5},
        {"PK_NAME",     SQL_VARCHAR, 128},
    };

    stmt->num_cols = 6;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 6; i++) {
        strncpy((char *)stmt->columns[i].name, pk_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(pk_cols[i].name);
        stmt->columns[i].sql_type = pk_cols[i].sql_type;
        stmt->columns[i].column_size = pk_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLPrimaryKeys ────────────────────────────────── */

static SQLRETURN sqlprimarykeys_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;

    /* If backend implements get_primary_keys, delegate */
    if (dbc && dbc->connected && dbc->backend &&
        dbc->backend->get_primary_keys) {
        char *catalog    = argus_str_dup_short(CatalogName, NameLength1);
        char *schema     = argus_str_dup_short(SchemaName,  NameLength2);
        char *table_name = argus_str_dup_short(TableName,   NameLength3);

        int rc = dbc->backend->get_primary_keys(
            dbc->backend_conn,
            catalog, schema, table_name,
            &stmt->op);

        free(catalog);
        free(schema);
        free(table_name);

        /* On success, dispatch the backend result. On failure (the backend
         * cannot produce primary-key metadata — e.g. Phoenix's Avatica
         * getPrimaryKeys is unsupported), fall through to the standard empty
         * result set rather than erroring: catalog functions must return the
         * correct column shape, and a malformed/erroring SQLPrimaryKeys
         * crashes strict clients enumerating metadata. */
        if (rc == 0)
            return catalog_dispatch(stmt);
        argus_diag_clear(&stmt->diag);
    }

    /* Return empty result set with proper metadata */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_primary_keys_metadata(stmt);

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;

    return SQL_SUCCESS;
}

/* ── Helper: setup standard SQLForeignKeys result metadata ──── */

static void setup_foreign_keys_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } fk_cols[] = {
        {"PKTABLE_CAT",    SQL_VARCHAR, 128},
        {"PKTABLE_SCHEM",  SQL_VARCHAR, 128},
        {"PKTABLE_NAME",   SQL_VARCHAR, 128},
        {"PKCOLUMN_NAME",  SQL_VARCHAR, 128},
        {"FKTABLE_CAT",    SQL_VARCHAR, 128},
        {"FKTABLE_SCHEM",  SQL_VARCHAR, 128},
        {"FKTABLE_NAME",   SQL_VARCHAR, 128},
        {"FKCOLUMN_NAME",  SQL_VARCHAR, 128},
        {"KEY_SEQ",        SQL_SMALLINT, 5},
        {"UPDATE_RULE",    SQL_SMALLINT, 5},
        {"DELETE_RULE",    SQL_SMALLINT, 5},
        {"FK_NAME",        SQL_VARCHAR, 128},
        {"PK_NAME",        SQL_VARCHAR, 128},
        {"DEFERRABILITY",  SQL_SMALLINT, 5},
    };

    stmt->num_cols = 14;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 14; i++) {
        strncpy((char *)stmt->columns[i].name, fk_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(fk_cols[i].name);
        stmt->columns[i].sql_type = fk_cols[i].sql_type;
        stmt->columns[i].column_size = fk_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLForeignKeys (empty result set) ─────────────── */

static SQLRETURN sqlforeignkeys_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *PKCatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *PKSchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *PKTableName,   SQLSMALLINT NameLength3,
    SQLCHAR   *FKCatalogName, SQLSMALLINT NameLength4,
    SQLCHAR   *FKSchemaName,  SQLSMALLINT NameLength5,
    SQLCHAR   *FKTableName,   SQLSMALLINT NameLength6)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3,
                                                        NameLength4, NameLength5, NameLength6);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, PKCatalogName,
                                                    PKSchemaName,
                                                    PKTableName,
                                                    FKCatalogName,
                                                    FKSchemaName,
                                                    FKTableName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (dbc && dbc->connected && dbc->backend && dbc->backend->get_foreign_keys) {
        SQLULEN mid = stmt->metadata_id;
        const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
        char *pk_cat = catalog_arg_dup(PKCatalogName, NameLength1, mid, qc);
        char *pk_sch = catalog_arg_dup(PKSchemaName, NameLength2, mid, qc);
        char *pk_tab = catalog_arg_dup(PKTableName, NameLength3, mid, qc);
        char *fk_cat = catalog_arg_dup(FKCatalogName, NameLength4, mid, qc);
        char *fk_sch = catalog_arg_dup(FKSchemaName, NameLength5, mid, qc);
        char *fk_tab = catalog_arg_dup(FKTableName, NameLength6, mid, qc);

        int rc = dbc->backend->get_foreign_keys(dbc->backend_conn,
                                                pk_cat, pk_sch, pk_tab,
                                                fk_cat, fk_sch, fk_tab,
                                                &stmt->op);
        free(pk_cat); free(pk_sch); free(pk_tab);
        free(fk_cat); free(fk_sch); free(fk_tab);

        if (rc != 0) return catalog_failed(stmt, "SQLForeignKeys");
        return catalog_delegate(stmt, "SQLForeignKeys");
    }

    (void)PKCatalogName; (void)NameLength1;
    (void)PKSchemaName;  (void)NameLength2;
    (void)PKTableName;   (void)NameLength3;
    (void)FKCatalogName; (void)NameLength4;
    (void)FKSchemaName;  (void)NameLength5;
    (void)FKTableName;   (void)NameLength6;

    /* No hook: the engine has no foreign keys, and an empty result set with
     * the right shape is the correct answer. fetch_started matters here — on a
     * connected statement without it, SQLFetch would go to the backend with a
     * NULL operation handle and fail instead of reporting SQL_NO_DATA. */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_foreign_keys_metadata(stmt);

    return SQL_SUCCESS;
}

/* ── Helper: setup standard SQLProcedures result metadata ────── */

static void setup_procedures_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } proc_cols[] = {
        {"PROCEDURE_CAT",    SQL_VARCHAR, 128},
        {"PROCEDURE_SCHEM",  SQL_VARCHAR, 128},
        {"PROCEDURE_NAME",   SQL_VARCHAR, 128},
        {"NUM_INPUT_PARAMS",  SQL_INTEGER, 10},
        {"NUM_OUTPUT_PARAMS", SQL_INTEGER, 10},
        {"NUM_RESULT_SETS",   SQL_INTEGER, 10},
        {"REMARKS",          SQL_VARCHAR, 254},
        {"PROCEDURE_TYPE",   SQL_SMALLINT, 5},
    };

    stmt->num_cols = 8;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 8; i++) {
        strncpy((char *)stmt->columns[i].name, proc_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(proc_cols[i].name);
        stmt->columns[i].sql_type = proc_cols[i].sql_type;
        stmt->columns[i].column_size = proc_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLProcedures (empty result set) ──────────────── */

static SQLRETURN sqlprocedures_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *ProcName,    SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    ProcName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (dbc && dbc->connected && dbc->backend && dbc->backend->get_procedures) {
        SQLULEN mid = stmt->metadata_id;
        const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
        char *cat  = catalog_arg_dup(CatalogName, NameLength1, mid, qc);
        char *sch  = catalog_arg_dup(SchemaName, NameLength2, mid, qc);
        char *proc = catalog_arg_dup(ProcName, NameLength3, mid, qc);

        int rc = dbc->backend->get_procedures(dbc->backend_conn,
                                              cat, sch, proc, &stmt->op);
        free(cat); free(sch); free(proc);

        if (rc != 0) return catalog_failed(stmt, "SQLProcedures");
        return catalog_delegate(stmt, "SQLProcedures");
    }

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)ProcName;    (void)NameLength3;

    /* No hook: an engine with no such objects, and the empty result set with
     * the right column shape is the correct answer. fetch_started matters —
     * on a connected statement without it, SQLFetch would go to the backend
     * with a NULL operation handle and fail instead of reporting
     * SQL_NO_DATA. */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_procedures_metadata(stmt);

    return SQL_SUCCESS;
}

/* ── Helper: setup standard SQLProcedureColumns result metadata  */

static void setup_procedure_columns_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } pc_cols[] = {
        {"PROCEDURE_CAT",    SQL_VARCHAR, 128},
        {"PROCEDURE_SCHEM",  SQL_VARCHAR, 128},
        {"PROCEDURE_NAME",   SQL_VARCHAR, 128},
        {"COLUMN_NAME",      SQL_VARCHAR, 128},
        {"COLUMN_TYPE",      SQL_SMALLINT, 5},
        {"DATA_TYPE",        SQL_SMALLINT, 5},
        {"TYPE_NAME",        SQL_VARCHAR, 128},
        {"COLUMN_SIZE",      SQL_INTEGER, 10},
        {"BUFFER_LENGTH",    SQL_INTEGER, 10},
        {"DECIMAL_DIGITS",   SQL_SMALLINT, 5},
        {"NUM_PREC_RADIX",   SQL_SMALLINT, 5},
        {"NULLABLE",         SQL_SMALLINT, 5},
        {"REMARKS",          SQL_VARCHAR, 254},
    };

    stmt->num_cols = 13;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 13; i++) {
        strncpy((char *)stmt->columns[i].name, pc_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(pc_cols[i].name);
        stmt->columns[i].sql_type = pc_cols[i].sql_type;
        stmt->columns[i].column_size = pc_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLProcedureColumns (empty result set) ────────── */

static SQLRETURN sqlprocedurecolumns_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *ProcName,    SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,  SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3,
                                                        NameLength4);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    ProcName,
                                                    ColumnName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (dbc && dbc->connected && dbc->backend &&
        dbc->backend->get_procedure_columns) {
        SQLULEN mid = stmt->metadata_id;
        const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
        char *cat  = catalog_arg_dup(CatalogName, NameLength1, mid, qc);
        char *sch  = catalog_arg_dup(SchemaName, NameLength2, mid, qc);
        char *proc = catalog_arg_dup(ProcName, NameLength3, mid, qc);
        char *col  = catalog_arg_dup(ColumnName, NameLength4, mid, qc);

        int rc = dbc->backend->get_procedure_columns(dbc->backend_conn,
                                                     cat, sch, proc, col,
                                                     &stmt->op);
        free(cat); free(sch); free(proc); free(col);

        if (rc != 0) return catalog_failed(stmt, "SQLProcedureColumns");
        return catalog_delegate(stmt, "SQLProcedureColumns");
    }

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)ProcName;    (void)NameLength3;
    (void)ColumnName;  (void)NameLength4;

    /* No hook: an engine with no such objects, and the empty result set with
     * the right column shape is the correct answer. fetch_started matters —
     * on a connected statement without it, SQLFetch would go to the backend
     * with a NULL operation handle and fail instead of reporting
     * SQL_NO_DATA. */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_procedure_columns_metadata(stmt);

    return SQL_SUCCESS;
}

/* ── Helper: setup standard SQLTablePrivileges result metadata ── */

static void setup_table_privileges_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } tp_cols[] = {
        {"TABLE_CAT",   SQL_VARCHAR, 128},
        {"TABLE_SCHEM", SQL_VARCHAR, 128},
        {"TABLE_NAME",  SQL_VARCHAR, 128},
        {"GRANTOR",     SQL_VARCHAR, 128},
        {"GRANTEE",     SQL_VARCHAR, 128},
        {"PRIVILEGE",   SQL_VARCHAR, 128},
        {"IS_GRANTABLE", SQL_VARCHAR, 3},
    };

    stmt->num_cols = 7;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 7; i++) {
        strncpy((char *)stmt->columns[i].name, tp_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(tp_cols[i].name);
        stmt->columns[i].sql_type = tp_cols[i].sql_type;
        stmt->columns[i].column_size = tp_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLTablePrivileges (empty result set) ─────────── */

static SQLRETURN sqltableprivileges_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    TableName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (dbc && dbc->connected && dbc->backend &&
        dbc->backend->get_table_privileges) {
        SQLULEN mid = stmt->metadata_id;
        const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
        char *cat = catalog_arg_dup(CatalogName, NameLength1, mid, qc);
        char *sch = catalog_arg_dup(SchemaName, NameLength2, mid, qc);
        char *tab = catalog_arg_dup(TableName, NameLength3, mid, qc);

        int rc = dbc->backend->get_table_privileges(dbc->backend_conn,
                                                    cat, sch, tab, &stmt->op);
        free(cat); free(sch); free(tab);

        if (rc != 0) return catalog_failed(stmt, "SQLTablePrivileges");
        return catalog_delegate(stmt, "SQLTablePrivileges");
    }

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;

    /* No hook: an engine with no such objects, and the empty result set with
     * the right column shape is the correct answer. fetch_started matters —
     * on a connected statement without it, SQLFetch would go to the backend
     * with a NULL operation handle and fail instead of reporting
     * SQL_NO_DATA. */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_table_privileges_metadata(stmt);

    return SQL_SUCCESS;
}

/* ── Helper: setup standard SQLColumnPrivileges result metadata  */

static void setup_column_privileges_metadata(argus_stmt_t *stmt)
{
    static const struct {
        const char *name;
        SQLSMALLINT sql_type;
        SQLULEN size;
    } cp_cols[] = {
        {"TABLE_CAT",    SQL_VARCHAR, 128},
        {"TABLE_SCHEM",  SQL_VARCHAR, 128},
        {"TABLE_NAME",   SQL_VARCHAR, 128},
        {"COLUMN_NAME",  SQL_VARCHAR, 128},
        {"GRANTOR",      SQL_VARCHAR, 128},
        {"GRANTEE",      SQL_VARCHAR, 128},
        {"PRIVILEGE",    SQL_VARCHAR, 128},
        {"IS_GRANTABLE", SQL_VARCHAR, 3},
    };

    stmt->num_cols = 8;
    stmt->metadata_fetched = true;
    for (int i = 0; i < 8; i++) {
        strncpy((char *)stmt->columns[i].name, cp_cols[i].name,
                ARGUS_MAX_COLUMN_NAME - 1);
        stmt->columns[i].name_len = (SQLSMALLINT)strlen(cp_cols[i].name);
        stmt->columns[i].sql_type = cp_cols[i].sql_type;
        stmt->columns[i].column_size = cp_cols[i].size;
        stmt->columns[i].decimal_digits = 0;
        stmt->columns[i].nullable = SQL_NULLABLE;
    }
}

/* ── ODBC API: SQLColumnPrivileges (empty result set) ────────── */

static SQLRETURN sqlcolumnprivileges_impl(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,  SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    {
        SQLRETURN len_ret = CATALOG_CHECK_LENGTHS(stmt, NameLength1, NameLength2, NameLength3,
                                                        NameLength4);
        if (len_ret != SQL_SUCCESS) return len_ret;
    }
    {
        SQLRETURN id_ret = CATALOG_CHECK_IDENTIFIERS(stmt, CatalogName,
                                                    SchemaName,
                                                    TableName,
                                                    ColumnName);
        if (id_ret != SQL_SUCCESS) return id_ret;
    }
    argus_stmt_reset(stmt);

    argus_dbc_t *dbc = stmt->dbc;
    if (dbc && dbc->connected && dbc->backend &&
        dbc->backend->get_column_privileges) {
        SQLULEN mid = stmt->metadata_id;
        const char *qc = argus_dialect_for(stmt->dbc)->quote_char;
        char *cat = catalog_arg_dup(CatalogName, NameLength1, mid, qc);
        char *sch = catalog_arg_dup(SchemaName, NameLength2, mid, qc);
        char *tab = catalog_arg_dup(TableName, NameLength3, mid, qc);
        char *col = catalog_arg_dup(ColumnName, NameLength4, mid, qc);

        int rc = dbc->backend->get_column_privileges(dbc->backend_conn,
                                                     cat, sch, tab, col,
                                                     &stmt->op);
        free(cat); free(sch); free(tab); free(col);

        if (rc != 0) return catalog_failed(stmt, "SQLColumnPrivileges");
        return catalog_delegate(stmt, "SQLColumnPrivileges");
    }

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)ColumnName;  (void)NameLength4;

    /* No hook: an engine with no such objects, and the empty result set with
     * the right column shape is the correct answer. fetch_started matters —
     * on a connected statement without it, SQLFetch would go to the backend
     * with a NULL operation handle and fail instead of reporting
     * SQL_NO_DATA. */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    stmt->fetch_started = true;
    setup_column_privileges_metadata(stmt);

    return SQL_SUCCESS;
}

/* ── Thread-safety wrappers ─────────────────────────────────────
 * ODBC requires per-handle thread safety; every entry point above
 * runs under its handle's mutex. Bodies were renamed *_impl and
 * must never call another locked public entry point (GMutex is
 * non-recursive). */

SQLRETURN SQL_API SQLTables(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName,  SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,   SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,    SQLSMALLINT NameLength3,
    SQLCHAR   *TableType,    SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqltables_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, TableType, NameLength4);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLColumns(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName,  SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,   SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,    SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,   SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlcolumns_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLGetTypeInfo(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT DataType)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlgettypeinfo_impl(StatementHandle, DataType);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLStatistics(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR     *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR     *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique,
    SQLUSMALLINT Reserved)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlstatistics_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, Unique, Reserved);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLSpecialColumns(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT IdentifierType,
    SQLCHAR     *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR     *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR     *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope,
    SQLUSMALLINT Nullable)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlspecialcolumns_impl(StatementHandle, IdentifierType, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, Scope, Nullable);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLPrimaryKeys(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlprimarykeys_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *PKCatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *PKSchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *PKTableName,   SQLSMALLINT NameLength3,
    SQLCHAR   *FKCatalogName, SQLSMALLINT NameLength4,
    SQLCHAR   *FKSchemaName,  SQLSMALLINT NameLength5,
    SQLCHAR   *FKTableName,   SQLSMALLINT NameLength6)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlforeignkeys_impl(StatementHandle, PKCatalogName, NameLength1, PKSchemaName, NameLength2, PKTableName, NameLength3, FKCatalogName, NameLength4, FKSchemaName, NameLength5, FKTableName, NameLength6);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLProcedures(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *ProcName,    SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlprocedures_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, ProcName, NameLength3);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLProcedureColumns(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *ProcName,    SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,  SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlprocedurecolumns_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, ProcName, NameLength3, ColumnName, NameLength4);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLTablePrivileges(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqltableprivileges_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}

SQLRETURN SQL_API SQLColumnPrivileges(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,  SQLSMALLINT NameLength4)
{
    argus_stmt_t *stmt_h = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt_h)) return SQL_INVALID_HANDLE;
    ARGUS_STMT_LOCK(stmt_h);
    SQLRETURN ret = sqlcolumnprivileges_impl(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
    ARGUS_STMT_UNLOCK(stmt_h);
    return ret;
}
