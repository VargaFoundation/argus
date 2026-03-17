#include "argus/handle.h"
#include "argus/odbc_api.h"
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

extern char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len);

/* ── Helper: dispatch catalog operation and setup result set ─── */

static SQLRETURN catalog_dispatch(argus_stmt_t *stmt)
{
    stmt->executed = true;

    /* Get metadata for the catalog result set */
    if (stmt->dbc->backend->get_result_metadata) {
        int ncols = 0;
        int rc = stmt->dbc->backend->get_result_metadata(
            stmt->dbc->backend_conn, stmt->op,
            stmt->columns, &ncols);
        if (rc == 0 && ncols > 0) {
            /* Validate minimum column count for known catalog functions */
            if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;
            stmt->num_cols = ncols;
            stmt->metadata_fetched = true;
        }
    }

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLTables ─────────────────────────────────────── */

SQLRETURN SQL_API SQLTables(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName,  SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,   SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,    SQLSMALLINT NameLength3,
    SQLCHAR   *TableType,    SQLSMALLINT NameLength4)
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

    if (!dbc->backend->get_tables) {
        return argus_set_not_implemented(&stmt->diag, "SQLTables");
    }

    char *catalog    = argus_str_dup_short(CatalogName, NameLength1);
    char *schema     = argus_str_dup_short(SchemaName,  NameLength2);
    char *table_name = argus_str_dup_short(TableName,   NameLength3);
    char *table_type = argus_str_dup_short(TableType,   NameLength4);

    int rc = dbc->backend->get_tables(
        dbc->backend_conn,
        catalog, schema, table_name, table_type,
        &stmt->op);

    free(catalog);
    free(schema);
    free(table_name);
    free(table_type);

    if (rc != 0) {
        if (stmt->diag.count == 0)
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Failed to get tables", 0);
        return SQL_ERROR;
    }

    return catalog_dispatch(stmt);
}

/* ── ODBC API: SQLColumns ────────────────────────────────────── */

SQLRETURN SQL_API SQLColumns(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName,  SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,   SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,    SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,   SQLSMALLINT NameLength4)
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

    if (!dbc->backend->get_columns) {
        return argus_set_not_implemented(&stmt->diag, "SQLColumns");
    }

    char *catalog     = argus_str_dup_short(CatalogName, NameLength1);
    char *schema      = argus_str_dup_short(SchemaName,  NameLength2);
    char *table_name  = argus_str_dup_short(TableName,   NameLength3);
    char *column_name = argus_str_dup_short(ColumnName,  NameLength4);

    int rc = dbc->backend->get_columns(
        dbc->backend_conn,
        catalog, schema, table_name, column_name,
        &stmt->op);

    free(catalog);
    free(schema);
    free(table_name);
    free(column_name);

    if (rc != 0) {
        if (stmt->diag.count == 0)
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Failed to get columns", 0);
        return SQL_ERROR;
    }

    return catalog_dispatch(stmt);
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

SQLRETURN SQL_API SQLGetTypeInfo(
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

SQLRETURN SQL_API SQLStatistics(
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
    setup_statistics_metadata(stmt);

    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)Unique;      (void)Reserved;

    return SQL_SUCCESS;
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

SQLRETURN SQL_API SQLSpecialColumns(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT IdentifierType,
    SQLCHAR     *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR     *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR     *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope,
    SQLUSMALLINT Nullable)
{
    (void)IdentifierType;
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)Scope;       (void)Nullable;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    stmt->executed = true;
    stmt->row_cache.exhausted = true;
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

SQLRETURN SQL_API SQLPrimaryKeys(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3)
{
    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
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

        if (rc != 0) {
            if (stmt->diag.count == 0)
                argus_set_error(&stmt->diag, "HY000",
                                "[Argus] Failed to get primary keys", 0);
            return SQL_ERROR;
        }

        return catalog_dispatch(stmt);
    }

    /* Return empty result set with proper metadata */
    stmt->executed = true;
    stmt->row_cache.exhausted = true;
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

SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *PKCatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *PKSchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *PKTableName,   SQLSMALLINT NameLength3,
    SQLCHAR   *FKCatalogName, SQLSMALLINT NameLength4,
    SQLCHAR   *FKSchemaName,  SQLSMALLINT NameLength5,
    SQLCHAR   *FKTableName,   SQLSMALLINT NameLength6)
{
    (void)PKCatalogName; (void)NameLength1;
    (void)PKSchemaName;  (void)NameLength2;
    (void)PKTableName;   (void)NameLength3;
    (void)FKCatalogName; (void)NameLength4;
    (void)FKSchemaName;  (void)NameLength5;
    (void)FKTableName;   (void)NameLength6;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    stmt->executed = true;
    stmt->row_cache.exhausted = true;
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

SQLRETURN SQL_API SQLProcedures(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *ProcName,    SQLSMALLINT NameLength3)
{
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)ProcName;    (void)NameLength3;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    stmt->executed = true;
    stmt->row_cache.exhausted = true;
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

SQLRETURN SQL_API SQLProcedureColumns(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *ProcName,    SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,  SQLSMALLINT NameLength4)
{
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)ProcName;    (void)NameLength3;
    (void)ColumnName;  (void)NameLength4;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    stmt->executed = true;
    stmt->row_cache.exhausted = true;
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

SQLRETURN SQL_API SQLTablePrivileges(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3)
{
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    stmt->executed = true;
    stmt->row_cache.exhausted = true;
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

SQLRETURN SQL_API SQLColumnPrivileges(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR   *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR   *TableName,   SQLSMALLINT NameLength3,
    SQLCHAR   *ColumnName,  SQLSMALLINT NameLength4)
{
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)ColumnName;  (void)NameLength4;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    stmt->executed = true;
    stmt->row_cache.exhausted = true;
    setup_column_privileges_metadata(stmt);

    return SQL_SUCCESS;
}
