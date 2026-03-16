#include "argus/handle.h"
#include "argus/odbc_api.h"
#include <stdlib.h>
#include <string.h>

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

    if (!dbc->backend->get_type_info) {
        return argus_set_not_implemented(&stmt->diag, "SQLGetTypeInfo");
    }

    int rc = dbc->backend->get_type_info(
        dbc->backend_conn, DataType, &stmt->op);

    if (rc != 0) {
        if (stmt->diag.count == 0)
            argus_set_error(&stmt->diag, "HY000",
                            "[Argus] Failed to get type info", 0);
        return SQL_ERROR;
    }

    return catalog_dispatch(stmt);
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
