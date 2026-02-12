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

/* ── ODBC API: SQLStatistics (empty result set) ──────────────── */

SQLRETURN SQL_API SQLStatistics(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR     *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR     *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique,
    SQLUSMALLINT Reserved)
{
    (void)CatalogName; (void)NameLength1;
    (void)SchemaName;  (void)NameLength2;
    (void)TableName;   (void)NameLength3;
    (void)Unique;      (void)Reserved;

    argus_stmt_t *stmt = (argus_stmt_t *)StatementHandle;
    if (!argus_valid_stmt(stmt)) return SQL_INVALID_HANDLE;

    argus_diag_clear(&stmt->diag);
    argus_stmt_reset(stmt);

    /* Return empty result set - Hive doesn't have indexes */
    stmt->executed = true;
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
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

    /* Return empty result set */
    stmt->executed = true;
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
}

/* ── ODBC API: SQLPrimaryKeys (empty result set) ─────────────── */

SQLRETURN SQL_API SQLPrimaryKeys(
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
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
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
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
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
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
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
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
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
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
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
    stmt->num_cols = 0;
    stmt->row_cache.exhausted = true;

    return SQL_SUCCESS;
}
