#include "trino_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/*
 * Trino does not have dedicated catalog API endpoints like Hive/Impala.
 * Instead, we implement catalog operations via SQL queries against
 * information_schema.
 */

/* ── Helper: execute a SQL query and return the operation ─────── */

static int trino_execute_query(trino_conn_t *conn, const char *query,
                                trino_operation_t **out_op)
{
    argus_backend_op_t raw_op = NULL;
    int rc = trino_execute((argus_backend_conn_t)conn, query, &raw_op);
    if (rc != 0) return -1;
    *out_op = (trino_operation_t *)raw_op;
    return 0;
}

/* Forward declaration */
int trino_execute(argus_backend_conn_t raw_conn,
                  const char *query,
                  argus_backend_op_t *out_op);

/* ── GetTables via information_schema ────────────────────────── */

int trino_get_tables(argus_backend_conn_t raw_conn,
                     const char *catalog,
                     const char *schema,
                     const char *table_name,
                     const char *table_types,
                     argus_backend_op_t *out_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn) return -1;

    char query[2048];
    int off = snprintf(query, sizeof(query),
        "SELECT "
        "table_catalog AS TABLE_CAT, "
        "table_schema AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "table_type AS TABLE_TYPE, "
        "CAST(NULL AS VARCHAR) AS REMARKS "
        "FROM information_schema.tables WHERE 1=1");

    if (catalog && *catalog)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_catalog = '%s'", catalog);
    if (schema && *schema)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema LIKE '%s'", schema);
    if (table_name && *table_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_name LIKE '%s'", table_name);
    if (table_types && *table_types)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_type IN ('%s')", table_types);

    snprintf(query + off, sizeof(query) - (size_t)off,
             " ORDER BY table_catalog, table_schema, table_name");

    trino_operation_t *op = NULL;
    int rc = trino_execute_query(conn, query, &op);
    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetColumns via information_schema ───────────────────────── */

int trino_get_columns(argus_backend_conn_t raw_conn,
                      const char *catalog,
                      const char *schema,
                      const char *table_name,
                      const char *column_name,
                      argus_backend_op_t *out_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn) return -1;

    char query[2048];
    int off = snprintf(query, sizeof(query),
        "SELECT "
        "table_catalog AS TABLE_CAT, "
        "table_schema AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "column_name AS COLUMN_NAME, "
        "data_type AS TYPE_NAME, "
        "ordinal_position AS ORDINAL_POSITION, "
        "is_nullable AS IS_NULLABLE "
        "FROM information_schema.columns WHERE 1=1");

    if (catalog && *catalog)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_catalog = '%s'", catalog);
    if (schema && *schema)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema LIKE '%s'", schema);
    if (table_name && *table_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_name LIKE '%s'", table_name);
    if (column_name && *column_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND column_name LIKE '%s'", column_name);

    snprintf(query + off, sizeof(query) - (size_t)off,
             " ORDER BY table_catalog, table_schema, table_name, ordinal_position");

    trino_operation_t *op = NULL;
    int rc = trino_execute_query(conn, query, &op);
    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetTypeInfo (static type list) ──────────────────────────── */

int trino_get_type_info(argus_backend_conn_t raw_conn,
                        SQLSMALLINT sql_type,
                        argus_backend_op_t *out_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn) return -1;
    (void)sql_type;

    /*
     * Trino doesn't have a dedicated type info endpoint.
     * We build a synthetic result set via UNION ALL of VALUES.
     */
    const char *query =
        "SELECT * FROM (VALUES "
        "('boolean', -7, 1, NULL, NULL, NULL, 1, 0, 3, NULL, 0, NULL, 'boolean', NULL, NULL, NULL, NULL, 10),"
        "('tinyint', -6, 3, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'tinyint', NULL, NULL, NULL, NULL, 10),"
        "('smallint', 5, 5, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'smallint', NULL, NULL, NULL, NULL, 10),"
        "('integer', 4, 10, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'integer', NULL, NULL, NULL, NULL, 10),"
        "('bigint', -5, 19, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'bigint', NULL, NULL, NULL, NULL, 10),"
        "('real', 7, 7, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'real', NULL, NULL, NULL, NULL, 10),"
        "('double', 8, 15, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'double', NULL, NULL, NULL, NULL, 10),"
        "('decimal', 3, 38, NULL, NULL, 'precision,scale', 1, 0, 2, 0, 0, 0, 'decimal', 0, 38, NULL, NULL, 10),"
        "('varchar', 12, 65535, '''', '''', 'max_length', 1, 1, 3, NULL, 0, NULL, 'varchar', NULL, NULL, NULL, NULL, NULL),"
        "('char', 1, 255, '''', '''', 'length', 1, 1, 3, NULL, 0, NULL, 'char', NULL, NULL, NULL, NULL, NULL),"
        "('varbinary', -3, 65535, NULL, NULL, 'max_length', 1, 0, 3, NULL, 0, NULL, 'varbinary', NULL, NULL, NULL, NULL, NULL),"
        "('date', 91, 10, '''', '''', NULL, 1, 0, 2, NULL, 0, NULL, 'date', NULL, NULL, NULL, NULL, NULL),"
        "('timestamp', 93, 29, '''', '''', 'precision', 1, 0, 2, NULL, 0, NULL, 'timestamp', NULL, NULL, NULL, NULL, NULL)"
        ") AS t(TYPE_NAME, DATA_TYPE, PRECISION1, LITERAL_PREFIX, LITERAL_SUFFIX, "
        "CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE, UNSIGNED_ATTRIBUTE, "
        "FIXED_PREC_SCALE, AUTO_UNIQUE_VALUE, LOCAL_TYPE_NAME, MINIMUM_SCALE, "
        "MAXIMUM_SCALE, SQL_DATA_TYPE, SQL_DATETIME_SUB, NUM_PREC_RADIX)";

    trino_operation_t *op = NULL;
    int rc = trino_execute_query(conn, query, &op);
    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetSchemas via information_schema ───────────────────────── */

int trino_get_schemas(argus_backend_conn_t raw_conn,
                      const char *catalog,
                      const char *schema,
                      argus_backend_op_t *out_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn) return -1;

    char query[1024];
    int off = snprintf(query, sizeof(query),
        "SELECT DISTINCT "
        "schema_name AS TABLE_SCHEM, "
        "catalog_name AS TABLE_CATALOG "
        "FROM information_schema.schemata WHERE 1=1");

    if (catalog && *catalog)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND catalog_name = '%s'", catalog);
    if (schema && *schema)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND schema_name LIKE '%s'", schema);

    snprintf(query + off, sizeof(query) - (size_t)off,
             " ORDER BY catalog_name, schema_name");

    trino_operation_t *op = NULL;
    int rc = trino_execute_query(conn, query, &op);
    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}

/* ── GetCatalogs ─────────────────────────────────────────────── */

int trino_get_catalogs(argus_backend_conn_t raw_conn,
                       argus_backend_op_t *out_op)
{
    trino_conn_t *conn = (trino_conn_t *)raw_conn;
    if (!conn) return -1;

    const char *query = "SHOW CATALOGS";

    trino_operation_t *op = NULL;
    int rc = trino_execute_query(conn, query, &op);
    if (rc != 0) return -1;

    *out_op = op;
    return 0;
}
