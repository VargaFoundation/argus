#include "mywire_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/*
 * Catalog operations are implemented as SQL queries against
 * information_schema, which MySQL, MariaDB, StarRocks, Doris and
 * ClickHouse all expose.
 *
 * The MySQL data model has a single namespace level (the database), so
 * we follow the MySQL Connector/ODBC convention: a database is reported
 * as a CATALOG (TABLE_CAT) and the schema column is left empty.
 */

/* ── Helper: run a query and hand back the operation ─────────── */

static int mywire_run(argus_backend_conn_t conn, const char *query,
                      argus_backend_op_t *out_op)
{
    return mywire_execute(conn, query, out_op);
}

/* ── GetTables via information_schema.tables ─────────────────── */

int mywire_get_tables(argus_backend_conn_t conn,
                      const char *catalog,
                      const char *schema,
                      const char *table_name,
                      const char *table_types,
                      argus_backend_op_t *out_op)
{
    char query[2048];
    int off = snprintf(query, sizeof(query),
        "SELECT "
        "table_schema AS TABLE_CAT, "
        "NULL AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END "
        "AS TABLE_TYPE, "
        "table_comment AS REMARKS "
        "FROM information_schema.tables WHERE 1=1");

    /* Database is reported as catalog; accept either arg as the filter. */
    if (catalog && *catalog)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema = '%s'", catalog);
    else if (schema && *schema)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema LIKE '%s'", schema);
    if (table_name && *table_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_name LIKE '%s'", table_name);
    if (table_types && *table_types) {
        /* The ODBC type list ("TABLE,VIEW", possibly quoted) must be turned
         * into a proper quoted IN list, translating the ODBC name "TABLE" to
         * information_schema's "BASE TABLE". */
        char in_list[256];
        int n = 0;
        char tmp[256];
        strncpy(tmp, table_types, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';
        char *save = NULL;
        for (char *tok = strtok_r(tmp, ",", &save); tok;
             tok = strtok_r(NULL, ",", &save)) {
            while (*tok == ' ' || *tok == '\'' || *tok == '"') tok++;
            char *end = tok + strlen(tok);
            while (end > tok &&
                   (end[-1] == ' ' || end[-1] == '\'' || end[-1] == '"'))
                *--end = '\0';
            if (!*tok) continue;
            const char *mapped =
                (strcmp(tok, "TABLE") == 0) ? "BASE TABLE" : tok;
            n += snprintf(in_list + n, sizeof(in_list) - (size_t)n,
                          "%s'%s'", n ? "," : "", mapped);
        }
        if (n > 0)
            off += snprintf(query + off, sizeof(query) - (size_t)off,
                            " AND table_type IN (%s)", in_list);
    }

    snprintf(query + off, sizeof(query) - (size_t)off,
             " ORDER BY table_schema, table_name");

    return mywire_run(conn, query, out_op);
}

/* ── GetColumns via information_schema.columns ───────────────── */

int mywire_get_columns(argus_backend_conn_t conn,
                       const char *catalog,
                       const char *schema,
                       const char *table_name,
                       const char *column_name,
                       argus_backend_op_t *out_op)
{
    char query[2048];
    int off = snprintf(query, sizeof(query),
        "SELECT "
        "table_schema AS TABLE_CAT, "
        "NULL AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "column_name AS COLUMN_NAME, "
        "data_type AS TYPE_NAME, "
        "ordinal_position AS ORDINAL_POSITION, "
        "is_nullable AS IS_NULLABLE, "
        "column_comment AS REMARKS "
        "FROM information_schema.columns WHERE 1=1");

    if (catalog && *catalog)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema = '%s'", catalog);
    else if (schema && *schema)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema LIKE '%s'", schema);
    if (table_name && *table_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_name LIKE '%s'", table_name);
    if (column_name && *column_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND column_name LIKE '%s'", column_name);

    snprintf(query + off, sizeof(query) - (size_t)off,
             " ORDER BY table_schema, table_name, ordinal_position");

    return mywire_run(conn, query, out_op);
}

/* ── GetSchemas (empty: databases are reported as catalogs) ──── */

int mywire_get_schemas(argus_backend_conn_t conn,
                       const char *catalog,
                       const char *schema,
                       argus_backend_op_t *out_op)
{
    (void)catalog;
    (void)schema;

    /* No sub-database schema level; return the correct shape, no rows. */
    const char *query =
        "SELECT schema_name AS TABLE_SCHEM, "
        "catalog_name AS TABLE_CATALOG "
        "FROM information_schema.schemata WHERE 1=0";

    return mywire_run(conn, query, out_op);
}

/* ── GetCatalogs (one row per database) ──────────────────────── */

int mywire_get_catalogs(argus_backend_conn_t conn,
                        argus_backend_op_t *out_op)
{
    return mywire_run(conn, "SHOW DATABASES", out_op);
}

/* ── GetTypeInfo (synthetic, engine-portable via UNION ALL) ──── */

int mywire_get_type_info(argus_backend_conn_t conn,
                         SQLSMALLINT sql_type,
                         argus_backend_op_t *out_op)
{
    (void)sql_type;

    /*
     * VALUES ROW(...) syntax is not portable across MySQL/StarRocks/Doris/
     * ClickHouse, so the type list is built with UNION ALL, carrying the
     * ODBC SQLGetTypeInfo column aliases on the first branch only.
     */
    const char *query =
        "SELECT 'tinyint' AS TYPE_NAME, -6 AS DATA_TYPE, 3 AS COLUMN_SIZE, "
        "NULL AS LITERAL_PREFIX, NULL AS LITERAL_SUFFIX, NULL AS CREATE_PARAMS, "
        "1 AS NULLABLE, 0 AS CASE_SENSITIVE, 2 AS SEARCHABLE, "
        "0 AS UNSIGNED_ATTRIBUTE, 0 AS FIXED_PREC_SCALE, 0 AS AUTO_UNIQUE_VALUE, "
        "NULL AS LOCAL_TYPE_NAME, 0 AS MINIMUM_SCALE, 0 AS MAXIMUM_SCALE, "
        "10 AS NUM_PREC_RADIX "
        "UNION ALL SELECT 'smallint', 5, 5, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 0, 10 "
        "UNION ALL SELECT 'int', 4, 10, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 0, 10 "
        "UNION ALL SELECT 'bigint', -5, 19, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 0, 10 "
        "UNION ALL SELECT 'float', 7, 7, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 0, 10 "
        "UNION ALL SELECT 'double', 8, 15, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 0, 10 "
        "UNION ALL SELECT 'decimal', 3, 38, NULL, NULL, 'precision,scale', 1, 0, 2, 0, 0, 0, NULL, 0, 38, 10 "
        "UNION ALL SELECT 'varchar', 12, 65535, '''', '''', 'length', 1, 1, 3, 0, 0, 0, NULL, 0, 0, NULL "
        "UNION ALL SELECT 'char', 1, 255, '''', '''', 'length', 1, 1, 3, 0, 0, 0, NULL, 0, 0, NULL "
        "UNION ALL SELECT 'date', 91, 10, '''', '''', NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 0, NULL "
        "UNION ALL SELECT 'datetime', 93, 19, '''', '''', NULL, 1, 0, 2, 0, 0, 0, NULL, 0, 6, NULL";

    return mywire_run(conn, query, out_op);
}

/* ── GetPrimaryKeys via information_schema.columns ───────────── */

int mywire_get_primary_keys(argus_backend_conn_t conn,
                            const char *catalog,
                            const char *schema,
                            const char *table_name,
                            argus_backend_op_t *out_op)
{
    char query[2048];
    int off = snprintf(query, sizeof(query),
        "SELECT "
        "table_schema AS TABLE_CAT, "
        "NULL AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "column_name AS COLUMN_NAME, "
        "ordinal_position AS KEY_SEQ, "
        "'PRIMARY' AS PK_NAME "
        "FROM information_schema.columns "
        "WHERE column_key = 'PRI'");

    if (catalog && *catalog)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema = '%s'", catalog);
    else if (schema && *schema)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_schema = '%s'", schema);
    if (table_name && *table_name)
        off += snprintf(query + off, sizeof(query) - (size_t)off,
                        " AND table_name = '%s'", table_name);

    snprintf(query + off, sizeof(query) - (size_t)off,
             " ORDER BY table_schema, table_name, ordinal_position");

    return mywire_run(conn, query, out_op);
}
