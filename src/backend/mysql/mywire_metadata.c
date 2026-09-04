#include "mywire_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

/*
 * Catalog operations are implemented as SQL queries against
 * information_schema, which MySQL, MariaDB, StarRocks, Doris and
 * ClickHouse all expose.
 *
 * The MySQL data model has a single namespace level (the database), so
 * we follow the MySQL Connector/ODBC convention: a database is reported
 * as a CATALOG (TABLE_CAT) and the schema column is left empty.
 *
 * Every ODBC search pattern reaches the query text through
 * mysql_real_escape_string(): the patterns come from the application, and
 * the client library knows the connection charset and whether the server
 * runs with NO_BACKSLASH_ESCAPES — the driver cannot guess either.
 */

/* ── Helper: run a query and hand back the operation ─────────── */

static int mywire_run(argus_backend_conn_t conn, const char *query,
                      argus_backend_op_t *out_op)
{
    return mywire_execute(conn, query, out_op);
}

/* ── Query builders (pure: an initialised MYSQL handle, unit-tested) ── */

/* Append " AND <column> <op> '<value>'" with the value escaped for this
 * connection; a NULL or empty value adds nothing. Returns false on OOM. */
static bool append_filter(GString *q, MYSQL *mysql, const char *column,
                          const char *op, const char *value)
{
    if (!value || !*value) return true;

    /* A pattern with no unescaped wildcard names one thing: equality, on the
     * value the escapes stood for. Otherwise LIKE with the escape character
     * SQLGetInfo advertises -- which happens to be MySQL's own default, but
     * saying it makes the query mean the same on a server that changed it. */
    bool is_like = strcmp(op, "LIKE") == 0;
    char *exact = is_like ? argus_sql_pattern_literal(value) : NULL;
    const char *v = exact ? exact : value;

    size_t len = strlen(v);
    char *esc = malloc(len * 2 + 1);
    if (!esc) { free(exact); return false; }
    unsigned long n = mysql_real_escape_string(mysql, esc, v,
                                               (unsigned long)len);
    g_string_append_printf(q, " AND %s %s '", column,
                           is_like ? (exact ? "=" : "LIKE") : op);
    g_string_append_len(q, esc, (gssize)n);
    g_string_append_c(q, '\'');
    if (is_like && !exact) g_string_append(q, " ESCAPE '\\\\'");
    free(esc);
    free(exact);
    return true;
}

/* The ODBC type list ("TABLE,VIEW", possibly quoted) must be turned into a
 * proper quoted IN list, translating the ODBC name "TABLE" to
 * information_schema's "BASE TABLE". */
static bool append_table_types(GString *q, MYSQL *mysql,
                               const char *table_types)
{
    if (!table_types || !*table_types) return true;
    GString *in_list = g_string_new(NULL);
    char **toks = g_strsplit(table_types, ",", -1);
    bool ok = true;
    for (int i = 0; ok && toks[i]; i++) {
        char *tok = toks[i];
        while (*tok == ' ' || *tok == '\'' || *tok == '"') tok++;
        char *end = tok + strlen(tok);
        while (end > tok &&
               (end[-1] == ' ' || end[-1] == '\'' || end[-1] == '"'))
            *--end = '\0';
        if (!*tok) continue;
        const char *mapped = (strcmp(tok, "TABLE") == 0) ? "BASE TABLE" : tok;
        size_t len = strlen(mapped);
        char *esc = malloc(len * 2 + 1);
        if (!esc) { ok = false; break; }
        unsigned long n = mysql_real_escape_string(mysql, esc, mapped,
                                                   (unsigned long)len);
        if (in_list->len) g_string_append_c(in_list, ',');
        g_string_append_c(in_list, '\'');
        g_string_append_len(in_list, esc, (gssize)n);
        g_string_append_c(in_list, '\'');
        free(esc);
    }
    g_strfreev(toks);
    if (ok && in_list->len)
        g_string_append_printf(q, " AND table_type IN (%s)", in_list->str);
    g_string_free(in_list, TRUE);
    return ok;
}

static char *finish(GString *q, bool ok, const char *order_by)
{
    if (!ok) { g_string_free(q, TRUE); return NULL; }
    g_string_append(q, order_by);
    return g_string_free(q, FALSE);
}

char *mywire_build_tables_query(MYSQL *mysql, const char *catalog,
                                const char *schema, const char *table_name,
                                const char *table_types)
{
    GString *q = g_string_new(
        "SELECT "
        "table_schema AS TABLE_CAT, "
        "NULL AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END "
        "AS TABLE_TYPE, "
        "table_comment AS REMARKS "
        "FROM information_schema.tables WHERE 1=1");
    bool ok;
    /* Database is reported as catalog; accept either arg as the filter. */
    if (catalog && *catalog)
        ok = append_filter(q, mysql, "table_schema", "=", catalog);
    else
        ok = append_filter(q, mysql, "table_schema", "LIKE", schema);
    ok = ok && append_filter(q, mysql, "table_name", "LIKE", table_name) &&
         append_table_types(q, mysql, table_types);
    return finish(q, ok, " ORDER BY table_schema, table_name");
}

char *mywire_build_columns_query(MYSQL *mysql, const char *catalog,
                                 const char *schema, const char *table_name,
                                 const char *column_name)
{
    GString *q = g_string_new(
        "SELECT "
        "table_schema AS TABLE_CAT, "
        "NULL AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "column_name AS COLUMN_NAME, "
        /* DATA_TYPE: numeric ODBC SQL type code (column 5 per the spec). */
        "CASE data_type "
        "WHEN 'bigint' THEN -5 "
        "WHEN 'int' THEN 4 WHEN 'integer' THEN 4 WHEN 'mediumint' THEN 4 "
        "WHEN 'smallint' THEN 5 "
        "WHEN 'tinyint' THEN -6 "
        "WHEN 'double' THEN 8 WHEN 'float' THEN 7 "
        "WHEN 'decimal' THEN 3 WHEN 'numeric' THEN 3 "
        "WHEN 'date' THEN 91 "
        "WHEN 'datetime' THEN 93 WHEN 'timestamp' THEN 93 "
        "ELSE 12 END AS DATA_TYPE, "
        "data_type AS TYPE_NAME, "
        "ordinal_position AS ORDINAL_POSITION, "
        "is_nullable AS IS_NULLABLE, "
        "column_comment AS REMARKS "
        "FROM information_schema.columns WHERE 1=1");
    bool ok;
    if (catalog && *catalog)
        ok = append_filter(q, mysql, "table_schema", "=", catalog);
    else
        ok = append_filter(q, mysql, "table_schema", "LIKE", schema);
    ok = ok && append_filter(q, mysql, "table_name", "LIKE", table_name) &&
         append_filter(q, mysql, "column_name", "LIKE", column_name);
    return finish(q, ok, " ORDER BY table_schema, table_name, ordinal_position");
}

char *mywire_build_primary_keys_query(MYSQL *mysql, const char *catalog,
                                      const char *schema,
                                      const char *table_name)
{
    GString *q = g_string_new(
        "SELECT "
        "table_schema AS TABLE_CAT, "
        "NULL AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "column_name AS COLUMN_NAME, "
        "ordinal_position AS KEY_SEQ, "
        "'PRIMARY' AS PK_NAME "
        "FROM information_schema.columns "
        "WHERE column_key = 'PRI'");
    bool ok;
    if (catalog && *catalog)
        ok = append_filter(q, mysql, "table_schema", "=", catalog);
    else
        ok = append_filter(q, mysql, "table_schema", "=", schema);
    ok = ok && append_filter(q, mysql, "table_name", "=", table_name);
    return finish(q, ok, " ORDER BY table_schema, table_name, ordinal_position");
}

static int run_built_query(argus_backend_conn_t conn, char *query,
                           argus_backend_op_t *out_op)
{
    if (!query) return -1;
    int rc = mywire_run(conn, query, out_op);
    g_free(query);
    return rc;
}

/* ── GetTables via information_schema.tables ─────────────────── */

int mywire_get_tables(argus_backend_conn_t conn,
                      const char *catalog,
                      const char *schema,
                      const char *table_name,
                      const char *table_types,
                      argus_backend_op_t *out_op)
{
    mywire_conn_t *c = (mywire_conn_t *)conn;
    if (!c || !c->mysql) return -1;
    return run_built_query(conn,
                           mywire_build_tables_query(c->mysql, catalog, schema,
                                                     table_name, table_types),
                           out_op);
}

/* ── GetColumns via information_schema.columns ───────────────── */

int mywire_get_columns(argus_backend_conn_t conn,
                       const char *catalog,
                       const char *schema,
                       const char *table_name,
                       const char *column_name,
                       argus_backend_op_t *out_op)
{
    mywire_conn_t *c = (mywire_conn_t *)conn;
    if (!c || !c->mysql) return -1;
    return run_built_query(conn,
                           mywire_build_columns_query(c->mysql, catalog, schema,
                                                      table_name, column_name),
                           out_op);
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
    mywire_conn_t *c = (mywire_conn_t *)conn;
    if (!c || !c->mysql) return -1;
    return run_built_query(conn,
                           mywire_build_primary_keys_query(c->mysql, catalog,
                                                           schema, table_name),
                           out_op);
}
