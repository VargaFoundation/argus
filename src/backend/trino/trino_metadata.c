#include "trino_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

/*
 * Trino does not have dedicated catalog API endpoints like Hive/Impala.
 * Instead, we implement catalog operations via SQL queries against
 * information_schema.
 *
 * Every ODBC search pattern or name reaches the query text through
 * argus_sql_quote_literal / argus_sql_quote_ident: the patterns come from
 * the application, and information_schema filters are as much SQL as any
 * other statement. Trino string literals are ANSI (no backslash escapes).
 */

/* Forward declaration */
int trino_execute(argus_backend_conn_t raw_conn,
                  const char *query,
                  argus_backend_op_t *out_op);

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

/* ── Query builders (pure: no connection, unit-tested) ────────── */

/* Append " AND <column> <op> '<value>'" with the value quoted; a NULL or
 * empty value adds nothing. Returns false on a value that cannot be quoted
 * (embedded NUL). */
static bool append_filter(GString *q, const char *column, const char *op,
                          const char *value)
{
    if (!value || !*value) return true;

    /*
     * A LIKE pattern gets ESCAPE '\\', which is the escape character
     * SQLGetInfo has always advertised and nothing acted on; and when the
     * pattern stands for exactly one name -- every escaped wildcard, which
     * is what SQL_ATTR_METADATA_ID produces -- equality is both correct and
     * something the catalog can index.
     */
    bool is_like = strcmp(op, "LIKE") == 0;
    char *exact = is_like ? argus_sql_pattern_literal(value) : NULL;
    char *lit = argus_sql_quote_literal(exact ? exact : value, false);
    free(exact);
    if (!lit) return false;
    if (is_like && !exact)
        g_string_append_printf(q, " AND %s LIKE %s ESCAPE '\\'", column, lit);
    else
        g_string_append_printf(q, " AND %s %s %s", column,
                               is_like ? "=" : op, lit);
    free(lit);
    return true;
}

/* ODBC clients ask for "TABLE"; Trino's information_schema reports the
 * SQL-standard "BASE TABLE". Parse the comma-separated (possibly quoted)
 * list into a quoted IN clause, translating "TABLE" -> "BASE TABLE". */
static bool append_table_types(GString *q, const char *table_types)
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
        char *lit = argus_sql_quote_literal(mapped, false);
        if (!lit) { ok = false; break; }
        if (in_list->len) g_string_append_c(in_list, ',');
        g_string_append(in_list, lit);
        free(lit);
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

char *trino_build_tables_query(const char *catalog, const char *schema,
                               const char *table_name, const char *table_types)
{
    GString *q = g_string_new(
        "SELECT "
        "table_catalog AS TABLE_CAT, "
        "table_schema AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END "
        "AS TABLE_TYPE, "
        "CAST(NULL AS VARCHAR) AS REMARKS "
        "FROM information_schema.tables WHERE 1=1");
    bool ok = append_filter(q, "table_catalog", "=", catalog) &&
              append_filter(q, "table_schema", "LIKE", schema) &&
              append_filter(q, "table_name", "LIKE", table_name) &&
              append_table_types(q, table_types);
    return finish(q, ok, " ORDER BY table_catalog, table_schema, table_name");
}

char *trino_build_columns_query(const char *catalog, const char *schema,
                                const char *table_name, const char *column_name)
{
    GString *q = g_string_new(
        "SELECT "
        "table_catalog AS TABLE_CAT, "
        "table_schema AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, "
        "column_name AS COLUMN_NAME, "
        /* DATA_TYPE: numeric ODBC SQL type code (column 5 per the spec). */
        "CASE "
        "WHEN data_type LIKE 'bigint%' THEN -5 "
        "WHEN data_type LIKE 'integer%' THEN 4 "
        "WHEN data_type LIKE 'smallint%' THEN 5 "
        "WHEN data_type LIKE 'tinyint%' THEN -6 "
        "WHEN data_type LIKE 'double%' THEN 8 "
        "WHEN data_type LIKE 'real%' THEN 7 "
        "WHEN data_type LIKE 'decimal%' THEN 3 "
        "WHEN data_type LIKE 'boolean%' THEN -7 "
        "WHEN data_type LIKE 'date%' THEN 91 "
        "WHEN data_type LIKE 'timestamp%' THEN 93 "
        "ELSE 12 END AS DATA_TYPE, "
        "data_type AS TYPE_NAME, "
        "ordinal_position AS ORDINAL_POSITION, "
        "is_nullable AS IS_NULLABLE "
        "FROM information_schema.columns WHERE 1=1");
    bool ok = append_filter(q, "table_catalog", "=", catalog) &&
              append_filter(q, "table_schema", "LIKE", schema) &&
              append_filter(q, "table_name", "LIKE", table_name) &&
              append_filter(q, "column_name", "LIKE", column_name);
    return finish(q, ok, " ORDER BY table_catalog, table_schema, table_name, "
                         "ordinal_position");
}

char *trino_build_schemas_query(const char *catalog, const char *schema)
{
    GString *q = g_string_new(
        "SELECT DISTINCT "
        "schema_name AS TABLE_SCHEM, "
        "catalog_name AS TABLE_CATALOG "
        "FROM information_schema.schemata WHERE 1=1");
    bool ok = append_filter(q, "catalog_name", "=", catalog) &&
              append_filter(q, "schema_name", "LIKE", schema);
    return finish(q, ok, " ORDER BY catalog_name, schema_name");
}

char *trino_build_primary_keys_query(const char *catalog, const char *schema,
                                     const char *table_name)
{
    GString *q = g_string_new(
        "SELECT table_cat, table_schem, table_name, "
        "column_name, key_seq, pk_name "
        "FROM system.jdbc.primary_keys "
        "WHERE 1=1");
    bool ok = append_filter(q, "table_cat", "=", catalog) &&
              append_filter(q, "table_schem", "=", schema) &&
              append_filter(q, "table_name", "=", table_name);
    return finish(q, ok, "");
}

/* SHOW STATS takes a table *name*, so each part is a delimited identifier:
 * a name that came back from SQLTables round-trips exactly. */
char *trino_build_statistics_query(const char *catalog, const char *schema,
                                   const char *table_name)
{
    if (!table_name || !*table_name) return NULL;
    GString *q = g_string_new("SHOW STATS FOR ");
    bool ok = true;
    const char *parts[3] = { NULL, NULL, table_name };
    if (schema && *schema) {
        parts[1] = schema;
        if (catalog && *catalog) parts[0] = catalog;
    }
    bool first = true;
    for (int i = 0; i < 3 && ok; i++) {
        if (!parts[i]) continue;
        char *ident = argus_sql_quote_ident(parts[i], '"');
        if (!ident) { ok = false; break; }
        if (!first) g_string_append_c(q, '.');
        g_string_append(q, ident);
        free(ident);
        first = false;
    }
    return finish(q, ok, "");
}

/* ── GetTables via information_schema ────────────────────────── */

static int run_built_query(trino_conn_t *conn, char *query,
                           argus_backend_op_t *out_op)
{
    if (!conn || !query) { g_free(query); return -1; }
    trino_operation_t *op = NULL;
    int rc = trino_execute_query(conn, query, &op);
    g_free(query);
    if (rc != 0) return -1;
    *out_op = op;
    return 0;
}

int trino_get_tables(argus_backend_conn_t raw_conn,
                     const char *catalog,
                     const char *schema,
                     const char *table_name,
                     const char *table_types,
                     argus_backend_op_t *out_op)
{
    return run_built_query((trino_conn_t *)raw_conn,
                           trino_build_tables_query(catalog, schema,
                                                    table_name, table_types),
                           out_op);
}

/* ── GetColumns via information_schema ───────────────────────── */

int trino_get_columns(argus_backend_conn_t raw_conn,
                      const char *catalog,
                      const char *schema,
                      const char *table_name,
                      const char *column_name,
                      argus_backend_op_t *out_op)
{
    return run_built_query((trino_conn_t *)raw_conn,
                           trino_build_columns_query(catalog, schema,
                                                     table_name, column_name),
                           out_op);
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
    return run_built_query((trino_conn_t *)raw_conn,
                           trino_build_schemas_query(catalog, schema),
                           out_op);
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

/* ── GetPrimaryKeys via system.jdbc.primary_keys ─────────────── */

int trino_get_primary_keys(argus_backend_conn_t raw_conn,
                            const char *catalog,
                            const char *schema,
                            const char *table_name,
                            argus_backend_op_t *out_op)
{
    return run_built_query((trino_conn_t *)raw_conn,
                           trino_build_primary_keys_query(catalog, schema,
                                                          table_name),
                           out_op);
}

/* ── GetStatistics via SHOW STATS ────────────────────────────── */

int trino_get_statistics(argus_backend_conn_t raw_conn,
                          const char *catalog,
                          const char *schema,
                          const char *table_name,
                          unsigned short unique,
                          unsigned short reserved,
                          argus_backend_op_t *out_op)
{
    (void)unique;
    (void)reserved;
    return run_built_query((trino_conn_t *)raw_conn,
                           trino_build_statistics_query(catalog, schema,
                                                        table_name),
                           out_op);
}
