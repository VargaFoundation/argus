#include "pg_common.h"
#include "argus/compat.h"
#include <string.h>

/*
 * Catalog functions, read from pg_catalog.
 *
 * Namespace model: PostgreSQL has a real two-level namespace, so a database is
 * TABLE_CAT and a schema is TABLE_SCHEM — the Trino shape, not the MySQL one
 * where a database is squeezed into the catalog column. A PostgreSQL session
 * cannot query across databases, so TABLE_CAT is always current_database() and
 * a catalog filter naming a different database correctly returns nothing.
 *
 * Two deliberate choices a generic ODBC driver does not make:
 *
 *  - System schemas (pg_catalog, information_schema, and the pg_toast and
 *    pg_temp families) are hidden unless the application asks for them by
 *    name. Otherwise the first thing a BI user sees when they open the
 *    connection is several hundred internal relations.
 *  - Relations the current user cannot read are hidden, via
 *    has_table_privilege. Listing a table and then failing to select from it
 *    is worse than not listing it.
 */

/* Schemas that are noise for every BI tool. Kept out unless the caller's
 * schema pattern actually names one. */
#define PG_SYSTEM_SCHEMA_FILTER                                  \
    " AND (n.nspname NOT IN ('pg_catalog','information_schema')" \
    " AND n.nspname NOT LIKE 'pg\\_toast%'"                      \
    " AND n.nspname NOT LIKE 'pg\\_temp%')"

/* True when the caller explicitly named a system schema, in which case the
 * filter above must not apply. */
static bool wants_system_schema(const char *schema)
{
    if (!schema || !*schema) return false;
    return strncasecmp(schema, "pg_", 3) == 0 ||
           strcasecmp(schema, "information_schema") == 0;
}

static int run_sql(pg_conn_t *conn, GString *sql, argus_backend_op_t *out_op)
{
    int rc = pg_execute_buffered(conn, sql->str, out_op);
    g_string_free(sql, TRUE);
    return rc;
}

/* ── SQLTables ───────────────────────────────────────────────── */

int pg_get_tables(argus_backend_conn_t raw_conn,
                  const char *catalog, const char *schema,
                  const char *table_name, const char *table_types,
                  argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg || !out_op) return -1;

    const char *remarks =
        (conn->profile && conn->profile->remarks_expr)
        ? conn->profile->remarks_expr(conn)
        : "obj_description(c.oid, 'pg_class')";

    GString *sql = g_string_new(
        "SELECT current_database() AS \"TABLE_CAT\", "
        "n.nspname AS \"TABLE_SCHEM\", "
        "c.relname AS \"TABLE_NAME\", "
        "CASE c.relkind "
        "WHEN 'r' THEN CASE WHEN n.nspname IN ('pg_catalog','information_schema') "
                          "THEN 'SYSTEM TABLE' ELSE 'TABLE' END "
        "WHEN 'p' THEN 'TABLE' "
        "WHEN 'v' THEN 'VIEW' "
        "WHEN 'm' THEN 'MATERIALIZED VIEW' "
        "WHEN 'f' THEN 'TABLE' "          /* gpfdist/PXF/FDW read like tables */
        "ELSE 'TABLE' END AS \"TABLE_TYPE\", ");
    g_string_append_printf(sql, "%s AS \"REMARKS\" ", remarks);
    g_string_append(sql,
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE has_table_privilege(c.oid, 'SELECT')");

    pg_append_relkinds(sql, "c.relkind", table_types);

    /* A catalog other than the one we are connected to cannot be inspected. */
    if (catalog && *catalog && strcmp(catalog, "%") != 0) {
        g_string_append(sql, " AND current_database() = ");
        pg_append_literal(sql, conn->pg, catalog);
    }

    if (!wants_system_schema(schema))
        g_string_append(sql, PG_SYSTEM_SCHEMA_FILTER);

    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "c.relname", table_name);

    /* Engine-specific exclusions — Greenplum/Cloudberry partition children. */
    if (conn->profile && conn->profile->tables_filter) {
        const char *extra = conn->profile->tables_filter(conn);
        if (extra && *extra) g_string_append(sql, extra);
    }

    /* SQLTables is specified to sort by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM,
     * TABLE_NAME. */
    g_string_append(sql, " ORDER BY 4, 1, 2, 3");

    return run_sql(conn, sql, out_op);
}

/* ── SQLColumns ──────────────────────────────────────────────── */

int pg_get_columns(argus_backend_conn_t raw_conn,
                   const char *catalog, const char *schema,
                   const char *table_name, const char *column_name,
                   argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg || !out_op) return -1;

    /*
     * a.atttypid is resolved through pg_type so that a domain reports its base
     * type: an application binding a column declared as a domain over integer
     * needs SQL_INTEGER, not "some user type". format_type() gives the
     * declared name for TYPE_NAME, which is what a user recognises.
     */
    GString *sql = g_string_new(
        "SELECT current_database() AS \"TABLE_CAT\", "
        "n.nspname AS \"TABLE_SCHEM\", "
        "c.relname AS \"TABLE_NAME\", "
        "a.attname AS \"COLUMN_NAME\", ");

    pg_append_odbc_type_case(sql,
        "COALESCE(NULLIF(t.typbasetype, 0), a.atttypid)", "a.atttypmod");

    g_string_append(sql,
        "::int2 AS \"DATA_TYPE\", "
        "format_type(a.atttypid, a.atttypmod) AS \"TYPE_NAME\", "
        /* COLUMN_SIZE: declared length for the character types, precision for
         * numeric, and the natural display size for everything else. */
        "CASE "
        "WHEN a.atttypid IN (1042,1043) AND a.atttypmod > 4 THEN a.atttypmod - 4 "
        "WHEN a.atttypid = 1700 AND a.atttypmod > 4 "
             "THEN ((a.atttypmod - 4) >> 16) & 65535 "
        "WHEN a.atttypid = 1700 THEN 38 "
        "WHEN a.atttypid = 21 THEN 5 "
        "WHEN a.atttypid IN (23,26) THEN 10 "
        "WHEN a.atttypid = 20 THEN 19 "
        "WHEN a.atttypid = 700 THEN 7 "
        "WHEN a.atttypid = 701 THEN 15 "
        "WHEN a.atttypid = 16 THEN 1 "
        "WHEN a.atttypid = 1082 THEN 10 "
        "WHEN a.atttypid IN (1083,1266) THEN 15 "
        "WHEN a.atttypid IN (1114,1184) THEN 26 "
        "WHEN a.atttypid = 2950 THEN 36 "
        "WHEN a.atttypid = 19 THEN 63 "
        "ELSE NULL END::int AS \"COLUMN_SIZE\", "
        "NULL::int AS \"BUFFER_LENGTH\", "
        "CASE "
        "WHEN a.atttypid = 1700 AND a.atttypmod > 4 "
             "THEN (a.atttypmod - 4) & 65535 "
        "WHEN a.atttypid IN (1083,1266,1114,1184) "
             "THEN COALESCE(NULLIF(a.atttypmod, -1), 6) "
        "ELSE NULL END::int2 AS \"DECIMAL_DIGITS\", "
        "CASE WHEN a.atttypid IN (20,21,23,26,700,701,1700) "
             "THEN 10 ELSE NULL END::int2 AS \"NUM_PREC_RADIX\", "
        /* SQL_NO_NULLS = 0, SQL_NULLABLE = 1 */
        "CASE WHEN a.attnotnull THEN 0 ELSE 1 END::int2 AS \"NULLABLE\", "
        "col_description(c.oid, a.attnum) AS \"REMARKS\", "
        "pg_get_expr(d.adbin, d.adrelid) AS \"COLUMN_DEF\", "
        "NULL::int2 AS \"SQL_DATA_TYPE\", "
        "NULL::int2 AS \"SQL_DATETIME_SUB\", "
        "CASE WHEN a.atttypid IN (1042,1043) AND a.atttypmod > 4 "
             "THEN a.atttypmod - 4 ELSE NULL END::int AS \"CHAR_OCTET_LENGTH\", "
        "a.attnum::int AS \"ORDINAL_POSITION\", "
        "CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS \"IS_NULLABLE\" "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
        "LEFT JOIN pg_catalog.pg_attrdef d "
             "ON d.adrelid = c.oid AND d.adnum = a.attnum "
        "WHERE a.attnum > 0 AND NOT a.attisdropped "
        "AND c.relkind IN ('r','p','v','m','f') "
        "AND has_table_privilege(c.oid, 'SELECT')");

    if (catalog && *catalog && strcmp(catalog, "%") != 0) {
        g_string_append(sql, " AND current_database() = ");
        pg_append_literal(sql, conn->pg, catalog);
    }
    if (!wants_system_schema(schema))
        g_string_append(sql, PG_SYSTEM_SCHEMA_FILTER);

    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "c.relname", table_name);
    pg_append_pattern(sql, conn->pg, "a.attname", column_name);

    if (conn->profile && conn->profile->tables_filter) {
        const char *extra = conn->profile->tables_filter(conn);
        if (extra && *extra) g_string_append(sql, extra);
    }

    g_string_append(sql, " ORDER BY 2, 3, \"ORDINAL_POSITION\"");

    return run_sql(conn, sql, out_op);
}

/* ── SQLTables with a schema pattern only: SQLTables(NULL,"%",NULL) ──
 * SQLSchemas, in ODBC terms. */

int pg_get_schemas(argus_backend_conn_t raw_conn,
                   const char *catalog, const char *schema,
                   argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg || !out_op) return -1;

    GString *sql = g_string_new(
        "SELECT n.nspname AS \"TABLE_SCHEM\", "
        "current_database() AS \"TABLE_CATALOG\" "
        "FROM pg_catalog.pg_namespace n "
        "WHERE has_schema_privilege(n.oid, 'USAGE')");

    if (catalog && *catalog && strcmp(catalog, "%") != 0) {
        g_string_append(sql, " AND current_database() = ");
        pg_append_literal(sql, conn->pg, catalog);
    }
    if (!wants_system_schema(schema))
        g_string_append(sql, PG_SYSTEM_SCHEMA_FILTER);

    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    g_string_append(sql, " ORDER BY 1");

    return run_sql(conn, sql, out_op);
}

/* ── SQLTables(catalog="%") ──────────────────────────────────── */

int pg_get_catalogs(argus_backend_conn_t raw_conn, argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg || !out_op) return -1;

    /*
     * Only the connected database, not every database on the server.
     *
     * A PostgreSQL session cannot reference another database, so listing them
     * all would put entries in a BI navigator that fail the moment anyone
     * expands them — Power BI's hierarchical navigator does exactly that. The
     * honest answer is the one catalog that can actually be queried, which is
     * also what psqlODBC reports. SHOWALLDATABASES=1 opts into the full list
     * for tools that use it to offer a reconnect target.
     */
    GString *sql = g_string_new(
        conn->show_all_databases
        ? "SELECT d.datname AS \"TABLE_CAT\" "
          "FROM pg_catalog.pg_database d "
          "WHERE d.datallowconn AND NOT d.datistemplate "
          "AND has_database_privilege(d.oid, 'CONNECT') "
          "ORDER BY 1"
        : "SELECT current_database() AS \"TABLE_CAT\"");

    return run_sql(conn, sql, out_op);
}

/* ── SQLPrimaryKeys ──────────────────────────────────────────── */

int pg_get_primary_keys(argus_backend_conn_t raw_conn,
                        const char *catalog, const char *schema,
                        const char *table_name, argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg || !out_op) return -1;

    /*
     * conkey is an array in declaration order, so KEY_SEQ comes from the
     * element's ordinality rather than from attnum — a primary key declared
     * (b, a) must report b as KEY_SEQ 1.
     */
    GString *sql = g_string_new(
        "SELECT current_database() AS \"TABLE_CAT\", "
        "n.nspname AS \"TABLE_SCHEM\", "
        "c.relname AS \"TABLE_NAME\", "
        "a.attname AS \"COLUMN_NAME\", "
        "k.ord::int2 AS \"KEY_SEQ\", "
        "con.conname AS \"PK_NAME\" "
        "FROM pg_catalog.pg_constraint con "
        "JOIN pg_catalog.pg_class c ON c.oid = con.conrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_catalog.pg_attribute a "
             "ON a.attrelid = c.oid AND a.attnum = k.attnum "
        "WHERE con.contype = 'p'");

    if (catalog && *catalog && strcmp(catalog, "%") != 0) {
        g_string_append(sql, " AND current_database() = ");
        pg_append_literal(sql, conn->pg, catalog);
    }
    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "c.relname", table_name);

    g_string_append(sql, " ORDER BY 2, 3, \"KEY_SEQ\"");

    return run_sql(conn, sql, out_op);
}

/* ── SQLStatistics ───────────────────────────────────────────── */

int pg_get_statistics(argus_backend_conn_t raw_conn,
                      const char *catalog, const char *schema,
                      const char *table_name,
                      unsigned short unique, unsigned short reserved,
                      argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)reserved;
    if (!conn || !conn->pg || !out_op) return -1;

    /*
     * Two kinds of row, as the specification requires: one SQL_TABLE_STAT row
     * per table carrying cardinality and pages, then one row per index column.
     * reltuples/relpages are planner estimates and are -1 on a table that was
     * never analysed; NULL is the correct ODBC answer for "unknown", so the
     * negative sentinel is mapped rather than passed on as a row count.
     */
    GString *sql = g_string_new(
        "SELECT current_database() AS \"TABLE_CAT\", "
        "n.nspname AS \"TABLE_SCHEM\", "
        "c.relname AS \"TABLE_NAME\", "
        "NULL::int2 AS \"NON_UNIQUE\", "
        "NULL::text AS \"INDEX_QUALIFIER\", "
        "NULL::text AS \"INDEX_NAME\", "
        "0::int2 AS \"TYPE\", "                 /* SQL_TABLE_STAT */
        "NULL::int2 AS \"ORDINAL_POSITION\", "
        "NULL::text AS \"COLUMN_NAME\", "
        "NULL::text AS \"ASC_OR_DESC\", "
        "CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint END AS \"CARDINALITY\", "
        "CASE WHEN c.relpages >= 0 THEN c.relpages::bigint END AS \"PAGES\", "
        "NULL::text AS \"FILTER_CONDITION\" "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind IN ('r','p','m','f')");

    if (catalog && *catalog && strcmp(catalog, "%") != 0) {
        g_string_append(sql, " AND current_database() = ");
        pg_append_literal(sql, conn->pg, catalog);
    }
    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "c.relname", table_name);

    g_string_append(sql,
        " UNION ALL "
        "SELECT current_database(), n.nspname, c.relname, "
        /* PostgreSQL has no direct boolean → smallint cast; the int step is
         * required, not decoration. */
        "(NOT i.indisunique)::int::int2, "
        "NULL::text, "
        "ic.relname, "
        "3::int2, "                              /* SQL_INDEX_OTHER */
        "k.ord::int2, "
        "a.attname, "
        "CASE WHEN i.indoption[k.ord - 1] & 1 = 1 THEN 'D' ELSE 'A' END, "
        "CASE WHEN ic.reltuples >= 0 THEN ic.reltuples::bigint END, "
        "CASE WHEN ic.relpages >= 0 THEN ic.relpages::bigint END, "
        "pg_get_expr(i.indpred, i.indrelid) "
        "FROM pg_catalog.pg_index i "
        "JOIN pg_catalog.pg_class c ON c.oid = i.indrelid "
        "JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_catalog.pg_attribute a "
             "ON a.attrelid = c.oid AND a.attnum = k.attnum "
        "WHERE i.indisvalid");

    /* SQL_INDEX_UNIQUE asks for unique indexes only. */
    if (unique == SQL_INDEX_UNIQUE)
        g_string_append(sql, " AND i.indisunique");

    if (catalog && *catalog && strcmp(catalog, "%") != 0) {
        g_string_append(sql, " AND current_database() = ");
        pg_append_literal(sql, conn->pg, catalog);
    }
    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "c.relname", table_name);

    /* NON_UNIQUE, then index name, then column position — the ODBC order. */
    g_string_append(sql, " ORDER BY 4, 6, 8");

    return run_sql(conn, sql, out_op);
}
