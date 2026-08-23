#include "pg_common.h"
#include "argus/compat.h"
#include <string.h>

/*
 * The catalog functions the driver used to answer with an empty result set for
 * every backend.
 *
 * Empty was right for Trino, Hive, Impala, Pinot and the rest: those engines
 * have no foreign keys, no stored procedures and no per-column privileges, and
 * a driver must not invent metadata a BI tool would then trust. PostgreSQL has
 * all of them, so here they carry real data.
 *
 * One caveat worth repeating in the documentation rather than hiding: on
 * Greenplum and Cloudberry, primary and foreign key constraints are *declared
 * but not enforced* unless the distribution key allows it. Reporting them is
 * still correct — BI tools read them as join hints, which is exactly what they
 * are — but nobody should read them as an integrity guarantee.
 */

static int run_sql(pg_conn_t *conn, GString *sql, argus_backend_op_t *out_op)
{
    int rc = pg_execute_buffered(conn, sql->str, out_op);
    g_string_free(sql, TRUE);
    return rc;
}

/* ── SQLForeignKeys ──────────────────────────────────────────── */

int pg_get_foreign_keys(argus_backend_conn_t raw_conn,
                        const char *pk_catalog, const char *pk_schema,
                        const char *pk_table,
                        const char *fk_catalog, const char *fk_schema,
                        const char *fk_table,
                        argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)pk_catalog;
    (void)fk_catalog;
    if (!conn || !conn->pg || !out_op) return -1;

    /*
     * conkey and confkey are parallel arrays in declaration order, so they are
     * unnested together with generate_subscripts rather than with two separate
     * WITH ORDINALITY clauses — the latter would produce a cross join, not a
     * zip, and a two-column foreign key would come back with four rows.
     */
    GString *sql = g_string_new(
        "SELECT current_database() AS \"PKTABLE_CAT\", "
        "pn.nspname AS \"PKTABLE_SCHEM\", "
        "pc.relname AS \"PKTABLE_NAME\", "
        "pa.attname AS \"PKCOLUMN_NAME\", "
        "current_database() AS \"FKTABLE_CAT\", "
        "fn.nspname AS \"FKTABLE_SCHEM\", "
        "fc.relname AS \"FKTABLE_NAME\", "
        "fa.attname AS \"FKCOLUMN_NAME\", "
        "k.i::int2 AS \"KEY_SEQ\", "
        /* ODBC: 0 CASCADE, 1 RESTRICT, 2 SET NULL, 3 NO ACTION, 4 SET DEFAULT */
        "CASE con.confupdtype WHEN 'c' THEN 0 WHEN 'r' THEN 1 WHEN 'n' THEN 2 "
             "WHEN 'a' THEN 3 WHEN 'd' THEN 4 ELSE 3 END::int2 AS \"UPDATE_RULE\", "
        "CASE con.confdeltype WHEN 'c' THEN 0 WHEN 'r' THEN 1 WHEN 'n' THEN 2 "
             "WHEN 'a' THEN 3 WHEN 'd' THEN 4 ELSE 3 END::int2 AS \"DELETE_RULE\", "
        "con.conname AS \"FK_NAME\", "
        "pcon.conname AS \"PK_NAME\", "
        /* 5 INITIALLY DEFERRED, 6 INITIALLY IMMEDIATE, 7 NOT DEFERRABLE */
        "CASE WHEN NOT con.condeferrable THEN 7 "
             "WHEN con.condeferred THEN 5 ELSE 6 END::int2 AS \"DEFERRABILITY\" "
        "FROM pg_catalog.pg_constraint con "
        "JOIN pg_catalog.pg_class fc ON fc.oid = con.conrelid "
        "JOIN pg_catalog.pg_namespace fn ON fn.oid = fc.relnamespace "
        "JOIN pg_catalog.pg_class pc ON pc.oid = con.confrelid "
        "JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace "
        "CROSS JOIN LATERAL generate_subscripts(con.conkey, 1) AS k(i) "
        "JOIN pg_catalog.pg_attribute fa "
             "ON fa.attrelid = con.conrelid AND fa.attnum = con.conkey[k.i] "
        "JOIN pg_catalog.pg_attribute pa "
             "ON pa.attrelid = con.confrelid AND pa.attnum = con.confkey[k.i] "
        /* The unique constraint or primary key the reference points at. */
        "LEFT JOIN pg_catalog.pg_constraint pcon "
             "ON pcon.conrelid = con.confrelid "
             "AND pcon.contype IN ('p','u') "
             "AND pcon.conindid = con.conindid "
        "WHERE con.contype = 'f'");

    pg_append_pattern(sql, conn->pg, "pn.nspname", pk_schema);
    pg_append_pattern(sql, conn->pg, "pc.relname", pk_table);
    pg_append_pattern(sql, conn->pg, "fn.nspname", fk_schema);
    pg_append_pattern(sql, conn->pg, "fc.relname", fk_table);

    /* ODBC orders by the foreign-key side when one is named, and by the
     * primary-key side otherwise. */
    if (fk_table && *fk_table)
        g_string_append(sql, " ORDER BY 6, 7, \"KEY_SEQ\"");
    else
        g_string_append(sql, " ORDER BY 2, 3, \"KEY_SEQ\"");

    return run_sql(conn, sql, out_op);
}

/* ── SQLSpecialColumns ───────────────────────────────────────── */

int pg_get_special_columns(argus_backend_conn_t raw_conn,
                           SQLUSMALLINT identifier_type,
                           const char *catalog, const char *schema,
                           const char *table,
                           SQLUSMALLINT scope, SQLUSMALLINT nullable,
                           argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)catalog;
    (void)nullable;
    if (!conn || !conn->pg || !out_op) return -1;

    if (identifier_type == SQL_ROWVER) {
        /*
         * xmin is a real row version — it changes on every UPDATE — but it is
         * a 32-bit counter that wraps, so an application caching it across a
         * long period can be told a row is unchanged when it is not. Off
         * unless ROWVERSIONING=1 asks for it, which is the same call psqlODBC
         * makes.
         */
        bool enabled = conn->row_versioning;

        GString *sql = g_string_new(
            "SELECT NULL::int2 AS \"SCOPE\", "
            "'xmin'::text AS \"COLUMN_NAME\", "
            "4::int2 AS \"DATA_TYPE\", "          /* SQL_INTEGER */
            "'xid'::text AS \"TYPE_NAME\", "
            "10::int AS \"COLUMN_SIZE\", "
            "4::int AS \"BUFFER_LENGTH\", "
            "NULL::int2 AS \"DECIMAL_DIGITS\", "
            "2::int2 AS \"PSEUDO_COLUMN\" "        /* SQL_PC_PSEUDO */
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r','p')");
        if (!enabled) g_string_append(sql, " AND false");
        pg_append_pattern(sql, conn->pg, "n.nspname", schema);
        pg_append_pattern(sql, conn->pg, "c.relname", table);
        return run_sql(conn, sql, out_op);
    }

    /*
     * SQL_BEST_ROWID: the primary key, or failing that a unique index whose
     * columns are all NOT NULL.
     *
     * ctid is deliberately not offered as a fallback. It is PostgreSQL's
     * physical row pointer and it changes on UPDATE and on VACUUM FULL, so it
     * does not satisfy ODBC's "identifies the row for the requested scope" for
     * any scope beyond a single statement — an application using it for a
     * positioned update would write to the wrong row. A table with no stable
     * identifier gets an empty result, which is the truthful answer.
     */
    GString *sql = g_string_new(
        "SELECT ");
    g_string_append_printf(sql, "%u::int2 AS \"SCOPE\", ", (unsigned)scope);
    g_string_append(sql,
        "a.attname AS \"COLUMN_NAME\", ");
    pg_append_odbc_type_case(sql, "a.atttypid", "a.atttypmod");
    g_string_append(sql,
        "::int2 AS \"DATA_TYPE\", "
        "format_type(a.atttypid, a.atttypmod) AS \"TYPE_NAME\", "
        "CASE WHEN a.atttypid IN (1042,1043) AND a.atttypmod > 4 "
             "THEN a.atttypmod - 4 "
        "WHEN a.atttypid = 21 THEN 5 WHEN a.atttypid IN (23,26) THEN 10 "
        "WHEN a.atttypid = 20 THEN 19 ELSE NULL END::int AS \"COLUMN_SIZE\", "
        "NULL::int AS \"BUFFER_LENGTH\", "
        "CASE WHEN a.atttypid = 1700 AND a.atttypmod > 4 "
             "THEN (a.atttypmod - 4) & 65535 END::int2 AS \"DECIMAL_DIGITS\", "
        "1::int2 AS \"PSEUDO_COLUMN\" "            /* SQL_PC_NOT_PSEUDO */
        "FROM pg_catalog.pg_index i "
        "JOIN pg_catalog.pg_class c ON c.oid = i.indrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_catalog.pg_attribute a "
             "ON a.attrelid = c.oid AND a.attnum = k.attnum "
        "WHERE i.indisvalid AND i.indisunique AND a.attnotnull "
        /* Expression indexes have attnum 0 and no usable column name. */
        "AND k.attnum > 0 "
        /* Only the best candidate: the primary key if there is one. */
        "AND i.indexrelid = ("
            "SELECT i2.indexrelid FROM pg_catalog.pg_index i2 "
            "WHERE i2.indrelid = c.oid AND i2.indisvalid AND i2.indisunique "
            "AND NOT EXISTS (SELECT 1 FROM unnest(i2.indkey) AS x(n) "
                            "JOIN pg_catalog.pg_attribute a2 "
                                 "ON a2.attrelid = c.oid AND a2.attnum = x.n "
                            "WHERE NOT a2.attnotnull) "
            "AND NOT EXISTS (SELECT 1 FROM unnest(i2.indkey) AS x(n) WHERE x.n = 0) "
            "ORDER BY i2.indisprimary DESC, array_length(i2.indkey::int2[], 1), "
                     "i2.indexrelid LIMIT 1)");

    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "c.relname", table);
    g_string_append(sql, " ORDER BY k.ord");

    return run_sql(conn, sql, out_op);
}

/* ── SQLProcedures ───────────────────────────────────────────── */

int pg_get_procedures(argus_backend_conn_t raw_conn,
                      const char *catalog, const char *schema,
                      const char *proc, argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)catalog;
    if (!conn || !conn->pg || !out_op) return -1;

    /*
     * prokind is PostgreSQL 11 and later; Greenplum 6 (PostgreSQL 9.4) has the
     * older proisagg/proiswindow booleans instead. Both are read through
     * to_jsonb on the catalog row so the query parses on either — referencing
     * a column that does not exist would fail at parse time, before any
     * version check could help.
     */
    bool has_prokind = (conn->pg_major >= 11);

    GString *sql = g_string_new(
        "SELECT current_database() AS \"PROCEDURE_CAT\", "
        "n.nspname AS \"PROCEDURE_SCHEM\", "
        "p.proname AS \"PROCEDURE_NAME\", "
        "NULL::int AS \"NUM_INPUT_PARAMS\", "
        "NULL::int AS \"NUM_OUTPUT_PARAMS\", "
        "p.proretset::int AS \"NUM_RESULT_SETS\", "
        "obj_description(p.oid, 'pg_proc') AS \"REMARKS\", ");

    /* SQL_PT_PROCEDURE = 1 (no return value), SQL_PT_FUNCTION = 2. */
    if (has_prokind)
        g_string_append(sql,
            "CASE WHEN p.prokind = 'p' THEN 1 ELSE 2 END::int2 "
            "AS \"PROCEDURE_TYPE\" ");
    else
        g_string_append(sql, "2::int2 AS \"PROCEDURE_TYPE\" ");

    g_string_append(sql,
        "FROM pg_catalog.pg_proc p "
        "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
        "AND has_function_privilege(p.oid, 'EXECUTE')");

    /* Aggregates and window functions are not callable as procedures. */
    if (has_prokind)
        g_string_append(sql, " AND p.prokind IN ('f','p')");
    else
        g_string_append(sql, " AND NOT p.proisagg AND NOT p.proiswindow");

    pg_append_pattern(sql, conn->pg, "n.nspname", schema);
    pg_append_pattern(sql, conn->pg, "p.proname", proc);

    g_string_append(sql, " ORDER BY 2, 3");

    return run_sql(conn, sql, out_op);
}

/* ── SQLProcedureColumns ─────────────────────────────────────── */

int pg_get_procedure_columns(argus_backend_conn_t raw_conn,
                             const char *catalog, const char *schema,
                             const char *proc, const char *column,
                             argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)catalog;
    if (!conn || !conn->pg || !out_op) return -1;

    bool has_prokind = (conn->pg_major >= 11);

    /*
     * Arguments come from proallargtypes when the function has OUT or INOUT
     * parameters and from proargtypes otherwise — proallargtypes is NULL in
     * the common all-IN case, which is why the COALESCE is needed rather than
     * a plain read.
     *
     * The subtlety that costs an afternoon: proargtypes is an oidvector, whose
     * lower bound is 0, while proallargtypes and proargnames are ordinary
     * arrays with lower bound 1. Casting oidvector to oid[] keeps the 0 bound,
     * so generate_subscripts would yield 0..n-1 and every argument name would
     * come back NULL — the types would be right and the names silently empty.
     * Rebuilding through unnest() normalises the bound to 1.
     */
    GString *sql = g_string_new(
        "WITH args AS ("
        "SELECT p.oid, p.proname, p.pronamespace, p.prorettype, p.proretset, "
        "COALESCE(p.proallargtypes, "
                 "ARRAY(SELECT unnest(p.proargtypes::oid[]))) AS argtypes, "
        "p.proargmodes, p.proargnames "
        "FROM pg_catalog.pg_proc p "
        "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
        "AND has_function_privilege(p.oid, 'EXECUTE')");
    if (has_prokind)
        g_string_append(sql, " AND p.prokind IN ('f','p')");
    else
        g_string_append(sql, " AND NOT p.proisagg AND NOT p.proiswindow");
    g_string_append(sql, ")");

    /* One row per argument … */
    g_string_append(sql,
        " SELECT current_database() AS \"PROCEDURE_CAT\", "
        "n.nspname AS \"PROCEDURE_SCHEM\", "
        "a.proname AS \"PROCEDURE_NAME\", "
        "COALESCE(a.proargnames[k.i], '') AS \"COLUMN_NAME\", "
        /* 1 IN, 2 INOUT, 3 RESULT, 4 OUT, 5 RETURN_VALUE */
        "CASE COALESCE(a.proargmodes[k.i], 'i') "
             "WHEN 'i' THEN 1 WHEN 'b' THEN 2 WHEN 'o' THEN 4 "
             "WHEN 't' THEN 3 WHEN 'v' THEN 1 ELSE 1 END::int2 AS \"COLUMN_TYPE\", ");
    pg_append_odbc_type_case(sql, "a.argtypes[k.i]", "-1");
    g_string_append(sql,
        "::int2 AS \"DATA_TYPE\", "
        "format_type(a.argtypes[k.i], NULL) AS \"TYPE_NAME\", "
        "NULL::int AS \"COLUMN_SIZE\", NULL::int AS \"BUFFER_LENGTH\", "
        "NULL::int2 AS \"DECIMAL_DIGITS\", NULL::int2 AS \"NUM_PREC_RADIX\", "
        "2::int2 AS \"NULLABLE\", "                 /* SQL_NULLABLE_UNKNOWN */
        "NULL::text AS \"REMARKS\", NULL::text AS \"COLUMN_DEF\", "
        "NULL::int2 AS \"SQL_DATA_TYPE\", NULL::int2 AS \"SQL_DATETIME_SUB\", "
        "NULL::int AS \"CHAR_OCTET_LENGTH\", "
        "k.i::int AS \"ORDINAL_POSITION\", "
        "'' AS \"IS_NULLABLE\" "
        "FROM args a "
        "JOIN pg_catalog.pg_namespace n ON n.oid = a.pronamespace "
        "CROSS JOIN LATERAL generate_subscripts(a.argtypes, 1) AS k(i) ");

    GString *filters = g_string_new(NULL);
    pg_append_pattern(filters, conn->pg, "n.nspname", schema);
    pg_append_pattern(filters, conn->pg, "a.proname", proc);
    pg_append_pattern(filters, conn->pg, "COALESCE(a.proargnames[k.i], '')",
                      column);
    if (filters->len > 0)
        g_string_append_printf(sql, "WHERE true%s ", filters->str);

    /* … plus the return value, which ODBC reports as ordinal 0. A set-returning
     * function is a result set rather than a return value. */
    g_string_append(sql,
        "UNION ALL "
        "SELECT current_database(), n.nspname, a.proname, '', "
        "5::int2, ");                                /* SQL_RETURN_VALUE */
    pg_append_odbc_type_case(sql, "a.prorettype", "-1");
    g_string_append(sql,
        "::int2, format_type(a.prorettype, NULL), "
        "NULL::int, NULL::int, NULL::int2, NULL::int2, 2::int2, "
        "NULL::text, NULL::text, NULL::int2, NULL::int2, NULL::int, "
        "0::int, '' "
        "FROM args a "
        "JOIN pg_catalog.pg_namespace n ON n.oid = a.pronamespace "
        "WHERE NOT a.proretset AND a.prorettype <> 2278 ");  /* 2278 = void */

    GString *f2 = g_string_new(NULL);
    pg_append_pattern(f2, conn->pg, "n.nspname", schema);
    pg_append_pattern(f2, conn->pg, "a.proname", proc);
    if (f2->len > 0) g_string_append(sql, f2->str);
    g_string_free(f2, TRUE);
    g_string_free(filters, TRUE);

    g_string_append(sql, " ORDER BY 2, 3, \"ORDINAL_POSITION\"");

    return run_sql(conn, sql, out_op);
}

/* ── SQLTablePrivileges / SQLColumnPrivileges ────────────────── */

/*
 * information_schema rather than a hand-parse of relacl: the views already
 * expand the ACL and follow role membership, which relacl parsing does not —
 * a privilege held through a group role would be invisible. They are slower,
 * but these calls are on-demand and never on the connection path.
 */

int pg_get_table_privileges(argus_backend_conn_t raw_conn,
                            const char *catalog, const char *schema,
                            const char *table, argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)catalog;
    if (!conn || !conn->pg || !out_op) return -1;

    GString *sql = g_string_new(
        "SELECT current_database() AS \"TABLE_CAT\", "
        "tp.table_schema::text AS \"TABLE_SCHEM\", "
        "tp.table_name::text AS \"TABLE_NAME\", "
        "tp.grantor::text AS \"GRANTOR\", "
        "tp.grantee::text AS \"GRANTEE\", "
        "tp.privilege_type::text AS \"PRIVILEGE\", "
        "CASE tp.is_grantable WHEN 'YES' THEN 'YES' ELSE 'NO' END "
             "AS \"IS_GRANTABLE\" "
        "FROM information_schema.table_privileges tp "
        "WHERE tp.table_schema NOT IN ('pg_catalog','information_schema')");

    pg_append_pattern(sql, conn->pg, "tp.table_schema::text", schema);
    pg_append_pattern(sql, conn->pg, "tp.table_name::text", table);

    g_string_append(sql, " ORDER BY 2, 3, 6");

    return run_sql(conn, sql, out_op);
}

int pg_get_column_privileges(argus_backend_conn_t raw_conn,
                             const char *catalog, const char *schema,
                             const char *table, const char *column,
                             argus_backend_op_t *out_op)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    (void)catalog;
    if (!conn || !conn->pg || !out_op) return -1;

    GString *sql = g_string_new(
        "SELECT current_database() AS \"TABLE_CAT\", "
        "cp.table_schema::text AS \"TABLE_SCHEM\", "
        "cp.table_name::text AS \"TABLE_NAME\", "
        "cp.column_name::text AS \"COLUMN_NAME\", "
        "cp.grantor::text AS \"GRANTOR\", "
        "cp.grantee::text AS \"GRANTEE\", "
        "cp.privilege_type::text AS \"PRIVILEGE\", "
        "CASE cp.is_grantable WHEN 'YES' THEN 'YES' ELSE 'NO' END "
             "AS \"IS_GRANTABLE\" "
        "FROM information_schema.column_privileges cp "
        "WHERE cp.table_schema NOT IN ('pg_catalog','information_schema')");

    pg_append_pattern(sql, conn->pg, "cp.table_schema::text", schema);
    pg_append_pattern(sql, conn->pg, "cp.table_name::text", table);
    pg_append_pattern(sql, conn->pg, "cp.column_name::text", column);

    g_string_append(sql, " ORDER BY 2, 3, 4, 7");

    return run_sql(conn, sql, out_op);
}
