#ifndef ARGUS_PG_COMMON_H
#define ARGUS_PG_COMMON_H

#include <libpq-fe.h>
#include <glib.h>
#include "argus/backend.h"
#include "argus/handle.h"
#include "argus/types.h"

/*
 * Shared libpq core for the PostgreSQL-family backends.
 *
 * Three backends are registered — postgres, greenplum and cloudberry — because
 * a BI tool has to be able to say which engine it is talking to: the dialect
 * table, SQL_DBMS_NAME and the Tableau connector are all keyed on the backend
 * name. What they do *not* need is three copies of the wire plumbing, so
 * everything that is byte-for-byte identical between the three (opening a
 * libpq connection, the fetch loop, error mapping, the COPY decoder, the base
 * OID table) lives here, and each engine directory owns only what genuinely
 * differs: its vtable, its pg_catalog queries, its type quirks and its
 * dialect.
 */

typedef enum pg_engine {
    PG_ENGINE_POSTGRES = 0,
    PG_ENGINE_GREENPLUM,
    PG_ENGINE_CLOUDBERRY
} pg_engine_t;

struct pg_conn;

/*
 * What actually differs between the three engines.
 *
 * The catalog SQL for SQLColumns, SQLPrimaryKeys, SQLForeignKeys and the rest
 * is the same query on all three — they are the same pg_catalog. What is not
 * the same is which relations SQLTables should show (Greenplum's partition
 * children must be hidden, and where they are recorded changed between GP6 and
 * GP7), and what is worth putting in REMARKS (a distribution key means
 * something on an MPP engine and nothing on PostgreSQL). Each engine directory
 * fills in this profile; the shared query builders read it.
 */
typedef struct pg_profile {
    pg_engine_t  engine;
    const char  *backend_name;   /* "postgres" / "greenplum" / "cloudberry" */

    /* Extra predicate ANDed into SQLTables, using alias `c` for pg_class and
     * `n` for pg_namespace. NULL when there is nothing to exclude. The
     * connection is passed so the answer can depend on the detected major
     * version. */
    const char *(*tables_filter)(const struct pg_conn *conn);

    /* SQL expression yielding the REMARKS column for SQLTables, same aliases.
     * NULL falls back to obj_description(c.oid, 'pg_class'). */
    const char *(*remarks_expr)(const struct pg_conn *conn);
} pg_profile_t;

/* Connection. Owned by the backend, freed by disconnect(). */
typedef struct pg_conn {
    PGconn      *pg;

    const pg_profile_t *profile;

    /* Which engine the connection string asked for, and which one actually
     * answered. They can disagree (BACKEND=greenplum pointed at a plain
     * PostgreSQL); connect() warns rather than failing, because the connection
     * itself is perfectly usable — only the engine-specific catalog will be
     * empty, and the user needs to know why. */
    pg_engine_t  declared;
    pg_engine_t  detected;

    int          engine_major;   /* Greenplum 6/7, Cloudberry 1/2, else = pg_major */
    int          pg_major;       /* PostgreSQL major underneath (server_version_num) */
    char         version_str[128]; /* engine version for SQL_DBMS_VER */

    char        *database;
    int          fetch_batch;    /* rows per streaming chunk (FetchBufferSize) */
    bool         show_partitions;/* SHOWPARTITIONS=1: stop hiding child partitions */
    bool         use_copy;       /* COPY BINARY fast path allowed */

    /* Most recent server error, kept for get_last_error() and so the ODBC
     * layer can surface PostgreSQL's own SQLSTATE instead of a made-up one. */
    char         last_error[1024];
    char         last_sqlstate[6];
} pg_conn_t;

/* One executed statement. */
typedef struct pg_op {
    pg_conn_t           *conn;

    /* Streaming state. `pending` holds the PGresult currently being drained;
     * in single-row mode that is one row, in chunked mode (libpq >= 17) up to
     * fetch_batch rows. */
    PGresult            *pending;
    int                  pending_row;
    bool                 streaming;
    bool                 drained;      /* server has no more rows */

    argus_column_desc_t *columns;
    int                  num_cols;
    bool                 metadata_fetched;

    long long            affected_rows; /* PQcmdTuples for DML/DDL */

    /* COPY BINARY path (pg_copy.c). NULL when not taken. */
    struct pg_copy_state *copy;
} pg_op_t;

/* ── pg_error.c ──────────────────────────────────────────────── */

/* Record a failed PGresult (or, when res is NULL, the connection-level error)
 * on the connection, and return -1 so callers can `return pg_fail(...)`. */
int  pg_fail(pg_conn_t *conn, PGresult *res, const char *fallback_sqlstate);
/* Push the recorded error onto a DBC diag with PostgreSQL's own SQLSTATE. */
void pg_push_diag(argus_dbc_t *dbc, pg_conn_t *conn, const char *fallback_sqlstate);
bool pg_get_last_error(argus_backend_conn_t conn, char *buf, size_t buflen);

/* ── pg_probe.c ──────────────────────────────────────────────── */

/* Identify the engine and version behind an open connection. Fills detected,
 * engine_major, pg_major and version_str. Returns 0 on success. */
int         pg_probe_identity(pg_conn_t *conn);
const char *pg_engine_name(pg_engine_t engine);      /* "PostgreSQL", ... */
const char *pg_engine_backend_name(pg_engine_t e);   /* "postgres", ... */
bool        pg_get_server_version(argus_backend_conn_t conn, char *buf, size_t buflen);

/* ── pg_session.c ────────────────────────────────────────────── */

int  pg_connect(const pg_profile_t *profile, argus_dbc_t *dbc,
                const char *host, int port,
                const char *username, const char *password,
                const char *database, const char *auth_mechanism,
                argus_backend_conn_t *out_conn);
void pg_disconnect(argus_backend_conn_t conn);
bool pg_is_alive(argus_backend_conn_t conn);

/* ── pg_query.c ──────────────────────────────────────────────── */

/* Streaming execute: the caller drains rows through fetch_results(). */
int  pg_execute(argus_backend_conn_t conn, const char *query,
                argus_backend_op_t *out_op);
/* Buffered execute, for catalog queries and other small internal result sets
 * where streaming would only add round trips. */
int  pg_execute_buffered(argus_backend_conn_t conn, const char *query,
                         argus_backend_op_t *out_op);
int  pg_get_operation_status(argus_backend_conn_t conn, argus_backend_op_t op,
                             bool *finished);
void pg_close_operation(argus_backend_conn_t conn, argus_backend_op_t op);
int  pg_cancel(argus_backend_conn_t conn, argus_backend_op_t op);

/* ── pg_fetch.c ──────────────────────────────────────────────── */

int  pg_fetch_results(argus_backend_conn_t conn, argus_backend_op_t op,
                      int max_rows, argus_row_cache_t *cache,
                      argus_column_desc_t *columns, int *num_cols);
int  pg_get_result_metadata(argus_backend_conn_t conn, argus_backend_op_t op,
                            argus_column_desc_t *columns, int *num_cols);
/* Fill the op's cached column descriptors from a PGresult. */
void pg_describe_result(pg_op_t *op, PGresult *res);

/* ── pg_types.c ──────────────────────────────────────────────── */

/* PostgreSQL type OIDs the driver knows by name. Values are from
 * catalog/pg_type.dat and are stable across every PostgreSQL, Greenplum and
 * Cloudberry release. */
#define PG_OID_BOOL         16
#define PG_OID_BYTEA        17
#define PG_OID_CHAR         18
#define PG_OID_NAME         19
#define PG_OID_INT8         20
#define PG_OID_INT2         21
#define PG_OID_INT4         23
#define PG_OID_TEXT         25
#define PG_OID_OID          26
#define PG_OID_XML          142
#define PG_OID_JSON         114
#define PG_OID_FLOAT4       700
#define PG_OID_FLOAT8       701
#define PG_OID_MONEY        790
#define PG_OID_BPCHAR       1042
#define PG_OID_VARCHAR      1043
#define PG_OID_DATE         1082
#define PG_OID_TIME         1083
#define PG_OID_TIMESTAMP    1114
#define PG_OID_TIMESTAMPTZ  1184
#define PG_OID_INTERVAL     1186
#define PG_OID_TIMETZ       1266
#define PG_OID_BIT          1560
#define PG_OID_VARBIT       1562
#define PG_OID_NUMERIC      1700
#define PG_OID_UUID         2950
#define PG_OID_JSONB        3802

SQLSMALLINT pg_oid_to_sql_type(Oid oid, int atttypmod);
SQLULEN     pg_column_size(Oid oid, int atttypmod);
SQLSMALLINT pg_decimal_digits(Oid oid, int atttypmod);
/* Which ARGUS_NATIVE_* kind, if any, this OID can be delivered as without a
 * text round trip. ARGUS_NATIVE_NONE when the text form is the value. */
uint8_t     pg_native_kind(Oid oid);
/* SQLGetTypeInfo, synthesised as a UNION ALL of literal rows. */
int         pg_get_type_info(argus_backend_conn_t conn, SQLSMALLINT sql_type,
                             argus_backend_op_t *out_op);

/* ── pg_metadata_base.c ──────────────────────────────────────────
 * The catalog queries that are identical on all three engines; the
 * engine-specific parts come from the connection's pg_profile_t. */

int pg_get_tables(argus_backend_conn_t conn, const char *catalog,
                  const char *schema, const char *table_name,
                  const char *table_types, argus_backend_op_t *out_op);
int pg_get_columns(argus_backend_conn_t conn, const char *catalog,
                   const char *schema, const char *table_name,
                   const char *column_name, argus_backend_op_t *out_op);
int pg_get_schemas(argus_backend_conn_t conn, const char *catalog,
                   const char *schema, argus_backend_op_t *out_op);
int pg_get_catalogs(argus_backend_conn_t conn, argus_backend_op_t *out_op);
int pg_get_primary_keys(argus_backend_conn_t conn, const char *catalog,
                        const char *schema, const char *table_name,
                        argus_backend_op_t *out_op);
int pg_get_statistics(argus_backend_conn_t conn, const char *catalog,
                      const char *schema, const char *table_name,
                      unsigned short unique, unsigned short reserved,
                      argus_backend_op_t *out_op);

/* ── pg_meta_util.c ──────────────────────────────────────────── */

/* Append `value` to `sql` as a correctly-quoted SQL literal, using libpq's own
 * escaping. Never interpolate a catalog filter by hand: the ODBC caller
 * controls these strings. */
void pg_append_literal(GString *sql, PGconn *pg, const char *value);
/* Append `AND <col> LIKE <pattern>` (or `=` when the pattern has no wildcard),
 * skipping the clause entirely when the pattern is NULL/empty. */
void pg_append_pattern(GString *sql, PGconn *pg, const char *col,
                       const char *pattern);
/* Translate an ODBC table-type list ("TABLE,'VIEW'") into a pg_class.relkind
 * IN-list. Appends nothing when the list is empty or matches everything. */
void pg_append_relkinds(GString *sql, const char *col, const char *table_types);
/* The SQL CASE expression mapping pg_type OIDs to numeric ODBC type codes,
 * for SQLColumns' DATA_TYPE column. `expr` is the OID-valued expression. */
void pg_append_odbc_type_case(GString *sql, const char *oid_expr,
                              const char *typmod_expr);

#endif /* ARGUS_PG_COMMON_H */
