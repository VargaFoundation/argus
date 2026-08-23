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

    /*
     * What this particular server's catalog actually contains.
     *
     * Greenplum 6, Greenplum 7 and Cloudberry do not agree on where partition
     * children, distribution policies and append-optimized storage are
     * recorded, and the engine version alone does not settle it — a fork can
     * carry a catalog its base version did not, or drop one. Probing for the
     * relations and columns directly costs one round trip at connect and means
     * a catalog query can never fail with "relation gp_… does not exist",
     * which is the failure mode of writing the SQL from documentation.
     */
    bool         has_gp_policy;      /* gp_distribution_policy               */
    bool         has_gp_policy_distkey; /* …with the GP6+ distkey column     */
    bool         has_pg_appendonly;  /* pg_appendonly (GP6 and GP7)          */
    bool         has_pg_exttable;    /* pg_exttable WITH urilocation (GP6)   */
    bool         has_partition_rule; /* pg_partition_rule (GP6 partitioning) */
    bool         has_relispartition; /* pg_class.relispartition (PG10+/GP7)  */
    bool         has_relam;          /* pg_class.relam (PG12+/GP7/CBDB)      */

    /* Transaction state (pg_txn.c). ODBC has no BEGIN: autocommit off means
     * every statement runs in a transaction, which is opened lazily so the
     * driver never manufactures an idle-in-transaction session. */
    bool         autocommit;
    char         isolation_sql[24];  /* "" until SQL_ATTR_TXN_ISOLATION is set */

    char        *database;
    int          fetch_batch;    /* rows per streaming chunk (FetchBufferSize) */
    bool         show_partitions;/* SHOWPARTITIONS=1: stop hiding child partitions */
    bool         use_copy;       /* COPY BINARY fast path allowed */

    /* SQL fragments assembled once from the probe flags above, because they
     * are rebuilt on every catalog call otherwise. Owned by the connection. */
    char        *mpp_remarks_expr;
    char        *mpp_tables_filter;

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
/* Same, plus PostgreSQL's own SQLSTATE, so the ODBC layer can report 42P01
 * rather than collapsing everything to HY000. */
bool pg_get_last_error_ex(argus_backend_conn_t conn, char sqlstate[6],
                          char *buf, size_t buflen);

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

/* ── pg_prepare.c ────────────────────────────────────────────────
 * Server-side Parse + Describe for SQLDescribeParam. Metadata only: execution
 * still renders parameters as literals, exactly as for every other backend. */

int pg_describe_params(argus_backend_conn_t conn, const char *query,
                       argus_column_desc_t *params, int *num_params);

/* ── pg_txn.c ────────────────────────────────────────────────── */

int  pg_set_autocommit(argus_backend_conn_t conn, bool on);
int  pg_end_transaction(argus_backend_conn_t conn, bool commit);
int  pg_set_isolation(argus_backend_conn_t conn, SQLUINTEGER odbc_isolation);
bool pg_reset_session(argus_backend_conn_t conn);
/* Open a transaction if autocommit is off and none is open. Called from the
 * execute path so the BEGIN travels with the statement that needs it. */
int  pg_txn_begin_if_needed(pg_conn_t *conn);

/* ── pg_mpp.c ────────────────────────────────────────────────────
 * The catalog fragments Greenplum and Cloudberry share. Both are MPP forks of
 * PostgreSQL with the same three questions to answer — which relations are
 * partition children, how a table is distributed, and how it is stored — so
 * the builders live here and each engine's profile decides whether to use
 * them. Everything they emit is gated on the probe flags, so a fragment can
 * never reference a catalog the server does not have. */

const char *pg_mpp_tables_filter(const struct pg_conn *conn);
const char *pg_mpp_remarks_expr(const struct pg_conn *conn);
/* The plain-PostgreSQL filter, used by the postgres profile and as the
 * fallback when an MPP backend finds itself talking to vanilla PostgreSQL. */
const char *pg_plain_tables_filter(const struct pg_conn *conn);

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

/* ── pg_metadata_ext.c ───────────────────────────────────────────
 * The catalog functions the ODBC layer used to answer with an empty result
 * set on every backend. PostgreSQL has all of them for real. */

int pg_get_foreign_keys(argus_backend_conn_t conn,
                        const char *pk_catalog, const char *pk_schema,
                        const char *pk_table,
                        const char *fk_catalog, const char *fk_schema,
                        const char *fk_table, argus_backend_op_t *out_op);
int pg_get_special_columns(argus_backend_conn_t conn,
                           SQLUSMALLINT identifier_type,
                           const char *catalog, const char *schema,
                           const char *table,
                           SQLUSMALLINT scope, SQLUSMALLINT nullable,
                           argus_backend_op_t *out_op);
int pg_get_procedures(argus_backend_conn_t conn, const char *catalog,
                      const char *schema, const char *proc,
                      argus_backend_op_t *out_op);
int pg_get_procedure_columns(argus_backend_conn_t conn, const char *catalog,
                             const char *schema, const char *proc,
                             const char *column, argus_backend_op_t *out_op);
int pg_get_table_privileges(argus_backend_conn_t conn, const char *catalog,
                            const char *schema, const char *table,
                            argus_backend_op_t *out_op);
int pg_get_column_privileges(argus_backend_conn_t conn, const char *catalog,
                             const char *schema, const char *table,
                             const char *column, argus_backend_op_t *out_op);

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
