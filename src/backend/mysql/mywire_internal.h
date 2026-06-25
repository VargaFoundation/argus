#ifndef ARGUS_MYWIRE_INTERNAL_H
#define ARGUS_MYWIRE_INTERNAL_H

#include <mysql.h>
#include "argus/backend.h"
#include "argus/handle.h"
#include "argus/types.h"

/*
 * MySQL-wire backend.
 *
 * Serves any engine that speaks the MySQL client/server protocol with a
 * single libmariadb-based implementation: MySQL, MariaDB, StarRocks
 * (FE query port 9030), Apache Doris (9030) and ClickHouse (MySQL
 * interface, 9004). One backend, several engines.
 *
 * The protocol is synchronous and the text result set is buffered with
 * mysql_store_result(), so the operation model is trivial: every
 * statement is "finished" as soon as execute() returns.
 */

typedef struct mywire_conn {
    MYSQL *mysql;
    char  *database;
} mywire_conn_t;

/* One executed statement plus its (optional) buffered result set. */
typedef struct mywire_op {
    MYSQL_RES           *result;          /* mysql_store_result(), NULL for DML/DDL */
    bool                 metadata_fetched;
    argus_column_desc_t *columns;         /* cached column metadata */
    int                  num_cols;
} mywire_op_t;

/* ── mywire_types.c ──────────────────────────────────────────── */
SQLSMALLINT mywire_field_to_sql_type(enum enum_field_types type,
                                     unsigned int flags);
SQLULEN     mywire_column_size(SQLSMALLINT sql_type,
                               unsigned long field_length);
SQLSMALLINT mywire_decimal_digits(SQLSMALLINT sql_type, unsigned int decimals);

/* ── mywire_backend.c (shared by the metadata helpers) ───────── */
int mywire_execute(argus_backend_conn_t conn, const char *query,
                   argus_backend_op_t *out_op);

/* ── mywire_metadata.c ───────────────────────────────────────── */
int mywire_get_tables(argus_backend_conn_t conn, const char *catalog,
                      const char *schema, const char *table_name,
                      const char *table_types, argus_backend_op_t *out_op);
int mywire_get_columns(argus_backend_conn_t conn, const char *catalog,
                       const char *schema, const char *table_name,
                       const char *column_name, argus_backend_op_t *out_op);
int mywire_get_schemas(argus_backend_conn_t conn, const char *catalog,
                       const char *schema, argus_backend_op_t *out_op);
int mywire_get_catalogs(argus_backend_conn_t conn, argus_backend_op_t *out_op);
int mywire_get_type_info(argus_backend_conn_t conn, SQLSMALLINT sql_type,
                         argus_backend_op_t *out_op);
int mywire_get_primary_keys(argus_backend_conn_t conn, const char *catalog,
                            const char *schema, const char *table_name,
                            argus_backend_op_t *out_op);

#endif /* ARGUS_MYWIRE_INTERNAL_H */
