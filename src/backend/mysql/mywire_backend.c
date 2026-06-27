#include "mywire_internal.h"
#include "argus/compat.h"
#include <stdlib.h>
#include <string.h>

/* ── Connection lifecycle ────────────────────────────────────── */

static int mywire_connect(argus_dbc_t *dbc,
                          const char *host, int port,
                          const char *username, const char *password,
                          const char *database,
                          const char *auth_mechanism,
                          argus_backend_conn_t *out_conn)
{
    (void)auth_mechanism;
    if (!out_conn) return -1;

    mywire_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) return -1;

    conn->mysql = mysql_init(NULL);
    if (!conn->mysql) {
        free(conn);
        return -1;
    }

    /* Full Unicode over the wire. */
    mysql_options(conn->mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");

    if (dbc && dbc->connect_timeout_sec > 0) {
        unsigned int t = (unsigned int)dbc->connect_timeout_sec;
        mysql_options(conn->mysql, MYSQL_OPT_CONNECT_TIMEOUT, &t);
    }

    /* SSL/TLS, driven by the same DBC attributes as the other backends. */
    if (dbc && dbc->ssl_enabled) {
        mysql_ssl_set(conn->mysql,
                      dbc->ssl_key_file, dbc->ssl_cert_file,
                      dbc->ssl_ca_file, NULL, NULL);
        my_bool verify = dbc->ssl_verify ? 1 : 0;
        mysql_options(conn->mysql,
                      MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &verify);
    }

    unsigned int p = (port > 0) ? (unsigned int)port : 3306;
    if (!mysql_real_connect(conn->mysql, host, username, password,
                            (database && *database) ? database : NULL,
                            p, NULL, 0)) {
        mysql_close(conn->mysql);
        free(conn);
        return -1;
    }

    if (database && *database) conn->database = strdup(database);
    *out_conn = conn;
    return 0;
}

static void mywire_disconnect(argus_backend_conn_t raw_conn)
{
    mywire_conn_t *conn = (mywire_conn_t *)raw_conn;
    if (!conn) return;
    if (conn->mysql) mysql_close(conn->mysql);
    free(conn->database);
    free(conn);
}

static bool mywire_is_alive(argus_backend_conn_t raw_conn)
{
    mywire_conn_t *conn = (mywire_conn_t *)raw_conn;
    if (!conn || !conn->mysql) return false;
    return mysql_ping(conn->mysql) == 0;
}

/* ── Query execution ─────────────────────────────────────────── */

int mywire_execute(argus_backend_conn_t raw_conn,
                   const char *query,
                   argus_backend_op_t *out_op)
{
    mywire_conn_t *conn = (mywire_conn_t *)raw_conn;
    if (!conn || !conn->mysql || !query || !out_op) return -1;

    if (mysql_real_query(conn->mysql, query, (unsigned long)strlen(query)) != 0)
        return -1;

    mywire_op_t *op = calloc(1, sizeof(*op));
    if (!op) return -1;

    /* Buffer the whole result set when the statement produced one. */
    if (mysql_field_count(conn->mysql) > 0) {
        op->result = mysql_store_result(conn->mysql);
        if (!op->result) {
            free(op);
            return -1;
        }
    }

    *out_op = op;
    return 0;
}

static int mywire_get_operation_status(argus_backend_conn_t conn,
                                       argus_backend_op_t op,
                                       bool *finished)
{
    (void)conn;
    (void)op;
    if (finished) *finished = true;   /* synchronous protocol */
    return 0;
}

static void mywire_close_operation(argus_backend_conn_t conn,
                                   argus_backend_op_t raw_op)
{
    (void)conn;
    mywire_op_t *op = (mywire_op_t *)raw_op;
    if (!op) return;
    if (op->result) mysql_free_result(op->result);
    free(op->columns);
    free(op);
}

static int mywire_cancel(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn;
    (void)op;
    /* Results are fetched synchronously; there is nothing to cancel. */
    return 0;
}

/* ── Result metadata ─────────────────────────────────────────── */

static void mywire_fill_columns(MYSQL_RES *result,
                                argus_column_desc_t *columns, int *num_cols)
{
    unsigned int n = mysql_num_fields(result);
    if (n > ARGUS_MAX_COLUMNS) n = ARGUS_MAX_COLUMNS;

    MYSQL_FIELD *fields = mysql_fetch_fields(result);

    for (unsigned int i = 0; i < n; i++) {
        argus_column_desc_t *col = &columns[i];
        memset(col, 0, sizeof(*col));

        const char *name = fields[i].name ? fields[i].name : "";
        strncpy((char *)col->name, name, ARGUS_MAX_COLUMN_NAME - 1);
        col->name_len = (SQLSMALLINT)strlen((char *)col->name);

        col->sql_type = mywire_field_to_sql_type(fields[i].type,
                                                 fields[i].flags);
        col->column_size = mywire_column_size(col->sql_type, fields[i].length);
        col->decimal_digits = mywire_decimal_digits(col->sql_type,
                                                    fields[i].decimals);
        col->nullable = (fields[i].flags & NOT_NULL_FLAG)
                        ? SQL_NO_NULLS : SQL_NULLABLE;

        if (fields[i].table)
            strncpy((char *)col->table_name, fields[i].table,
                    ARGUS_MAX_COLUMN_NAME - 1);
        if (fields[i].db)
            strncpy((char *)col->schema_name, fields[i].db,
                    ARGUS_MAX_COLUMN_NAME - 1);
    }

    *num_cols = (int)n;
}

static int mywire_cache_metadata(mywire_op_t *op,
                                 argus_column_desc_t *columns, int *num_cols)
{
    mywire_fill_columns(op->result, columns, num_cols);
    op->num_cols = *num_cols;
    op->columns = calloc((size_t)(op->num_cols > 0 ? op->num_cols : 1),
                         sizeof(argus_column_desc_t));
    if (op->columns)
        memcpy(op->columns, columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
    op->metadata_fetched = true;
    return 0;
}

static int mywire_get_result_metadata(argus_backend_conn_t raw_conn,
                                      argus_backend_op_t raw_op,
                                      argus_column_desc_t *columns,
                                      int *num_cols)
{
    mywire_conn_t *conn = (mywire_conn_t *)raw_conn;
    mywire_op_t *op = (mywire_op_t *)raw_op;
    if (!conn || !op || !columns || !num_cols) return -1;

    if (op->metadata_fetched && op->columns) {
        memcpy(columns, op->columns,
               (size_t)op->num_cols * sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
        return 0;
    }

    if (!op->result) {
        *num_cols = 0;
        return 0;
    }

    return mywire_cache_metadata(op, columns, num_cols);
}

/* ── Result fetching ─────────────────────────────────────────── */

static int mywire_fetch_results(argus_backend_conn_t raw_conn,
                                argus_backend_op_t raw_op,
                                int max_rows,
                                argus_row_cache_t *cache,
                                argus_column_desc_t *columns,
                                int *num_cols)
{
    mywire_conn_t *conn = (mywire_conn_t *)raw_conn;
    mywire_op_t *op = (mywire_op_t *)raw_op;
    if (!conn || !op || !cache) return -1;

    /* Hand back column metadata (cached after the first call). */
    if (columns && num_cols) {
        if (op->metadata_fetched && op->columns) {
            memcpy(columns, op->columns,
                   (size_t)op->num_cols * sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        } else if (op->result) {
            mywire_cache_metadata(op, columns, num_cols);
        } else {
            *num_cols = 0;
        }
    }

    /* DML/DDL: no result set to read. */
    if (!op->result) {
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    int ncols = op->num_cols > 0
                ? op->num_cols
                : (int)mysql_num_fields(op->result);
    int batch = (max_rows > 0) ? max_rows : 1000;

    cache->rows = calloc((size_t)batch, sizeof(argus_row_t));
    if (!cache->rows) return -1;
    cache->capacity = (size_t)batch;
    cache->num_cols = ncols;

    size_t r = 0;
    MYSQL_ROW row;
    while (r < (size_t)batch && (row = mysql_fetch_row(op->result)) != NULL) {
        unsigned long *lengths = mysql_fetch_lengths(op->result);

        cache->rows[r].cells = calloc((size_t)ncols, sizeof(argus_cell_t));
        if (!cache->rows[r].cells) return -1;

        for (int c = 0; c < ncols; c++) {
            argus_cell_t *cell = &cache->rows[r].cells[c];
            if (!row[c]) {
                cell->is_null = true;
                cell->data = NULL;
                cell->data_len = 0;
                continue;
            }
            size_t len = lengths ? (size_t)lengths[c] : strlen(row[c]);
            cell->data = malloc(len + 1);
            if (!cell->data) return -1;
            memcpy(cell->data, row[c], len);
            cell->data[len] = '\0';
            cell->data_len = len;
            cell->is_null = false;
        }
        r++;
    }

    cache->num_rows = r;
    if (r < (size_t)batch)
        cache->exhausted = true;   /* mysql_fetch_row returned NULL → end */

    return 0;
}

/* ── Last error message ──────────────────────────────────────── */

static bool mywire_get_last_error(argus_backend_conn_t raw_conn,
                                  char *buf, size_t buflen)
{
    mywire_conn_t *conn = (mywire_conn_t *)raw_conn;
    if (!conn || !conn->mysql || buflen == 0) return false;
    const char *e = mysql_error(conn->mysql);
    if (!e || !*e) return false;
    strncpy(buf, e, buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
}

/* ── Backend vtable ──────────────────────────────────────────── */

static const argus_backend_t mywire_backend = {
    .name                  = "mysql",
    .connect               = mywire_connect,
    .disconnect            = mywire_disconnect,
    .is_alive              = mywire_is_alive,
    .execute               = mywire_execute,
    .get_operation_status  = mywire_get_operation_status,
    .close_operation       = mywire_close_operation,
    .cancel                = mywire_cancel,
    .fetch_results         = mywire_fetch_results,
    .get_result_metadata   = mywire_get_result_metadata,
    .get_tables            = mywire_get_tables,
    .get_columns           = mywire_get_columns,
    .get_type_info         = mywire_get_type_info,
    .get_schemas           = mywire_get_schemas,
    .get_catalogs          = mywire_get_catalogs,
    .get_primary_keys      = mywire_get_primary_keys,
    .get_last_error        = mywire_get_last_error,
};

const argus_backend_t *argus_mysql_backend_get(void)
{
    return &mywire_backend;
}
