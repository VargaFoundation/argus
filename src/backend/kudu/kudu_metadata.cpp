/*
 * Kudu catalog operations (metadata queries).
 * Implements get_tables, get_columns, get_schemas, get_catalogs, get_type_info
 * via the Kudu C++ client API.
 */
#include <kudu/client/client.h>
#include <string>
#include <vector>
#include <cstring>

extern "C" {
#include "kudu_internal.h"
#include "argus/log.h"
}

using kudu::client::KuduClient;
using kudu::client::KuduTable;
using kudu::client::KuduSchema;
using kudu::client::KuduColumnSchema;
using kudu::Status;

/* ── Helper: get KuduClient from opaque pointer ──────────────── */

static KuduClient *get_client(void *client_ptr)
{
    auto *sp = static_cast<std::shared_ptr<KuduClient> *>(client_ptr);
    return sp->get();
}

/* ── List tables via KuduClient ──────────────────────────────── */

extern "C"
int kudu_cpp_list_tables(void *client, const char *table_prefix,
                         const char *filter, char ***out_tables,
                         int *out_count)
{
    KuduClient *kclient = get_client(client);
    if (!kclient) return -1;

    std::vector<std::string> tables;
    Status s = kclient->ListTables(&tables);
    if (!s.ok()) {
        ARGUS_LOG_ERROR("Kudu ListTables failed: %s", s.ToString().c_str());
        return -1;
    }

    /* Filter by prefix and pattern */
    std::vector<std::string> filtered;
    std::string prefix_str;
    if (table_prefix && *table_prefix &&
        strcmp(table_prefix, "default") != 0) {
        prefix_str = std::string(table_prefix) + ".";
    }

    for (const auto &t : tables) {
        /* If there's a prefix, only include matching tables */
        if (!prefix_str.empty()) {
            if (t.compare(0, prefix_str.size(), prefix_str) != 0)
                continue;
        }

        /* If there's a filter pattern, do simple substring match */
        if (filter && *filter && filter[0] != '%') {
            /* Simple LIKE pattern: only handle % wildcards */
            std::string f(filter);
            if (f == "%") {
                /* Match all */
            } else if (f.front() == '%' && f.back() == '%') {
                std::string sub = f.substr(1, f.size() - 2);
                if (t.find(sub) == std::string::npos) continue;
            } else if (f.back() == '%') {
                std::string sub = f.substr(0, f.size() - 1);
                if (t.compare(0, sub.size(), sub) != 0) continue;
            } else if (f.front() == '%') {
                std::string sub = f.substr(1);
                if (t.size() < sub.size()) continue;
                if (t.compare(t.size() - sub.size(), sub.size(), sub) != 0)
                    continue;
            } else {
                if (t != f && t != prefix_str + f) continue;
            }
        }

        filtered.push_back(t);
    }

    int count = static_cast<int>(filtered.size());
    char **result = static_cast<char **>(calloc(count, sizeof(char *)));
    if (!result) return -1;

    for (int i = 0; i < count; i++) {
        /* Strip prefix for display */
        std::string display_name = filtered[i];
        if (!prefix_str.empty() &&
            display_name.compare(0, prefix_str.size(), prefix_str) == 0) {
            display_name = display_name.substr(prefix_str.size());
        }
        result[i] = strdup(display_name.c_str());
    }

    *out_tables = result;
    *out_count = count;
    return 0;
}

/* ── Get table schema ────────────────────────────────────────── */

extern "C"
int kudu_cpp_get_table_schema(void *client, const char *table_name,
                              argus_column_desc_t *columns, int *num_cols,
                              int **out_kudu_types)
{
    KuduClient *kclient = get_client(client);
    if (!kclient) return -1;

    std::shared_ptr<KuduTable> table;
    Status s = kclient->OpenTable(table_name, &table);
    if (!s.ok()) {
        ARGUS_LOG_ERROR("Kudu OpenTable failed: %s", s.ToString().c_str());
        return -1;
    }

    const KuduSchema &schema = table->schema();
    int ncols = schema.num_columns();
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;

    int *kudu_types = nullptr;
    if (out_kudu_types) {
        kudu_types = static_cast<int *>(calloc(ncols, sizeof(int)));
    }

    for (int i = 0; i < ncols; i++) {
        const KuduColumnSchema &col = schema.Column(i);
        argus_column_desc_t *desc = &columns[i];
        memset(desc, 0, sizeof(*desc));

        const std::string &name = col.name();
        strncpy(reinterpret_cast<char *>(desc->name), name.c_str(),
                ARGUS_MAX_COLUMN_NAME - 1);
        desc->name_len = static_cast<SQLSMALLINT>(name.size());

        desc->sql_type = kudu_type_to_sql_type(col.type());
        desc->column_size = kudu_type_column_size(desc->sql_type);
        desc->decimal_digits = kudu_type_decimal_digits(desc->sql_type);
        desc->nullable = col.is_nullable() ? SQL_NULLABLE : SQL_NO_NULLS;

        if (kudu_types) {
            kudu_types[i] = col.type();
        }
    }

    *num_cols = ncols;
    if (out_kudu_types) *out_kudu_types = kudu_types;
    return 0;
}

/* ── Helper: build synthetic result set ──────────────────────── */

static kudu_operation_t *create_synthetic_op(int ncols,
                                              const char **col_names,
                                              SQLSMALLINT *col_types)
{
    kudu_operation_t *op = kudu_operation_new();
    if (!op) return nullptr;

    op->is_synthetic = true;
    op->has_result_set = true;
    op->metadata_fetched = true;
    op->finished = true;

    op->columns = static_cast<argus_column_desc_t *>(
        calloc(ncols, sizeof(argus_column_desc_t)));
    if (!op->columns) {
        kudu_operation_free(op);
        return nullptr;
    }
    op->num_cols = ncols;

    for (int i = 0; i < ncols; i++) {
        argus_column_desc_t *col = &op->columns[i];
        strncpy(reinterpret_cast<char *>(col->name), col_names[i],
                ARGUS_MAX_COLUMN_NAME - 1);
        col->name_len = static_cast<SQLSMALLINT>(strlen(col_names[i]));
        col->sql_type = col_types[i];
        col->column_size = kudu_type_column_size(col_types[i]);
        col->nullable = SQL_NULLABLE_UNKNOWN;
    }

    op->synthetic_cache = static_cast<argus_row_cache_t *>(
        calloc(1, sizeof(argus_row_cache_t)));
    if (!op->synthetic_cache) {
        kudu_operation_free(op);
        return nullptr;
    }

    return op;
}

static void add_synthetic_row(argus_row_cache_t *cache, int ncols, ...)
{
    va_list args;
    va_start(args, ncols);

    size_t new_count = cache->num_rows + 1;
    cache->rows = static_cast<argus_row_t *>(
        realloc(cache->rows, new_count * sizeof(argus_row_t)));

    argus_row_t *row = &cache->rows[cache->num_rows];
    row->cells = static_cast<argus_cell_t *>(
        calloc(ncols, sizeof(argus_cell_t)));

    for (int i = 0; i < ncols; i++) {
        const char *val = va_arg(args, const char *);
        if (val) {
            row->cells[i].is_null = false;
            row->cells[i].data = strdup(val);
            row->cells[i].data_len = strlen(val);
        } else {
            row->cells[i].is_null = true;
        }
    }

    cache->num_rows = new_count;
    cache->capacity = new_count;
    cache->num_cols = ncols;
    va_end(args);
}

/* ── GetTables ───────────────────────────────────────────────── */

extern "C"
int kudu_get_tables(argus_backend_conn_t raw_conn,
                    const char *catalog,
                    const char *schema,
                    const char *table_name,
                    const char *table_types,
                    argus_backend_op_t *out_op)
{
    kudu_conn_t *conn = static_cast<kudu_conn_t *>(raw_conn);
    if (!conn) return -1;
    (void)schema;
    (void)table_types;

    /* List tables from Kudu */
    char **tables = nullptr;
    int count = 0;
    int rc = kudu_cpp_list_tables(conn->client, conn->database,
                                  table_name, &tables, &count);
    if (rc != 0) return -1;

    /* Build synthetic result set matching ODBC SQLTables columns */
    const char *col_names[] = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
        "TABLE_TYPE", "REMARKS"
    };
    SQLSMALLINT col_types[] = {
        SQL_VARCHAR, SQL_VARCHAR, SQL_VARCHAR,
        SQL_VARCHAR, SQL_VARCHAR
    };

    kudu_operation_t *op = create_synthetic_op(5, col_names, col_types);
    if (!op) {
        for (int i = 0; i < count; i++) free(tables[i]);
        free(tables);
        return -1;
    }

    const char *cat = (catalog && *catalog) ? catalog : conn->database;

    for (int i = 0; i < count; i++) {
        add_synthetic_row(op->synthetic_cache, 5,
                          cat, "default", tables[i], "TABLE", nullptr);
        free(tables[i]);
    }
    free(tables);

    *out_op = op;
    return 0;
}

/* ── GetColumns ──────────────────────────────────────────────── */

extern "C"
int kudu_get_columns(argus_backend_conn_t raw_conn,
                     const char *catalog,
                     const char *schema,
                     const char *table_name,
                     const char *column_name,
                     argus_backend_op_t *out_op)
{
    kudu_conn_t *conn = static_cast<kudu_conn_t *>(raw_conn);
    if (!conn) return -1;
    (void)schema;
    (void)column_name;

    if (!table_name || !*table_name) return -1;

    /* Build full table name */
    std::string full_name;
    if (conn->database && *conn->database &&
        strcmp(conn->database, "default") != 0) {
        full_name = std::string(conn->database) + "." + table_name;
    } else {
        full_name = table_name;
    }

    /* Get schema from Kudu */
    argus_column_desc_t tbl_cols[ARGUS_MAX_COLUMNS];
    int ncols = 0;
    int *kudu_types = nullptr;
    int rc = kudu_cpp_get_table_schema(conn->client, full_name.c_str(),
                                       tbl_cols, &ncols, &kudu_types);
    if (rc != 0) return -1;

    /* Build synthetic result set matching ODBC SQLColumns output */
    const char *col_names[] = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
        "COLUMN_NAME", "TYPE_NAME", "ORDINAL_POSITION",
        "IS_NULLABLE"
    };
    SQLSMALLINT col_types[] = {
        SQL_VARCHAR, SQL_VARCHAR, SQL_VARCHAR,
        SQL_VARCHAR, SQL_VARCHAR, SQL_INTEGER,
        SQL_VARCHAR
    };

    kudu_operation_t *op = create_synthetic_op(7, col_names, col_types);
    if (!op) {
        free(kudu_types);
        return -1;
    }

    const char *cat = (catalog && *catalog) ? catalog : conn->database;

    for (int i = 0; i < ncols; i++) {
        char ordinal[16];
        snprintf(ordinal, sizeof(ordinal), "%d", i + 1);

        const char *type_name = kudu_types ?
            kudu_type_id_to_name(kudu_types[i]) : "UNKNOWN";
        const char *nullable = tbl_cols[i].nullable == SQL_NULLABLE ?
            "YES" : "NO";

        add_synthetic_row(op->synthetic_cache, 7,
                          cat, "default", table_name,
                          reinterpret_cast<const char *>(tbl_cols[i].name),
                          type_name, ordinal, nullable);
    }

    free(kudu_types);

    *out_op = op;
    return 0;
}

/* ── GetTypeInfo ─────────────────────────────────────────────── */

extern "C"
int kudu_get_type_info(argus_backend_conn_t raw_conn,
                       SQLSMALLINT sql_type,
                       argus_backend_op_t *out_op)
{
    (void)raw_conn;
    (void)sql_type;

    /* Static type info for Kudu types */
    const char *col_names[] = {
        "TYPE_NAME", "DATA_TYPE", "COLUMN_SIZE",
        "NULLABLE", "SEARCHABLE"
    };
    SQLSMALLINT col_types[] = {
        SQL_VARCHAR, SQL_SMALLINT, SQL_INTEGER,
        SQL_SMALLINT, SQL_SMALLINT
    };

    kudu_operation_t *op = create_synthetic_op(5, col_names, col_types);
    if (!op) return -1;

    /* Add one row per Kudu type */
    add_synthetic_row(op->synthetic_cache, 5, "INT8",    "-6", "3",    "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "INT16",   "5",  "5",    "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "INT32",   "4",  "10",   "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "INT64",   "-5", "19",   "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "FLOAT",   "7",  "7",    "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "DOUBLE",  "8",  "15",   "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "BOOL",    "-7", "1",    "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "STRING",  "12", "65535","1", "3");
    add_synthetic_row(op->synthetic_cache, 5, "BINARY",  "-3", "65535","1", "0");
    add_synthetic_row(op->synthetic_cache, 5, "UNIXTIME_MICROS", "93", "29", "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "DECIMAL", "3",  "38",   "1", "2");
    add_synthetic_row(op->synthetic_cache, 5, "VARCHAR", "12", "65535","1", "3");
    add_synthetic_row(op->synthetic_cache, 5, "DATE",    "91", "10",   "1", "2");

    *out_op = op;
    return 0;
}

/* ── GetSchemas ──────────────────────────────────────────────── */

extern "C"
int kudu_get_schemas(argus_backend_conn_t raw_conn,
                     const char *catalog,
                     const char *schema,
                     argus_backend_op_t *out_op)
{
    kudu_conn_t *conn = static_cast<kudu_conn_t *>(raw_conn);
    if (!conn) return -1;
    (void)schema;

    /* Kudu has no schema concept — return single "default" schema */
    const char *col_names[] = {"TABLE_SCHEM", "TABLE_CATALOG"};
    SQLSMALLINT col_types[] = {SQL_VARCHAR, SQL_VARCHAR};

    kudu_operation_t *op = create_synthetic_op(2, col_names, col_types);
    if (!op) return -1;

    const char *cat = (catalog && *catalog) ? catalog : conn->database;
    add_synthetic_row(op->synthetic_cache, 2, "default", cat);

    *out_op = op;
    return 0;
}

/* ── GetCatalogs ─────────────────────────────────────────────── */

extern "C"
int kudu_get_catalogs(argus_backend_conn_t raw_conn,
                      argus_backend_op_t *out_op)
{
    kudu_conn_t *conn = static_cast<kudu_conn_t *>(raw_conn);
    if (!conn) return -1;

    const char *col_names[] = {"TABLE_CAT"};
    SQLSMALLINT col_types[] = {SQL_VARCHAR};

    kudu_operation_t *op = create_synthetic_op(1, col_names, col_types);
    if (!op) return -1;

    add_synthetic_row(op->synthetic_cache, 1, conn->database);

    *out_op = op;
    return 0;
}
