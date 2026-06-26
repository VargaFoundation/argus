#include "flightsql_internal.h"
#include "flightsql_convert.h"

#include "argus/backend.h"
#include "argus/handle.h"
#include "argus/log.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

/*
 * NOTE: this translation unit requires libarrow-flight-sql at build time and a
 * live Flight SQL endpoint to exercise at runtime; it is compiled only when
 * ARGUS_BUILD_FLIGHTSQL is enabled. The Arrow→ODBC conversion it relies on
 * (flightsql_convert.cpp) is independently unit tested.
 */

/* ── Helper: drain one stream reader into the row cache ──────── */

static int drain_reader(flight::FlightStreamReader* reader,
                        argus_row_cache_t* cache,
                        argus_column_desc_t* columns, int* num_cols,
                        bool* have_meta)
{
    if (!*have_meta) {
        auto schema_res = reader->GetSchema();
        if (schema_res.ok() && columns && num_cols) {
            *num_cols = flightsql_schema_to_columns(schema_res.ValueOrDie(),
                                                    columns);
            *have_meta = true;
        }
    }

    while (true) {
        auto chunk_res = reader->Next();
        if (!chunk_res.ok()) return -1;
        flight::FlightStreamChunk chunk = std::move(chunk_res).ValueOrDie();
        if (!chunk.data) break;            /* end of stream */
        if (flightsql_append_batch(chunk.data, cache) != 0) return -1;
    }
    return 0;
}

/* ── Connection lifecycle ────────────────────────────────────── */

static int flightsql_connect(argus_dbc_t* dbc,
                             const char* host, int port,
                             const char* username, const char* password,
                             const char* database,
                             const char* auth_mechanism,
                             argus_backend_conn_t* out_conn)
{
    (void)database;
    (void)auth_mechanism;
    if (!out_conn || !host) return -1;

    int p = port > 0 ? port : 32010;   /* common Flight SQL default */

    auto loc_res = (dbc && dbc->ssl_enabled)
        ? flight::Location::ForGrpcTls(host, p)
        : flight::Location::ForGrpcTcp(host, p);
    if (!loc_res.ok()) return -1;

    auto client_res = flight::FlightClient::Connect(loc_res.ValueOrDie());
    if (!client_res.ok()) {
        ARGUS_LOG_ERROR("Flight SQL: connect failed: %s",
                        client_res.status().ToString().c_str());
        return -1;
    }

    auto* conn = new (std::nothrow) flightsql_conn();
    if (!conn) return -1;
    conn->host = host;
    conn->port = p;

    std::shared_ptr<flight::FlightClient> shared_client =
        std::move(client_res).ValueOrDie();

    /* Authentication:
     *  - username + password -> Flight handshake (basic token) -> bearer header
     *  - password only       -> treat as a bearer/JWT token directly */
    if (username && *username && password && *password) {
        auto auth_res = shared_client->AuthenticateBasicToken(
            conn->call_options, username, password);
        if (!auth_res.ok()) {
            ARGUS_LOG_ERROR("Flight SQL: authentication failed: %s",
                            auth_res.status().ToString().c_str());
            delete conn;
            return -1;
        }
        auto header = std::move(auth_res).ValueOrDie();   /* {name, value} */
        if (!header.first.empty())
            conn->call_options.headers.emplace_back(header.first, header.second);
    } else if (password && *password) {
        conn->call_options.headers.emplace_back(
            "authorization", std::string("Bearer ") + password);
    }

    conn->client = std::make_unique<flightsql::FlightSqlClient>(shared_client);
    *out_conn = conn;
    return 0;
}

static void flightsql_disconnect(argus_backend_conn_t raw_conn)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    delete conn;
}

static bool flightsql_is_alive(argus_backend_conn_t raw_conn)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    return conn && conn->client != nullptr;
}

/* ── Execution ───────────────────────────────────────────────── */

static int run_to_op(flightsql_conn* conn,
                     arrow::Result<std::unique_ptr<flight::FlightInfo>> info_res,
                     argus_backend_op_t* out_op)
{
    if (!info_res.ok()) {
        ARGUS_LOG_ERROR("Flight SQL: request failed: %s",
                        info_res.status().ToString().c_str());
        return -1;
    }
    auto* op = new (std::nothrow) flightsql_op();
    if (!op) return -1;
    op->info = std::move(info_res).ValueOrDie();
    *out_op = op;
    return 0;
}

static int flightsql_execute(argus_backend_conn_t raw_conn,
                             const char* query,
                             argus_backend_op_t* out_op)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client || !query || !out_op) return -1;
    return run_to_op(conn, conn->client->Execute(conn->call_options, query),
                     out_op);
}

static int flightsql_get_operation_status(argus_backend_conn_t conn,
                                          argus_backend_op_t op,
                                          bool* finished)
{
    (void)conn;
    (void)op;
    if (finished) *finished = true;   /* Flight delivers synchronously here */
    return 0;
}

static void flightsql_close_operation(argus_backend_conn_t conn,
                                      argus_backend_op_t raw_op)
{
    (void)conn;
    auto* op = static_cast<flightsql_op*>(raw_op);
    if (!op) return;
    free(op->columns);
    delete op;
}

static int flightsql_cancel(argus_backend_conn_t conn, argus_backend_op_t op)
{
    (void)conn;
    (void)op;
    return 0;
}

/* ── Result fetching ─────────────────────────────────────────── */

static int flightsql_fetch_results(argus_backend_conn_t raw_conn,
                                   argus_backend_op_t raw_op,
                                   int max_rows,
                                   argus_row_cache_t* cache,
                                   argus_column_desc_t* columns,
                                   int* num_cols)
{
    (void)max_rows;
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    auto* op = static_cast<flightsql_op*>(raw_op);
    if (!conn || !conn->client || !op || !cache) return -1;

    /* Already drained: hand back cached metadata, report exhaustion. */
    if (op->fetched) {
        if (columns && num_cols && op->columns) {
            std::memcpy(columns, op->columns,
                        static_cast<size_t>(op->num_cols) *
                            sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        }
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    bool have_meta = false;
    int local_cols = 0;
    argus_column_desc_t local_meta[ARGUS_MAX_COLUMNS];

    /* Flight can shard a result across endpoints; drain them in order. */
    for (const auto& endpoint : op->info->endpoints()) {
        auto reader_res = conn->client->DoGet(conn->call_options,
                                              endpoint.ticket);
        if (!reader_res.ok()) {
            ARGUS_LOG_ERROR("Flight SQL: DoGet failed: %s",
                            reader_res.status().ToString().c_str());
            return -1;
        }
        auto reader = std::move(reader_res).ValueOrDie();
        if (drain_reader(reader.get(), cache, local_meta, &local_cols,
                         &have_meta) != 0)
            return -1;
    }

    /* Cache metadata for subsequent get_result_metadata / fetch calls. */
    if (have_meta) {
        op->columns = static_cast<argus_column_desc_t*>(
            std::calloc(static_cast<size_t>(local_cols > 0 ? local_cols : 1),
                        sizeof(argus_column_desc_t)));
        if (op->columns) {
            std::memcpy(op->columns, local_meta,
                        static_cast<size_t>(local_cols) *
                            sizeof(argus_column_desc_t));
            op->num_cols = local_cols;
            op->metadata_fetched = true;
        }
        if (columns && num_cols) {
            std::memcpy(columns, local_meta,
                        static_cast<size_t>(local_cols) *
                            sizeof(argus_column_desc_t));
            *num_cols = local_cols;
        }
    }

    op->fetched = true;
    cache->exhausted = true;
    return 0;
}

static int flightsql_get_result_metadata(argus_backend_conn_t raw_conn,
                                         argus_backend_op_t raw_op,
                                         argus_column_desc_t* columns,
                                         int* num_cols)
{
    (void)raw_conn;
    auto* op = static_cast<flightsql_op*>(raw_op);
    if (!op || !columns || !num_cols) return -1;

    if (op->metadata_fetched && op->columns) {
        std::memcpy(columns, op->columns,
                    static_cast<size_t>(op->num_cols) *
                        sizeof(argus_column_desc_t));
        *num_cols = op->num_cols;
        return 0;
    }

    /* Metadata is materialized on first fetch; fall back to the FlightInfo
     * schema when present. */
    if (op->info) {
        std::shared_ptr<arrow::Schema> schema;
        auto st = op->info->GetSchema(nullptr, &schema);
        if (st.ok() && schema) {
            *num_cols = flightsql_schema_to_columns(schema, columns);
            return 0;
        }
    }
    *num_cols = 0;
    return 0;
}

/* ── Catalog operations (native Flight SQL metadata RPCs) ────── */

static int flightsql_get_tables(argus_backend_conn_t raw_conn,
                                const char* catalog, const char* schema,
                                const char* table_name, const char* table_types,
                                argus_backend_op_t* out_op)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client) return -1;

    std::string cat, sch, tbl;
    std::vector<std::string> types;
    const std::string* cat_p = nullptr;
    const std::string* sch_p = nullptr;
    const std::string* tbl_p = nullptr;
    if (catalog && *catalog)    { cat = catalog;    cat_p = &cat; }
    if (schema && *schema)      { sch = schema;     sch_p = &sch; }
    if (table_name && *table_name) { tbl = table_name; tbl_p = &tbl; }
    if (table_types && *table_types) types.emplace_back(table_types);

    return run_to_op(conn,
        conn->client->GetTables(conn->call_options, cat_p, sch_p, tbl_p,
                                /*include_schema=*/false, &types),
        out_op);
}

static int flightsql_get_columns(argus_backend_conn_t raw_conn,
                                 const char* catalog, const char* schema,
                                 const char* table_name, const char* column_name,
                                 argus_backend_op_t* out_op)
{
    /* Flight SQL has no dedicated GetColumns RPC; GetTables(include_schema=true)
     * carries the column schema. A future refinement maps that schema into the
     * ODBC SQLColumns shape. For now request the table(s) with schema. */
    (void)column_name;
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client) return -1;

    std::string cat, sch, tbl;
    const std::string* cat_p = nullptr;
    const std::string* sch_p = nullptr;
    const std::string* tbl_p = nullptr;
    if (catalog && *catalog)    { cat = catalog;    cat_p = &cat; }
    if (schema && *schema)      { sch = schema;     sch_p = &sch; }
    if (table_name && *table_name) { tbl = table_name; tbl_p = &tbl; }

    return run_to_op(conn,
        conn->client->GetTables(conn->call_options, cat_p, sch_p, tbl_p,
                                /*include_schema=*/true, nullptr),
        out_op);
}

static int flightsql_get_schemas(argus_backend_conn_t raw_conn,
                                 const char* catalog, const char* schema,
                                 argus_backend_op_t* out_op)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client) return -1;

    std::string cat, sch;
    const std::string* cat_p = nullptr;
    const std::string* sch_p = nullptr;
    if (catalog && *catalog) { cat = catalog; cat_p = &cat; }
    if (schema && *schema)   { sch = schema;  sch_p = &sch; }

    return run_to_op(conn,
        conn->client->GetDbSchemas(conn->call_options, cat_p, sch_p), out_op);
}

static int flightsql_get_catalogs(argus_backend_conn_t raw_conn,
                                  argus_backend_op_t* out_op)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client) return -1;
    return run_to_op(conn, conn->client->GetCatalogs(conn->call_options),
                     out_op);
}

static int flightsql_get_type_info(argus_backend_conn_t raw_conn,
                                   SQLSMALLINT sql_type,
                                   argus_backend_op_t* out_op)
{
    (void)sql_type;
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client) return -1;
    return run_to_op(conn, conn->client->GetXdbcTypeInfo(conn->call_options),
                     out_op);
}

static int flightsql_get_primary_keys(argus_backend_conn_t raw_conn,
                                      const char* catalog, const char* schema,
                                      const char* table_name,
                                      argus_backend_op_t* out_op)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client || !table_name) return -1;

    flightsql::TableRef ref;
    if (catalog && *catalog) ref.catalog = catalog;
    if (schema && *schema)   ref.db_schema = schema;
    ref.table = table_name;

    return run_to_op(conn,
        conn->client->GetPrimaryKeys(conn->call_options, ref), out_op);
}

/* ── Backend vtable ──────────────────────────────────────────── */

static const argus_backend_t flightsql_backend = {
    /* name                 */ "flightsql",
    /* connect              */ flightsql_connect,
    /* disconnect           */ flightsql_disconnect,
    /* is_alive             */ flightsql_is_alive,
    /* execute              */ flightsql_execute,
    /* get_operation_status */ flightsql_get_operation_status,
    /* close_operation      */ flightsql_close_operation,
    /* cancel               */ flightsql_cancel,
    /* fetch_results        */ flightsql_fetch_results,
    /* get_result_metadata  */ flightsql_get_result_metadata,
    /* get_tables           */ flightsql_get_tables,
    /* get_columns          */ flightsql_get_columns,
    /* get_type_info        */ flightsql_get_type_info,
    /* get_schemas          */ flightsql_get_schemas,
    /* get_catalogs         */ flightsql_get_catalogs,
    /* get_primary_keys     */ flightsql_get_primary_keys,
    /* get_statistics       */ nullptr,
};

extern "C" const argus_backend_t* argus_flightsql_backend_get(void)
{
    return &flightsql_backend;
}
