#include "flightsql_internal.h"
#include "flightsql_convert.h"

#include <arrow/ipc/dictionary.h>

/* The argus C headers have no extern "C" guards; wrap them so the C functions
 * they declare (e.g. argus_log_write) keep C linkage, as the Kudu backend does. */
extern "C" {
#include "argus/backend.h"
#include "argus/handle.h"
#include "argus/log.h"
}

#include <cstdlib>
#include <cstring>
#include <sstream>
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

/* ── Connection lifecycle ────────────────────────────────────── */

static int flightsql_connect(argus_dbc_t* dbc,
                             const char* host, int port,
                             const char* username, const char* password,
                             const char* database,
                             const char* auth_mechanism,
                             argus_backend_conn_t* out_conn)
{
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

    /* InfluxDB 3 (and other multi-tenant servers) select the target database via
     * a gRPC "database" call header rather than a SQL catalog. */
    if (database && *database)
        conn->call_options.headers.emplace_back("database", database);

    conn->client = std::make_unique<flightsql::FlightSqlClient>(shared_client);

    /* Flight's Connect is lazy and does not itself authenticate, so validate the
     * connection with a lightweight metadata RPC. This makes SQLConnect fail
     * fast on bad credentials or an unreachable server, as ODBC expects, rather
     * than only surfacing the error on the first query. */
    auto check = conn->client->GetCatalogs(conn->call_options);
    if (!check.ok()) {
        ARGUS_LOG_ERROR("Flight SQL: connection validation failed: %s",
                        check.status().ToString().c_str());
        delete conn;
        return -1;
    }

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
        conn->last_error = info_res.status().ToString();
        ARGUS_LOG_ERROR("Flight SQL: request failed: %s",
                        conn->last_error.c_str());
        return -1;
    }
    conn->last_error.clear();
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
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    auto* op = static_cast<flightsql_op*>(raw_op);
    if (!conn || !conn->client || !op || !cache) return -1;

    /* Copy whatever column metadata we already have to the caller. */
    auto provide_meta = [&]() {
        if (columns && num_cols && op->columns && op->metadata_fetched) {
            std::memcpy(columns, op->columns,
                        static_cast<size_t>(op->num_cols) *
                            sizeof(argus_column_desc_t));
            *num_cols = op->num_cols;
        }
    };

    if (op->done) {
        provide_meta();
        cache->num_rows = 0;
        cache->exhausted = true;
        return 0;
    }

    const auto& endpoints = op->info->endpoints();
    /* Accumulate up to one ODBC block per call (bounded memory); when no block
     * size is given, return after the first non-empty batch. */
    const size_t target = (max_rows > 0) ? static_cast<size_t>(max_rows) : 1;

    while (true) {
        /* Open the next endpoint's stream on demand. */
        if (!op->reader) {
            if (op->endpoint_idx >= endpoints.size()) {
                op->done = true;
                provide_meta();
                cache->exhausted = true;   /* hand back any accumulated rows */
                return 0;
            }
            auto reader_res = conn->client->DoGet(
                conn->call_options, endpoints[op->endpoint_idx].ticket);
            if (!reader_res.ok()) {
                ARGUS_LOG_ERROR("Flight SQL: DoGet failed: %s",
                                reader_res.status().ToString().c_str());
                return -1;
            }
            op->reader = std::move(reader_res).ValueOrDie();

            /* Capture column metadata from the first stream's schema. */
            if (!op->metadata_fetched) {
                auto schema_res = op->reader->GetSchema();
                if (schema_res.ok() && schema_res.ValueOrDie()) {
                    argus_column_desc_t local_meta[ARGUS_MAX_COLUMNS];
                    int local_cols = flightsql_schema_to_columns(
                        schema_res.ValueOrDie(), local_meta);
                    op->columns = static_cast<argus_column_desc_t*>(
                        std::calloc(static_cast<size_t>(
                                        local_cols > 0 ? local_cols : 1),
                                    sizeof(argus_column_desc_t)));
                    if (op->columns) {
                        std::memcpy(op->columns, local_meta,
                                    static_cast<size_t>(local_cols) *
                                        sizeof(argus_column_desc_t));
                        op->num_cols = local_cols;
                        op->metadata_fetched = true;
                    }
                }
            }
        }

        provide_meta();

        auto chunk_res = op->reader->Next();
        if (!chunk_res.ok()) {
            ARGUS_LOG_ERROR("Flight SQL: stream read failed: %s",
                            chunk_res.status().ToString().c_str());
            return -1;
        }
        flight::FlightStreamChunk chunk = std::move(chunk_res).ValueOrDie();
        if (!chunk.data) {
            /* End of this endpoint's stream; advance to the next one. */
            op->reader.reset();
            op->endpoint_idx++;
            continue;
        }
        if (flightsql_append_batch(chunk.data, cache) != 0) return -1;

        /* The ODBC layer treats a 0-row block as end-of-data, so keep reading
         * past empty batches; return once the block has rows. */
        if (cache->num_rows == 0) continue;
        if (cache->num_rows >= target) {
            cache->exhausted = false;
            return 0;
        }
    }
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
        arrow::ipc::DictionaryMemo memo;
        auto schema_res = op->info->GetSchema(&memo);
        if (schema_res.ok() && schema_res.ValueOrDie()) {
            *num_cols = flightsql_schema_to_columns(schema_res.ValueOrDie(),
                                                    columns);
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

    /* table_types is an ODBC comma-separated list (e.g. "TABLE,VIEW", possibly
     * quoted). Split it. ODBC clients use "TABLE", but Flight SQL servers differ
     * on the spelling: InfluxDB 3 reports user tables as the SQL-standard
     * "BASE TABLE" while Dremio uses "TABLE". Request both so either matches. */
    if (table_types && *table_types) {
        std::stringstream ss(table_types);
        std::string tok;
        while (std::getline(ss, tok, ',')) {
            size_t a = tok.find_first_not_of(" '\"");
            size_t b = tok.find_last_not_of(" '\"");
            if (a == std::string::npos) continue;
            std::string t = tok.substr(a, b - a + 1);
            types.push_back(t);
            if (t == "TABLE") types.push_back("BASE TABLE");
        }
    }

    return run_to_op(conn,
        conn->client->GetTables(conn->call_options, cat_p, sch_p, tbl_p,
                                /*include_schema=*/false,
                                types.empty() ? nullptr : &types),
        out_op);
}

static int flightsql_get_columns(argus_backend_conn_t raw_conn,
                                 const char* catalog, const char* schema,
                                 const char* table_name, const char* column_name,
                                 argus_backend_op_t* out_op)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || !conn->client) return -1;

    /* Flight SQL has no dedicated GetColumns RPC (the native GetTables only
     * carries a serialized Arrow schema, not the ODBC SQLColumns shape). Query
     * information_schema.columns instead, which every Flight SQL engine we
     * target (InfluxDB 3, Dremio, Doris, StarRocks) exposes, aliasing to the
     * ODBC column names — the same approach as the Trino/MySQL backends. */
    std::string q =
        "SELECT table_catalog AS TABLE_CAT, table_schema AS TABLE_SCHEM, "
        "table_name AS TABLE_NAME, column_name AS COLUMN_NAME, "
        "data_type AS TYPE_NAME, ordinal_position AS ORDINAL_POSITION, "
        "is_nullable AS IS_NULLABLE "
        "FROM information_schema.columns WHERE 1=1";
    if (catalog && *catalog)    { q += " AND table_catalog = '"; q += catalog; q += "'"; }
    if (schema && *schema)      { q += " AND table_schema LIKE '"; q += schema; q += "'"; }
    if (table_name && *table_name) { q += " AND table_name LIKE '"; q += table_name; q += "'"; }
    if (column_name && *column_name) { q += " AND column_name LIKE '"; q += column_name; q += "'"; }
    q += " ORDER BY table_schema, table_name, ordinal_position";

    return run_to_op(conn, conn->client->Execute(conn->call_options, q), out_op);
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

/* ── Last error message ──────────────────────────────────────── */

static bool flightsql_get_last_error(argus_backend_conn_t raw_conn,
                                     char* buf, size_t buflen)
{
    auto* conn = static_cast<flightsql_conn*>(raw_conn);
    if (!conn || conn->last_error.empty() || buflen == 0) return false;
    std::strncpy(buf, conn->last_error.c_str(), buflen - 1);
    buf[buflen - 1] = '\0';
    return true;
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
    /* get_last_error       */ flightsql_get_last_error,
};

extern "C" const argus_backend_t* argus_flightsql_backend_get(void)
{
    return &flightsql_backend;
}
