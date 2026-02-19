/*
 * Kudu query execution: parse SQL → KuduScanner with predicates.
 * Uses the Kudu C++ client library with extern "C" wrappers.
 */
#include <kudu/client/client.h>
#include <string>
#include <vector>
#include <memory>
#include <cstring>

extern "C" {
#include "kudu_internal.h"
#include "argus/log.h"
}

using kudu::client::KuduClient;
using kudu::client::KuduTable;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduPredicate;
using kudu::client::KuduValue;
using kudu::client::KuduSchema;
using kudu::client::KuduColumnSchema;
using kudu::Slice;
using kudu::Status;

/* ── Helper: get KuduClient from opaque pointer ──────────────── */

static KuduClient *get_client(void *client_ptr)
{
    auto *sp = static_cast<std::shared_ptr<KuduClient> *>(client_ptr);
    return sp->get();
}

/* ── Build table name with optional prefix ───────────────────── */

static std::string build_table_name(const char *prefix,
                                     const char *table_name)
{
    if (prefix && *prefix && strcmp(prefix, "default") != 0) {
        return std::string(prefix) + "." + table_name;
    }
    return std::string(table_name);
}

/* ── Execute a scan based on parsed SQL query ────────────────── */

extern "C"
int kudu_cpp_execute_scan(void *client, const kudu_parsed_query_t *query,
                          const char *table_prefix, int timeout_sec,
                          kudu_operation_t *op)
{
    KuduClient *kclient = get_client(client);
    if (!kclient || !query || !query->table_name) return -1;

    std::string table_name = build_table_name(table_prefix,
                                               query->table_name);

    /* Open the table */
    std::shared_ptr<KuduTable> table;
    Status s = kclient->OpenTable(table_name, &table);
    if (!s.ok()) {
        ARGUS_LOG_ERROR("Kudu OpenTable failed for '%s': %s",
                        table_name.c_str(), s.ToString().c_str());
        return -1;
    }

    /* Create scanner */
    KuduScanner *scanner = new KuduScanner(table.get());

    if (timeout_sec > 0) {
        scanner->SetTimeoutMillis(timeout_sec * 1000);
    }

    /* Project columns */
    const KuduSchema &schema = table->schema();
    if (query->columns && query->num_columns > 0) {
        std::vector<std::string> proj_cols;
        for (int i = 0; i < query->num_columns; i++) {
            proj_cols.push_back(query->columns[i]);
        }
        s = scanner->SetProjectedColumnNames(proj_cols);
        if (!s.ok()) {
            ARGUS_LOG_ERROR("Kudu SetProjectedColumns failed: %s",
                            s.ToString().c_str());
            delete scanner;
            return -1;
        }
    }

    /* Add predicates */
    for (int i = 0; i < query->num_predicates; i++) {
        kudu_predicate_t *pred = &query->predicates[i];
        int col_idx = schema.find_column(pred->column);
        if (col_idx < 0) {
            ARGUS_LOG_ERROR("Kudu column not found: %s", pred->column);
            delete scanner;
            return -1;
        }

        const KuduColumnSchema &col_schema = schema.Column(col_idx);
        KuduPredicate *kpred = nullptr;

        switch (pred->op) {
        case KUDU_OP_IS_NULL:
            kpred = table->NewIsNullPredicate(pred->column);
            break;

        case KUDU_OP_IS_NOT_NULL:
            kpred = table->NewIsNotNullPredicate(pred->column);
            break;

        case KUDU_OP_IN: {
            std::vector<KuduValue *> values;
            for (int j = 0; j < pred->num_in_values; j++) {
                switch (col_schema.type()) {
                case KuduColumnSchema::INT8:
                case KuduColumnSchema::INT16:
                case KuduColumnSchema::INT32:
                case KuduColumnSchema::INT64:
                    values.push_back(KuduValue::FromInt(
                        std::stoll(pred->in_values[j])));
                    break;
                case KuduColumnSchema::FLOAT:
                case KuduColumnSchema::DOUBLE:
                    values.push_back(KuduValue::FromDouble(
                        std::stod(pred->in_values[j])));
                    break;
                case KuduColumnSchema::BOOL:
                    values.push_back(KuduValue::FromBool(
                        strcasecmp(pred->in_values[j], "true") == 0 ||
                        strcmp(pred->in_values[j], "1") == 0));
                    break;
                default:
                    values.push_back(
                        KuduValue::CopyString(pred->in_values[j]));
                    break;
                }
            }
            kpred = table->NewInListPredicate(pred->column, &values);
            break;
        }

        default: {
            /* Comparison operators: EQ, LT, LE, GT, GE */
            KuduPredicate::ComparisonOp cmp_op;
            switch (pred->op) {
            case KUDU_OP_EQ: cmp_op = KuduPredicate::EQUAL; break;
            case KUDU_OP_LT: cmp_op = KuduPredicate::LESS; break;
            case KUDU_OP_LE: cmp_op = KuduPredicate::LESS_EQUAL; break;
            case KUDU_OP_GT: cmp_op = KuduPredicate::GREATER; break;
            case KUDU_OP_GE: cmp_op = KuduPredicate::GREATER_EQUAL; break;
            default:
                delete scanner;
                return -1;
            }

            KuduValue *val = nullptr;
            switch (col_schema.type()) {
            case KuduColumnSchema::INT8:
            case KuduColumnSchema::INT16:
            case KuduColumnSchema::INT32:
            case KuduColumnSchema::INT64:
                val = KuduValue::FromInt(std::stoll(pred->value));
                break;
            case KuduColumnSchema::FLOAT:
            case KuduColumnSchema::DOUBLE:
                val = KuduValue::FromDouble(std::stod(pred->value));
                break;
            case KuduColumnSchema::BOOL:
                val = KuduValue::FromBool(
                    strcasecmp(pred->value, "true") == 0 ||
                    strcmp(pred->value, "1") == 0);
                break;
            default:
                val = KuduValue::CopyString(pred->value);
                break;
            }

            kpred = table->NewComparisonPredicate(pred->column, cmp_op, val);
            break;
        }
        }

        if (kpred) {
            s = scanner->AddConjunctPredicate(kpred);
            if (!s.ok()) {
                ARGUS_LOG_WARN("Kudu AddPredicate failed: %s",
                               s.ToString().c_str());
            }
        }
    }

    /* Set batch size / limit */
    if (query->limit > 0) {
        scanner->SetLimit(query->limit);
    }

    /* Open the scanner */
    s = scanner->Open();
    if (!s.ok()) {
        ARGUS_LOG_ERROR("Kudu scanner open failed: %s",
                        s.ToString().c_str());
        delete scanner;
        return -1;
    }

    /* Build column metadata from the projected schema */
    const KuduSchema &proj_schema = scanner->GetProjectionSchema();
    int ncols = proj_schema.num_columns();
    if (ncols > ARGUS_MAX_COLUMNS) ncols = ARGUS_MAX_COLUMNS;

    op->columns = static_cast<argus_column_desc_t *>(
        calloc(ncols, sizeof(argus_column_desc_t)));
    if (!op->columns) {
        delete scanner;
        return -1;
    }

    for (int i = 0; i < ncols; i++) {
        const KuduColumnSchema &col = proj_schema.Column(i);
        argus_column_desc_t *desc = &op->columns[i];
        memset(desc, 0, sizeof(*desc));

        const std::string &name = col.name();
        strncpy(reinterpret_cast<char *>(desc->name), name.c_str(),
                ARGUS_MAX_COLUMN_NAME - 1);
        desc->name_len = static_cast<SQLSMALLINT>(name.size());

        desc->sql_type = kudu_type_to_sql_type(col.type());
        desc->column_size = kudu_type_column_size(desc->sql_type);
        desc->decimal_digits = kudu_type_decimal_digits(desc->sql_type);
        desc->nullable = col.is_nullable() ? SQL_NULLABLE : SQL_NO_NULLS;
    }

    op->num_cols = ncols;
    op->metadata_fetched = true;
    op->has_result_set = true;
    op->scanner = scanner;
    op->batch_offset = 0;

    return 0;
}

/* ── Fetch a batch of rows from KuduScanner ──────────────────── */

extern "C"
int kudu_cpp_fetch_batch(kudu_operation_t *op,
                         argus_row_cache_t *cache, int max_rows)
{
    KuduScanner *scanner = static_cast<KuduScanner *>(op->scanner);
    if (!scanner) return -1;

    if (!scanner->HasMoreRows()) {
        cache->num_rows = 0;
        cache->exhausted = true;
        op->finished = true;
        return 0;
    }

    KuduScanBatch batch;
    Status s = scanner->NextBatch(&batch);
    if (!s.ok()) {
        ARGUS_LOG_ERROR("Kudu NextBatch failed: %s", s.ToString().c_str());
        return -1;
    }

    int nrows = static_cast<int>(batch.NumRows());
    if (max_rows > 0 && nrows > max_rows) nrows = max_rows;
    if (nrows == 0) {
        cache->num_rows = 0;
        if (!scanner->HasMoreRows()) {
            cache->exhausted = true;
            op->finished = true;
        }
        return 0;
    }

    cache->rows = static_cast<argus_row_t *>(
        calloc(nrows, sizeof(argus_row_t)));
    if (!cache->rows) return -1;
    cache->num_rows = nrows;
    cache->capacity = nrows;
    cache->num_cols = op->num_cols;

    for (int r = 0; r < nrows; r++) {
        KuduScanBatch::RowPtr row = batch.Row(r);
        cache->rows[r].cells = static_cast<argus_cell_t *>(
            calloc(op->num_cols, sizeof(argus_cell_t)));
        if (!cache->rows[r].cells) return -1;

        for (int c = 0; c < op->num_cols; c++) {
            argus_cell_t *cell = &cache->rows[r].cells[c];

            if (row.IsNull(c)) {
                cell->is_null = true;
                cell->data = nullptr;
                cell->data_len = 0;
                continue;
            }

            cell->is_null = false;
            char buf[64];

            /* Read value based on column type */
            const KuduSchema &proj = scanner->GetProjectionSchema();
            switch (proj.Column(c).type()) {
            case KuduColumnSchema::INT8: {
                int8_t v;
                row.GetInt8(c, &v);
                snprintf(buf, sizeof(buf), "%d", v);
                cell->data = strdup(buf);
                break;
            }
            case KuduColumnSchema::INT16: {
                int16_t v;
                row.GetInt16(c, &v);
                snprintf(buf, sizeof(buf), "%d", v);
                cell->data = strdup(buf);
                break;
            }
            case KuduColumnSchema::INT32: {
                int32_t v;
                row.GetInt32(c, &v);
                snprintf(buf, sizeof(buf), "%d", v);
                cell->data = strdup(buf);
                break;
            }
            case KuduColumnSchema::INT64: {
                int64_t v;
                row.GetInt64(c, &v);
                snprintf(buf, sizeof(buf), "%lld",
                         static_cast<long long>(v));
                cell->data = strdup(buf);
                break;
            }
            case KuduColumnSchema::FLOAT: {
                float v;
                row.GetFloat(c, &v);
                snprintf(buf, sizeof(buf), "%.7g", v);
                cell->data = strdup(buf);
                break;
            }
            case KuduColumnSchema::DOUBLE: {
                double v;
                row.GetDouble(c, &v);
                snprintf(buf, sizeof(buf), "%.15g", v);
                cell->data = strdup(buf);
                break;
            }
            case KuduColumnSchema::BOOL: {
                bool v;
                row.GetBool(c, &v);
                cell->data = strdup(v ? "true" : "false");
                break;
            }
            case KuduColumnSchema::STRING: {
                Slice v;
                row.GetString(c, &v);
                cell->data = strndup(v.data(), v.size());
                break;
            }
            case KuduColumnSchema::BINARY: {
                Slice v;
                row.GetBinary(c, &v);
                cell->data = strndup(v.data(), v.size());
                break;
            }
            case KuduColumnSchema::UNIXTIME_MICROS: {
                int64_t v;
                row.GetUnixTimeMicros(c, &v);
                /* Convert microseconds to timestamp string */
                time_t secs = static_cast<time_t>(v / 1000000);
                int usecs = static_cast<int>(v % 1000000);
                struct tm tm_buf;
                gmtime_r(&secs, &tm_buf);
                char ts_buf[64];
                strftime(ts_buf, sizeof(ts_buf),
                         "%Y-%m-%d %H:%M:%S", &tm_buf);
                snprintf(buf, sizeof(buf), "%s.%06d", ts_buf, usecs);
                cell->data = strdup(buf);
                break;
            }
            default: {
                cell->data = strdup(row.ToString().c_str());
                break;
            }
            }

            if (cell->data)
                cell->data_len = strlen(cell->data);
        }
    }

    if (!scanner->HasMoreRows()) {
        cache->exhausted = true;
        op->finished = true;
    }

    return 0;
}

/* ── Close scanner ───────────────────────────────────────────── */

extern "C"
void kudu_cpp_close_scanner(kudu_operation_t *op)
{
    if (!op || !op->scanner) return;
    KuduScanner *scanner = static_cast<KuduScanner *>(op->scanner);
    scanner->Close();
    delete scanner;
    op->scanner = nullptr;
}

/* ── Execute a SQL query (parse then scan) ───────────────────── */

extern "C"
int kudu_execute(argus_backend_conn_t raw_conn,
                 const char *query,
                 argus_backend_op_t *out_op)
{
    kudu_conn_t *conn = static_cast<kudu_conn_t *>(raw_conn);
    if (!conn || !query) return -1;

    /* Parse the SQL query */
    kudu_parsed_query_t parsed;
    const char *error_msg = nullptr;
    if (kudu_sql_parse(query, &parsed, &error_msg) != 0) {
        ARGUS_LOG_ERROR("Kudu SQL parse error: %s",
                        error_msg ? error_msg : "unknown");
        return -1;
    }

    kudu_operation_t *op = kudu_operation_new();
    if (!op) {
        kudu_parsed_query_free(&parsed);
        return -1;
    }

    /* Execute the scan */
    int rc = kudu_cpp_execute_scan(conn->client, &parsed,
                                   conn->database,
                                   conn->query_timeout_sec, op);
    kudu_parsed_query_free(&parsed);

    if (rc != 0) {
        kudu_operation_free(op);
        return -1;
    }

    *out_op = op;
    return 0;
}

/* ── Get operation status ────────────────────────────────────── */

extern "C"
int kudu_get_operation_status(argus_backend_conn_t raw_conn,
                               argus_backend_op_t raw_op,
                               bool *finished)
{
    (void)raw_conn;
    kudu_operation_t *op = static_cast<kudu_operation_t *>(raw_op);
    if (!op) return -1;

    *finished = op->finished;
    return 0;
}

/* ── Cancel ──────────────────────────────────────────────────── */

extern "C"
int kudu_cancel(argus_backend_conn_t raw_conn,
                argus_backend_op_t raw_op)
{
    (void)raw_conn;
    kudu_operation_t *op = static_cast<kudu_operation_t *>(raw_op);
    if (!op) return -1;

    kudu_cpp_close_scanner(op);
    op->finished = true;
    return 0;
}

/* ── Close operation ─────────────────────────────────────────── */

extern "C"
void kudu_close_operation(argus_backend_conn_t raw_conn,
                           argus_backend_op_t raw_op)
{
    (void)raw_conn;
    kudu_operation_t *op = static_cast<kudu_operation_t *>(raw_op);
    if (!op) return;

    kudu_cpp_close_scanner(op);
    kudu_operation_free(op);
}
