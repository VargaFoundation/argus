#include "flightsql_convert.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <arrow/api.h>

/* ── Arrow logical type → ODBC SQL type ──────────────────────── */

SQLSMALLINT flightsql_arrow_to_sql_type(int arrow_type_id)
{
    switch (static_cast<arrow::Type::type>(arrow_type_id)) {
    case arrow::Type::BOOL:          return SQL_BIT;
    case arrow::Type::INT8:
    case arrow::Type::UINT8:         return SQL_TINYINT;
    case arrow::Type::INT16:
    case arrow::Type::UINT16:        return SQL_SMALLINT;
    case arrow::Type::INT32:
    case arrow::Type::UINT32:        return SQL_INTEGER;
    case arrow::Type::INT64:
    case arrow::Type::UINT64:        return SQL_BIGINT;
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:         return SQL_REAL;
    case arrow::Type::DOUBLE:        return SQL_DOUBLE;
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256:    return SQL_DECIMAL;
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:        return SQL_TYPE_DATE;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:        return SQL_TYPE_TIME;
    case arrow::Type::TIMESTAMP:     return SQL_TYPE_TIMESTAMP;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:  return SQL_VARCHAR;
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::FIXED_SIZE_BINARY: return SQL_VARBINARY;
    /* Nested/complex types are surfaced as their JSON-ish text form. */
    default:                         return SQL_VARCHAR;
    }
}

/* ── Column-size / scale heuristics (mirrors the other backends) ─ */

static SQLULEN column_size_for(SQLSMALLINT sql_type,
                               const std::shared_ptr<arrow::DataType>& type)
{
    switch (sql_type) {
    case SQL_BIT:            return 1;
    case SQL_TINYINT:        return 3;
    case SQL_SMALLINT:       return 5;
    case SQL_INTEGER:        return 10;
    case SQL_BIGINT:         return 19;
    case SQL_REAL:           return 7;
    case SQL_DOUBLE:         return 15;
    case SQL_DECIMAL: {
        if (type && (type->id() == arrow::Type::DECIMAL128 ||
                     type->id() == arrow::Type::DECIMAL256)) {
            auto dec = std::static_pointer_cast<arrow::DecimalType>(type);
            return static_cast<SQLULEN>(dec->precision());
        }
        return 38;
    }
    case SQL_TYPE_DATE:      return 10;
    case SQL_TYPE_TIME:      return 8;
    case SQL_TYPE_TIMESTAMP: return 19;
    default:                 return 255;
    }
}

static SQLSMALLINT decimal_digits_for(SQLSMALLINT sql_type,
                                      const std::shared_ptr<arrow::DataType>& type)
{
    if (sql_type == SQL_DECIMAL && type &&
        (type->id() == arrow::Type::DECIMAL128 ||
         type->id() == arrow::Type::DECIMAL256)) {
        auto dec = std::static_pointer_cast<arrow::DecimalType>(type);
        return static_cast<SQLSMALLINT>(dec->scale());
    }
    return 0;
}

/* ── Field → ODBC column descriptor ──────────────────────────── */

void flightsql_field_to_column(const std::shared_ptr<arrow::Field>& field,
                               argus_column_desc_t* col)
{
    std::memset(col, 0, sizeof(*col));

    const std::string& name = field->name();
    std::strncpy(reinterpret_cast<char*>(col->name), name.c_str(),
                 ARGUS_MAX_COLUMN_NAME - 1);
    col->name_len = static_cast<SQLSMALLINT>(
        std::strlen(reinterpret_cast<char*>(col->name)));

    auto type = field->type();
    /* Dictionary-encoded columns (e.g. InfluxDB tags are Dictionary(Int32,Utf8))
     * are surfaced as their decoded value type. */
    if (type && type->id() == arrow::Type::DICTIONARY)
        type = std::static_pointer_cast<arrow::DictionaryType>(type)->value_type();

    col->sql_type = flightsql_arrow_to_sql_type(type ? type->id() : arrow::Type::NA);
    col->column_size = column_size_for(col->sql_type, type);
    col->decimal_digits = decimal_digits_for(col->sql_type, type);
    col->nullable = field->nullable() ? SQL_NULLABLE : SQL_NO_NULLS;
}

int flightsql_schema_to_columns(const std::shared_ptr<arrow::Schema>& schema,
                                argus_column_desc_t* columns)
{
    if (!schema) return 0;
    int n = schema->num_fields();
    if (n > ARGUS_MAX_COLUMNS) n = ARGUS_MAX_COLUMNS;
    for (int i = 0; i < n; i++)
        flightsql_field_to_column(schema->field(i), &columns[i]);
    return n;
}

/* ── RecordBatch → text row cache ────────────────────────────── */

static char* dup_str(const std::string& s, size_t* out_len)
{
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (!p) return nullptr;
    std::memcpy(p, s.data(), s.size());
    p[s.size()] = '\0';
    *out_len = s.size();
    return p;
}

int flightsql_append_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                           argus_row_cache_t* cache)
{
    if (!batch || !cache) return -1;

    const int ncols = batch->num_columns();
    const int64_t nrows = batch->num_rows();
    if (nrows == 0) return 0;

    /* Grow the row array to hold the existing rows plus this batch. */
    size_t needed = cache->num_rows + static_cast<size_t>(nrows);
    if (needed > cache->capacity) {
        size_t cap = cache->capacity ? cache->capacity : 16;
        while (cap < needed) cap *= 2;
        argus_row_t* grown = static_cast<argus_row_t*>(
            std::realloc(cache->rows, cap * sizeof(argus_row_t)));
        if (!grown) return -1;
        cache->rows = grown;
        cache->capacity = cap;
    }
    cache->num_cols = ncols;

    /* Pre-fetch typed string arrays once per column for the common case. */
    for (int64_t r = 0; r < nrows; r++) {
        argus_row_t* row = &cache->rows[cache->num_rows];
        row->cells = static_cast<argus_cell_t*>(
            std::calloc(static_cast<size_t>(ncols), sizeof(argus_cell_t)));
        if (!row->cells) return -1;

        for (int c = 0; c < ncols; c++) {
            argus_cell_t* cell = &row->cells[c];
            auto array = batch->column(c);

            if (array->IsNull(r)) {
                cell->is_null = true;
                cell->data = nullptr;
                cell->data_len = 0;
                continue;
            }

            std::string text;
            switch (array->type_id()) {
            case arrow::Type::STRING: {
                auto a = std::static_pointer_cast<arrow::StringArray>(array);
                text.assign(a->GetView(r));
                break;
            }
            case arrow::Type::LARGE_STRING: {
                auto a = std::static_pointer_cast<arrow::LargeStringArray>(array);
                text.assign(a->GetView(r));
                break;
            }
            case arrow::Type::DICTIONARY: {
                /* Decode to the dictionary value (InfluxDB tag columns are
                 * Dictionary(Int32, Utf8)); GetScalar()->ToString() on a
                 * dictionary scalar does not yield the plain value. */
                auto da = std::static_pointer_cast<arrow::DictionaryArray>(array);
                int64_t idx = da->GetValueIndex(r);
                auto values = da->dictionary();
                if (values->type_id() == arrow::Type::STRING) {
                    text.assign(std::static_pointer_cast<arrow::StringArray>(
                        values)->GetView(idx));
                } else if (values->type_id() == arrow::Type::LARGE_STRING) {
                    text.assign(std::static_pointer_cast<arrow::LargeStringArray>(
                        values)->GetView(idx));
                } else {
                    auto sc = values->GetScalar(idx);
                    if (sc.ok()) text = sc.ValueOrDie()->ToString();
                }
                break;
            }
            default: {
                /* Uniform, type-correct text for every other type. */
                auto scalar_res = array->GetScalar(r);
                if (scalar_res.ok())
                    text = scalar_res.ValueOrDie()->ToString();
                break;
            }
            }

            cell->is_null = false;
            cell->data = dup_str(text, &cell->data_len);
            if (!cell->data) return -1;
        }

        cache->num_rows++;
    }

    return 0;
}
