/*
 * Unit test for the Flight SQL Arrow -> ODBC conversion layer.
 *
 * Depends only on plain libarrow, so it can run without a Flight SQL endpoint.
 * Built only when ARGUS_BUILD_FLIGHTSQL is enabled (libarrow present).
 */
#include <cassert>
#include <cstdio>
#include <cstring>
#include <memory>
#include <arrow/api.h>

#include "flightsql_convert.h"

static std::shared_ptr<arrow::RecordBatch> make_batch()
{
    arrow::Int32Builder id;
    arrow::StringBuilder name;
    arrow::DoubleBuilder val;

    (void)id.Append(1);
    (void)id.AppendNull();      /* row 1, column 0 is NULL */
    (void)id.Append(3);

    (void)name.Append("alpha");
    (void)name.Append("beta");
    (void)name.Append("gamma");

    (void)val.Append(1.5);
    (void)val.Append(2.5);
    (void)val.Append(3.5);

    std::shared_ptr<arrow::Array> id_a, name_a, val_a;
    (void)id.Finish(&id_a);
    (void)name.Finish(&name_a);
    (void)val.Finish(&val_a);

    auto schema = arrow::schema({
        arrow::field("id", arrow::int32(), /*nullable=*/true),
        arrow::field("name", arrow::utf8(), /*nullable=*/false),
        arrow::field("val", arrow::float64(), /*nullable=*/true),
    });

    return arrow::RecordBatch::Make(schema, 3, {id_a, name_a, val_a});
}

/* Second scenario: the types InfluxDB 3 actually returns — boolean, timestamp,
 * and dictionary-encoded utf8 (tag columns). */
static std::shared_ptr<arrow::RecordBatch> make_batch2()
{
    arrow::BooleanBuilder flag;
    (void)flag.Append(true);
    (void)flag.Append(false);

    auto ts_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    arrow::TimestampBuilder ts(ts_type, arrow::default_memory_pool());
    (void)ts.Append(1000000);   /* 1970-01-01 00:00:01 UTC */
    (void)ts.Append(2000000);

    arrow::StringDictionaryBuilder region;
    (void)region.Append("eu");
    (void)region.Append("us");

    std::shared_ptr<arrow::Array> flag_a, ts_a, region_a;
    (void)flag.Finish(&flag_a);
    (void)ts.Finish(&ts_a);
    (void)region.Finish(&region_a);

    auto schema = arrow::schema({
        arrow::field("flag", arrow::boolean(), true),
        arrow::field("ts", ts_type, true),
        arrow::field("region", arrow::dictionary(arrow::int32(), arrow::utf8()), true),
    });
    return arrow::RecordBatch::Make(schema, 2, {flag_a, ts_a, region_a});
}

/* Third scenario: decimal128 (precision/scale) and date32. */
static std::shared_ptr<arrow::RecordBatch> make_batch3()
{
    auto dec_type = arrow::decimal128(10, 2);
    arrow::Decimal128Builder dec(dec_type);
    (void)dec.Append(arrow::Decimal128(12345));   /* 123.45 at scale 2 */
    (void)dec.Append(arrow::Decimal128(-67890));  /* -678.90 */

    arrow::Date32Builder dt;
    (void)dt.Append(19884);   /* 2024-06-10 */
    (void)dt.Append(0);       /* 1970-01-01 */

    std::shared_ptr<arrow::Array> dec_a, dt_a;
    (void)dec.Finish(&dec_a);
    (void)dt.Finish(&dt_a);

    auto schema = arrow::schema({
        arrow::field("amount", dec_type, true),
        arrow::field("day", arrow::date32(), true),
    });
    return arrow::RecordBatch::Make(schema, 2, {dec_a, dt_a});
}

int main(void)
{
    auto batch = make_batch();

    /* Schema -> ODBC columns */
    argus_column_desc_t cols[ARGUS_MAX_COLUMNS];
    int ncols = flightsql_schema_to_columns(batch->schema(), cols);
    assert(ncols == 3);
    assert(std::strcmp(reinterpret_cast<char*>(cols[0].name), "id") == 0);
    assert(cols[0].sql_type == SQL_INTEGER);
    assert(cols[0].nullable == SQL_NULLABLE);
    assert(cols[1].sql_type == SQL_VARCHAR);
    assert(cols[1].nullable == SQL_NO_NULLS);
    assert(cols[2].sql_type == SQL_DOUBLE);

    /* RecordBatch -> row cache */
    argus_row_cache_t cache;
    std::memset(&cache, 0, sizeof(cache));
    int rc = flightsql_append_batch(batch, &cache);
    assert(rc == 0);
    assert(cache.num_rows == 3);
    assert(cache.num_cols == 3);

    /* Row 0: 1 / alpha / 1.5. Numeric columns are stored as native typed
     * values (no per-cell string allocated); the string column stays text. */
    assert(!cache.rows[0].cells[0].is_null);
    assert(cache.rows[0].cells[0].native_kind == ARGUS_NATIVE_I64);
    assert(cache.rows[0].cells[0].native.i64 == 1);
    assert(cache.rows[0].cells[0].data == nullptr);   /* no text allocated */
    assert(std::strcmp(cache.rows[0].cells[1].data, "alpha") == 0);
    assert(cache.rows[0].cells[2].native_kind == ARGUS_NATIVE_F64);
    assert(cache.rows[0].cells[2].native.f64 == 1.5);

    /* Row 1: NULL id, string still present */
    assert(cache.rows[1].cells[0].is_null);
    assert(cache.rows[1].cells[0].data == nullptr);
    assert(std::strcmp(cache.rows[1].cells[1].data, "beta") == 0);

    /* Row 2 */
    assert(cache.rows[2].cells[0].native_kind == ARGUS_NATIVE_I64);
    assert(cache.rows[2].cells[0].native.i64 == 3);
    assert(cache.rows[2].cells[2].native.f64 == 3.5);

    /* Appending a second batch grows the cache (multi-endpoint results). */
    rc = flightsql_append_batch(batch, &cache);
    assert(rc == 0);
    assert(cache.num_rows == 6);
    assert(std::strcmp(cache.rows[3].cells[1].data, "alpha") == 0);

    /* --- Scenario 2: bool / timestamp / dictionary(utf8) --- */
    auto b2 = make_batch2();
    argus_column_desc_t c2[ARGUS_MAX_COLUMNS];
    int n2 = flightsql_schema_to_columns(b2->schema(), c2);
    assert(n2 == 3);
    assert(c2[0].sql_type == SQL_BIT);
    assert(c2[1].sql_type == SQL_TYPE_TIMESTAMP);
    assert(c2[2].sql_type == SQL_VARCHAR);   /* dictionary(utf8) -> value type */

    argus_row_cache_t ch2;
    std::memset(&ch2, 0, sizeof(ch2));
    rc = flightsql_append_batch(b2, &ch2);
    assert(rc == 0);
    assert(ch2.num_rows == 2);
    /* BOOL is stored natively (1/0); SQLGetData formats it on demand. */
    assert(ch2.rows[0].cells[0].native_kind == ARGUS_NATIVE_I64);
    assert(ch2.rows[0].cells[0].native.i64 == 1);
    assert(ch2.rows[1].cells[0].native.i64 == 0);
    assert(std::strstr(ch2.rows[0].cells[1].data, "1970-01-01") != nullptr);
    assert(std::strcmp(ch2.rows[0].cells[2].data, "eu") == 0);   /* dict decoded */
    assert(std::strcmp(ch2.rows[1].cells[2].data, "us") == 0);

    /* --- Scenario 3: decimal128(10,2) / date32 --- */
    auto b3 = make_batch3();
    argus_column_desc_t c3[ARGUS_MAX_COLUMNS];
    int n3 = flightsql_schema_to_columns(b3->schema(), c3);
    assert(n3 == 2);
    assert(c3[0].sql_type == SQL_DECIMAL);
    assert(c3[0].column_size == 10);     /* precision */
    assert(c3[0].decimal_digits == 2);   /* scale */
    assert(c3[1].sql_type == SQL_TYPE_DATE);

    argus_row_cache_t ch3;
    std::memset(&ch3, 0, sizeof(ch3));
    rc = flightsql_append_batch(b3, &ch3);
    assert(rc == 0);
    assert(ch3.num_rows == 2);
    assert(std::strcmp(ch3.rows[0].cells[0].data, "123.45") == 0);
    assert(std::strcmp(ch3.rows[1].cells[0].data, "-678.90") == 0);
    assert(std::strstr(ch3.rows[0].cells[1].data, "2024-06-10") != nullptr);

    std::printf("test_flightsql_convert: OK (scenario1 %zu rows %d cols; "
                "scenario2 bool/ts/dict; scenario3 decimal/date)\n",
                cache.num_rows, cache.num_cols);
    return 0;
}
