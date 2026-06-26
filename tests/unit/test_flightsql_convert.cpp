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

    /* Row 0: 1 / alpha / 1.5 */
    assert(!cache.rows[0].cells[0].is_null);
    assert(std::strcmp(cache.rows[0].cells[0].data, "1") == 0);
    assert(std::strcmp(cache.rows[0].cells[1].data, "alpha") == 0);
    assert(std::strcmp(cache.rows[0].cells[2].data, "1.5") == 0);

    /* Row 1: NULL id, string still present */
    assert(cache.rows[1].cells[0].is_null);
    assert(cache.rows[1].cells[0].data == nullptr);
    assert(std::strcmp(cache.rows[1].cells[1].data, "beta") == 0);

    /* Row 2 */
    assert(std::strcmp(cache.rows[2].cells[0].data, "3") == 0);
    assert(std::strcmp(cache.rows[2].cells[2].data, "3.5") == 0);

    /* Appending a second batch grows the cache (multi-endpoint results). */
    rc = flightsql_append_batch(batch, &cache);
    assert(rc == 0);
    assert(cache.num_rows == 6);
    assert(std::strcmp(cache.rows[3].cells[1].data, "alpha") == 0);

    std::printf("test_flightsql_convert: OK (%zu rows, %d cols)\n",
                cache.num_rows, cache.num_cols);
    return 0;
}
