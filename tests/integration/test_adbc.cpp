/*
 * ADBC driver integration test.
 *
 * Exercises the full Argus ADBC surface against a live Trino (tpch): the
 * AdbcDriverInit vtable, typed Arrow output, batched streaming, bound
 * parameters, and the metadata calls (GetTableSchema/GetTableTypes/GetObjects).
 * Every result is imported with Arrow C++'s own importer, which validates the
 * emitted C Data Interface structures.
 *
 * Requires a Trino reachable at TRINO_HOST:TRINO_PORT (default 127.0.0.1:8080).
 */
#include "argus/adbc.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unistd.h>

static std::string conn_str()
{
    const char* h = std::getenv("TRINO_HOST"); if (!h || !*h) h = "127.0.0.1";
    const char* p = std::getenv("TRINO_PORT"); if (!p || !*p) p = "8080";
    return std::string("BACKEND=trino;HOST=") + h + ";PORT=" + p +
           ";UID=adbc;Database=tpch";
}

static void check(AdbcStatusCode rc, AdbcError* e, const char* what)
{
    if (rc != ADBC_STATUS_OK) {
        std::fprintf(stderr, "%s failed: %s\n", what,
                     (e && e->message) ? e->message : "(no message)");
        std::abort();
    }
}

static std::shared_ptr<arrow::RecordBatchReader>
run(AdbcConnection* conn, const char* sql)
{
    AdbcStatement st{}; AdbcError e{};
    check(AdbcStatementNew(conn, &st, &e), &e, "StatementNew");
    check(AdbcStatementSetSqlQuery(&st, sql, &e), &e, "SetSqlQuery");
    ArrowArrayStream s{}; int64_t ra = 0;
    check(AdbcStatementExecuteQuery(&st, &s, &ra, &e), &e, "ExecuteQuery");
    auto rr = arrow::ImportRecordBatchReader(&s);
    assert(rr.ok());
    AdbcStatementRelease(&st, &e);
    return *rr;
}

static int64_t drain(const std::shared_ptr<arrow::RecordBatchReader>& r, int64_t* batches = nullptr)
{
    std::shared_ptr<arrow::RecordBatch> b; int64_t n = 0, nb = 0;
    while (r->ReadNext(&b).ok() && b) { n += b->num_rows(); nb++; }
    if (batches) *batches = nb;
    return n;
}

int main()
{
    /* AdbcDriverInit: the driver-manager vtable is fully populated. */
    AdbcDriver drv{}; AdbcError err{};
    assert(AdbcDriverInit(ADBC_VERSION_1_0_0, &drv, &err) == ADBC_STATUS_OK);
    assert(drv.DatabaseNew && drv.ConnectionInit && drv.StatementExecuteQuery &&
           drv.StatementBind && drv.ConnectionGetObjects &&
           drv.ConnectionGetTableSchema && drv.ConnectionGetTableTypes);
    std::printf("ok AdbcDriverInit vtable\n");

    std::string cs = conn_str();
    AdbcDatabase db{}; AdbcConnection conn{};
    check(AdbcDatabaseNew(&db, &err), &err, "DatabaseNew");
    check(AdbcDatabaseSetOption(&db, "uri", cs.c_str(), &err), &err, "SetOption");
    check(AdbcDatabaseInit(&db, &err), &err, "DatabaseInit");
    check(AdbcConnectionNew(&conn, &err), &err, "ConnectionNew");

    /* Tolerate a still-warming-up server. */
    AdbcStatusCode crc = ADBC_STATUS_UNKNOWN;
    for (int i = 0; i < 6; i++) {
        AdbcError e2{};
        crc = AdbcConnectionInit(&conn, &db, &e2);
        if (crc == ADBC_STATUS_OK) break;
        sleep(3);
    }
    check(crc, &err, "ConnectionInit");
    std::printf("ok connect\n");

    /* 1. Typed Arrow output: each SQL type maps to its natural Arrow type. */
    {
        auto r = run(&conn,
            "SELECT CAST(42 AS BIGINT) k, CAST(3.5 AS DOUBLE) d, true b, "
            "DATE '2020-01-15' dt, TIMESTAMP '2021-06-01 12:30:45' ts, 'hi' s");
        auto sch = r->schema();
        assert(sch->field(0)->type()->id() == arrow::Type::INT64);
        assert(sch->field(1)->type()->id() == arrow::Type::DOUBLE);
        assert(sch->field(2)->type()->id() == arrow::Type::BOOL);
        assert(sch->field(3)->type()->id() == arrow::Type::DATE32);
        assert(sch->field(4)->type()->id() == arrow::Type::TIMESTAMP);
        assert(sch->field(5)->type()->id() == arrow::Type::STRING);
        assert(drain(r) == 1);
        std::printf("ok typed columns (int64/double/bool/date32/timestamp/utf8)\n");
    }

    /* 2. Batched streaming: a large result arrives as several Arrow batches. */
    {
        auto r = run(&conn,
            "SELECT orderkey, totalprice FROM tpch.sf1.orders "
            "ORDER BY orderkey LIMIT 10000");
        int64_t batches = 0;
        int64_t n = drain(r, &batches);
        assert(n == 10000);
        assert(batches >= 2);   /* ADBC_BATCH_ROWS = 4096 */
        std::printf("ok streaming: %lld rows in %lld batches\n",
                    (long long)n, (long long)batches);
    }

    /* 3. Bound parameters: an Arrow param set substituted into ? markers. */
    {
        AdbcStatement st{}; AdbcError e{};
        check(AdbcStatementNew(&conn, &st, &e), &e, "StatementNew");
        arrow::Int64Builder ib;  (void)ib.Append(1); auto ia = *ib.Finish();
        arrow::StringBuilder sb; (void)sb.Append("A"); auto sa = *sb.Finish();
        auto psch = arrow::schema({arrow::field("p0", arrow::int64()),
                                   arrow::field("p1", arrow::utf8())});
        auto pb = arrow::RecordBatch::Make(psch, 1, {ia, sa});
        ArrowArray cA{}; ArrowSchema cS{};
        (void)arrow::ExportRecordBatch(*pb, &cA, &cS);
        check(AdbcStatementBind(&st, &cA, &cS, &e), &e, "Bind");
        check(AdbcStatementSetSqlQuery(&st,
            "SELECT name FROM tpch.tiny.nation "
            "WHERE regionkey = ? AND name > ? ORDER BY name", &e), &e, "SetSql");
        ArrowArrayStream s{}; int64_t ra = 0;
        check(AdbcStatementExecuteQuery(&st, &s, &ra, &e), &e, "Exec");
        auto rr = arrow::ImportRecordBatchReader(&s); assert(rr.ok());
        int64_t n = drain(*rr);
        assert(n == 5);   /* region 1 nations with name > 'A' */
        AdbcStatementRelease(&st, &e);
        std::printf("ok bound params: %lld rows\n", (long long)n);
    }

    /* 4. GetTableSchema: a table's columns as a typed Arrow schema. */
    {
        ArrowSchema sc{}; AdbcError e{};
        check(AdbcConnectionGetTableSchema(&conn, "tpch", "tiny", "nation", &sc, &e),
              &e, "GetTableSchema");
        auto sr = arrow::ImportSchema(&sc); assert(sr.ok());
        assert((*sr)->num_fields() == 4);
        assert((*sr)->GetFieldByName("nationkey")->type()->id() == arrow::Type::INT64);
        assert((*sr)->GetFieldByName("name")->type()->id() == arrow::Type::STRING);
        std::printf("ok GetTableSchema\n");
    }

    /* 5. GetTableTypes. */
    {
        ArrowArrayStream s{}; AdbcError e{};
        check(AdbcConnectionGetTableTypes(&conn, &s, &e), &e, "GetTableTypes");
        auto rr = arrow::ImportRecordBatchReader(&s); assert(rr.ok());
        assert(drain(*rr) == 2);   /* TABLE, VIEW */
        std::printf("ok GetTableTypes\n");
    }

    /* 6. GetObjects: the nested catalog hierarchy schema + catalog level. */
    {
        ArrowArrayStream s{}; AdbcError e{};
        check(AdbcConnectionGetObjects(&conn, 0, nullptr, nullptr, nullptr, nullptr,
                                       nullptr, &s, &e), &e, "GetObjects");
        auto rr = arrow::ImportRecordBatchReader(&s); assert(rr.ok());
        assert((*rr)->schema()->field(0)->name() == "catalog_name");
        assert((*rr)->schema()->field(1)->type()->id() == arrow::Type::LIST);
        int64_t n = drain(*rr);
        assert(n >= 1);   /* at least the tpch catalog */
        std::printf("ok GetObjects: %lld catalogs\n", (long long)n);
    }

    AdbcConnectionRelease(&conn, &err);
    AdbcDatabaseRelease(&db, &err);
    std::printf("ALL ADBC INTEGRATION TESTS PASSED\n");
    return 0;
}
