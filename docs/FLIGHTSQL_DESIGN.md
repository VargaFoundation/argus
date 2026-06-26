# Arrow Flight SQL backend — design & status

Status: **implemented, building against Arrow C++ 24.0.0, and validated
end-to-end against a live InfluxDB 3 Core**, behind the `ARGUS_BUILD_FLIGHTSQL`
cmake switch (auto-detected from `arrow-flight-sql`). Reaches Dremio,
InfluxDB 3.x, Apache Doris and StarRocks (`BACKEND=flightsql`).

End-to-end validation (InfluxDB 3 Core 3.10.0, `--without-auth`, `:8181`):
`SELECT host, usage, cores FROM cpu` returns the 3 written rows with correct
string/double/int conversion; `SQLTables` returns the `iox.cpu` table via the
`GetTables` RPC. Reproduce with the smoke tests in the scratchpad against
`BACKEND=flightsql;HOST=127.0.0.1;PORT=8181;DATABASE=testdb`.

What exists and is verified:
- `flightsql_convert.{h,cpp}` — Arrow `Schema`/`RecordBatch` → ODBC columns + text
  cells. Depends only on plain libarrow. **Unit tested and passing**
  (`tests/unit/test_flightsql_convert.cpp`, runs without a live endpoint:
  schema→columns, typed values, NULLs, multi-batch append).
- `flightsql_backend.cpp` — the vtable over `arrow::flight::sql::FlightSqlClient`:
  connect (TCP/TLS + basic-token or bearer auth), execute, multi-endpoint
  `DoGet` fetch, result metadata, and the native metadata RPCs (GetTables,
  GetDbSchemas, GetCatalogs, GetPrimaryKeys, GetXdbcTypeInfo). **Compiles clean
  against Arrow 24**; the whole project links `libarrow-flight-sql` and the
  20 existing unit tests still pass.
- cmake gating + registry wiring (`argus_flightsql_backend_get`).

## Build requirements (learned from the Arrow 24 build)

- **Apache Arrow APT repo** — Arrow C++ is *not* in the default Ubuntu repos.
  Install `apache-arrow-apt-source` first, then `libarrow-flight-sql-dev`
  (pulls `libarrow-dev`, `libarrow-flight-dev`, gRPC, protobuf, …).
- **GCC 14+** — Arrow 24's `type.h` (the `CTypeImpl` template with a nested-enum
  non-type parameter) hits a parser bug in GCC 13; it compiles on GCC 14.
  Configure with `-DCMAKE_C_COMPILER=gcc-14 -DCMAKE_CXX_COMPILER=g++-14`
  (or `CC=gcc-14 CXX=g++-14`).
- **C++20** — required by modern Arrow headers (`std::span`/`std::bit_width`);
  set for the Flight SQL target.
- **`BOOL` macro clash** — unixODBC's `<sql.h>` does `#define BOOL int`, which
  corrupts `arrow::Type::BOOL`; the Flight SQL headers `#undef BOOL` after the
  ODBC headers. The argus C headers are wrapped in `extern "C"` from the C++ TU
  (they have no guards of their own), as the Kudu backend does.

Build it:
```
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-noble.deb
sudo apt update && sudo apt install -y -V libarrow-flight-sql-dev g++-14 gcc-14
CC=gcc-14 CXX=g++-14 cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build && (cd build && ctest -L unit)   # -> test_flightsql_convert passes
```

## Server specifics learned from the InfluxDB 3 run

- **Database selection** — InfluxDB 3 selects the target database with a gRPC
  `database` call header, not a SQL catalog. The backend sends the `DATABASE`
  connection attribute as that header on every call (harmless for servers that
  ignore it).
- **Table types** — ODBC clients ask `SQLTables` for type `"TABLE"`, but Flight
  SQL servers report `"BASE TABLE"`. `get_tables` parses the ODBC comma list and
  translates `TABLE` → `BASE TABLE` so BI tools see user tables.

What still needs doing / validation:
- Validate against **Dremio** and **Doris/StarRocks** Flight SQL too (auth via
  basic-token handshake / bearer; different catalog conventions).
- Map `GetColumns` output (currently `GetTables(include_schema=true)`) into the
  exact ODBC `SQLColumns` shape.
- The Arrow-native (zero-copy) fetch path — see Phase 2 in `ROADMAP.md`.

The sections below remain the reference for the architecture and the type mapping.

## Why

Arrow Flight SQL is the modern, columnar, gRPC-based wire protocol for analytic
engines. A single backend reaches **Dremio, InfluxDB 3.x, Apache Doris and
StarRocks** (all expose Flight SQL endpoints), and lays the foundation for an
eventual **ADBC** surface — relevant because Power BI / Fabric is migrating to
ADBC by default and will retire its embedded ODBC drivers (service end 2026,
Desktop spring 2027). See `ROADMAP.md` §2.

## Why it is a separate, larger effort

Unlike the MySQL-wire backend (one synchronous C library, `libmariadb`), Flight
SQL pulls in a heavy C++ stack:

- `libarrow`, `libarrow-flight`, `libarrow-flight-sql` (+ gRPC, protobuf, ~100 MB+).
- A **C++** translation unit, like the Kudu backend (`enable_language(CXX)`).
- Results arrive as Arrow `RecordBatch`es, not text rows — a real columnar→cell
  conversion layer is required, not the trivial passthrough MySQL-wire uses.
- No Flight SQL endpoint is available in the current test environment, so the
  backend is **compile-verifiable only** here; runtime validation needs a live
  Dremio/InfluxDB 3 (or a `pyarrow.flight` mock) in CI.

## CMake gating (to add)

```cmake
# Optional: Arrow Flight SQL backend (Dremio / InfluxDB 3 / Doris / StarRocks)
pkg_check_modules(ARROW_FLIGHT_SQL arrow-flight-sql)
if(ARROW_FLIGHT_SQL_FOUND)
    set(ARGUS_BUILD_FLIGHTSQL ON)
    message(STATUS "Arrow Flight SQL backend: ENABLED")
else()
    set(ARGUS_BUILD_FLIGHTSQL OFF)
    message(STATUS "Arrow Flight SQL backend: DISABLED (install libarrow-flight-sql-dev)")
endif()
```

Plus a `src/backend/flightsql/` subdir (C++), `ARGUS_HAS_FLIGHTSQL` compile def,
and registration in `src/backend/backend.c` (`argus_flightsql_backend_get`),
mirroring the Kudu wiring.

## Mapping onto the `argus_backend_t` vtable

| vtable op | Flight SQL realization |
|-----------|------------------------|
| `connect` | `FlightClient::Connect(Location)`, then `FlightSqlClient`. Auth via call headers (Bearer/JWT) or mTLS; `Handshake` for user/pass. |
| `execute` | `FlightSqlClient::Execute(query)` → `FlightInfo`; keep its `endpoints`. |
| `fetch_results` | For each endpoint, `DoGet(ticket)` → `FlightStreamReader`; pull `RecordBatch`es, convert each column to `argus_cell_t` text (reuse a shared Arrow→string formatter). Honor `max_rows` by bounding rows per call across batches. |
| `get_result_metadata` | From the stream `schema` (Arrow `Schema`); map each `arrow::DataType` → ODBC SQL type (table below). |
| `get_tables` / `get_columns` / `get_schemas` / `get_catalogs` / `get_primary_keys` | Native Flight SQL metadata RPCs: `GetTables`, `GetSqlInfo`/`GetTables(include_schema)`, `GetDbSchemas`, `GetCatalogs`, `GetPrimaryKeys`. These already return ODBC-shaped columns. |
| `get_type_info` | `GetXdbcTypeInfo`. |
| `cancel` | `FlightClient::CancelFlightInfo` (or close the reader). |

### Arrow type → ODBC SQL type (sketch)

- `int8/16/32/64` → `SQL_TINYINT/SMALLINT/INTEGER/BIGINT`
- `float/double` → `SQL_REAL/SQL_DOUBLE`
- `decimal128/256` → `SQL_DECIMAL` (precision/scale from the type)
- `utf8/large_utf8` → `SQL_VARCHAR`; `binary` → `SQL_VARBINARY`
- `bool` → `SQL_BIT`
- `date32/64` → `SQL_TYPE_DATE`; `time32/64` → `SQL_TYPE_TIME`
- `timestamp` → `SQL_TYPE_TIMESTAMP`
- `list/struct/map` → `SQL_VARCHAR` (JSON-encoded), matching the Trino backend

## Auth

Reuse the patterns already implemented for Trino: Bearer/JWT and OAuth2 M2M
(with the new 401-refresh) map directly to Flight call headers; Kerberos/mTLS
via the gRPC channel credentials. Factor the token logic into
`src/backend/common/` when this lands so Trino and Flight SQL share it.

## Result conversion — the one non-trivial piece

The existing row cache stores **text** cells (`argus_cell_t.data`). The pragmatic
first cut converts each Arrow array element to its string form (one formatter per
Arrow type), so the rest of Argus (ODBC conversion in `fetch.c`) is unchanged.
A later optimization keeps Arrow buffers and converts lazily in `SQLGetData`
(the Arrow-native path from `ROADMAP.md` Phase 2) — that is where the columnar
throughput win actually materializes.

## Testing plan

1. Compile-only in CI when `libarrow-flight-sql-dev` is present.
2. Runtime: a `pyarrow.flight` mock server (a few tables + a static result),
   plus an optional Dremio container in the integration compose file.
3. Validate metadata RPCs and a multi-endpoint `DoGet` (Flight can shard a
   result across endpoints — the fetch loop must drain them in order).

## Effort estimate

~1.5–3 weeks: dependency/build plumbing (1–2 d), connect+execute+single-endpoint
fetch (3–4 d), Arrow→cell conversion for the common types (2–3 d), metadata RPCs
(2–3 d), auth wiring (1–2 d), mock + integration tests (2–3 d).
