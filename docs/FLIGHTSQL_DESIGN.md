# Arrow Flight SQL backend — design & status

Status: **initial implementation landed** in `src/backend/flightsql/`, behind the
`ARGUS_BUILD_FLIGHTSQL` cmake switch (auto-detected from `arrow-flight-sql`).
Reaches Dremio, InfluxDB 3.x, Apache Doris and StarRocks (`BACKEND=flightsql`).

What exists:
- `flightsql_convert.{h,cpp}` — Arrow `Schema`/`RecordBatch` → ODBC columns + text
  cells. Depends only on plain libarrow and is unit tested
  (`tests/unit/test_flightsql_convert.cpp`, runs without a live endpoint).
- `flightsql_backend.cpp` — the vtable over `arrow::flight::sql::FlightSqlClient`:
  connect (TCP/TLS + basic-token or bearer auth), execute, multi-endpoint
  `DoGet` fetch, result metadata, and the native metadata RPCs (GetTables,
  GetDbSchemas, GetCatalogs, GetPrimaryKeys, GetXdbcTypeInfo).
- cmake gating + registry wiring (`argus_flightsql_backend_get`).

What still needs doing / validation:
- **Build + run in an environment with `libarrow-flight-sql-dev`** (Arrow C++ is
  not in the default Ubuntu repos; the local sandbox could not link it). Compile
  the backend TU and run the integration path against a live Dremio/InfluxDB 3 or
  a `pyarrow.flight` mock.
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
