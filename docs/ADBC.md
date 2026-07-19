# Argus ADBC driver

Argus exposes an [Arrow ADBC](https://arrow.apache.org/adbc/) surface
(`libargus_adbc.so`) layered over its existing, validated ODBC stack. A query's
result is returned as an Arrow **C Data Interface** stream of typed columns, so
Arrow-native consumers (Power BI/Fabric's ADBC path, the Python/R ADBC bindings,
DuckDB, etc.) get columnar data directly.

This anticipates the industry shift to ADBC. Note what that shift is and is not:
Power BI/Fabric is moving to ADBC by default and retiring the ODBC drivers it
**embeds** (service end of 2026, Desktop spring 2027), but Microsoft
[states](https://learn.microsoft.com/en-us/power-query/transition-to-adbc) that
the transition "doesn't change behavior for the ODBC connector when you use a
separately installed ODBC driver". Argus is separately installed, so its ODBC
path and its Power BI connector are not affected, and this ADBC surface is an
addition rather than a migration. See [BI_TOOLS.md](BI_TOOLS.md).

## Design

The driver is a thin Arrow adapter, not a reimplementation: a connection is an
Argus ODBC connection (`SQLDriverConnect` with the usual `BACKEND=...;HOST=...`
string), and `AdbcStatementExecuteQuery` runs the SQL through
`SQLExecDirect`/`SQLFetch` and emits the rows as an `ArrowArrayStream`. Every
backend, the streaming fetch and the native typed cells are reused unchanged.

Column types are mapped to Arrow:

| SQL type | Arrow type | C Data format |
|---|---|---|
| TINYINT/SMALLINT/INTEGER/BIGINT | int64 | `l` |
| REAL/FLOAT/DOUBLE | double | `g` |
| BIT | bool | `b` |
| DATE | date32 (days) | `tdD` |
| TIMESTAMP | timestamp, microseconds | `tsu:` |
| everything else (incl. DECIMAL) | utf8 | `u` |

The Arrow C Data Interface is a stable, dependency-free ABI, so the driver emits
it without linking any Arrow library.

## Usage

```c
#include "argus/adbc.h"

struct AdbcError err = {0};
struct AdbcDatabase db = {0};
AdbcDatabaseNew(&db, &err);
AdbcDatabaseSetOption(&db, "uri",
    "BACKEND=trino;HOST=trino;PORT=8080;UID=analyst;Database=tpch", &err);
AdbcDatabaseInit(&db, &err);

struct AdbcConnection conn = {0};
AdbcConnectionNew(&conn, &err);
AdbcConnectionInit(&conn, &db, &err);

struct AdbcStatement stmt = {0};
AdbcStatementNew(&conn, &stmt, &err);
AdbcStatementSetSqlQuery(&stmt, "SELECT * FROM tpch.tiny.nation", &err);

struct ArrowArrayStream stream = {0};
int64_t rows_affected = 0;
AdbcStatementExecuteQuery(&stmt, &stream, &rows_affected, &err);
/* consume `stream` with any Arrow C Data Interface importer */
```

Build with `-DBUILD_ADBC=ON` (default). Covered by `tests/integration/test_adbc.cpp`
(built when `BUILD_ADBC` + the Flight SQL backend are on), which runs the whole
surface against a live Trino and imports every result with Arrow C++'s own
`ImportRecordBatchReader`/`ImportSchema` — the reference C Data Interface validator.

## Status & roadmap

All first-round ADBC items are implemented (vtable, typed Arrow, batched
streaming, bound params, GetTableSchema/GetTableTypes/GetObjects).

Covers `SELECT` to a materialized record batch with int64/double/utf8 columns,
plus the driver-manager `AdbcDriverInit` vtable (ADBC 1.0.0), so the driver loads
via any ADBC driver manager by name, and typed Arrow output for
int64/double/bool/date32/timestamp/utf8 (DECIMAL stays utf8 to preserve
arbitrary precision), and **batched streaming** — the result statement stays open
and each `get_next` pulls one Arrow batch (up to 4096 rows) lazily, so memory is
bounded regardless of result size, **bound parameters** (`AdbcStatementBind`)
— the Arrow parameter array (row 0) is decoded and substituted into the query's
`?` markers as SQL literals — and the metadata calls `AdbcConnectionGetTableSchema`
(a table's columns as an Arrow schema, via `SQLColumns`) and
`AdbcConnectionGetTableTypes`, and `AdbcConnectionGetObjects` (the full nested
catalog/schema/table/column Arrow schema, with the catalog level populated;
populating the deeper schema/table/column levels is a follow-up).
