# Argus ADBC driver

Argus exposes an [Arrow ADBC](https://arrow.apache.org/adbc/) surface
(`libargus_adbc.so`) layered over its existing, validated ODBC stack. A query's
result is returned as an Arrow **C Data Interface** stream of typed columns, so
Arrow-native consumers (Power BI/Fabric's ADBC path, the Python/R ADBC bindings,
DuckDB, etc.) get columnar data directly.

This anticipates the industry shift to ADBC — Power BI/Fabric is moving to ADBC
by default and retiring its embedded ODBC drivers (service end of 2026, Desktop
spring 2027), which covers Argus's backends.

## Design

The driver is a thin Arrow adapter, not a reimplementation: a connection is an
Argus ODBC connection (`SQLDriverConnect` with the usual `BACKEND=...;HOST=...`
string), and `AdbcStatementExecuteQuery` runs the SQL through
`SQLExecDirect`/`SQLFetch` and emits the rows as an `ArrowArrayStream`. Every
backend, the streaming fetch and the native typed cells are reused unchanged.

Column types are mapped to Arrow:

| SQL type | Arrow type | C Data format |
|---|---|---|
| TINYINT/SMALLINT/INTEGER/BIGINT/BIT | int64 | `l` |
| REAL/FLOAT/DOUBLE | double | `g` |
| everything else | utf8 | `u` |

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

Build with `-DBUILD_ADBC=ON` (default). Validated end-to-end against Trino: the
emitted stream is imported by Arrow C++'s own `ImportRecordBatchReader`, yielding
`int64`/`double`/`utf8` columns with exact values.

## Status & roadmap

First increment — covers `SELECT` to a materialized record batch with
int64/double/utf8 columns. Planned next: the driver-manager `AdbcDriverInit`
vtable (so it loads via the ADBC driver manager by name), richer Arrow types
(decimal, timestamp, date), batched streaming (emit one Arrow batch per fetch
block rather than materializing), bind parameters, and the ADBC metadata calls
(`GetObjects`, `GetTableSchema`).
