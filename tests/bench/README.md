# Fetch-path micro-benchmark

`argus_bench` times a query's execute + full fetch loop against any backend and
reports rows, wall-clock (avg/min/max over N iterations) and throughput
(rows/s and cells/s). It's a manual tool for comparing backends and tracking
the fetch path as the Arrow-native / Cloud-Fetch work (ROADMAP Phase 2) lands.

## Build

```bash
cmake -B build -DBUILD_INTEGRATION_TESTS=ON && cmake --build build --target argus_bench
```

## Run

```
argus_bench "CONNECTION_STRING" "SQL" [iterations]
```

```bash
# Trino (built-in tpch — no data setup needed)
argus_bench "BACKEND=trino;HOST=localhost;PORT=8080;UID=test;Database=tpch" \
            "SELECT * FROM tiny.lineitem" 5

# MySQL-wire (StarRocks / Doris / ClickHouse / MariaDB)
argus_bench "BACKEND=mysql;HOST=127.0.0.1;PORT=3306;UID=root;PWD=pw;Database=db" \
            "SELECT * FROM big_table" 10

# Arrow Flight SQL (InfluxDB 3 / Dremio)
argus_bench "BACKEND=flightsql;HOST=127.0.0.1;PORT=8181;Database=testdb" \
            "SELECT * FROM cpu" 10
```

A warm-up iteration (untimed) primes connection/plan caches before the timed
runs. Every column of every row is read with `SQLGetData` so the whole row is
materialized, making the numbers representative of a real client drain.

## Measured: PostgreSQL, Argus vs psqlODBC

Both drivers, the same server, the same query, the same client loop
(`SQLGetData(SQL_C_CHAR)` on every column of every row), through the same
unixODBC driver manager. Table: 1 500 000 rows × 9 columns (bigint, int,
float8, varchar, smallint, date, numeric(12,4), boolean, timestamp), 121 MB
on disk; PostgreSQL 16.15 over loopback; best of three runs.

| | rows/s | peak RSS |
|---|---|---|
| **Argus** (`BACKEND=postgres`, default `FetchBufferSize=1000`) | **727 000** | **50 MB** |
| psqlODBC, defaults | 391 000 | 629 MB |
| psqlODBC, `UseDeclareFetch=1;Fetch=1000` | 352 000 | 12 MB |
| psqlODBC, `UseDeclareFetch=1;Fetch=10000` | 378 000 | 15 MB |

Two things to read out of it. Argus is **~1.9× faster** on the same work in
every psqlODBC configuration. And its memory is flat in the size of the result
(50 MB for 1.5 M rows, the same for 15 M): psqlODBC only gets that property by
turning on `UseDeclareFetch`, which is **off by default** — out of the box it
materialises the whole result set client-side, which is the 629 MB above and
the reason a large extract can take a workstation down.

Reproducing the psqlODBC column needs a driver-manager client rather than
`argus_bench` (which links the driver directly); any ODBC program will do.

## Why there is no COPY BINARY path

The plan for this backend included a `COPY … TO STDOUT (FORMAT binary)` fast
path. It was measured before it was built, and the measurement said not to
build it. Raw libpq on the query above, no ODBC layer at all:

| libpq mode | rows/s | bytes on the wire |
|---|---|---|
| `PQexec` (buffer everything) | 720 000 | 99.5 MB |
| `PQsetSingleRowMode` (what the driver uses) | **1 179 000** | 99.5 MB |
| `COPY … (FORMAT binary)` | 1 289 000 | **132.7 MB** |

COPY binary is only **9% faster than the path already in use**, and for this
schema it is **33% *larger* on the wire** — the per-field length prefixes and
the fixed-width binary forms of `int8`, `timestamp` and `numeric` cost more
than their compact text. Meanwhile the driver is at 727 k of an achievable
1 179 k, so the remaining 38% is the row cache and ODBC conversion, which a
binary wire format does not touch: most types would still have to be rendered
to text for the row cache, adding work back.

Against that, the binary path is the highest-defect-density code in the design
— NBASE-10000 numeric reconstruction with weight and dscale, `PQgetCopyData`
buffers that do not align to tuple boundaries, and a text-vs-binary
`timestamptz` divergence that is silent and data-dependent. Nine percent of the
protocol half of the cost is not worth that, so it is not implemented. If the
row cache is ever reworked to avoid the per-cell allocation, the arithmetic
changes and this is worth re-measuring.

`FetchBufferSize` was swept over 100 / 1 000 / 10 000 / 50 000 on the same
query: 679 k / 743 k / 715 k / 711 k rows/s. The default of 1 000 is already
the best of them, so there is no tuning advice to give beyond leaving it alone.
