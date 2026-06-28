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
