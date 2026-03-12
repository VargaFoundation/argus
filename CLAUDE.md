# Argus ODBC Driver — Development Guide

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build
```

## Tests

Unit tests:
```bash
cd build && ctest --output-on-failure -L unit
```

Integration tests (requires Docker):
```bash
cmake -B build -DBUILD_INTEGRATION_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build
docker compose -f tests/integration/docker-compose.yml up -d
cd build && ctest --output-on-failure -L integration
```

## Architecture

Two layers:
- **ODBC API** (`src/odbc/`): implements the ODBC entry points (SQLConnect, SQLExecDirect, etc.)
- **Backends** (`src/backend/`): pluggable database backends (Hive, Impala, Trino, Phoenix, Kudu)

Handle hierarchy: `ENV → DBC → STMT`

Backend vtable interface defined in `include/argus/backend.h`.

## Backends

Backends are enabled by auto-detection of dependencies at cmake time:
- `ARGUS_BUILD_THRIFT_BACKENDS` — Hive + Impala (requires thrift_c_glib)
- `ARGUS_BUILD_TRINO` — Trino (requires libcurl + json-glib)
- `ARGUS_BUILD_PHOENIX` — Phoenix (requires libcurl + json-glib)
- `ARGUS_BUILD_KUDU` — Kudu (requires libkudu_client)

## Code Style

- C11, compiled with `-Wall -Wextra -Wpedantic`
- No `//` comments in C source files — use `/* */` only
- `argus_` prefix for all public symbols
- snake_case for functions and variables
- Constants: `ARGUS_UPPER_SNAKE_CASE`
