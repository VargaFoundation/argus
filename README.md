# Argus ODBC Driver

[![CI](https://github.com/VargaFoundation/argus/actions/workflows/ci.yml/badge.svg)](https://github.com/VargaFoundation/argus/actions/workflows/ci.yml)
[![CodeQL](https://github.com/VargaFoundation/argus/actions/workflows/codeql.yml/badge.svg)](https://github.com/VargaFoundation/argus/actions/workflows/codeql.yml)

Multi-backend ODBC driver for analytics engines — Hive, Impala, Trino, Phoenix, Pinot, Druid, Kudu, MySQL-wire (StarRocks/Doris/ClickHouse) and Arrow Flight SQL (Dremio/InfluxDB 3) — with comprehensive logging, SSL/TLS, OAuth2 and an Arrow ADBC surface built over the same stack.

## Features

### Core ODBC Support
- **107 ODBC entry points** (ANSI + Unicode `W` variants) — ODBC 3.80, **Level 1 interface conformance** (`SQL_OIC_LEVEL1`, SQL-92 Entry), plus ODBC 2.x compatibility (`SQLAllocConnect`, `SQLError`, `SQLExtendedFetch`, ...). This matches the commercial Simba/Starburst drivers for these engines; stored procedures and transactions — the two OLTP features Level 1 also names — are reported absent (`SQL_PROCEDURES="N"`, `SQL_TXN_CAPABLE=SQL_TC_NONE`), as they are on Trino/BigQuery/Hive themselves.
- **Statement-level asynchronous execution** (`SQL_ASYNC_MODE = SQL_AM_STATEMENT`): async `SQLExecDirect`/`SQLExecute` on a worker thread, with `SQLCompleteAsync` and `SQLCancelHandle` (ODBC 3.8).
- **10 backends**, enabled by dependency auto-detection at configure time
- **Cross-platform**: Linux, macOS and Windows x64
- **Arrow ADBC driver** (`libargus_adbc`) exposing the same backends through the Arrow C Data Interface

### Backends

| `BACKEND=` | Engine | Protocol | Build dependency | In Windows installer |
|------------|--------|----------|------------------|----------------------|
| `hive` | Apache Hive — also Spark Thrift Server and Flink SQL Gateway (HiveServer2 protocol) | Thrift binary or HTTP | `thrift_c_glib` | yes |
| `impala` | Apache Impala | Thrift | `thrift_c_glib` | yes |
| `trino` | Trino | HTTP/JSON | libcurl + json-glib | yes |
| `phoenix` | Apache Phoenix (Avatica) | HTTP/JSON | libcurl + json-glib | yes |
| `pinot` | Apache Pinot | HTTP/JSON | libcurl + json-glib | yes |
| `druid` | Apache Druid | HTTP/JSON | libcurl + json-glib | yes |
| `bigquery` | Google BigQuery — incl. sovereign clouds (S3NS): every Google endpoint is configurable | REST/JSON | libcurl + json-glib (+ OpenSSL for key files) | yes |
| `mysql` | StarRocks / Doris / ClickHouse / MySQL / MariaDB | MySQL wire | libmariadb | yes |
| `flightsql` | Dremio / InfluxDB 3 / any Arrow Flight SQL server | gRPC / Arrow | arrow-flight-sql (C++) | no |
| `kudu` | Apache Kudu (deprecated — prefer `BACKEND=impala`) | kudu_client | libkudu_client | no |

The Windows installer ships Hive, Impala, Trino, Phoenix, Pinot, Druid, BigQuery and MySQL-wire (StarRocks/Doris/ClickHouse); Flight SQL and Kudu need dependencies MSYS2 does not provide. Hive/Impala speak through a GIO socket transport (portable, with timeouts and TLS); the installer bundles the glib-networking TLS backend, which the driver loads automatically.

### Production Features

#### Logging System
- **7 Log Levels**: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE
- **Thread-Safe**: Mutex-protected log operations
- **Flexible Output**: File-based or stderr
- **Configuration**:
  - Connection string: `LogLevel=5;LogFile=/tmp/argus.log`
  - Environment variables: `ARGUS_LOG_LEVEL`, `ARGUS_LOG_FILE`

#### SSL/TLS and Authentication
- **Trino**: full HTTPS with certificate verification, plus OAuth2 — client credentials, device code (RFC 8628) and authorization code with PKCE + browser SSO, with OIDC discovery
- **Hive/Impala**: Thrift SSL sockets; Kerberos over binary Thrift (system GSSAPI on Linux/macOS, native SSPI on Windows — no MIT Kerberos needed); HTTP transport with SPNEGO/Kerberos or Bearer/JWT tokens (Databricks personal access tokens)
- Certificate, key and CA configuration; hostname verification toggle

#### Connection Resilience
- **Automatic Retry**: Configurable retry count and delay
- **Timeouts**: Connect, query, and socket timeouts
- **Connection String**: `RetryCount=3;RetryDelay=2;ConnectTimeout=30`

#### Data Type Conversions
- **Basic Types**: INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, BIT
- **Date/Time**: DATE, TIME, TIMESTAMP with proper parsing
- **Numeric**: DECIMAL/NUMERIC with 128-bit precision
- **Binary**: Raw binary data support
- **Unsigned**: ULONG, USHORT, UTINYINT, UBIGINT
- **Wide Char**: UTF-8 to UTF-16LE conversion

#### Query Management
- **SQLCancel**: Cancel running queries across all backends
- **Application Name**: Identify queries with a custom app name (`X-Trino-Source`, `hive.query.source`)

#### Fetch Optimization
- **Configurable Buffer Size**: `FetchBufferSize=5000`
- **Block fetch**: `SQL_ATTR_ROW_ARRAY_SIZE` rowsets and `SQL_ATTR_PARAMSET_SIZE` parameter arrays
- **Static scrollable cursors** (`SQLFetchScroll` NEXT/PRIOR/FIRST/LAST/ABSOLUTE/RELATIVE)
- **DOM-free Trino decode**: result pages are scanned straight into cells instead of building a json-glib DOM — ~65% faster fetch on large extracts (proven byte-identical; kill-switch `ARGUS_TRINO_NOFASTJSON`). Trino spooling and a numeric fast-path (no text round-trip) are used where available.

## How Argus compares

Argus is measured against the commercial baseline — Simba/Magnitude's SimbaEngine
drivers (OEM'd as the Databricks/Spark, Impala, Hive, Athena and BigQuery ODBC
drivers) and Starburst's Trino driver. The full evidence, including a black-box
inspection of real Simba binaries and a live Tableau TDVT run, is in
[docs/SIMBA_PARITY.md](docs/SIMBA_PARITY.md).

| | **Argus** | Simba / Starburst (per engine) | Generic *Other Databases (ODBC)* |
|---|---|---|---|
| Engines per driver | **10 in one binary** | one driver per engine | any, but dialect-blind |
| Licensing | **open** (Varga Foundation) | proprietary, per-seat | bundled |
| ODBC level | 3.8, Level 1, SQL-92 Entry | 3.8, Level 1/2 | depends on driver |
| Exported ODBC entry points | **107** | 89 (SimbaEngine core) | n/a |
| Unicode (`W`) | full | full | varies |
| Async execution | **yes** (`SQL_AM_STATEMENT`) | yes | usually no |
| Auth | Kerberos (GSSAPI+SSPI), OAuth2 (M2M + device flow), JWT, LDAP, TLS | yes | varies |
| Connection pooling | yes | yes | driver-manager only |
| Tableau TDVT | **91.4%** measured | certified (>90%) | not applicable |
| Large-result decode | DOM-free JSON (~65% faster), spooling, Arrow via ADBC | Arrow / Cloud Fetch | row-wise |
| Dialect correctness | per-backend dialect + ODBC escape translation | per-engine | **none** — SQL passed through |
| Arrow surface | ADBC driver + Flight SQL backend | JDBC/ODBC | no |

### Why Argus

- **One driver, ten engines.** Commercial connectivity means a separate licensed
  driver per engine — Simba Spark *and* Simba Impala *and* Starburst Trino, each
  with its own installer, DSN scheme and support contract. Argus is a single
  binary with one connection-string grammar and a per-backend dialect layer, so
  a BI deployment installs and governs **one** driver.
- **At parity on the contract that matters, open.** Parity is measured on the
  observable ODBC contract, not marketing: ODBC 3.8, full Unicode, Level 1
  conformance, real async, pooling, the full auth matrix, a **broader raw ODBC
  surface (107 vs 89 entry points)**, and a **91.4% Tableau TDVT** pass rate on
  the same tests certified connectors run — with no per-seat licence.
- **Honest capabilities.** Argus advertises only what it can do. Where an engine
  has no foreign keys, stored procedures or row-version columns, the catalog
  functions return empty — exactly as Simba's drivers for those engines do —
  rather than inventing metadata a BI tool would then trust.
- **Fast where it counts.** The Trino fetch path decodes result pages without a
  JSON DOM (~65% faster on large extracts, proven byte-identical to the
  reference path), on top of Trino spooling and a numeric fast-path.
- **Dialect-correct, unlike the generic ODBC entry.** Tableau itself warns that
  with *Other Databases (ODBC)* "compatibility is not guaranteed"; Argus ships a
  per-backend SQL dialect and translates ODBC escape sequences (`{fn …}`, `{d}`,
  `{ts}`, `{oj}`) into each engine's own grammar, so `{fn}`-generating tools
  (Tableau, Excel, Qlik) get correct SQL.
- **Sovereign and configurable.** Every Google endpoint is configurable for
  BigQuery, including S3NS sovereign clouds — connectivity that closed vendor
  drivers do not expose.
- **Arrow-native.** The same backends are exposed through an Arrow ADBC driver,
  and Arrow Flight SQL is a first-class backend.

## Building

See [docs/BUILDING.md](docs/BUILDING.md) for the full matrix.

**Linux:**
```bash
sudo apt-get install cmake build-essential unixodbc-dev libglib2.0-dev
# Hive/Impala (optional)
sudo apt-get install libthrift-c-glib-dev thrift-compiler
# Trino/Phoenix/Pinot/Druid (optional)
sudo apt-get install libcurl4-openssl-dev libjson-glib-dev
# MySQL-wire (optional)
sudo apt-get install libmariadb-dev

cmake -B build && cmake --build build
cd build && ctest --output-on-failure -L unit
```

**Windows (MSYS2 UCRT64, what the CI builds):**
```bash
pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cmake \
          mingw-w64-ucrt-x86_64-ninja mingw-w64-ucrt-x86_64-pkgconf \
          mingw-w64-ucrt-x86_64-glib2 mingw-w64-ucrt-x86_64-curl \
          mingw-w64-ucrt-x86_64-json-glib mingw-w64-ucrt-x86_64-openssl \
          mingw-w64-ucrt-x86_64-glib-networking mingw-w64-ucrt-x86_64-libmariadbclient \
          mingw-w64-ucrt-x86_64-thrift mingw-w64-ucrt-x86_64-cmocka
# thrift c_glib runtime (MSYS2 only ships the compiler):
bash scripts/build-thrift-c-glib.sh "$PWD/thrift-c-glib-prefix" 0.23.0
PKG_CONFIG_PATH="$PWD/thrift-c-glib-prefix/lib/pkgconfig" \
    cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

**macOS:** Homebrew `unixodbc glib json-glib curl` then the same CMake invocation.

## Connection String Parameters

### Basic Connection
```
HOST=localhost;PORT=10000;UID=myuser;PWD=mypass;DATABASE=default;BACKEND=hive
```

### Common Parameters

| Parameter | Description | Example | Default |
|-----------|-------------|---------|---------|
| **HOST** / SERVER | Server hostname | `localhost` | `localhost` |
| **PORT** | Server port | `10000` | Backend-specific |
| **UID** / USERNAME | Username | `admin` | `` |
| **PWD** / PASSWORD | Password | `secret` | `` |
| **DATABASE** / SCHEMA | Database name | `mydb` | `default` |
| **BACKEND** | `hive`, `impala`, `trino`, `phoenix`, `pinot`, `druid`, `bigquery`, `mysql`, `flightsql`, `kudu` | `trino` | `hive` |
| **SSL** / UseSSL | Enable SSL | `1`, `true` | `false` |
| **SSLCertFile** | Client certificate | `/path/cert.pem` | - |
| **SSLKeyFile** | Client key | `/path/key.pem` | - |
| **SSLCAFile** | CA certificate | `/path/ca.pem` | - |
| **SSLVerify** | Verify server cert | `1`, `true` | `true` |
| **AUTHMECH** | Auth mechanism (backend-specific) | `LDAP`, `JWT`, `OAUTH2` | - |
| **LogLevel** | Log level (0-6) | `5` (DEBUG) | `0` |
| **LogFile** | Log file path | `/tmp/argus.log` | stderr |
| **ConnectTimeout** | Connection timeout (sec) | `30` | `0` |
| **QueryTimeout** | Query timeout (sec) | `300` | `0` |
| **SocketTimeout** | Socket timeout (sec) | `60` | `0` |
| **RetryCount** | Retry attempts | `3` | `0` |
| **RetryDelay** | Delay between retries (sec) | `2` | `2` |
| **ApplicationName** | App identifier | `MyApp` | - |
| **FetchBufferSize** | Rows per fetch | `5000` | `1000` |

The complete list (OAuth2 endpoints, HTTP transport, spooling, ...) is in [docs/CONFIGURATION.md](docs/CONFIGURATION.md); ready-made strings per engine are in [CONNECTION_EXAMPLES.md](CONNECTION_EXAMPLES.md).

## BI tools

Most BI tools need nothing but the driver and a DSN — the driver translates the
ODBC escape sequences (`{fn ...}`, `{ts ...}`, `{oj ...}`) that Tableau, Excel,
Qlik and Alteryx generate, per backend dialect. Two tools get a packaged
connector, attached to each [release](https://github.com/VargaFoundation/argus/releases):

| Tool | Artifact | Why |
|---|---|---|
| Power BI Desktop / Service | [`Argus.mez` / `.pqx`](connectors/powerbi) | Generic ODBC cannot fold `LIMIT`/top-N in Power Query |
| Tableau Desktop / Server | [`argus-*.taco`](connectors/tableau) | Named connector: fixed dialect, publishable, supported |

Excel cannot load Power Query custom connectors (Power BI Desktop only) and uses
a DSN. DBeaver, Superset, Metabase and Looker are JDBC/SQLAlchemy and out of
scope for an ODBC driver.

[docs/BI_TOOLS.md](docs/BI_TOOLS.md) has the full matrix and the per-tool setup.

## Windows

The [NSIS installer](installer/) registers the driver and bundles its DLLs. The driver reads DSNs from the registry (user DSNs first, then system). The setup has no configuration dialog, so create DSNs from PowerShell:

```powershell
Add-OdbcDsn -Name MyTrino -DriverName "Argus ODBC Driver" -Platform 64-bit `
    -SetPropertyValue @("BACKEND=trino", "HOST=trino.example.com", "PORT=8443", "SSL=1")
```

or use DSN-less connection strings:

```
DRIVER={Argus ODBC Driver};BACKEND=trino;HOST=trino.example.com;PORT=8443;SSL=1
```

## Usage Examples

### Secure Connection with SSL (Trino)
```c
const char *conn_str =
    "HOST=trino.example.com;PORT=8443;UID=admin;"
    "SSL=1;SSLCAFile=/etc/ssl/certs/ca.pem;SSLVerify=1;"
    "BACKEND=trino;LogLevel=5;LogFile=/var/log/argus.log";

SQLDriverConnect(dbc, NULL, (SQLCHAR*)conn_str, SQL_NTS,
                 NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
```

### Connection with Retry and Timeout
```c
const char *conn_str =
    "HOST=impala-server;PORT=21050;UID=user;"
    "BACKEND=impala;RetryCount=3;RetryDelay=2;"
    "ConnectTimeout=30;QueryTimeout=300;SocketTimeout=60;"
    "ApplicationName=DataPipeline";

SQLDriverConnect(dbc, NULL, (SQLCHAR*)conn_str, SQL_NTS,
                 NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
```

### Query Execution with Cancel
```c
SQLHSTMT stmt;
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

/* Execute in background thread */
SQLExecDirect(stmt, (SQLCHAR*)"SELECT * FROM huge_table", SQL_NTS);

/* Cancel from another thread */
SQLCancel(stmt);

SQLFreeHandle(SQL_HANDLE_STMT, stmt);
```

## Logging

Enable debug logging:

```bash
export ARGUS_LOG_LEVEL=6
export ARGUS_LOG_FILE=/tmp/argus-debug.log
```

Log levels: 0=OFF, 1=FATAL, 2=ERROR, 3=WARN, 4=INFO, 5=DEBUG, 6=TRACE

## Telemetry & privacy

Argus supports **opt-in, anonymous** usage telemetry — it is **off by default**
and never phones home unless you enable it (`TELEMETRY=1` per connection or
`ARGUS_TELEMETRY=1` machine-wide; `ARGUS_TELEMETRY=0` disables it hard, and
`-DARGUS_ENABLE_TELEMETRY=OFF` compiles it out). Only non-identifying signals are
sent (backend, latencies, OS, SQLSTATE codes) — never hostnames, credentials,
database/table names, or query text. See [docs/TELEMETRY.md](docs/TELEMETRY.md)
and [PRIVACY.md](PRIVACY.md).

## Troubleshooting

### Connection Fails
1. Enable logging: `LogLevel=6;LogFile=/tmp/argus.log`
2. Check firewall rules
3. Verify credentials
4. Try retry: `RetryCount=3;RetryDelay=2`

### SSL Errors
1. Verify certificate paths
2. Check certificate validity
3. Try `SSLVerify=0` (testing only!)

### Query Hangs
1. Set timeout: `QueryTimeout=300`
2. Use SQLCancel from another thread

## Version History

See [CHANGELOG.md](CHANGELOG.md).
