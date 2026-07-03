# Argus ODBC Driver

[![CI](https://github.com/VargaFoundation/argus/actions/workflows/ci.yml/badge.svg)](https://github.com/VargaFoundation/argus/actions/workflows/ci.yml)
[![CodeQL](https://github.com/VargaFoundation/argus/actions/workflows/codeql.yml/badge.svg)](https://github.com/VargaFoundation/argus/actions/workflows/codeql.yml)

Multi-backend ODBC driver for analytics engines — Hive, Impala, Trino, Phoenix, Pinot, Druid, Kudu, MySQL-wire (StarRocks/Doris/ClickHouse) and Arrow Flight SQL (Dremio/InfluxDB 3) — with comprehensive logging, SSL/TLS, OAuth2 and an Arrow ADBC surface built over the same stack.

## Features

### Core ODBC Support
- **99 ODBC entry points** (68 ANSI + 31 Unicode `W` variants) — ODBC 3.x API Level 1, plus ODBC 2.x compatibility (`SQLAllocConnect`, `SQLError`, `SQLExtendedFetch`, ...)
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
| `mysql` | StarRocks / Doris / ClickHouse / MySQL / MariaDB | MySQL wire | libmariadb | no |
| `flightsql` | Dremio / InfluxDB 3 / any Arrow Flight SQL server | gRPC / Arrow | arrow-flight-sql (C++) | no |
| `kudu` | Apache Kudu (deprecated — prefer `BACKEND=impala`) | kudu_client | libkudu_client | no |

The Windows installer ships Hive, Impala, Trino, Phoenix, Pinot, Druid and BigQuery; MySQL, Flight SQL and Kudu need dependencies MSYS2 does not provide. Hive/Impala speak through a GIO socket transport (portable, with timeouts and TLS); the installer bundles the glib-networking TLS backend, which the driver loads automatically.

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
- **Hive/Impala**: Thrift SSL sockets, HTTP transport with SPNEGO/Kerberos or Bearer/JWT tokens (Databricks personal access tokens)
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
