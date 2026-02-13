# Argus ODBC Driver

Production-ready ODBC driver for Hive, Impala, and Trino with comprehensive logging, SSL/TLS support, and enterprise features.

## Features

### Core ODBC Support
- **52 ODBC Functions** implemented (ODBC 3.x API Level 1)
- **Multiple Backends**: Apache Hive, Apache Impala, Trino  
- **Cross-Platform**: Linux and Windows support
- **Connection Pooling Ready**: Designed for production environments

### Production Features

#### Logging System
- **7 Log Levels**: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE
- **Thread-Safe**: Mutex-protected log operations
- **Flexible Output**: File-based or stderr
- **Configuration**:
  - Connection string: `LogLevel=5;LogFile=/tmp/argus.log`
  - Environment variables: `ARGUS_LOG_LEVEL`, `ARGUS_LOG_FILE`

#### SSL/TLS Support
- **Trino**: Full HTTPS support with certificate verification
  - SSL certificate, key, and CA configuration
  - Hostname verification toggle
- **Hive/Impala**: Thrift SSL sockets (requires OpenSSL)
  - CA certificate configuration
  - Transparent SSL negotiation

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
  - Trino: DELETE /v1/query/{id}
  - Hive/Impala: TCancelOperationReq via Thrift
- **Application Name**: Identify queries with custom app name
  - Trino: X-Trino-Source header
  - Hive: hive.query.source configuration

#### Fetch Optimization
- **Configurable Buffer Size**: `FetchBufferSize=5000`
- **Default Batch Size**: 1000 rows per fetch
- **Row Array Fetching**: Support for SQLSetStmtAttr row arrays

## Building

### Requirements

**Linux:**
\`\`\`bash
# Base dependencies
sudo apt-get install cmake build-essential
sudo apt-get install libodbc1 unixodbc-dev
sudo apt-get install libglib2.0-dev libgio2.0-dev

# Hive/Impala backend (optional)
sudo apt-get install libthrift-c-glib-dev

# Trino backend (optional)
sudo apt-get install libcurl4-openssl-dev libjson-glib-1.0-dev

# SSL support for Hive/Impala (optional)
sudo apt-get install libssl-dev
\`\`\`

**Windows:**
- Visual Studio 2019+ or MinGW-w64
- vcpkg for dependencies: \`glib\`, \`gio\`
- Optional: \`libcurl\`, \`json-glib-1.0\` for Trino

### Build Instructions

\`\`\`bash
cmake -B build
cmake --build build

# Run tests
cd build
ctest --output-on-failure -L unit
\`\`\`

## Connection String Parameters

### Basic Connection
\`\`\`
HOST=localhost;PORT=10000;UID=myuser;PWD=mypass;DATABASE=default;BACKEND=hive
\`\`\`

### All Parameters

| Parameter | Description | Example | Default |
|-----------|-------------|---------|---------|
| **HOST** / SERVER | Server hostname | \`localhost\` | \`localhost\` |
| **PORT** | Server port | \`10000\` | Backend-specific |
| **UID** / USERNAME | Username | \`admin\` | \`\` |
| **PWD** / PASSWORD | Password | \`secret\` | \`\` |
| **DATABASE** / SCHEMA | Database name | \`mydb\` | \`default\` |
| **BACKEND** | Backend type | \`hive\`, \`impala\`, \`trino\` | \`hive\` |
| **SSL** / UseSSL | Enable SSL | \`1\`, \`true\` | \`false\` |
| **SSLCertFile** | Client certificate | \`/path/cert.pem\` | - |
| **SSLKeyFile** | Client key | \`/path/key.pem\` | - |
| **SSLCAFile** | CA certificate | \`/path/ca.pem\` | - |
| **SSLVerify** | Verify server cert | \`1\`, \`true\` | \`true\` |
| **LogLevel** | Log level (0-6) | \`5\` (DEBUG) | \`0\` |
| **LogFile** | Log file path | \`/tmp/argus.log\` | stderr |
| **ConnectTimeout** | Connection timeout (sec) | \`30\` | \`0\` |
| **QueryTimeout** | Query timeout (sec) | \`300\` | \`0\` |
| **SocketTimeout** | Socket timeout (sec) | \`60\` | \`0\` |
| **RetryCount** | Retry attempts | \`3\` | \`0\` |
| **RetryDelay** | Delay between retries (sec) | \`2\` | \`2\` |
| **ApplicationName** | App identifier | \`MyApp\` | - |
| **FetchBufferSize** | Rows per fetch | \`5000\` | \`1000\` |

## Usage Examples

### Secure Connection with SSL (Trino)
\`\`\`c
const char *conn_str =
    "HOST=trino.example.com;PORT=8443;UID=admin;"
    "SSL=1;SSLCAFile=/etc/ssl/certs/ca.pem;SSLVerify=1;"
    "BACKEND=trino;LogLevel=5;LogFile=/var/log/argus.log";

SQLDriverConnect(dbc, NULL, (SQLCHAR*)conn_str, SQL_NTS, 
                 NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
\`\`\`

### Connection with Retry and Timeout
\`\`\`c
const char *conn_str =
    "HOST=impala-server;PORT=21050;UID=user;"
    "BACKEND=impala;RetryCount=3;RetryDelay=2;"
    "ConnectTimeout=30;QueryTimeout=300;SocketTimeout=60;"
    "ApplicationName=DataPipeline";

SQLDriverConnect(dbc, NULL, (SQLCHAR*)conn_str, SQL_NTS,
                 NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
\`\`\`

### Query Execution with Cancel
\`\`\`c
SQLHSTMT stmt;
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

// Execute in background thread
SQLExecDirect(stmt, (SQLCHAR*)"SELECT * FROM huge_table", SQL_NTS);

// Cancel from another thread
SQLCancel(stmt);

SQLFreeHandle(SQL_HANDLE_STMT, stmt);
\`\`\`

## Logging

Enable debug logging:

\`\`\`bash
export ARGUS_LOG_LEVEL=6
export ARGUS_LOG_FILE=/tmp/argus-debug.log
\`\`\`

Log levels: 0=OFF, 1=FATAL, 2=ERROR, 3=WARN, 4=INFO, 5=DEBUG, 6=TRACE

## Troubleshooting

### Connection Fails
1. Enable logging: \`LogLevel=6;LogFile=/tmp/argus.log\`
2. Check firewall rules
3. Verify credentials
4. Try retry: \`RetryCount=3;RetryDelay=2\`

### SSL Errors
1. Verify certificate paths
2. Check certificate validity
3. Try \`SSLVerify=0\` (testing only!)

### Query Hangs
1. Set timeout: \`QueryTimeout=300\`
2. Use SQLCancel from another thread

## Version History

### v0.2.0 (Current)
- ✅ Production logging system
- ✅ SSL/TLS support
- ✅ Connection retry logic
- ✅ Query timeout enforcement
- ✅ SQLCancel implementation
- ✅ Extended data type conversions
- ✅ Application name support

### v0.1.0
- Initial release with 52 ODBC functions
- Hive, Impala, Trino backends
