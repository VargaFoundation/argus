# Changelog

All notable changes to the Argus ODBC Driver project.

## [0.2.0] - 2025-02-13

### Added - Production Features

#### 1. Logging System
- Thread-safe logging with 7 levels (OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE)
- File-based or stderr output
- Configuration via connection string (`LogLevel`, `LogFile`) or environment variables
- Integrated throughout codebase (connect, execute, fetch, error paths)
- Platform-specific mutex (Windows CRITICAL_SECTION, Linux pthread_mutex)

**Files Modified:**
- `include/argus/log.h` (NEW)
- `src/odbc/log.c` (NEW)
- `src/odbc/api_entry.c` - Added init/cleanup
- `src/odbc/connect.c` - Added logging calls
- `src/odbc/execute.c` - Added logging calls
- `src/odbc/diag.c` - Added error logging
- `src/CMakeLists.txt` - Added log.c to sources

#### 2. Extended Connection Handle Fields
- Added 14 new fields to `argus_dbc_t`:
  - SSL: `ssl_enabled`, `ssl_cert_file`, `ssl_key_file`, `ssl_ca_file`, `ssl_verify`
  - Timeouts: `socket_timeout_sec`, `connect_timeout_sec`, `query_timeout_sec`
  - Retry: `retry_count`, `retry_delay_sec`
  - Other: `app_name`, `fetch_buffer_size`, `http_path`, `log_level`, `log_file`

**Files Modified:**
- `include/argus/handle.h` - Extended `argus_dbc_t` structure
- `src/odbc/handle.c` - Initialize/free new fields

#### 3. Connection String Parsing
- Added 18 new connection string parameters:
  - SSL: `SSL`, `SSLCertFile`, `SSLKeyFile`, `SSLCAFile`, `SSLVerify`
  - Logging: `LogLevel`, `LogFile`
  - Timeouts: `ConnectTimeout`, `QueryTimeout`, `SocketTimeout`
  - Retry: `RetryCount`, `RetryDelay`
  - Other: `ApplicationName`, `FetchBufferSize`, `HTTPPath`

**Files Modified:**
- `src/odbc/connect.c` - Parse and apply new parameters
- Added Windows-compatible `strcasecmp` macro

#### 4. Connection Retry Logic
- Automatic retry with configurable count and delay
- Clears diagnostics between attempts
- Detailed logging per retry attempt
- Sleep between retries (Windows/Linux compatible)

**Files Modified:**
- `src/odbc/connect.c` - Implement retry loop in `do_connect()`

#### 5. SSL/TLS Support - Trino
- HTTPS scheme when SSL enabled
- curl SSL options: `CURLOPT_SSL_VERIFYPEER`, `CURLOPT_SSL_VERIFYHOST`
- Certificate configuration: `CURLOPT_SSLCERT`, `CURLOPT_SSLKEY`, `CURLOPT_CAINFO`
- Applied to all HTTP helpers (POST, GET, DELETE)
- Timeout enforcement via curl options

**Files Modified:**
- `src/backend/trino/trino_internal.h` - Added SSL fields to `trino_conn_t`
- `src/backend/trino/trino_session.c` - SSL configuration and HTTPS support
- Added `trino_apply_curl_settings()` helper function

#### 6. SSL/TLS Support - Hive/Impala
- Thrift SSL sockets (`THRIFT_TYPE_SSL_SOCKET`)
- CA certificate configuration via `thrift_ssl_socket_set_ca_certificate()`
- Conditional compilation with `ARGUS_HAS_THRIFT_SSL` (requires OpenSSL)
- Socket timeout via `g_socket_set_timeout()`
- Graceful fallback when SSL not available

**Files Modified:**
- `src/backend/hive/hive_internal.h` - Conditional SSL includes
- `src/backend/hive/hive_session.c` - SSL socket creation and timeout
- `src/backend/impala/impala_internal.h` - Conditional SSL includes
- `src/backend/impala/impala_session.c` - SSL socket creation and timeout
- `CMakeLists.txt` - Added GIO2 dependency for `g_socket_set_timeout`
- `src/CMakeLists.txt` - Link GIO2 library

#### 7. Timeout Enforcement
- **Trino**: `CURLOPT_CONNECTTIMEOUT` and `CURLOPT_TIMEOUT`
- **Hive/Impala**: `g_socket_set_timeout()` on underlying GSocket
- **Fetch**: Uses `fetch_buffer_size` if set, otherwise `ARGUS_DEFAULT_BATCH_SIZE`

**Files Modified:**
- `src/backend/trino/trino_session.c` - Apply timeout to curl
- `src/backend/hive/hive_session.c` - Apply timeout to socket
- `src/backend/impala/impala_session.c` - Apply timeout to socket
- `src/odbc/fetch.c` - Use configurable batch size

#### 8. Extended Data Type Conversions
Added support for:
- **Date/Time**: `SQL_C_TYPE_DATE`, `SQL_C_TYPE_TIME`, `SQL_C_TYPE_TIMESTAMP`
  - Parse formats: "YYYY-MM-DD", "HH:MM:SS", "YYYY-MM-DD HH:MM:SS.fff"
- **Numeric**: `SQL_C_NUMERIC` with 128-bit little-endian representation
- **Unsigned**: `SQL_C_ULONG`, `SQL_C_USHORT`, `SQL_C_UTINYINT`, `SQL_C_UBIGINT`
- **Binary**: `SQL_C_BINARY` with proper truncation handling
- **Wide Char**: Improved `SQL_C_WCHAR` UTF-8 to UTF-16LE conversion

**Files Modified:**
- `src/odbc/fetch.c` - Added 9 new conversion cases in `convert_cell_to_target()`

#### 9. SQLCancel Implementation
- **Backend Interface**: Added `cancel()` to `argus_backend_t` vtable
- **Trino**: DELETE request to `/v1/query/{queryId}`
- **Hive**: `TCancelOperationReq` via Thrift
- **Impala**: `TCancelOperationReq` via Thrift
- **ODBC API**: Replaced stub with real implementation

**Files Modified:**
- `include/argus/backend.h` - Added `cancel()` to vtable
- `src/backend/trino/trino_internal.h` - Added `trino_cancel()` declaration
- `src/backend/trino/trino_query.c` - Implemented `trino_cancel()`
- `src/backend/trino/trino_backend.c` - Added to vtable
- `src/backend/hive/hive_internal.h` - Added `hive_cancel()` declaration
- `src/backend/hive/hive_query.c` - Implemented `hive_cancel()`
- `src/backend/hive/hive_backend.c` - Added to vtable
- `src/backend/impala/impala_internal.h` - Added `impala_cancel()` declaration
- `src/backend/impala/impala_query.c` - Implemented `impala_cancel()`
- `src/backend/impala/impala_backend.c` - Added to vtable
- `src/odbc/execute.c` - Replaced stub with real `SQLCancel()` implementation

#### 10. Application Name Support
- **Trino**: `X-Trino-Source` HTTP header
- **Hive**: `hive.query.source` session configuration
- Configured via `ApplicationName` or `AppName` connection string parameter

**Files Modified:**
- `src/backend/trino/trino_session.c` - Add header if app_name set
- `src/backend/hive/hive_session.c` - Add session config if app_name set

#### 11. Backend-Aware SQLGetInfo
- `SQL_DBMS_NAME` now returns backend-specific names:
  - "Apache Hive" for hive backend
  - "Apache Impala" for impala backend
  - "Trino" for trino backend

**Files Modified:**
- `src/odbc/info.c` - Dynamic DBMS name based on `dbc->backend->name`
- `tests/unit/test_info.c` - Updated test to initialize backend

### Testing
- All 6 existing unit tests pass
- Added backend initialization to test_info.c
- Verified 52 ODBC function exports maintained

### Build System
- Added GIO2 dependency for socket timeout support
- Conditional SSL support (requires OpenSSL headers)
- Cross-platform sleep macro (Windows/Linux)

### Documentation
- Created comprehensive README.md with:
  - Feature documentation
  - Connection string parameter reference
  - Usage examples
  - Troubleshooting guide
- Created CHANGELOG.md (this file)

## [0.1.0] - Initial Release

### Added
- 52 ODBC 3.x functions implemented
- Hive backend (Thrift C GLib)
- Impala backend (Thrift C GLib)
- Trino backend (libcurl + json-glib)
- Basic connection management
- Query execution and result fetching
- Catalog operations (SQLTables, SQLColumns, SQLGetTypeInfo)
- Linux and Windows support
- Unit test suite
- CI/CD pipelines

### Known Limitations
- No SSL/TLS support
- No logging system
- No timeout enforcement
- Limited data type conversions
- No query cancellation
- No connection retry logic
