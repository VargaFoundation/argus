# Architecture

Argus is a two-layer ODBC driver with a pluggable backend system.

## Layer 1: ODBC API Layer (`src/odbc/`)

This layer implements the ODBC specification. It handles:

- **Handle management** (`handle.c`): Environment, connection, and statement lifecycle with runtime signature checking
- **Connection** (`connect.c`): Connection string parsing, backend selection, and connection lifecycle
- **Execution** (`execute.c`): Statement preparation and execution dispatch
- **Fetching** (`fetch.c`): Batch row cache, column binding, and type conversion
- **Catalog** (`catalog.c`): SQLTables, SQLColumns, SQLGetTypeInfo dispatch to backend
- **Info** (`info.c`): 50+ SQLGetInfo types for BI tool compatibility (PowerBI, Tableau)
- **Diagnostics** (`diag.c`): SQLSTATE error reporting and diagnostic records
- **Attributes** (`attr.c`): Environment, connection, and statement attributes

The ODBC layer never talks to a database directly. It delegates all data operations to the backend layer through the vtable interface.

## Layer 2: Backend Abstraction (`src/backend/`)

Each backend implements the `argus_backend_t` vtable defined in `include/argus/backend.h`:

```c
typedef struct argus_backend {
    const char *name;
    int (*connect)(...);
    void (*disconnect)(...);
    int (*execute)(...);
    int (*fetch_results)(...);
    int (*get_result_metadata)(...);
    int (*get_tables)(...);
    int (*get_columns)(...);
    // ...
} argus_backend_t;
```

### Hive Backend (`src/backend/hive/`)

The Hive backend communicates with HiveServer2 using the TCLIService Thrift protocol:

- **hive_session.c**: OpenSession/CloseSession via Thrift (protocol V10)
- **hive_query.c**: ExecuteStatement, GetOperationStatus, CloseOperation
- **hive_fetch.c**: FetchResults with columnar TRowSet parsing
- **hive_metadata.c**: GetTables, GetColumns, GetSchemas, GetTypeInfo
- **hive_types.c**: Hive type -> ODBC SQL type mapping

### Impala Backend (`src/backend/impala/`)

The Impala backend uses the same TCLIService Thrift protocol as Hive with key differences:

- **impala_session.c**: OpenSession with protocol V6 (not V10), post-connect `USE <db>` statement
- **impala_query.c**: Same Thrift execution pattern as Hive
- **impala_fetch.c**: Same columnar TRowSet parsing as Hive
- **impala_metadata.c**: Same Thrift catalog operations as Hive
- **impala_types.c**: Same type mapping as Hive plus `REAL` type

### Trino Backend (`src/backend/trino/`)

The Trino backend uses a completely different protocol -- HTTP REST API with JSON:

- **trino_session.c**: HTTP client (libcurl) initialization, connectivity check
- **trino_query.c**: POST `/v1/statement`, DELETE `/v1/query/<id>` for cancel
- **trino_fetch.c**: GET `nextUri` polling, JSON data array parsing
- **trino_metadata.c**: Catalog operations via `information_schema` SQL queries
- **trino_types.c**: Trino type names (lowercase) -> ODBC SQL type mapping

## Data Flow

### Hive/Impala (Thrift)

```
1. App calls SQLExecDirect("SELECT * FROM t")
2. ODBC layer validates handle, stores query
3. ODBC calls backend->execute() -> hive_execute()
4. Hive backend sends TExecuteStatementReq via Thrift
5. App calls SQLFetch()
6. ODBC layer checks row cache, if empty:
   a. Calls backend->fetch_results() -> hive_fetch_results()
   b. Hive sends TFetchResultsReq, gets TRowSet (columnar)
   c. Parses columns into row cache (1000 rows)
7. ODBC layer reads from cache, converts to app's bound types
8. Returns SQL_SUCCESS (or SQL_NO_DATA when exhausted)
```

### Trino (REST)

```
1. App calls SQLExecDirect("SELECT * FROM t")
2. ODBC layer validates handle, stores query
3. ODBC calls backend->execute() -> trino_execute()
4. Trino backend POSTs to /v1/statement, gets JSON with nextUri
5. App calls SQLFetch()
6. ODBC layer checks row cache, if empty:
   a. Calls backend->fetch_results() -> trino_fetch_results()
   b. Trino GETs nextUri, gets JSON with data array
   c. Parses JSON arrays into row cache
7. ODBC layer reads from cache, converts to app's bound types
8. Returns SQL_SUCCESS (or SQL_NO_DATA when exhausted)
```

## Handle Hierarchy

```
SQLHENV (argus_env_t)
  +-- SQLHDBC (argus_dbc_t)
       |-- backend vtable pointer
       |-- backend connection handle (opaque)
       +-- SQLHSTMT (argus_stmt_t)
            |-- backend operation handle (opaque)
            |-- column descriptors
            |-- row cache (batch)
            +-- column bindings
```

Each handle has a 4-byte magic signature for runtime type checking, preventing invalid handle casts.

## Type Mapping

| Hive/Impala Type | Trino Type | ODBC SQL Type | C Default |
|------------------|------------|---------------|-----------|
| BOOLEAN | boolean | SQL_BIT | SQL_C_BIT |
| TINYINT | tinyint | SQL_TINYINT | SQL_C_TINYINT |
| SMALLINT | smallint | SQL_SMALLINT | SQL_C_SHORT |
| INT | integer | SQL_INTEGER | SQL_C_LONG |
| BIGINT | bigint | SQL_BIGINT | SQL_C_SBIGINT |
| FLOAT | real | SQL_FLOAT/SQL_REAL | SQL_C_FLOAT |
| DOUBLE | double | SQL_DOUBLE | SQL_C_DOUBLE |
| STRING/VARCHAR | varchar | SQL_VARCHAR | SQL_C_CHAR |
| TIMESTAMP | timestamp | SQL_TYPE_TIMESTAMP | SQL_C_CHAR |
| DATE | date | SQL_TYPE_DATE | SQL_C_CHAR |
| DECIMAL | decimal | SQL_DECIMAL | SQL_C_CHAR |
| BINARY | varbinary | SQL_BINARY/SQL_VARBINARY | SQL_C_CHAR (hex) |
| ARRAY/MAP/STRUCT | array/map/row | SQL_VARCHAR | SQL_C_CHAR (JSON) |

Complex types (ARRAY, MAP, STRUCT) are serialized as VARCHAR strings, matching the behavior of commercial ODBC drivers.

## Platform Support

Argus builds on Linux, macOS, and Windows:

- **Linux/macOS**: GCC/Clang, `__attribute__((constructor))` for initialization
- **Windows**: MinGW (MSYS2/UCRT64), `DllMain` for initialization
- **Portability**: `include/argus/compat.h` provides cross-platform macros for `strcasecmp`, `strdup`, `strtok_r`, `strndup`
