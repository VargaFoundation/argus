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

- **hive_session.c**: OpenSession/CloseSession via Thrift
- **hive_query.c**: ExecuteStatement, GetOperationStatus, CloseOperation
- **hive_fetch.c**: FetchResults with columnar TRowSet parsing
- **hive_metadata.c**: GetTables, GetColumns, GetSchemas, GetTypeInfo
- **hive_types.c**: Hive type -> ODBC SQL type mapping

## Data Flow

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

## Handle Hierarchy

```
SQLHENV (argus_env_t)
  └─ SQLHDBC (argus_dbc_t)
       ├─ backend vtable pointer
       ├─ backend connection handle (opaque)
       └─ SQLHSTMT (argus_stmt_t)
            ├─ backend operation handle (opaque)
            ├─ column descriptors
            ├─ row cache (batch)
            └─ column bindings
```

Each handle has a 4-byte magic signature for runtime type checking, preventing invalid handle casts.

## Type Mapping

| Hive Type | ODBC SQL Type | C Default |
|-----------|---------------|-----------|
| BOOLEAN   | SQL_BIT       | SQL_C_BIT |
| TINYINT   | SQL_TINYINT   | SQL_C_TINYINT |
| SMALLINT  | SQL_SMALLINT  | SQL_C_SHORT |
| INT       | SQL_INTEGER   | SQL_C_LONG |
| BIGINT    | SQL_BIGINT    | SQL_C_SBIGINT |
| FLOAT     | SQL_FLOAT     | SQL_C_FLOAT |
| DOUBLE    | SQL_DOUBLE    | SQL_C_DOUBLE |
| STRING    | SQL_VARCHAR   | SQL_C_CHAR |
| TIMESTAMP | SQL_TYPE_TIMESTAMP | SQL_C_CHAR |
| DATE      | SQL_TYPE_DATE | SQL_C_CHAR |
| DECIMAL   | SQL_DECIMAL   | SQL_C_CHAR |
| BINARY    | SQL_BINARY    | SQL_C_CHAR (hex) |
| ARRAY/MAP/STRUCT | SQL_VARCHAR | SQL_C_CHAR (JSON-like) |

Complex types (ARRAY, MAP, STRUCT) are serialized as VARCHAR strings, matching the behavior of commercial Hive ODBC drivers.
