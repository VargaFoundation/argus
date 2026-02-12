# Adding New Backends

Argus uses a pluggable backend architecture. Each backend (Hive, Impala, Trino, Spark) implements a vtable of function pointers. The ODBC layer delegates all data operations through this interface.

## Backend Interface

The vtable is defined in `include/argus/backend.h`:

```c
typedef struct argus_backend {
    const char *name;  // e.g. "impala", "trino"

    // Connection lifecycle
    int (*connect)(argus_dbc_t *dbc,
                   const char *host, int port,
                   const char *username, const char *password,
                   const char *database, const char *auth_mechanism,
                   argus_backend_conn_t *out_conn);
    void (*disconnect)(argus_backend_conn_t conn);

    // Query execution
    int (*execute)(argus_backend_conn_t conn, const char *query,
                   argus_backend_op_t *out_op);
    int (*get_operation_status)(argus_backend_conn_t conn,
                                argus_backend_op_t op, bool *finished);
    void (*close_operation)(argus_backend_conn_t conn,
                            argus_backend_op_t op);

    // Result fetching
    int (*fetch_results)(argus_backend_conn_t conn,
                         argus_backend_op_t op, int max_rows,
                         argus_row_cache_t *cache,
                         argus_column_desc_t *columns, int *num_cols);
    int (*get_result_metadata)(argus_backend_conn_t conn,
                               argus_backend_op_t op,
                               argus_column_desc_t *columns, int *num_cols);

    // Catalog operations
    int (*get_tables)(...);
    int (*get_columns)(...);
    int (*get_type_info)(...);
    int (*get_schemas)(...);
    int (*get_catalogs)(...);
} argus_backend_t;
```

## Existing Backends

### Hive (`src/backend/hive/`)

- Protocol: Thrift TCLIService (binary), protocol V10
- Default port: 10000
- Database: Set via `use:database` config in OpenSession
- 7 files: internal header, backend, session, query, fetch, metadata, types

### Impala (`src/backend/impala/`)

- Protocol: Thrift TCLIService (binary), protocol V6
- Default port: 21050
- Database: Set via `USE <db>` statement after connect
- Same Thrift operations as Hive with minor protocol differences
- 7 files: same structure as Hive

### Trino (`src/backend/trino/`)

- Protocol: HTTP REST API with JSON (libcurl + json-glib)
- Default port: 8080
- Database: Maps to Trino catalog via X-Trino-Catalog header
- Catalog operations via `information_schema` SQL queries
- 7 files: same structure pattern

## Step-by-Step: Adding a New Backend

### 1. Create the directory

```
src/backend/mybackend/
    mybackend_internal.h  # Internal structs
    mybackend_backend.c   # Vtable + registration
    mybackend_session.c   # Connect/disconnect
    mybackend_query.c     # Execute/status/close
    mybackend_fetch.c     # Fetch results + metadata
    mybackend_metadata.c  # Catalog operations
    mybackend_types.c     # Type mapping
```

### 2. Implement the vtable

```c
// mybackend_backend.c
#include "argus/backend.h"

// Forward declarations
int mybackend_connect(...);
void mybackend_disconnect(...);
// ... etc

static const argus_backend_t mybackend = {
    .name = "mybackend",
    .connect = mybackend_connect,
    .disconnect = mybackend_disconnect,
    // ... all 13 function pointers
};

const argus_backend_t *argus_mybackend_backend_get(void) {
    return &mybackend;
}
```

### 3. Register the backend

In `src/backend/backend.c`, add:

```c
extern const argus_backend_t *argus_mybackend_backend_get(void);

void argus_backends_init(void) {
    argus_backend_register(argus_hive_backend_get());
    argus_backend_register(argus_impala_backend_get());
    argus_backend_register(argus_trino_backend_get());
    argus_backend_register(argus_mybackend_backend_get());  // NEW
}
```

### 4. Add to CMake

In `src/CMakeLists.txt`, add the new source files and include directories:

```cmake
set(ARGUS_SOURCES
    ...
    backend/mybackend/mybackend_backend.c
    backend/mybackend/mybackend_session.c
    backend/mybackend/mybackend_query.c
    backend/mybackend/mybackend_fetch.c
    backend/mybackend/mybackend_metadata.c
    backend/mybackend/mybackend_types.c
)
```

### 5. Use the backend

Users select the backend via connection string:

```
DRIVER=Argus;Backend=mybackend;HOST=my-host;PORT=12345;...
```

## Key Requirements

1. **connect()** must return 0 on success, -1 on failure. On failure, push diagnostics to `dbc->diag`.
2. **fetch_results()** fills `argus_row_cache_t` with string representations of cell values. The ODBC layer handles type conversion.
3. **get_result_metadata()** fills `argus_column_desc_t` array with column name, SQL type, size, etc.
4. All opaque handles (`argus_backend_conn_t`, `argus_backend_op_t`) are `void*` - cast to your backend's internal structs.
5. The backend owns the memory for its internal structs. `disconnect()` and `close_operation()` must free them.
6. Include `argus/compat.h` for cross-platform support (strcasecmp, strdup, strtok_r, strndup).

## Backend Differences

| Feature | Hive | Impala | Trino |
|---------|------|--------|-------|
| Protocol | Thrift (TCLIService) | Thrift (TCLIService) | REST (HTTP/JSON) |
| Default Port | 10000 | 21050 | 8080 |
| Protocol Version | V10 | V6 | N/A |
| Auth | NOSASL, PLAIN | NOSASL, PLAIN | HTTP headers |
| Catalog | Hive Metastore | Hive Metastore | information_schema |
| Dependencies | thrift_c_glib | thrift_c_glib | libcurl, json-glib |
