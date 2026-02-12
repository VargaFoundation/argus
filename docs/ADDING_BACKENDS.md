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

## Step-by-Step: Adding an Impala Backend

### 1. Create the directory

```
src/backend/impala/
    impala_backend.c    # Vtable + registration
    impala_session.c    # Connect/disconnect
    impala_query.c      # Execute/fetch
    impala_metadata.c   # Catalog operations
    impala_types.c      # Type mapping
```

### 2. Implement the vtable

```c
// impala_backend.c
#include "argus/backend.h"

// Forward declarations
int impala_connect(...);
void impala_disconnect(...);
// ... etc

static const argus_backend_t impala_backend = {
    .name = "impala",
    .connect = impala_connect,
    .disconnect = impala_disconnect,
    .execute = impala_execute,
    .get_operation_status = impala_get_operation_status,
    .close_operation = impala_close_operation,
    .fetch_results = impala_fetch_results,
    .get_result_metadata = impala_get_result_metadata,
    .get_tables = impala_get_tables,
    .get_columns = impala_get_columns,
    .get_type_info = impala_get_type_info,
    .get_schemas = impala_get_schemas,
    .get_catalogs = impala_get_catalogs,
};

const argus_backend_t *argus_impala_backend_get(void) {
    return &impala_backend;
}
```

### 3. Register the backend

In `src/backend/backend.c`, add:

```c
extern const argus_backend_t *argus_impala_backend_get(void);

void argus_backends_init(void) {
    argus_backend_register(argus_hive_backend_get());
    argus_backend_register(argus_impala_backend_get());  // NEW
}
```

### 4. Add to CMake

In `src/CMakeLists.txt`, add the new source files:

```cmake
set(ARGUS_SOURCES
    ...
    backend/impala/impala_backend.c
    backend/impala/impala_session.c
    backend/impala/impala_query.c
    backend/impala/impala_metadata.c
    backend/impala/impala_types.c
)
```

### 5. Use the backend

Users select the backend via connection string:

```
DRIVER=Argus;Backend=impala;HOST=impala-host;PORT=21050;...
```

## Key Requirements

1. **connect()** must return 0 on success, -1 on failure. On failure, push diagnostics to `dbc->diag`.
2. **fetch_results()** fills `argus_row_cache_t` with string representations of cell values. The ODBC layer handles type conversion.
3. **get_result_metadata()** fills `argus_column_desc_t` array with column name, SQL type, size, etc.
4. All opaque handles (`argus_backend_conn_t`, `argus_backend_op_t`) are `void*` - cast to your backend's internal structs.
5. The backend owns the memory for its internal structs. `disconnect()` and `close_operation()` must free them.

## Backend Differences

| Feature | Hive | Impala | Trino |
|---------|------|--------|-------|
| Protocol | Thrift (TCLIService) | Thrift (Beeswax/HS2) | REST (HTTP) |
| Default Port | 10000 | 21050 | 8080 |
| Auth | NOSASL, PLAIN, Kerberos | NOSASL, PLAIN, LDAP | Basic, JWT |
| Catalog | Hive Metastore | Hive Metastore | Connectors |
