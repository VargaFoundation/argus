# Architecture

How the driver is layered, where a query travels, and where to plug in. For
adding a backend, see `ADDING_BACKENDS.md`; for the Flight SQL specifics,
`FLIGHTSQL_DESIGN.md`; for the ADBC surface, `ADBC.md`.

```
  Application (Tableau, Power BI, Excel, custom code)
        │  ODBC 3.8 — 104 entry points (ANSI + W)
        ▼
  ┌──────────────────────── src/odbc/ ────────────────────────────┐
  │ handle.c   env/dbc/stmt/desc lifecycle, SQLFreeStmt           │
  │ connect.c  DSN + connection-string parsing, retry,            │
  │            multi-host, obs_hooks connect taps                 │
  │ execute.c  prepare/execute, param render, async worker, DAE   │
  │ escape.c   {fn}/{d}/{ts}/{oj}/{interval} → native grammar     │
  │ dialect.c  per-backend dialect registry (quote char, literal  │
  │            style, backslash escaping, verified fn maps)       │
  │ fetch.c    row delivery, block/scrollable cursors, row-wise & │
  │            column-wise binding, SQLGetData, type conversion   │
  │ catalog.c  SQLTables/Columns/TypeInfo/PrimaryKeys/Statistics  │
  │ attr.c     env/dbc/stmt attributes, SQLEndTran                │
  │ desc.c     explicit + implicit descriptors                    │
  │ diag.c     diagnostics (lazily-allocated records, per-handle  │
  │            locking), SQLGetDiagRec/Field, SQLError            │
  │ unicode.c  every W entry point, UTF-16LE ↔ UTF-8              │
  │ info.c     SQLGetInfo/SQLGetFunctions (derived from dialect)  │
  │ log.c      leveled logging   telemetry.c  opt-in telemetry    │
  │ obs_hooks.c weak no-op tap points (see below)                 │
  └──────────────┬────────────────────────────────────────────────┘
                 │  argus_backend_t vtable (include/argus/backend.h)
                 ▼
  ┌──────────────────────── src/backend/ ─────────────────────────┐
  │ hive/     HiveServer2 Thrift (binary + HTTP, SASL/Kerberos,   │
  │           also Spark Thrift Server & Flink SQL Gateway)       │
  │ impala/   Impala Thrift (shares the HS2 lineage with hive/)   │
  │ trino/    HTTP/JSON, DOM-free page decode, spooling, OAuth2   │
  │ phoenix/  Avatica JSON     pinot/  broker JSON                │
  │ druid/    router SQL JSON  bigquery/ REST/JSON (+S3NS)        │
  │ mysql/    MySQL wire via libmariadb (StarRocks/Doris/CH)      │
  │ flightsql/ Arrow Flight SQL (C++)   kudu/ (deprecated)        │
  │ shared: backend.c registry · thrift_gio_transport.c ·         │
  │         thrift_sasl.c (GSSAPI/SSPI) · http_client.c           │
  └───────────────────────────────────────────────────────────────┘

  src/adbc/argus_adbc.c — Arrow ADBC driver over the same stack
```

## The backend contract

`include/argus/backend.h` defines `argus_backend_t`: a name plus ~19 function
pointers (connect, execute, fetch_results, catalog getters, `cancel`,
`is_alive`, `get_last_error`, `get_server_version`, …). Registration is
compile-time (`#ifdef` per backend) into the registry in
`src/backend/backend.c`; lookup is by case-insensitive name from the
`BACKEND=` connection-string key.

Two contract rules matter more than the rest:

- **NULL beats a lie.** A backend that cannot implement a hook leaves it NULL
  and the ODBC layer reports "not supported" (`HYC00`); a no-op that returns
  success is a bug class this driver has been burned by (cancel, liveness).
- **`is_alive` answers `SQL_ATTR_CONNECTION_DEAD`**, which is what the
  Driver Manager's connection pool checks before handing a parked connection
  to the next borrower: it must be a real probe (Druid/Pinot ping their
  health endpoints; MySQL pings the wire) or document why a pointer check is
  genuinely sufficient (BigQuery: stateless REST). `reset_session` is the
  other half of that contract (`SQL_ATTR_RESET_CONNECTION`). The driver does
  not pool connections itself: pooling is the Driver Manager's job, and a
  driver-side pool keyed on host and user cannot tell a caller with the
  wrong password from the one who opened the session.

The row cache (`argus_row_cache_t`, `include/argus/types.h`) is the fetch
contract: backends deliver cells as strings (with an i64/f64 numeric
fast-path); `fetch.c` converts to the application's bound C types. This is the
known architectural ceiling for very large extracts — the planned columnar
path is tracked in `ROADMAP.md`.

## Dialect and escape translation

`dialect.c` is the single source of truth for what each engine's SQL looks
like: identifier quote character, temporal literal style, `{oj}` support,
**string-literal backslash escaping** (parameter rendering consults this), and
a per-engine scalar-function map. `SQLGetInfo`'s function bitmaps are derived
from the same map, so the driver can never advertise a function it cannot
translate. Unverified backends deliberately under-claim (3 functions) until
probed against a live server.

## Threading model

ODBC requires per-handle thread safety. The driver uses one `GMutex` per
dbc/stmt/env; `execute.c` and `fetch.c` lock around execution and delivery,
`diag.c` locks reads (`SQLGetDiagRec`/`SQLGetDiagField`/`SQLError`) against a
concurrent writer. Async execution (`SQL_AM_STATEMENT`) runs on a worker
thread; completion is published through an atomic + `g_thread_join` barrier.
The remaining unguarded attribute paths are being closed incrementally —
treat any new shared-state access as lock-required by default.

## Memory model

Handles are `calloc`'d. Diagnostics records and parameter-binding arrays are
**lazily allocated** and grown geometrically (a statement handle costs a few
hundred bytes until something actually errors or binds). Passwords use
`argus_secure_free`; telemetry never sees message text, only SQLSTATEs.

## The observability seam (`obs_hooks`)

`include/argus/obs_hooks.h` declares twelve tap points (connect, statement,
disconnect, secret resolution, token cache, fetch presets, statement guards,
a connection-admission gate, host pick/result, unload), defined as weak
no-ops in `src/odbc/obs_hooks.c`. In this Apache-2.0 build they do nothing;
an out-of-tree add-on may link strong definitions (see the README's
"Observability hooks" section for the disclosure). Signatures are primitives
only, so the driver never depends on external types. Note that
`__attribute__((weak))` override semantics are only guaranteed for
static/whole-archive linking on GCC/Clang — the seam is not a stable dynamic
ABI.

## Library lifecycle

`src/odbc/api_entry.c` owns process-wide setup and teardown (`curl_global_init`,
logging, the backend registry, telemetry) from the library constructor —
`DllMain` on Windows. Teardown is split in two because of how Driver Managers
unload a driver: they free the last environment handle first and
`dlclose()`/`FreeLibrary()` afterwards, and a Windows `DllMain` cannot wait
for a thread (it holds the loader lock the exiting thread needs too). So the
last `SQLFreeHandle(SQL_HANDLE_ENV)` is where the driver *quiesces*
(`include/argus/lifecycle.h`): it fires `argus_obs_hook_unload(1)` for an
add-on's threads and stops the telemetry sender, waiting a bounded time for
both — a POST in flight is aborted through libcurl's progress callback rather
than waited for. The destructor repeats that (without waiting under
`DllMain`) and only then releases libcurl and the log; if anything still
runs, the teardown is skipped rather than pulled out from under a thread.
Everything restarts lazily when the application allocates a new environment.

## Quality gates

- Unit tests (cmocka) link `argus_odbc_static` with no live engine.
- Integration tests run against real engines via
  `tests/integration/docker-compose.yml`; the default CI job runs a fast
  subset, the `integration-full` workflow-dispatch job runs everything.
- ASan/UBSan/LSan over the unit suite; CodeQL on every push.
- libFuzzer harnesses (`fuzz/`) cover the escape translator and
  connection-string parser; `ENABLE_FUZZING=ON` under Clang.
