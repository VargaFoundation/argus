# Changelog

All notable changes to the Argus ODBC Driver project.

## [Unreleased]

### Fixed: the Tableau connectors were never packageable
The `build-tableau-connector` CI job has failed since it was added, so no
`.taco` has ever been produced — the release workflow builds only the Power BI
`.mez`. Four separate faults, each hiding the next:

- `build.ps1` pinned `SdkRef` to `v2024.2.0`, a tag that does not exist:
  `tableau/connector-plugin-sdk` publishes `tdvt-*` tags (plus `v1.4*` and
  `2020.1`) and has never had a `v2024.x`. Now pinned to `tdvt-2.13.7`.
- Every connector declared `<field name="database">`. Tableau restricts
  connection-field names to a fixed platform list plus a `v-` vendor prefix,
  and `database` is in neither, so the packager rejected all eight. The field
  was redundant anyway: each `.tdr` already lists `<attr>dbname</attr>` and
  each `connectionBuilder.js` already reads `connectionHelper.attributeDatabase`,
  so Tableau renders its own Database input — exactly as the SDK's own
  `postgres_odbc` sample does. The field is gone; the connection attribute is
  unchanged.
- The genuinely vendor-specific fields — `krbservicename` and `krbhostfqdn` on
  Hive and Impala, `project`, `location`, `keyfile`, `accesstoken`, `endpoint`
  and `tokenendpoint` on BigQuery — now carry the required `v-` prefix, in the
  field list, the `.tdr` attribute list and the connection builder together.
- The current packager no longer accepts `--package-only` or the `-a`/`-ks`
  signing flags, and names its output from the manifest
  (`argus_postgres-v0.1.0.taco`) rather than from the directory. `build.ps1`
  now packages into a scratch directory and takes whatever single file appears,
  and signs with `jarsigner` itself, mirroring what `ci.yml` already did.

Verified by running the CI command itself under PowerShell 7.4.6: all eight
connectors package, and `validate-xml.py` still reports 8/8 against Tableau's
XSDs.

### PostgreSQL family: per-connection options and the last ODBC gaps
- **`SHOWPARTITIONS`, `SHOWALLDATABASES`, `ROWVERSIONING`, `SSLMODE` and
  `SEARCHPATH` are connection-string and DSN keys**, not environment variables.
  A process routinely holds connections to several servers, and a machine-wide
  switch cannot say "show partition children on staging but not on production".
  The `ARGUS_PG_*` variables remain as a fallback. `SSLMODE` is handed to libpq
  verbatim, which is the only way to express `prefer` or `verify-ca`.
- **Fixed: `SQLTables`' enumeration forms never worked.**
  `SQLTables("%", "", "")` (list catalogs) and `SQLTables("", "%", "")` (list
  schemas) fell through to `get_tables`, so asking for the schema list returned
  the table list. Every backend has implemented `get_catalogs` and
  `get_schemas` since the vtable was written and nothing called them. Power BI's
  hierarchical navigator and Tableau's schema picker both open with one of these.
- **`SQLRowCount` after DML** (new optional `get_affected_rows` hook): INSERT,
  UPDATE and DELETE report rows affected instead of -1. DDL stays -1, which ODBC
  defines as "not available" and is not the same as 0. Backends without the hook
  are unchanged.
- **`{call f(a)}` is translated** to `SELECT * FROM f(a)` — what psqlODBC
  generates and what an ODBC application means, since the thing that returns a
  result set in PostgreSQL is a function. `{?= call …}` is accepted too. Driven
  by a new `call_tmpl` field on the dialect, and `SQL_PROCEDURES` is now
  *derived* from it rather than from a separate flag, so it cannot claim the
  invocation syntax works when the dialect cannot render it. Backends without a
  template still reject `{call}` with HYC00.
- **Domains and enums are resolved.** A column over
  `CREATE DOMAIN postcode AS varchar(10)` reports SQL_VARCHAR size 10 rather
  than an unbounded string, and takes the same native fast path its base type
  would; an enum reports a bounded string, since PostgreSQL caps labels at 63
  bytes. The map is built in one query at connect, because in streaming mode the
  connection is busy exactly when an unknown OID first appears.

### BI connectors
- **Power BI**: `postgres`, `greenplum` and `cloudberry` added to the connector's
  backend list. They stay out of `AnsiOffsetBackends` — PostgreSQL has accepted
  ANSI `OFFSET…FETCH` since 8.4 and either form folds, but `LIMIT…OFFSET` is what
  a user recognises in `pg_stat_activity`, is safe on Greenplum 6's
  PostgreSQL 9.4-era planner, and is the better-exercised Power Query path.
  `FractionalSecondsScale` becomes per-backend: 6 for the PostgreSQL family,
  because reporting 3 against a `timestamp(6)` column silently truncates a
  pushed-down predicate and folds to the wrong rows.
- **Tableau**: three new connectors, `argus-postgres`, `argus-greenplum` and
  `argus-cloudberry`. They are the only Argus connectors that enable both
  namespace levels and that do **not** suppress transactions, `SQLStatistics` or
  `SQLForeignKeys` — those are real on these backends, and the last two are how
  Tableau discovers join keys and cardinality. Base dialect is
  `PostgreSQL91Dialect`, deliberately not newer: Greenplum 6 is PostgreSQL 9.4.
  Temporary tables are left disabled despite PostgreSQL supporting them, because
  the customization could not be run through TDVT here and an untested "yes"
  breaks a workbook at refresh time. All eight connectors validate against
  Tableau's XSDs.

### Fixed
- **Memory leak in every backend's fetch path.** `argus_row_cache_clear()` kept
  the row array on the theory that the next batch would reuse it, but no
  backend ever did: each assigns `cache->rows` unconditionally on entry to
  `fetch_results`, so the array was overwritten and leaked — one per batch,
  8 KB at the default `FetchBufferSize`, ~8 MB per million rows. Invisible on a
  single-batch result and steady growth in a long-lived BI process. The cache
  now owns and releases its row array, which makes all thirteen backends
  correct without touching any of them; the ones that grow the array with
  realloc see NULL/0 and allocate. Found by running the PostgreSQL integration
  suite under AddressSanitizer, and audited across every backend before the
  shared helper was changed. Also frees `cursor_name` in the wide-API test's
  fake statement, so the whole suite is ASan-clean.
- **Memory leak in the PostgreSQL fetch path.** `pg_fetch_results` allocated a
  fresh row array on every call, but `argus_row_cache_clear()` deliberately
  keeps the array it already has — so every batch after the first leaked one
  array (8 KB at the default `FetchBufferSize`, ~8 MB per million rows). Found
  the same root cause as above, fixed first locally and then centrally.
- `SQL_ATTR_TXN_ISOLATION` on a backend with no isolation hook fell off the end
  of `SQLSetConnectAttr` instead of returning HY092.

### Performance, measured
- **PostgreSQL fetch measured against psqlODBC** on the same server, query and
  client loop: **727 k rows/s vs 391 k** (1.5 M rows × 9 columns), with peak RSS
  **50 MB vs 629 MB**. psqlODBC reaches bounded memory only with
  `UseDeclareFetch=1`, which is off by default; there it is 352–378 k rows/s.
- **A planned `COPY … (FORMAT binary)` fetch path was measured and then not
  built.** Against raw libpq it is 9% faster than the single-row mode already in
  use (1 289 k vs 1 179 k rows/s) and 33% *larger* on the wire for a typical
  schema, while the driver's own gap to that ceiling — the row cache and ODBC
  conversion — is untouched by it. Nine percent of the protocol half of the cost
  does not justify NBASE numeric decoding and non-tuple-aligned COPY buffers.
  The numbers, the method and the conditions under which it becomes worth
  revisiting are in `tests/bench/README.md`.
- `FetchBufferSize` swept 100/1 000/10 000/50 000: the default 1 000 is already
  the best, so there is no tuning advice to give.

### ODBC-layer extensions (per-backend capabilities, transactions, catalog)
- **`argus_backend_caps_t`** (`include/argus/caps.h`): SQLGetInfo answers that
  are properties of the engine — transaction support, what a schema is called,
  identifier length, SQL conformance — become per-backend instead of constants.
  Every zero or NULL field means the value SQLGetInfo returned before, so a
  backend that declares nothing is answered exactly as it was;
  `tests/unit/test_backend_caps.c` walks every registered backend and asserts
  it, field by field.
- **Real transactions for the PostgreSQL family.** `SQL_TXN_CAPABLE` reports
  `SQL_TC_ALL`, `SQL_ATTR_AUTOCOMMIT` and `SQLEndTran` do real work, and
  `SQL_ATTR_TXN_ISOLATION` is honoured. `BEGIN` is issued lazily with the first
  statement, so the driver never manufactures an idle-in-transaction session.
  The other ten backends keep `SQL_TC_NONE` and a no-op `SQLEndTran`.
- **Pooled connections are reset before reuse** (new `reset_session` hook):
  rollback plus `DISCARD ALL`, and a connection that cannot be cleaned is
  discarded rather than parked. Without this a connection returned
  mid-transaction poisons the next borrower with held locks and an aborted
  transaction — the failure mode that makes transaction support dangerous
  rather than merely incomplete.
- **`SQLForeignKeys`, `SQLProcedures`, `SQLProcedureColumns`,
  `SQLTablePrivileges`, `SQLColumnPrivileges` and `SQLSpecialColumns` are real**
  for PostgreSQL/Greenplum/Cloudberry, via six new optional vtable hooks. A
  NULL hook still returns the correctly-shaped empty result set, which stays the
  right answer for engines that have no such objects. `SQL_BEST_ROWID` uses the
  primary key or a fully-NOT-NULL unique index and deliberately does **not**
  fall back to `ctid`, which UPDATE and VACUUM FULL invalidate.
- **Fixed: `SQLFetch` after an empty catalog call failed on a connected
  statement.** `SQLForeignKeys`, `SQLProcedures`, `SQLProcedureColumns` and the
  two privilege calls never set `fetch_started`, so the fetch path went to the
  backend with a NULL operation handle and returned SQL_ERROR instead of
  SQL_NO_DATA. Any BI tool that called one of them and then fetched hit it.
- **Real `SQLDescribeParam`** for the PostgreSQL family (new `describe_params`
  hook): a server-side Parse and Describe reports the parameter types
  PostgreSQL inferred. Metadata only — execution still renders parameters as
  literals. `SQLNumParams` prefers the server's count. `SQL_DESCRIBE_PARAMETER`
  is `"Y"` only where the hook exists.
- **Server SQLSTATEs at statement level** (new `get_last_error_ex` hook):
  `SELECT * FROM missing` now reports `42P01`, a unique violation `23505`, a
  cancelled statement `HY008`. Backends without the hook keep reporting HY000.
- `SQLDriverConnect` returns **`SQL_SUCCESS_WITH_INFO`** when a successful
  connect left diagnostics, instead of dropping them.

### Greenplum and Apache Cloudberry backends (`BACKEND=greenplum` / `cloudberry`)
- **Two MPP backends over the same libpq core**, with their own vtables,
  dialect entries and `SQL_DBMS_NAME` so a BI tool can name the engine it is
  talking to.
- **Partition children are hidden from `SQLTables`/`SQLColumns` on both catalog
  layouts** — Greenplum 6's inheritance plus `pg_partition_rule`, and Greenplum
  7 / Cloudberry's declarative partitioning. A warehouse with a few hundred
  monthly-partitioned fact tables is tens of thousands of child relations that a
  driver filtering on `relkind` alone puts in the connection dialog.
- **`REMARKS` carries the distribution policy, append-optimized storage and
  external-table location** — `[DISTRIBUTED BY (customer_id)]`, `[AO column]`,
  `[external: gpfdist]`. Both BI tools display it as the table description.
- **Catalog SQL is chosen by probing the server, not by its version string.**
  One connect-time query asks whether `gp_distribution_policy`, `pg_appendonly`,
  `pg_exttable` and `pg_class.relispartition` exist, so a catalog call can never
  fail with "relation gp_… does not exist" and pointing `BACKEND=greenplum` at a
  plain PostgreSQL degrades to PostgreSQL behaviour with a `01000` warning
  rather than breaking `SQLTables`.
- `SQLDriverConnect` now returns **`SQL_SUCCESS_WITH_INFO`** when a successful
  connect left diagnostics, instead of swallowing them behind `SQL_SUCCESS`.
  Scoped to records the successful attempt itself produced, so failover still
  reports plain success.
- **Verification status, stated plainly:** neither engine has a maintained
  public container image, so the MPP catalog SQL has not been run against a real
  cluster. It is exercised against a *simulated* Greenplum catalog built on
  PostgreSQL (`tests/integration/test_pg_mpp_sim.c` — same relation and column
  names and types), which proves the SQL parses and the logic holds, including
  the int2vector `distkey` decoding and declaration-order distribution keys. The
  dialect tables are inherited from PostgreSQL and marked not-live-verified in
  the header of `src/odbc/dialect.c`. Compose services exist behind
  `--profile greenplum` / `--profile cloudberry`.

### PostgreSQL backend (`BACKEND=postgres`)
- **New backend over the PostgreSQL wire protocol** (libpq), auto-detected at
  configure time from `libpq-dev`. Shares a core (`src/backend/pgcommon/`) with
  the Greenplum and Cloudberry backends that build on it.
- **Streaming fetch.** Rows are read in bounded chunks rather than materialised
  in client memory before the first one is visible, so peak memory follows
  `FetchBufferSize` and not the size of the answer. Numeric columns use the row
  cache's native typed path, skipping the value→text→value round trip.
- **Real `SQLCancel`** — libpq's out-of-band cancel request stops the statement
  server-side. The other synchronous backends can only return success.
- **Server SQLSTATEs.** `PG_DIAG_SQLSTATE` is passed through instead of being
  collapsed to a driver-invented code, so a missing relation surfaces as
  PostgreSQL's own `42P01`.
- **Catalog from `pg_catalog`,** with the full catalog/schema/table namespace and
  every application-supplied filter escaped through `PQescapeLiteral` rather
  than interpolated. `SQLTables`, `SQLColumns`, `SQLPrimaryKeys`,
  `SQLStatistics`, `SQLGetTypeInfo` and the schema/catalog lists are all real.
- **Partition and inheritance children are hidden** from `SQLTables` and
  `SQLColumns` (`ARGUS_PG_SHOW_PARTITIONS=1` restores them). A ten-year monthly
  partitioned table is one row in a BI navigator instead of 120 — the case where
  generic PostgreSQL ODBC makes a connection dialog unusable.
- **atttypmod is decoded**, so `varchar(20)` reports column size 20 and
  `numeric(12,3)` reports precision 12 / scale 3 rather than driver defaults.
- **PostgreSQL dialect** with every `{fn …}` entry executed against a live
  PostgreSQL 16 and its value checked
  (`tests/integration/test_postgres_escapes.c`). `ROUND`/`TRUNCATE` cast to
  `numeric` because PostgreSQL has no two-argument form for `double precision`;
  `WEEK` is deliberately not advertised because `extract(week)` is ISO-8601 and
  would return wrong week numbers for ODBC's definition.
- TLS maps `SSL`/`SSLVerify` onto `sslmode` (`disable` / `require` /
  `verify-full`); `AUTHMECH=KERBEROS` uses libpq's own GSSAPI/SSPI;
  `QueryTimeout` becomes a server-side `statement_timeout`.
- An absent `DATABASE` connects to `postgres` rather than failing on the
  literal `default` the ODBC layer substitutes.
- `ARGUS_MAX_BACKENDS` raised to 24, and a backend dropped for want of a slot is
  now logged instead of vanishing silently.
- Tests: `tests/unit/test_postgres_types.c`,
  `tests/integration/test_postgres_{connect,query,escapes}.c`, a `postgres`
  compose service and its seed, and `libpq` added to all three CI platforms.
  `tests/integration/test_bi_escapes.c` gained `BI_UID`/`BI_PWD`/`BI_TABLE`/
  `BI_TABLE2`/`BI_JOIN_COL`/`BI_TEXT_COL`/`BI_TEXT_VAL` so the shared probe runs
  on any engine (defaults unchanged, so Trino runs exactly as before).

### Telemetry (opt-in, off by default)
- **Anonymous usage telemetry**, disabled by default and gated behind explicit
  opt-in (`TELEMETRY=1` per connection, `ARGUS_TELEMETRY=1` machine-wide;
  `ARGUS_TELEMETRY=0` is a hard kill switch; `-DARGUS_ENABLE_TELEMETRY=OFF`
  removes it at build time). Reports only a strict whitelist — backend name,
  connect/statement latencies, bucketed row counts, OS/arch/version, and
  **SQLSTATE codes only** — and never hostnames, credentials, database/table
  names, query text, or backend error messages. Delivery is asynchronous,
  bounded, and best-effort (a background sender that can never block or fail an
  ODBC call). New: `src/odbc/telemetry.c`, a shared `src/backend/http_client.c`,
  a first-run notice, a resettable random install id, `docs/TELEMETRY.md`,
  `PRIVACY.md`, and a reference collector in `tests/tools/`. Also adds the
  previously-missing `curl_global_init()` at library load (benefits all curl
  backends).

### BI-tool parity & connectors
- **Tableau connectors** (`.taco`) for Trino, Hive, Impala, MySQL-wire and
  BigQuery, plus a turnkey **TDVT** harness (`connectors/tableau/tdvt/`). The
  Trino connector was run through Tableau's Datasource Verification Tool against
  a live server: **91.4%** pass rate (703/769) with four native-Trino
  `dialect.tdd` overrides.
- **Per-backend SQL dialect + ODBC escape translation** (`{fn …}`, `{d}`, `{t}`,
  `{ts}`, `{oj}`) with the `SQLGetInfo` scalar bitmaps derived from what the
  driver can actually translate; **Level 1 interface conformance**
  (`SQL_OIC_LEVEL1`). Power BI custom connector (`.mez`).

### ODBC conformance
- **Real statement-level asynchronous execution** on a worker thread:
  `SQL_ASYNC_MODE = SQL_AM_STATEMENT`, `SQLExecDirect`/`SQLExecute` return
  `SQL_STILL_EXECUTING`, plus `SQLCompleteAsync` and `SQLCancelHandle` (ODBC 3.8).
  Verified against live Trino.
- **Real ODBC descriptors** (ARD/APD/IRD/IPD via `SQLAllocHandle(SQL_HANDLE_DESC)`)
  and row/column-wise binding; added the Unicode descriptor accessors
  `SQLGetDescFieldW`, `SQLSetDescFieldW`, `SQLGetDescRecW`.
- Real `SQL_DBMS_VER` from a per-backend `get_server_version` hook (Trino,
  MySQL-wire).

### Performance
- **DOM-free Trino result decode**: result pages are scanned straight into cells
  instead of building a json-glib DOM (~half of fetch time). ~65% faster fetch on
  large extracts, proven byte-identical to the reference path; kill-switch
  `ARGUS_TRINO_NOFASTJSON`. `tests/bench` gains `ARGUS_BENCH_NODECODE` and
  `ARGUS_BENCH_CKSUM`.

### Documentation
- `docs/SIMBA_PARITY.md` — an evidence-based comparison against Simba/Starburst
  (black-box ABI inspection of real Simba binaries, published docs, source
  audit), and a "How Argus compares" section in the README.

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
