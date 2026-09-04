# Changelog

All notable changes to the Argus ODBC Driver project.

## [Unreleased]

### Fixed: memory safety and handle lifetime
- **A negative length that is not `SQL_NTS`** (`SQLExecDirect`, `SQLPrepare`,
  `SQLNativeSql`, `SQLDriverConnect`, `SQLBrowseConnect`, `SQLConnect`,
  `SQLSetCursorName` and the catalog functions) became `malloc(0)` followed
  by a multi-gigabyte `memcpy`. Every entry point now answers `HY090`,
  before it touches the statement's open result set.
- **Data-at-execution parameters shared one buffer** per statement and one
  static length per process: with two `SQL_DATA_AT_EXEC` parameters the
  first one's bytes were overwritten by the second's, and re-executing the
  prepared statement read the freed buffer instead of asking for the data
  again. Each parameter now collects into its own buffer, owned by the
  statement; the application's bindings are never modified; `SQLExecDirect`
  honours `SQL_DATA_AT_EXEC` instead of rendering the token pointer as the
  value. `test_dae` drives the whole cycle under ASan.
- **`SQL_C_BINARY` parameter with a negative `BufferLength`** and no
  indicator wrapped the hex buffer's size and wrote the value past it; it is
  `HYC00` like any other unrenderable parameter.
- **`SQLFreeHandle(SQL_HANDLE_DESC)` on an implicit descriptor** called
  `free()` on a pointer into the middle of the statement. It is `HY017`, and
  `SQLGetDiagRec`/`SQLGetDiagField` accept `SQL_HANDLE_DESC` so the
  diagnostic (and those `SQLSetDescField` records) can be read.
- **`SQLDisconnect` left the connection's statements behind** with a
  pointer to a dead backend, leaked their operations, ran without the
  connection lock and did not refuse (`HY010`) while an asynchronous execute
  was in flight. The connection now tracks its statements and explicit
  descriptors and frees them on disconnect, as ODBC has it; an explicit
  descriptor no longer reads the freed statement it was associated with.
- **The `SQLTables`/`SQLColumns` metadata cache had no lock of its own**: two
  statements on one connection corrupted the hash table, and search patterns
  longer than 2 KiB shared one cache entry.
- **`SQLFreeStmt(SQL_CLOSE)` and `SQLCloseCursor` discarded the prepared
  statement**, the bindings and `SQL_ATTR_PARAMSET_SIZE`, so the prepare /
  execute / close / execute cycle failed with `HY010` and columns bound once
  fetched nothing after the first close. Closing the cursor now drops only
  the result set; a re-executed static cursor no longer serves the previous
  result from `SQLFetchScroll`.

### Fixed: `SQLCancel` and asynchronous execution
- **`SQLCancel` could not interrupt anything**: it took the statement lock
  that the running `SQLExecDirect`/`SQLFetch` held, so it only ran once the
  call had finished on its own, and the asynchronous worker wrote the
  diagnostics, columns and operation without the lock while other calls
  read them. The worker now executes under the statement lock; polling
  answers `SQL_STILL_EXECUTING` without waiting and no longer clears the
  worker's diagnostic (its error, or `HY008` after a cancel). `SQLCancel`
  from another thread returns at once: the running call answers
  `SQL_ERROR`/`HY008` at its next checkpoint and closes the operation on the
  thread that owns it. On the PostgreSQL family the cancel reaches the
  server immediately (libpq's out-of-band request, marked by the new
  `cancel_from_any_thread` vtable flag), so a blocked call returns early;
  the other backends are cancelled when their call returns. The README
  paragraph on `SQLCancel` now describes what each backend actually does.

### Fixed: parameter markers inside string literals
- **A backslash-escaped quote ended a literal** for the marker scanner and
  the escape parser alike: on Hive, Impala, MySQL wire and BigQuery,
  `SELECT 'a\'b?c'` had a bound value substituted into the string (or a
  brace inside it taken for an escape sequence). PostgreSQL's `E'...'`
  strings and `$tag$...$tag$` dollar quoting were unknown, so a function
  body failed with `07002` for the `?` in it. Both scanners share one set
  of dialect-aware lexical helpers (`argus_sql_skip_quoted` and friends,
  `pg_strings` on the dialect); `"..."` is a literal on the backtick
  engines and an identifier elsewhere; comments are stepped over by the
  escape parser too. `SQLNumParams` counts with the dialect.

### Fixed: binary, DECIMAL and the types the engines actually declare
- **BINARY values reached the application in the engine's wire encoding.**
  A `SQL_C_BINARY` bind got whatever text the backend had produced — hex on
  Pinot, base64 on Trino, BigQuery and Avatica, `\x`-hex on PostgreSQL,
  Arrow's debug rendering on Flight SQL — and `SQLGetData` guessed, decoding
  any even-length hex string it saw, including a `VARCHAR` that happened to
  look like one. Each backend now decodes its own encoding into the cell
  (`argus_cache_decode_binary`), so which columns are binary comes from the
  result metadata and the spelling from the engine; the value is never
  sniffed. Text a decoder rejects keeps its text rather than becoming
  nothing.
- **Hive and Impala re-encoded bytes they had already received.**
  `TBinaryColumn` is bytes on the wire; the fetch path rendered them to hex
  for `SQLGetData` to decode again — twice the payload for the same value.
  They go into the cell as bytes.
- **Kudu truncated every binary value at its first NUL** (`strndup` on a
  `Slice`), and MySQL-wire cells held the bytes without saying so, so a
  character target got the raw bytes instead of their hex.
- **`DECIMAL(p,s)`, `CHAR(n)` and `VARCHAR(n)` were reported with the
  family maximums.** Hive and Impala described a column by mapping its type
  id to a name and the name back, dropping the `TTypeQualifiers` the reply
  already carried, so `DECIMAL(5,2)` came back as precision 38 scale 10 and
  `CHAR(3)` as 65535 — which is what Excel and Tableau size their columns
  from. Trino's `decimal(18,4)`, `varchar(20)` and `timestamp(6)` are read
  off the type name the same way; Pinot and Druid report a scale at all.
- **`DOUBLE` lost two digits on Hive and Impala.** `"%.15g"` turned
  `0.1 + 0.2` into `0.3`; the text is now the shortest that reads back as
  the same double.
- Numeric cells on Hive, Impala and Phoenix carry their native value beside
  the text, so a numeric `SQLGetData` target no longer parses back a string
  the driver had just formatted.
- BigQuery's `BYTES` is `SQL_VARBINARY` instead of `SQL_VARCHAR`, and Hive's
  `TIMESTAMP WITH LOCAL TIME ZONE` is described as `SQL_VARCHAR` so the zone
  the engine prints is not silently dropped; `SQL_C_TYPE_TIMESTAMP` still
  converts it.

### Fixed: Trino sessions, retries and the OAuth2 redirect
- **The session the server set was thrown away.** Trino carries session
  changes in response headers and expects the client to send them back:
  `USE`, `SET SESSION`, `SET ROLE`, `PREPARE` and `START TRANSACTION` all
  work that way. The driver never read them, so each of those reported
  success and then had no effect — the next statement ran in the session the
  connection opened with, and a transaction was never joined. The connection
  now tracks `X-Trino-Set-Catalog`, `-Set-Schema`, `-Set-Session`,
  `-Clear-Session`, `-Set-Role`, `-Added-Prepare`, `-Deallocated-Prepare`,
  `-Started-Transaction-Id` and `-Clear-Transaction-Id`, and echoes them.
  Only the coordinator is listened to: a spooled-segment host cannot rewrite
  the catalog or the transaction id.
- **`X-Trino-Time-Zone` was never sent**, so `TIMESTAMP WITH TIME ZONE` and
  `now()` were rendered in the coordinator's zone rather than the
  application's.
- **A 502, 503, 504 or 429 while polling `nextUri` failed the whole fetch.**
  Those are what a coordinator behind a load balancer answers while it
  restarts or sheds load, and polling is the long tail of every query. Four
  retries with a doubling delay.
- **The OAuth2 authorization-code flow did not check its own `state`.** The
  value was generated and sent but never compared, and the code was found
  with `strstr(buf, "code=")`, which matches `error_code=` — an IdP
  redirecting with an error was read as a successful sign-in. The redirect is
  parsed as a query string now, `state` must come back unchanged, an `error`
  parameter is reported, and the browser is told which of those failed.
- **PKCE was dead on Windows**: the verifier and the state came from
  `/dev/urandom`, which does not exist there, so both were left as they were.
  They come from `BCryptGenRandom`; with no source of randomness the flow
  stops instead of running unprotected.
- **A DSN value with a CR/LF in it could add a header of its own** to every
  request (`X-Trino-User`, `-Catalog`, `-Schema`, `-Source` and the bearer
  token all come from the connection string, which a shared `.odc` or `.tds`
  carries). Control characters are filtered out of header values.
- Trino's `time` and `time(p)` are `SQL_TYPE_TIME` instead of a timestamp.

### Fixed: one HTTP floor, and the timeouts an application asks for
- **Every HTTP backend grew its response buffer without a ceiling.** Trino,
  Phoenix, Pinot, Druid and BigQuery each carried their own copy of the same
  unbounded `realloc` write callback, so a server — or anything answering in
  its place — could grow the host application's heap until it died. The five
  copies are one shared buffer with a 256 MiB ceiling; passing it aborts the
  transfer, so a truncated body is never read as a complete one. All five
  now also get gzip decoding, a redirect limit and a low-speed abort.
- **`SQL_ATTR_LOGIN_TIMEOUT` and `SQL_ATTR_CONNECTION_TIMEOUT` were stored,
  read back, and never used.** Only the proprietary `CONNECTTIMEOUT` and
  `SOCKETTIMEOUT` keywords reached a backend, so an application setting the
  standard attributes — which every driver manager and BI tool does — got no
  timeout at all. They feed the same two settings; the connection string
  still wins when it names one.
- **The first statement's `SQL_ATTR_QUERY_TIMEOUT` became the connection's
  for good.** It was copied only while the connection's own was still zero,
  so every later statement's value was ignored. It is published for the
  execution that set it and the connection's own restored after.
- **MySQL-wire had no read or write timeout**: `MYSQL_OPT_CONNECT_TIMEOUT`
  only covers the handshake, so a server that accepted the connection and
  then stopped answering hung the calling thread for good.
- **Telemetry could be redirected to a plain `http://` collector** by
  `ARGUS_TELEMETRY_ENDPOINT`, against what `PRIVACY.md` promises. Anything
  but https turns telemetry off, except on loopback, where nothing reaches a
  network.

## [0.6.1] — 2026-09-03

A corrective release. An audit of the driver as v0.6.0 shipped it found that
the published Linux and macOS artefacts did not contain the PostgreSQL family
or MySQL-wire that the 0.6.0 notes announce, that the macOS `.pkg` had never
contained Hive/Impala, that the Windows installer had no PostgreSQL, and a set
of faults that make a BI extract silently wrong or that expose the desktop the
driver runs on. Nothing here is a new feature; every
item is either something that was shipped broken or a check that keeps it
from happening again. Users of the PostgreSQL, Greenplum, Cloudberry or
MySQL-wire backends on Linux/macOS should install this release: 0.6.0 refused
those `BACKEND=` values with `Unknown backend: postgres`.

### Fixed: the release did not contain what it announced
- `release.yml` never installed `libpq`/`libmariadb` on the Linux and macOS
  runners (nor `postgresql` on Windows), and CMake answered a missing client
  library with a `STATUS` line and a smaller driver. Backend selection is now
  explicit: `-DARGUS_WITH_<BACKEND>=AUTO|ON|OFF` per backend, `ON` turning a
  missing dependency into a configure error that names the package to
  install, and `-DARGUS_RELEASE=ON` (used by CI and the release workflow)
  requiring every backend a release ships with.
- The macOS jobs installed no Thrift at all (Homebrew's `thrift` is the
  compiler and the C++ runtime, not c_glib), so no macOS artefact ever had
  Hive/Impala. They now build the portable c_glib subset with
  `scripts/build-thrift-c-glib.sh`, as the Windows job does; the script no
  longer needs GNU `install`. The Homebrew formula still builds without the
  two backends.
- The built driver embeds one `argus-build <version> hive impala trino …`
  line naming exactly what it contains (`strings -a libargus_odbc.so |
  grep '^argus-build '`, also the first line it logs at `INFO`).
  `scripts/check-build-manifest.sh` reads it back and CI, the release
  workflow and the Homebrew formula's `test do` all fail when a backend is
  missing. CodeQL now analyses the MySQL-wire, libpq and Kerberos code it
  was skipping for lack of those headers.

### Fixed: silently wrong or truncated results
- **`SQLFetch` reported a failed first row as `SQL_NO_DATA`.** A dropped
  connection or a server error on the first `fetch_results()` looked like a
  clean end of the result set and BI tools saved truncated extracts without a
  warning. A failure is `SQL_ERROR` with its diagnostic; only a genuinely
  empty result set is `SQL_NO_DATA`.
- **Floating-point values followed `LC_NUMERIC`.** Excel, Tableau and Power
  BI Desktop call `setlocale(LC_ALL, "")`, so on a French or German desktop a
  bound `SQL_C_DOUBLE` reached the server as `WHERE x = 1,5`, a fetched
  `"1.5"` converted to `1.0`, and DOUBLE columns rendered by the Hive/Impala,
  Pinot, Druid, Phoenix, Kudu and ADBC paths carried a comma. Every render
  and parse goes through `argus_dtoa()`/`argus_strtod()` (`g_ascii_*`);
  `test_locale_numbers` runs the ODBC calls under `fr_FR`/`de_DE`, and CI
  installs the locale so the test cannot skip.
- **HiveServer2/Impala**: a row set whose only populated column was BINARY
  counted as zero rows (end of result); an empty batch with
  `hasMoreRows=true` (Impala's `FETCH_ROWS_TIMEOUT_MS`, a gateway still
  producing) ended the fetch; every Impala batch leaked its payload through
  an extra `g_object_get` reference; a `FetchResults` error status was
  reported as a bare "Failed to fetch results" without the server's message.
  `hasMoreRows=false` is deliberately not used as a terminator — Hive and
  Spark Thrift Server hard-code it — the reasoning is in `hs2_fetch.h`.
- **Trino spooled protocol**: a segment that failed to download returned
  fewer rows instead of failing the fetch.

### Fixed: security
- **Driver-side connection pool removed.** It keyed parked connections on
  host, port, backend and user name only, so with `SQL_ATTR_CONNECTION_POOLING`
  on the environment (unixODBC, Tableau) a second `SQLDriverConnect` with the
  wrong password, another database, another `AuthMech` or no TLS was served
  somebody else's authenticated session. Pooling is the Driver Manager's job;
  the driver now gives its pool what it needs: `SQL_ATTR_CONNECTION_DEAD` is
  answered by the backend's `is_alive` probe, and `SQL_ATTR_RESET_CONNECTION`
  (ODBC 3.8) runs the backend's `reset_session` (PostgreSQL: `ROLLBACK` +
  `DISCARD ALL`) before a connection is parked. The undocumented `POOL*`
  keywords and `ARGUS_POOL_*` variables are gone.
- **Command execution from a DSN.** The Trino authorization-code flow built a
  shell command from `OAuth2AuthEndpoint` (`AuthURI`) and `$BROWSER` and ran
  it through `system()`: a shared DSN, an `.odc` or a `.tds` carrying
  `AuthURI=https://x/'; cmd; '` ran `cmd` at connect time inside the host
  process. The browser is launched through `g_spawn_async` argv (or
  `ShellExecuteW`) with the URL as one argument, the endpoint must be an
  `https://` URL made of URL characters (`http://` only for the local
  machine), and a rejected endpoint fails the connection with `08001`.
- **SASL pre-authentication DoS.** The negotiation reader passed the 4-byte
  frame length announced by the server to `g_malloc()` before any credential
  was checked — up to 4 GiB, or an abort of Excel/Tableau on failure. Frames
  are capped at 1 MiB with `g_try_malloc` and complete reads; the PLAIN
  rejection path no longer derives a `memcpy` bound from `errmsg_size - 60`
  (which underflows). The framed/buffered transports get an explicit
  `ThriftConfiguration` (64 MiB frames, 100 MiB messages, recursion 64) and
  Thrift-over-HTTP refuses bodies above the same cap.
- **Trino credentials left the connection's origin.** The bearer token,
  Basic/Negotiate credentials and `X-Trino-*` headers were attached to every
  URL the server handed back, including spooled-segment URIs on S3/GCS or any
  host the coordinator named. Session headers now go to `base_url`'s origin
  only; segments are fetched with the headers of their descriptor and nothing
  else. Every curl handle in the driver (Trino, Pinot, Druid, Phoenix,
  BigQuery, Thrift HTTP, telemetry) is now limited to `http`/`https` for
  requests and redirects, with a TLS 1.2 floor (`src/backend/curl_common.c`).
- **Catalog search patterns were interpolated verbatim** into the
  `information_schema` queries of Trino, MySQL-wire and Druid (`… LIKE '%s'`),
  and Trino's table-type list accumulated into a 256-byte buffer with an
  unchecked `snprintf` offset (CodeQL `cpp/overflowing-snprintf`). Shared
  `argus_sql_quote_literal()`/`argus_sql_quote_ident()` helpers (also used by
  the parameter renderer), `GString` builders, and
  `mysql_real_escape_string()` on the MySQL-wire connection so the server's
  charset and `NO_BACKSLASH_ESCAPES` are honoured.
- **`OutConnectionString` masked only `PWD=`/`PASSWORD=`** at the start of a
  pair: `ClientSecret`, `OAuth2ClientSecret`, `AccessToken`, `BQAccessToken`
  came back in clear (BI tools persist that string in their workbooks), and
  `; PWD = x` or `PWD={a;b}` were not masked at all. One key classifier
  (`argus_connstr_key_is_secret()`: `PWD`, `PASSWORD`, `PASSPHRASE`, `SECRET`,
  `TOKEN`, `APIKEY`, `AUTHHEADER` suffixes, so the enterprise addon's
  `LicenseToken`/`AuditKey`/`OtlpAuthHeader` are covered) drives both the
  returned string and the observability copy; endpoints such as
  `OAuth2TokenEndpoint` are no longer over-masked.

### Fixed: unloading the driver
- **The library destructor joined the telemetry sender unconditionally**: a
  POST in flight held `dlclose()` or process exit for up to the 10 s HTTP
  timeout, and the same call from `DllMain(DLL_PROCESS_DETACH)` could only
  deadlock (a thread cannot exit while the loader lock is held). The driver
  now quiesces when the last environment handle is freed — where a Driver
  Manager goes before it unloads a driver, and where waiting is allowed: the
  sender gets 500 ms to finish, is then told to abort its POST through
  libcurl's progress callback, and is joined within about two seconds;
  `DllMain` only signals. The same point fires the new
  `argus_obs_hook_unload(may_wait)` tap, so an add-on that started threads
  stops them before the code they run is unmapped — nothing in the driver
  told an add-on it was going away before. Everything restarts lazily on
  the next environment the application allocates.

### Build: hardening, exports, signing
- `ARGUS_HARDENING` (default `ON`): `-fstack-protector-strong`,
  `-fstack-clash-protection`, `-fcf-protection=full` where the compiler takes
  them, `_FORTIFY_SOURCE=2` on optimising non-sanitized builds, full RELRO +
  `BIND_NOW` + non-executable stack on ELF, DEP/ASLR/high-entropy ASLR on
  MinGW, `/GS /guard:cf /DYNAMICBASE /HIGHENTROPYVA /NXCOMPAT` on MSVC.
  Ubuntu's GCC defaults had been giving the Linux build a canary and FORTIFY
  by accident; macOS and Windows had neither, and no platform had `BIND_NOW`.
- The generated Thrift code is built with hidden visibility like the rest of
  the driver: 104 `SQL*` exports instead of 385 (281 of them
  `t_c_l_i_service_*`/`toString_*` internals that two Thrift c_glib drivers in
  one process resolved against each other). The export set is also pinned at
  link time (`src/argus_odbc.map` on ELF, `src/argus_odbc.exports` on Mach-O),
  because hidden visibility does not reach the static archives the link pulls
  in: the portable `thrift_c_glib` subset leaked 163 symbols from the macOS
  driver, and libgcov leaked seven from a `--coverage` build.
- `scripts/check-exports.sh` and `scripts/check-hardening.sh` read both
  properties back out of a `.so`/`.dylib`/`.dll`; `ctest -L unit` runs them as
  `check_exports`/`check_hardening` on every platform, and the release
  workflow runs them on each artefact with `--release`.
- The ten signing steps of the release (GPG, Apple codesign/notarize, Azure
  Trusted Signing, Tableau jarsigner) were `continue-on-error`, so an expired
  key produced a green tag with unsigned packages. They still run only when
  their secret is configured, but fail the release when it is and they
  break. The release job now checks that every platform artefact and the
  Power BI connector are present, always writes `SHA256SUMS`, verifies it
  with `sha256sum -c`, and signs it in a separate step.

## [0.6.0] — 2026-08-25

The first v0.6.0 tag (2026-08-24) never produced a release: the pipeline
failed on Windows (weak-symbol DLL link) and macOS (a test asserting the
Linux driver file name). 0.6.0 is re-cut here with those fixed, and picks up
the PostgreSQL-family work that landed in between. Continues the v0.5.x tag
line — the v0.1–v0.5.9 tags predate this changelog's revival.

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
- **`backslash_escapes` is false for all three backends.** 0.6.0 made bound-
  parameter escaping dialect-aware; PostgreSQL has defaulted
  `standard_conforming_strings` to on since 9.1, so `\` in a literal is an
  ordinary character and doubling it would deliver `C:\\path` where the
  application bound `C:\path`. Verified live, and `test_postgres_escapes` now
  binds a backslash and reads it back so the flag cannot be flipped silently.
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
- **The release pipeline itself**: `libargus_odbc.dll` failed to link (PE has
  no weak-symbol override, so the `argus_obs_hook_*` no-ops are plain
  definitions on Windows, with a new `ARGUS_OBS_HOOKS_EXTERNAL` macro that
  empties `obs_hooks.c` for builds compiling their own strong tap definitions
  into the driver target), and `test_info` asserted the Linux driver file name
  on every platform (the build now hands the test the same
  `ARGUS_DRIVER_NAME` the driver reports).

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

Everything below was part of the first (unreleased) 2026-08-24 cut and ships
in 0.6.0 as well.

### Correctness
- **Parameter escaping is now dialect-aware**: `\` is doubled only on engines
  where it is a string-literal escape character (Hive, Impala, MySQL-wire,
  BigQuery). On ANSI-literal engines (Trino, Phoenix, Pinot, Druid) a bound
  value such as `C:\path` previously arrived server-side as `C:\\path`.
- **`?` inside SQL comments** (`--`, `/* */`) is no longer counted or
  substituted as a parameter marker.
- **`SQLEndTran`/`SQLTransact` no longer fake rollbacks**: `SQL_COMMIT` remains
  a vacuous success (the connection is auto-commit only,
  `SQL_TXN_CAPABLE=SQL_TC_NONE`), but `SQL_ROLLBACK` now returns `SQL_ERROR`
  (`HYC00`) instead of claiming work was undone that had already committed.
- **Real liveness probes for Druid and Pinot** (`/status/health`, `/health`):
  the connection pool no longer hands out connections to a dead broker on
  these backends.
- **Telemetry no longer writes a persistent `install_id` unless telemetry is
  actually enabled** for a connection, and the first-run notice is now also
  printed to stderr, not only the log file.

### Runtime validation campaign (live engines)
- **Kerberos/GSSAPI over binary Thrift validated against a real MIT KDC** —
  connect + SELECT through the hand-written SASL layer
  (`tests/integration/kerberos/`), closing the roadmap's "runtime validation
  pending" item. The HTTP/SPNEGO transport still needs an `HTTP/` service
  principal and a TLS-enabled Kerberized HS2 in the test stack.
- **Spark Thrift Server and Flink SQL Gateway validated live** through the
  hive backend (connect + query). The Flink recipe that actually works is now
  codified in `docker-compose.yml` (java8 image, session cluster, standalone
  metastore, sha256-pinned connector jars via
  `tests/integration/flink-lib/fetch-jars.sh`, and an underscore-free compose
  network — Hive's URI canonicalization rejects `_` in hostnames).
- Full integration label green on the fresh stack: Trino (+TLS), Hive 4,
  MySQL-wire/MariaDB, Pinot, BigQuery emulator, Spark, Flink, async,
  failover, cursors, BI escapes — 21/22, `test_hive_http` being the SPNEGO
  infra gap above. Phoenix is blocked by the only available PQS image
  (PROTOBUF-only, confirmed live); Kudu's image refuses its own defaults and
  the backend is deprecated.

### Fetch memory model
- **Single-allocation rows** (`argus_row_alloc_block`): a row's cell array
  and all its payloads can live in one malloc block, freed with one free —
  ~10x fewer allocations on wide results. The MySQL-wire backend (which
  knows every column length up front) is converted; ownership-transfer
  semantics (scroll cache) are unchanged and the classic per-cell layout
  remains supported. Trino/HS2 conversion is the follow-up.

### Memory & threading
- **Statement handles shrank from ~360 KB to a few hundred bytes**: the five
  embedded 64-record diagnostic arrays (~66 KB each) and the 256-entry
  parameter-binding array (~14 KB) are now lazily allocated on first use and
  grown geometrically; storage is released on handle free.
- **Diagnostics reads are now thread-safe**: `SQLGetDiagRec`,
  `SQLGetDiagField` and `SQLError` lock the owning handle's mutex (records
  being lazily reallocated made the previously-unlocked read a
  use-after-free risk); the environment handle gained its own mutex.

### Fuzzing & CI
- **libFuzzer harnesses** (`fuzz/`, `ENABLE_FUZZING=ON` under Clang) for the
  ODBC escape translator and the connection-string parser, with seed corpora
  and a CI job (90 s + 60 s budget per push); initial campaign: 4.4 M
  executions, zero findings.
- **`integration-full` CI job** (workflow_dispatch + weekly): starts every
  compose service — including Phoenix, Kudu, Spark and Flink, which the
  default job never ran — and runs the complete integration label.
- `docs/ARCHITECTURE.md` rewritten to describe the actual system (all
  backends, dialect layer, threading and memory model, obs_hooks seam,
  quality gates) — it previously covered 3 of 10 backends.

### Documentation honesty
- Entry-point count corrected to **104** (the previous "107" double-counted
  the three W-descriptor functions).
- `SIMBA_PARITY.md`: the DSN-dialog row no longer claims parity (Argus's
  `ConfigDSN` is deliberately UI-less), the bottom line no longer contradicts
  the table (async and catalog have closed), and the TDVT figure is labelled
  self-measured rather than certified.
- The three `*_PARAMETERS_COMPARISON.md` files carry status banners correcting
  their stale Kerberos/OAuth2/async conclusions.
- The `obs_hooks` tap seam is now disclosed in the README.

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
