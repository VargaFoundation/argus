# Argus vs Simba (SimbaEngine) — ODBC parity

An evidence-based comparison of the Argus ODBC driver against Simba/Magnitude's
SimbaEngine drivers — the de-facto commercial baseline, OEM'd as the Databricks
Spark, Impala, Hive, Athena and BigQuery ODBC drivers.

## Method

Parity on ODBC is measured on the **observable contract** — what the driver
advertises and can do through the public ODBC API — not on internal
implementation. This comparison uses three legitimate sources, and **no
decompilation** of Simba's proprietary binaries (their EULA forbids it, and
reading their code would put Argus at legal risk):

1. **Black-box ABI inspection** of real Simba binaries shipped with Power BI
   Desktop (`SparkODBC_sb64.dll`, `HiveODBC_sb64.dll`) — the PE export table,
   i.e. the ODBC entry points they actually implement, via `objdump -p`.
2. **Published capability docs** (Simba/Databricks install guides, Simba SDK
   product pages).
3. **A source audit of Argus** (`src/odbc/`, `src/backend/`).

The Simba Hive and Spark drivers export an **identical 89-function surface** —
confirming it is the shared SimbaEngine core, not a per-data-store surface.

## Result

| Axis | Simba | Argus | Verdict |
|---|---|---|---|
| ODBC version | 3.8 | 3.8, `SQL_OIC_LEVEL1`, SQL-92 Entry | Parity |
| Exported ODBC entry points | 89 | **104** (incl. the 3 W-descriptor functions) | Argus broader — but raw export count is a weak proxy for capability |
| Unicode / `W` entry points | full | full (`src/odbc/unicode.c`, UTF-16↔UTF-8) | Parity |
| Connection pooling | yes | Driver Manager pooling, with the two hooks it needs from the driver: a backend-probed `SQL_ATTR_CONNECTION_DEAD` and `SQL_ATTR_RESET_CONNECTION` (ODBC 3.8) | Parity — the driver deliberately does not pool on its own (a driver-side pool keyed on host/user could hand a caller with the wrong password an authenticated session) |
| Auth: Kerberos/SASL/OAuth2/SSL | yes | GSSAPI + SSPI, SASL, OAuth2 (M2M + device flow), JWT, LDAP/Basic, TLS | Parity |
| Platform / driver manager | Win / unixODBC / macOS | Win (`ConfigDSN` **with a configuration dialog + Test Connection**, plus the attribute-list scripted path, NSIS, Intune), unixODBC, macOS pkg, RPM/DEB | ~Parity — the dialog is newly added (in-memory DLGTEMPLATE, cross-compile-verified); first interactive validation on a real Windows desktop still pending |
| Type mapping | extensive | WCHAR/NUMERIC/GUID/BINARY/INTERVAL; minor tinyint/float & interval-subtype gaps | ~Parity |
| Bulk / array | param arrays, row arrays | row arrays + param arrays; `SQLBulkOperations`→HYC00 (Simba Spark doesn't export it either) | ~Parity |
| Tableau TDVT | certified (>90%) | **91.4%** measured (703/769) | Parity |
| Backends | one per driver | **10** in one binary | Argus broader |
| Async (ODBC 3.8) | yes | **yes** — worker-thread execute, `SQL_AM_STATEMENT`, `SQLCompleteAsync` | Parity |
| Catalog completeness | full for the engine | Tables/Columns/TypeInfo/PrimaryKeys real; ForeignKeys/SpecialColumns/Procedures/Privileges correctly empty | Parity (see note) |
| **Large-result decode** | Arrow + Cloud Fetch (columnar) | Trino spooling transport, but **row-wise** decode (Arrow only via the ADBC layer) | **Simba ahead on decode** |
| **Client-side SQL engine** | SQLEngine + Collaborative Query Execution | delegate-only (Kudu has a minimal SELECT parser) | Simba ahead *architecturally* |

## Remaining gaps, ranked

1. **Columnar / Arrow wire format** — the one substantive gap left, and it is in
   the *wire format*, not the ODBC decode loop. Measured with `tests/bench`
   against live Trino (`SELECT * FROM tpch.sf1.orders`, SQL_C_CHAR, drained):

   | rows × cols | fetch + decode | fetch only (`ARGUS_BENCH_NODECODE=1`) | ODBC decode share |
   |---|---|---|---|
   | 300000 × 9 | 1007 ms | 868 ms | **138 ms — 14%** |
   | 100000 × 9 | 435 ms  | 358 ms | 77 ms — 18% |

   So ~86% of fetch time is the backend fetch + JSON parse, and only ~14% is the
   ODBC decode (already tight). Profiling the 86% pinpointed the cost: building
   the json-glib **DOM** for each result page was ~47% of fetch time (a JsonNode
   per cell, millions of them, copied out then freed) — bigger than the network.

   **Closed:** `trino_fetch.c` now scans the `data` array straight into cells
   without a DOM (`sj_*` fast scanner; the small envelope — columns/nextUri —
   still uses json-glib). Proven byte-identical to the DOM path by checksum over
   orders/customer/lineitem and a crafted unicode+surrogate string, and **~65%
   faster** end to end (300k × 9: 573 ms vs 948 ms; 523k vs 316k rows/s).
   Kill-switch `ARGUS_TRINO_NOFASTJSON` falls back to the DOM path.

   A true **columnar Arrow wire format** (Trino spooled Arrow segments, like
   Simba Cloud Fetch) would cut network transfer further, but the dominant
   decode cost is now gone; it is an optional follow-on, not a standing gap.
2. **Client-side SQL engine** — only matters for *non-SQL* sources (Salesforce,
   Mongo). Argus's 10 backends are all full-SQL engines (Kudu, the exception,
   has a minimal parser), so this is not required for the current targets.

## Closed since this study began

- **The 3-function ABI gap** (`SQLGetDescFieldW`, `SQLGetDescRecW`,
  `SQLSetDescFieldW`) found by the black-box inspection — `src/odbc/unicode.c`.
- **Async execution** — now real statement-level async on a worker thread,
  advertised as `SQL_AM_STATEMENT`, with `SQLCompleteAsync`; verified against
  live Trino (`tests/integration/test_async.c`).

## Note on catalog "completeness"

`SQLForeignKeys`, `SQLProcedures`, `SQLProcedureColumns`, `SQLColumnPrivileges`,
`SQLTablePrivileges` and `SQLSpecialColumns` are **answered where the engine has
the objects and empty where it does not** — the driver reports what is there
rather than a fixed answer either way.

Empty is the correct result for Trino, Hive, Impala, Spark, Pinot, Druid and
BigQuery: they have no foreign keys, no stored procedures, no per-column
privileges in the ODBC sense and no row-version or rowid columns. Simba's own
drivers for those engines return empty here too, and a driver must not invent
metadata a BI tool would then trust.

The PostgreSQL family has all of them, and reports them from `pg_catalog`:
foreign keys with their update and delete rules and deferrability, functions and
their argument modes and types, table and column privileges expanded through
role membership, and a best-row-id drawn from the primary key or a
fully-NOT-NULL unique index. Two deliberate omissions there, for the same reason
the empty results are deliberate elsewhere: `ctid` is **not** offered as a
`SQL_BEST_ROWID` fallback (it is invalidated by UPDATE and VACUUM FULL, so it
does not identify a row for any scope ODBC defines), and `SQL_PROCEDURES` still
answers `"N"` because that info type also promises the driver accepts ODBC's
`{call ...}` syntax, which `escape.c` does not yet translate.

Transactions moved the same way. `SQL_TXN_CAPABLE` is per-backend: `SQL_TC_NONE`
for the analytics engines, which have no transactions, and `SQL_TC_ALL` for the
PostgreSQL family, where `SQL_ATTR_AUTOCOMMIT` and `SQLEndTran` do real work and
a connection the Driver Manager's pool is about to park is rolled back and
`DISCARD ALL`-ed (`SQL_ATTR_RESET_CONNECTION`) before it is reused.
`SQL_TXN_READ_UNCOMMITTED` is not advertised even though PostgreSQL accepts the
syntax: it silently gives READ COMMITTED, so claiming it would be a promise the
server does not keep.

## PostgreSQL: measured against psqlODBC

The PostgreSQL-family backends have a directly comparable incumbent, so they
were measured against it rather than argued about. Same server, same query,
same client loop, same driver manager; 1.5 M rows × 9 columns; best of three.

| | rows/s | peak RSS |
|---|---|---|
| **Argus** | **727 000** | **50 MB** |
| psqlODBC, defaults | 391 000 | 629 MB |
| psqlODBC, `UseDeclareFetch=1` | 352 000–378 000 | 12–15 MB |

~1.9× the throughput in every psqlODBC configuration, with memory flat in the
size of the result. psqlODBC reaches bounded memory only with
`UseDeclareFetch=1`, which is off by default.

The same exercise killed a planned feature. A `COPY … (FORMAT binary)` fetch
path was measured before being built: against raw libpq it is 9% faster than
the single-row mode the driver already uses (1 289 k vs 1 179 k rows/s) and
33% *larger* on the wire for a typical schema, while the driver's own gap to
that ceiling — the row cache and ODBC conversion — is untouched by it. Nine
percent of the protocol half, in exchange for NBASE numeric decoding and
non-tuple-aligned COPY buffers, is a bad trade. The numbers and the reasoning
are in `tests/bench/README.md`.

## Bottom line

On the axes a BI tool actually exercises — ODBC 3.8, Unicode, dialect/escape
correctness, auth, pooling, platform coverage, and the Tableau TDVT bar — Argus
is at parity with SimbaEngine, with a **broader raw ODBC surface (104 vs 89
entry points) and more backends in one binary**. Async execution and the
catalog functions, open when this study began, have since closed (see above).
Simba stays ahead on two things worth a roadmap: **columnar/Arrow result
decoding for large extracts** (Arrow serialization / Cloud Fetch), and an
**interactive DSN configuration dialog** on Windows. The TDVT figure is
self-measured on the Trino connector's expression suite, not a vendor
certification — the method and overrides are documented in
`connectors/tableau/tdvt/`.
