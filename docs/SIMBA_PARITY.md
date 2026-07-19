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
| Exported ODBC entry points | 89 | **107** (was 104; +3 W-descriptor) | Argus broader |
| Unicode / `W` entry points | full | full (`src/odbc/unicode.c`, UTF-16↔UTF-8) | Parity |
| Connection pooling | yes | yes (`src/odbc/pool.c`, opt-in) | Parity |
| Auth: Kerberos/SASL/OAuth2/SSL | yes | GSSAPI + SSPI, SASL, OAuth2 (M2M + device flow), JWT, LDAP/Basic, TLS | Parity |
| Platform / driver manager | Win / unixODBC / macOS | Win (`ConfigDSN` + NSIS + Intune), unixODBC, macOS pkg, RPM/DEB | Parity |
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

   So ~86% of fetch time is the backend fetch + JSON parse (network +
   `trino_fetch.c`), and only ~14% is the ODBC decode — which is already tight
   (`convert_cell_to_target` has a typed fast path for numerics and cached
   lengths, no per-cell `strlen`/allocation). The real win, matching Simba's
   Arrow/Cloud Fetch, is a **columnar binary wire format**: decode Trino's
   spooled Arrow segments (`trino_spooling.c` already carries the transport)
   column-by-column instead of parsing row-oriented JSON. That is a scoped,
   Trino-specific feature that needs an Arrow-encoded spooling endpoint to build
   and verify — it is deliberately not hacked in unverified. Optimising the ODBC
   decode loop instead would cap at the measured ~14%.
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

The empty `SQLForeignKeys`, `SQLProcedures`, `SQLColumnPrivileges`,
`SQLTablePrivileges`, and `SQLSpecialColumns` results are **correct** for Argus's
target engines, not a deficiency: Trino, Hive, Impala, Spark, Pinot, Druid and
BigQuery have no foreign keys, stored procedures, per-column privileges in the
ODBC sense, or row-version/rowid columns. Simba's own ODBC drivers for these
same engines return empty here too. A driver must not invent metadata the engine
does not have, so these stay empty by design; `SQLTables`, `SQLColumns`,
`SQLGetTypeInfo` and `SQLPrimaryKeys` (where the engine has keys) carry the real
data.

## Bottom line

On the axes a BI tool actually exercises — ODBC 3.8, Unicode, dialect/escape
correctness, auth, pooling, platform coverage, and the Tableau TDVT bar — Argus
is at parity with SimbaEngine, with a **broader raw ODBC surface (107 vs 89
entry points) and more backends (10)**. Simba stays ahead on three things worth
a roadmap: async execution, a couple of catalog functions, and columnar/Arrow
result decoding for large extracts.
