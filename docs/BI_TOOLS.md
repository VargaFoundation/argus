# BI tools

Which BI tools can reach Argus, what each one needs, and what Argus ships for it.

## The short version

Most BI tools need **nothing but the driver**. Only two justify a packaged
connector: Power BI (because generic ODBC cannot fold `LIMIT`/`TOP`) and Tableau
(because a named connector fixes the dialect and can be published). Everything
else either speaks generic ODBC — in which case a correct driver is the whole
answer — or does not speak ODBC at all, in which case no connector can help.

| Tool | Speaks | Argus artifact | Status |
|---|---|---|---|
| Power BI Desktop / Service | ODBC via a custom connector | `Argus.mez` / `Argus.pqx` | shipped ([connectors/powerbi](../connectors/powerbi)) |
| Tableau Desktop / Server | ODBC via a `.taco`, or generic ODBC | `argus-{trino,hive,impala,mysql,bigquery}.taco` | see [connectors/tableau](../connectors/tableau) |
| Tableau Cloud | ODBC **through Tableau Bridge only** | `.taco` + Bridge | limited — see below |
| Excel (Windows / Mac) | generic ODBC only | none possible | driver only |
| Qlik Sense (Windows) / QlikView | ODBC Connector Package | none needed | driver only |
| Qlik Sense SaaS | no ODBC | — | out of scope |
| Alteryx Designer | generic ODBC | none needed | driver only |
| MicroStrategy | generic ODBC | none needed | driver only |
| SSIS / SSRS | generic ODBC | none needed | driver only |
| SAS (9.4 / Viya) | ODBC via SAS/ACCESS to ODBC | none needed | driver only |
| KNIME / Dataiku / Spotfire | ODBC or JDBC | none needed | driver only, via ODBC |
| DBeaver | JDBC only | — | out of scope |
| Apache Superset | SQLAlchemy | — | separate project |
| Metabase | JDBC driver plugin | — | separate project |
| Looker / Looker Studio / Amazon QuickSight | managed, no local driver | — | out of scope |

## Why the driver matters more than the connectors

ODBC makes `SQLGetInfo` a contract. A driver that reports `SQL_FN_STR_UCASE` in
`SQL_STRING_FUNCTIONS` is promising to accept `{fn UCASE(x)}` in
`SQLExecDirect`; the spec is explicit that *"the escape sequence is recognized
and parsed by drivers, which replace the escape sequences with DBMS-specific
grammar"*
([ODBC: Escape Sequences](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/escape-sequences-in-odbc)).

Tableau, Excel, Qlik and Alteryx all read those bitmaps and then emit
`{fn ...}`, `{ts '...'}` and `{oj ...}`. **Power Query does not** — it builds SQL
from its own AST driven by `SqlCapabilities`. That is why the Power BI connector
worked for a long time while the driver translated no escapes at all: Power BI
never exercised the promise.

Argus now translates them in `src/odbc/escape.c`, and the scalar-function
bitmaps in `SQLGetInfo` are **derived** from the same per-backend dialect table
(`src/odbc/dialect.c`) that drives the translator — so the driver cannot
advertise a function it has no rendering for.

What this buys, in one change: Excel, Qlik, Alteryx, MicroStrategy, SSIS and SAS
all work through generic ODBC, and Tableau's SQL stops being rejected.

### What the driver translates

`{fn ...}` (scalar functions), `{d ...}` / `{t ...}` / `{ts ...}` (date, time,
timestamp literals), `{escape ...}` (LIKE escape), `{oj ...}` (outer joins) and
`{interval ...}`.

`{call ...}` and `{?= call ...}` are rejected with `HYC00`, consistent with
`SQLProcedures` returning an empty result set — Argus exposes no procedures.

An escape the backend's dialect cannot render is rejected with `42000` and a
message naming the function. It is never forwarded: `SQLGetInfo` never
advertised it, so the application should not have sent it, and a clear driver
error beats an opaque server syntax error.

The exact function set is **per backend** and lives in `src/odbc/dialect.c`. Read
it from `SQLGetInfo` (`SQL_STRING_FUNCTIONS`, `SQL_NUMERIC_FUNCTIONS`,
`SQL_TIMEDATE_FUNCTIONS`, `SQL_SYSTEM_FUNCTIONS`) rather than trusting a table in
a document — the driver's answer is authoritative by construction.

How far each dialect has been checked differs, and it is worth knowing which one
you are relying on:

| Backend | Function set | Verified against a live server? |
|---|---|---|
| `trino` | full | **yes** — every entry executed and its value checked |
| `pinot` | full | **yes** |
| `mysql` | full | **yes** — against MariaDB 11 (covers StarRocks/Doris) |
| `hive` | full | **yes** — against HiveServer2 3.1.3 |
| `impala` | full | **yes** — against the Impala 4.5 quickstart |
| `bigquery` | full | **yes** — against the emulator; the 2-arg `TRUNCATE` is the one form it cannot exercise |
| `phoenix`, `druid`, `flightsql`, `kudu` | minimal (3 functions) | n/a — deliberately under-claimed |

Every full table above was probed function-by-function through the driver
against a live server, and every probe found at least one mapping the
documentation made look obvious and that was wrong — `hive` has no `now()`
(it is `current_timestamp()`), and `impala` rejects the ANSI `TIMESTAMP '…'`
literal and needs `CAST('…' AS TIMESTAMP)`. A viable **Phoenix** Query Server
image now exists too (`boostport/hbase-phoenix-all-in-one`), but its server
defaults to Avatica protobuf while the driver speaks Avatica JSON, so Phoenix
stays on the minimal table until a JSON-configured server is wired up.

`mysql` deliberately omits `LOCATE`, `SPACE` and `DAYOFWEEK`: MariaDB/StarRocks/
Doris support them, but ClickHouse (also reached with `BACKEND=mysql`) spells or
numbers them differently, and one shared table cannot serve both. The driver
refuses those three rather than send a query that would mean something else on
ClickHouse — verified that both halves of that behaviour hold on live MariaDB.

The distinction is not pedantry — every engine probed so far had at least one
mapping that the documentation made look obvious and that was wrong:

- **Trino**'s `repeat()` returns an *array*, not a string, so `{fn REPEAT(...)}`
  has to be rendered `array_join(repeat(x, n), '')`.
- **Pinot**'s `concat()` takes a *separator* as its third argument
  (`concat('a','b','-')` is `"a-b"`), so ODBC's variadic `CONCAT` maps only to
  the two-argument form. A variadic mapping would have returned wrong values
  with no error at all. Its `strpos()` is also 0-based where ODBC's `LOCATE` is
  1-based, and its `round()` is not decimal rounding — `roundDecimal()` is.

The three unverified tables plausibly contain mistakes of the same kind. **If a
server rejects a `{fn ...}` the driver advertised, that is a bug in
`src/odbc/dialect.c`** — please report it.

To verify one, bring the backend up from
`tests/integration/docker-compose.yml` and point the escape test at it:

```bash
BI_BACKEND=hive BI_PORT=10000 ctest -R test_bi_escapes --output-on-failure
```

The minimal dialect advertises only `UCASE`, `LCASE` and `ABS`. It used to also
claim `CURRENT_DATE` and `CURRENT_TIMESTAMP` — SQL-92 basics that every engine
on that list "obviously" has — until probing Pinot showed it resolves neither.
Under-claiming costs some pushdown; over-claiming breaks queries the driver
promised to handle, so the short list stands until someone checks the engine.

## Per-tool notes

### Power BI

Use the connector. See [connectors/powerbi/README.md](../connectors/powerbi/README.md).

**The ADBC transition does not affect Argus.** Power BI and Fabric are
[moving away from embedded ODBC drivers](https://learn.microsoft.com/en-us/power-query/transition-to-adbc)
(Simba Spark/Snowflake/BigQuery, etc.) toward ADBC, with ODBC drivers due to
leave the service in late 2026 and Power BI Desktop in spring 2027. That page
states the transition "doesn't change behavior for the ODBC connector when you
use a separately installed ODBC driver" — which is exactly what Argus is. Only
the drivers Microsoft ships in the box are affected.

One consequence is an opportunity: Power BI's built-in **Hive** connector is
listed as *Deprecated* with no replacement driver, and **Impala** moves to a
HiveServer2 ADBC driver. Argus's `BACKEND=hive` serves HiveServer2 (and Spark
and Flink) through a connector Microsoft is not retiring.

### Tableau

Use the `.taco` where one exists — Trino, Hive (also Spark and Flink), Impala,
StarRocks/Doris/MySQL, and BigQuery. See
[connectors/tableau/README.md](../connectors/tableau/README.md) for the full
list, the build, and the signing constraint (a `.taco` is a JAR: public-CA
code-signing certificate plus a mandatory timestamp).

ClickHouse is reached with `BACKEND=mysql` but is **not** covered by the
`argus-mysql` connector: its SQL is not MySQL's, so use generic ODBC for it.

Generic **Other Databases (ODBC)** works and is a reasonable fallback for
backends with no `.taco` yet, but Tableau
[does not guarantee feature compatibility](https://help.tableau.com/current/pro/desktop/en-us/examples_otherdatabases.htm)
with it, offers limited support, and requires the same DSN on every machine that
opens the workbook. Only 64-bit drivers are supported (Tableau 2023.3+).

**Tableau Cloud** cannot host an ODBC driver, so a Cloud workbook reaches Argus
only through **Tableau Bridge** running on a machine where the driver and DSN
are installed.

### Excel

Excel **cannot load Power Query custom connectors** — `.mez`/`.pqx` are Power BI
Desktop (and the on-premises data gateway) only. There is no Excel equivalent
and nothing for Argus to ship. Excel reaches Argus through generic ODBC:

1. Install the Argus ODBC driver.
2. Create a DSN (64-bit `odbcad32.exe` on Windows; see
   [docs/CONFIGURATION.md](CONFIGURATION.md) for the keywords).
3. Excel → **Data** → **Get Data** → **From Other Sources** → **From ODBC**, and
   pick the DSN.

Excel emits `{fn ...}`, so this depends entirely on the driver's escape
translation.

### Qlik

Qlik Sense on Windows and QlikView reach Argus through the **Qlik ODBC Connector
Package**, which is built in — create a DSN and select it in the Data load
editor. No Qlik-specific artifact is needed. The QVX SDK exists for custom
connectors but buys nothing here, since ODBC already works.

**Qlik Sense SaaS** has no ODBC path.

### Alteryx, MicroStrategy, SSIS, SAS

Generic ODBC against a DSN. Nothing Argus-specific to install beyond the driver.

### DBeaver, Superset, Metabase, Looker

Out of reach for an ODBC driver:

- **DBeaver** is JDBC-only; the JDBC-ODBC bridge was dropped from the Community
  Edition in 23.1. It would need a JDBC driver.
- **Superset** needs a SQLAlchemy dialect (Python).
- **Metabase** needs a JDBC driver plugin (a `.jar`).
- **Looker**, **Looker Studio** and **QuickSight** are managed services with no
  local driver hook.

Each of these is a separate project, not a wrapper over the ODBC driver. Note
that several of the backends Argus targets (Trino, Hive, Impala, StarRocks,
BigQuery) already have first-party JDBC drivers these tools support directly.

## Related

- [connectors/powerbi/README.md](../connectors/powerbi/README.md) — Power BI connector
- [connectors/tableau/README.md](../connectors/tableau/README.md) — Tableau connectors
- [docs/CONFIGURATION.md](CONFIGURATION.md) — connection keywords and DSN setup
- [CONNECTION_EXAMPLES.md](../CONNECTION_EXAMPLES.md) — connection strings per backend
