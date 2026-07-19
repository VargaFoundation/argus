# Running TDVT against the Argus Tableau connectors

[TDVT](https://tableau.github.io/connector-plugin-sdk/docs/tdvt) (Tableau
Datasource Verification Tool) is the objective bar that Simba/Starburst pass: it
drives Tableau's own query engine to generate SQL from a few thousand
logical/expression tests, runs them through the connector + driver against a
live database, and compares to expected results — producing a **pass rate**.
That number turns "we think we're at parity" into a fact, and it is a
prerequisite for a Tableau Exchange submission.

**This has been run for real** (argus-trino, expression suite, 769 tests):

| Configuration | Pass rate |
|---|---|
| Baseline, no tuning | **90.0 %** (692/769) |
| + the four `dialect.tdd` overrides below | **91.4 %** (703/769) |

At 91.4 % there are **zero Trino SQL errors and zero Argus driver bugs** left —
every remaining failure is a value-level diff (canonical TestV1 data conventions
and empty-string edge cases in Tableau's PrestoDialect string functions), the
same class of gap commercial connectors close with `.tdd` tweaks and the
standard exclusion list.

**Why it can't run in headless CI:** TDVT invokes `tabquerytool.exe`, which ships
only with **Tableau Desktop**. It must run on a Windows or macOS machine with
Tableau installed and **licensed**. Everything below is the turnkey recipe.

---

## What you need

- **Tableau Desktop** (for `tabquerytool.exe`) — a free 14-day trial is enough,
  but it must be **activated** (see the licensing note in step 4).
- **Python 3.9+** on the same OS as Tableau.
- The **Argus ODBC driver** installed (64-bit) — the same one the `.taco`
  targets. On Windows it registers as `Argus ODBC Driver`.
- A reachable **Trino** you can create tables in.
- This repo, and the
  [connector-plugin-sdk](https://github.com/tableau/connector-plugin-sdk)
  (for TDVT itself + the test data).

## Step 1 — Load the test data into Trino

TDVT needs two tables, **Calcs** and **Staples** (the TestV1 dataset). The
simplest writable Trino target is the built-in **`memory`** connector.

1. Enable it: add `etc/catalog/memory.properties` with `connector.name=memory`
   on the coordinator, then restart Trino. (On `trinodb/trino`: write that file
   into the container and `docker restart`.)
2. Create the schema and tables, then fill them:
   ```bash
   pip install trino
   python trino-testdata/load.py \
       --datadir <sdk>/tests/datasets/TestV1 \
       --host <trino-host> --port 8080 --user test \
       --catalog memory --schema tdvt
   ```
   (`ddl.sql` + `load.py` create `memory.tdvt.{Calcs,Staples}`; empty CSV fields
   become SQL NULL, which TDVT requires.) A Trino in WSL is reachable from a
   Windows Tableau at `http://localhost:8080` via WSL2 localhost forwarding.

   Sanity: `SELECT count(*) FROM memory.tdvt."Calcs"` → 17, `… "Staples"` → 54860.

## Step 2 — Install TDVT and lay out a workspace

```bat
py -3 -m venv venv
venv\Scripts\python -m pip install "setuptools<81" -e <sdk>\tdvt
```
(`setuptools<81` is required — newer setuptools drops `pkg_resources`, which TDVT
imports.)

Create a workspace directory (e.g. `C:\tdvt-workspace`) with:

```
tdvt-workspace\
  connectors\argus_trino\      <- copy of connectors/tableau/argus-trino/ (unpacked)
  tds\
    cast_calcs.argus_trino.tds <- see tds/ in this folder
    Staples.argus_trino.tds
  config\
    argus_trino.ini            <- datasource config (below)
    tdvt\tdvt.ini              <- points at tabquerytool.exe
```

`config\tdvt\tdvt.ini`:
```ini
[DEFAULT]
TAB_CLI_EXE_X64 = C:\Program Files\Tableau\Tableau 2024.3\bin\tabquerytool.exe
```

`config\argus_trino.ini` — **must be ASCII (no BOM)**, or TDVT's config parser
fails with "File contains no section headers":
```ini
[Datasource]
Name = argus_trino
LogicalQueryFormat = simple_public
CommandLineOverride = -DConnectPluginsPath=C:\tdvt-workspace\connectors -DDisableVerifyConnectorPluginSignature=true

[StandardTests]

[LODTests]

[UnionTest]
```

The ready-made `tds/*.tds` in this folder connect through the `argus_trino`
connector to `memory` / schema `tdvt`, with the named-connection called `leaf`
(TDVT requires that name).

## Step 3 — Deploy the connector

Copy `connectors/tableau/argus-trino/` (unpacked) both into the workspace
`connectors\argus_trino\` and into
`%USERPROFILE%\Documents\My Tableau Repository\Connectors\argus_trino\`.
`-DDisableVerifyConnectorPluginSignature=true` (already in the ini) lets Tableau
load the unsigned connector.

## Step 4 — Activate Tableau, then run

**Licensing gotcha (this is the one that bites):** `tabquerytool` exits `17`
with *"la source de données … ne fait l'objet d'aucune licence"* until Tableau
Desktop is **actually activated** — launch the GUI once and click **Start trial
now** through to the start page (registering an account is *not* enough). Verify
with `"…\bin\custactutil.exe" -view` → `Fulfillment Type: TRIAL, Status:
ENABLED`. When licensed, tabquerytool logs `lc_checkout: LM_NOERROR`.

```bat
cd C:\tdvt-workspace
venv\Scripts\python -m tdvt.tdvt run argus_trino -e     :: expression suite
venv\Scripts\python -m tdvt.tdvt run argus_trino -q     :: logical-query suite
venv\Scripts\python -m tdvt.tdvt run argus_trino        :: everything
```

## Step 5 — Read the result

- Console prints `Test Count / Passed tests / Failed tests`.
- **`test_results_combined.csv`** — every test with `Passed`, the `Generated
  SQL`, and `Actual`/`Expected` tuples. This is the work-list: each failure
  points at a concrete gap.
- Certified connectors aim for **> 90 %**.

## Step 6 — Feed failures back

Each failure maps to a fix:
- **Trino SQL error / rejected function** → override it in
  `connectors/tableau/argus-trino/dialect.tdd`. Render **native Trino SQL, never
  an ODBC `{fn}` escape** — the connector must not depend on driver-side escape
  translation.
- **Wrong capability advertised** → the `<customizations>` in `manifest.xml`, or
  `SQLGetInfo` in `src/odbc/info.c`.
- **Value diff only** → usually a canonical-data convention or a Tableau
  string-function edge case; compare against what postgres/other connectors
  exclude in their `.ini` before assuming a bug.

### `dialect.tdd` overrides currently shipped (and why)

All four exist because Tableau's `PrestoDialect` emits SQL Trino rejects; each is
native Trino SQL. Lessons learned writing them:

- Function overrides **must** be wrapped in `<function-map>` inside `<dialect>`
  (a bare `<function>` child makes Tableau report "Malformed XML: dialect.tdd"
  and silently skip the whole connector → *"Tableau does not recognize the data
  source type argus_trino"*).
- Argument type is `int`, not `integer`.
- To *replace* a base-dialect function the override's argument signature must
  match the base exactly (e.g. `ROUND`'s second argument is `real`, not `int`).

| Function | Reason | Rendering |
|---|---|---|
| `ROUND`  | Trino wants precision as INTEGER, base sends BIGINT | `ROUND(%1, CAST(%2 AS INTEGER))` |
| `ATAN2`  | not mapped by base → "function unknown"             | `ATAN2(%1, %2)` |
| `ASCII`  | Trino has no `ASCII()`                              | `CODEPOINT(CAST(SUBSTR(%1,1,1) AS VARCHAR(1)))`, `''`→NULL |
| `SPACE`  | Trino has no `SPACE()`                              | `ARRAY_JOIN(REPEAT(' ', CAST(%1 AS INTEGER)), '')` |
