# Argus — Power BI custom connector

A thin Power Query (M) connector that sits **on top of the Argus ODBC driver**.
It does not replace or reimplement the driver — it drives the installed Argus
ODBC driver through `Odbc.DataSource` and adds the one thing generic ODBC cannot
do in Power BI: **query folding of top-N / LIMIT** (and full DirectQuery).

```
Power BI  ──M──►  Argus.mez (this)  ──ODBC──►  Argus ODBC driver  ──►  Trino / Hive / …
```

## Why a connector (and why Import already works without one)

Power BI's generic **ODBC** source already works with the Argus driver in Import
mode: filters and GROUP BY fold to SQL. But **no generic ODBC source can fold
`LIMIT`/`TOP`** — that is a Power Query architecture limit, not an Argus defect.
A connector declares, via `SqlCapabilities.LimitClauseKind`, how to emit top-N
for the backend's dialect, which unlocks top-N folding and **DirectQuery**.
Snowflake, Databricks and Dremio all ship the same driver-plus-connector pair.

## What it configures

- **`LimitClauseKind` per backend** — Trino → `AnsiSql2008` (`OFFSET … ROWS FETCH
  NEXT … ROWS ONLY`); all others → `LimitOffset` (`LIMIT … OFFSET …`).
- **Authentication** — Anonymous (open Trino), Username/Password (LDAP,
  StarRocks/Doris/ClickHouse, Hive PLAIN), Windows (Kerberos via native SSPI).
- **Hierarchical Navigator**, DirectQuery support, and the folding-relevant
  capabilities. The driver already reports a correct SQLGetInfo profile (quote
  char, predicates, aggregates, GROUP BY), so the connector only pins what
  folding requires.

## Prerequisites

1. **Argus ODBC driver installed** (the connector references it by the name
   `Argus ODBC Driver`). Windows: the signed installer. It must be the 64-bit
   driver for 64-bit Power BI Desktop.
2. **Power Query SDK** — the "Power Query SDK" VS Code extension, or `MakePQX`.

## Build the `.mez`

The `.mez` is just a zip of this folder's sources; it is not checked in — build it:

- **VS Code**: open this folder, install the *Power Query SDK* extension, then
  run **“Build”** (or the `MakePQX pack` task). Output: `bin/…/Argus.mez`.
- **CLI**: `MakePQX.exe pack -mz Argus.mez .`

For production, **sign** the `.mez` (`MakePQX.exe pack -c <cert> …`) so it can be
distributed without lowering every user's security setting.

## Install into Power BI Desktop

1. Copy `Argus.mez` to
   `Documents\Power BI Desktop\Custom Connectors\`
   (create the folder if needed).
2. Power BI Desktop → **File → Options and settings → Options → Security →
   Data Extensions** → allow *"(Not Recommended) Allow any extension to load
   without validation or warning"* — or install a **signed** `.mez` and trust
   its certificate (preferred).
3. Restart Power BI Desktop.

## Use

**Get Data → Argus**, then supply:

| Field | Example | Notes |
|-------|---------|-------|
| Server | `trino.example.com` | host of the backend |
| Port | `8080` | backend port |
| Backend | `trino` | one of hive, impala, trino, phoenix, pinot, druid, mysql, bigquery, flightsql, kudu |
| Database / catalog | `tpch` | optional; defaults to `default` |

Pick the authentication kind, choose a privacy level, then in the Navigator pick
a table. Apply a filter and a "Keep Top N" in Power Query — with the connector,
both **fold** (visible in *View Native Query*), and you can switch the model to
**DirectQuery**.

## Verify folding

- In Power Query, right-click a step → **View Native Query** should be enabled and
  show the generated SQL with the correct quote char and `LIMIT`/`FETCH`.
- Headless: the repository's `tests/integration/test_bi_folding.c` exercises the
  same generated-SQL patterns at the ODBC layer against a live server.

## Backend dialect notes

`LimitClauseKind` is chosen from the `Backend` value (see `AnsiOffsetBackends` in
`Argus.pq`). If you add a backend whose top-N syntax differs, adjust that set —
Trino/Presto-family use ANSI `OFFSET…FETCH`; Hive/Impala/MySQL-wire/BigQuery/
Pinot/Druid use `LIMIT…OFFSET`.

## Files

| File | Purpose |
|------|---------|
| `Argus.pq` | Connector M code (data source, folding capabilities, auth) |
| `Argus.query.pq` | Smoke tests (folding paths) for the SDK |
| `resources.resx` | UI strings |
| `Argus.mproj` | Power Query SDK project |
| `Argus{16..80}.png` | Get Data icons |
