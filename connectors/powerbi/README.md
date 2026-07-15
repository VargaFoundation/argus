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
2. **Power Query SDK** — only to *build* the connector: the `build.ps1` script
   below, the "Power Query SDK" VS Code extension, or `MakePQX` directly.

## Build the `.mez`

The `.mez` is a zip of this folder's sources; it is not checked in — build it with
Microsoft's `MakePQX` (from the `Microsoft.PowerQuery.SdkTools` NuGet package).
`build.ps1` automates the whole dance (fetch the tool, stage the sources, compile,
and optionally sign) and is what CI runs:

- **Script (recommended)** — `pwsh connectors/powerbi/build.ps1 -OutDir dist`
  produces `dist/Argus.mez`. Windows + PowerShell only (`MakePQX` is a Windows
  tool). It downloads the SDK tools on first run and caches them.
- **VS Code** — open this folder, install the *Power Query SDK* extension, then
  run **Build**. Output: `bin/…/Argus.mez`.

`MakePQX compile` performs a real M parse: a syntax error aborts before any `.mez`
is written, so a produced file means the connector compiled.

**CI builds it automatically.** Every push/PR compiles the `.mez` as a smoke test
(`.github/workflows/ci.yml` → `build-powerbi-connector`), and tagged releases
attach `Argus.mez` — plus a signed `Argus.pqx` when a certificate is configured —
to the GitHub Release (`.github/workflows/release.yml`).

## Signing

A signed connector is a **`.pqx`**, not a signed `.mez`: `MakePQX pack` wraps the
`.mez` and `MakePQX sign` signs it with a **code-signing `.pfx`**. Power BI clients
then trust it by its certificate **thumbprint**, with no per-user security toggle.

`build.ps1` emits `Argus.pqx` (and prints the thumbprint) when a cert is supplied
through two environment variables:

```powershell
$env:PQ_SIGNING_CERT_BASE64   = [Convert]::ToBase64String([IO.File]::ReadAllBytes('codesign.pfx'))
$env:PQ_SIGNING_CERT_PASSWORD = 'your-pfx-password'
pwsh connectors/powerbi/build.ps1 -OutDir dist    # -> dist/Argus.pqx (+ Argus.pqx.thumbprint.txt)
```

For CI, set those same two values as **secrets** `PQ_SIGNING_CERT_BASE64` and
`PQ_SIGNING_CERT_PASSWORD` in the `main` environment. With no cert configured the
build degrades to an unsigned `.mez` and never blocks the release.

> **Why a separate certificate from the driver?** The driver DLL and installer are
> Authenticode-signed via **Azure Trusted Signing**. `MakePQX` signs a `.pqx` only
> with a local `.pfx` (no Trusted Signing / Key Vault path), so connector signing
> keeps its own PFX secret. A self-signed cert is fine for internal distribution:
>
> ```powershell
> $c = New-SelfSignedCertificate -Type CodeSigning -Subject 'CN=Argus Connector' `
>        -CertStoreLocation Cert:\CurrentUser\My
> Export-PfxCertificate -Cert $c -FilePath codesign.pfx `
>        -Password (ConvertTo-SecureString 'your-pfx-password' -AsPlainText -Force)
> ```

Give clients the thumbprint (printed by the build, or from `MakePQX verify
Argus.pqx`) to trust it: add the thumbprint to the `REG_MULTI_SZ` value
`TrustedCertificateThumbprints` under
`HKLM\Software\Policies\Microsoft\Power BI Desktop` (admin/GPO-controlled). Power
BI then loads the signed `.pqx` at the *Recommended* security level with no
downgrade — see
[Microsoft's guidance](https://learn.microsoft.com/power-bi/desktop-trusted-third-party-connectors).

## Install into Power BI Desktop

Copy the connector to `Documents\Power BI Desktop\Custom Connectors\` (create the
folder if needed) and restart Power BI Desktop. Then either:

- **Unsigned `Argus.mez`** — File → Options and settings → Options → Security →
  Data Extensions → allow *"(Not Recommended) Allow any extension to load without
  validation or warning"*.
- **Signed `Argus.pqx`** (preferred) — trust its thumbprint via the trusted
  third-party connectors registry/GPO setting; no security downgrade required.

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
| `build.ps1` | Build/sign the `.mez`/`.pqx` (used locally and by CI) |
