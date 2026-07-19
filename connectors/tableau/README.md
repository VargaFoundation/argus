# Argus — Tableau connectors

Tableau connectors (`.taco`) for the Argus ODBC driver.

> **Path to a signed, published connector** (the two steps that remain, both
> external to this repo):
> 1. **A public-CA code-signing certificate.** `build.ps1` already wires
>    `jarsigner` (via Tableau's `connector-packager`, with a keystore and a
>    mandatory timestamp); it only needs a certificate from a trusted CA — a
>    self-signed one builds a `.taco` but Tableau refuses to load it. This is
>    procurement, not code.
> 2. **Tableau Exchange submission**, which requires a
>    [TDVT](https://tableau.github.io/connector-plugin-sdk/docs/tdvt) pass rate.
>    TDVT drives Tableau Desktop's `tabquerytool`, so it must run on a machine
>    with Tableau installed — it cannot run in a headless Linux CI. It **has**
>    been run for `argus-trino` (expression suite: **91.4 %**, 703/769, with the
>    `dialect.tdd` overrides); see [`tdvt/README.md`](tdvt/README.md) for the
>    turnkey recipe.
>
> Kerberos, by contrast, is **not** a gap: the Linux release builds already ship
> it (CI installs `libkrb5-dev`, the `.deb` depends on `libgssapi-krb5-2`), and
> Windows uses native SSPI.

## Why a connector rather than "Other Databases (ODBC)"

Tableau can already reach Argus through its generic **Other Databases (ODBC)**
entry, but Tableau
[says plainly](https://help.tableau.com/current/pro/desktop/en-us/examples_otherdatabases.htm)
that with it "the outcome may vary and compatibility with Tableau Desktop
features is not guaranteed", support is limited, and every viewer must
independently install the driver and configure a matching DSN. A named connector
fixes the dialect, ships the capability customizations, and can be published.

Note what this connector does **not** do: it does not make `{fn ...}` work. The
driver translates ODBC escape sequences itself (`src/odbc/escape.c`), which is
what generic ODBC clients — Excel, Qlik, Alteryx — depend on too. The `.taco`
adds dialect-accurate SQL generation and a real connection dialog on top of a
driver that is already correct.

## One connector per backend

A `.taco` declares exactly **one** `<dialect>`. Unlike the Power BI `.mez` —
which picks its dialect at runtime from a Backend dropdown — the SQL dialect is
fixed when the connector is built, so each backend family needs its own
connector. Tableau ships base dialects that line up with Argus's backends:

| Connector | `BACKEND=` | Default port | Base dialect |
|---|---|---|---|
| `argus-trino` | `trino` | 8080 | `PrestoDialect` |
| `argus-hive` | `hive` | 10000 | `Hive12Dialect` |
| `argus-impala` | `impala` | 21050 | `Impala23Dialect` |
| `argus-mysql` | `mysql` | 9030 | `MySQL8Dialect` |
| `argus-bigquery` | `bigquery` | — (REST) | `BigQuerySQLDialect` |

`argus-hive` also serves **Spark Thrift Server** and **Flink SQL Gateway**, which
speak HiveServer2.

Two scoping notes worth knowing before you pick one:

- **ClickHouse is not covered by `argus-mysql`.** It speaks the MySQL wire
  protocol (so `BACKEND=mysql` reaches it) but its SQL is its own, and
  `MySQL8Dialect` would generate functions it spells differently. Use the
  generic ODBC connector for ClickHouse. The same split is called out in
  `src/odbc/dialect.c`.
- **`argus-bigquery` is the odd one out.** BigQuery is a REST service, so the
  driver ignores host and port and the dialog asks for a project instead. That
  shape is unusual for Tableau's ODBC superclass, which makes this the connector
  most likely to need adjusting once tried in Desktop.

The remaining backends (phoenix, pinot, druid, flightsql, kudu) have no close
base dialect and would need a full custom `dialect.tdd`. They work through
Tableau's generic **Other Databases (ODBC)** in the meantime.

## Files

| File | Role |
|---|---|
| `manifest.xml` | Declares the connector; carries `<connection-customization>` — the **embedded TDC** (`CAP_*` capabilities). A separate `.tdc` file is not needed. |
| `connectionFields.xml` | The connection dialog's fields |
| `connectionMetadata.xml` | Whether database/schema levels exist |
| `connectionResolver.tdr` | Matches the installed `Argus ODBC Driver`; points at the builder script |
| `connectionBuilder.js` | Maps Tableau's attributes onto the driver's connection keywords |
| `dialect.tdd` | SQL generation; inherits a Tableau base dialect |

The driver is **not** bundled — install it separately from the
[releases](https://github.com/VargaFoundation/argus/releases).

## Build

```powershell
pwsh connectors/tableau/build.ps1 -OutDir dist
```

Uses Tableau's own `connector-packager` (cloned at a pinned SDK ref), which
validates, zips and optionally signs.

To sign — required for any real deployment:

```powershell
$env:TACO_KEYSTORE = 'C:\secure\codesign.jks'
$env:TACO_ALIAS    = 'varga'
pwsh connectors/tableau/build.ps1 -OutDir dist
```

### Signing is the hard constraint

A `.taco` is functionally a JAR. Tableau
[verifies it](https://tableau.github.io/connector-plugin-sdk/docs/package-sign)
against the JRE's default keystore and requires:

- a **code-signing certificate from a public CA** (self-signed will not load),
- a **timestamp** (`jarsigner -tsa ...`) — Tableau rejects an untimestamped
  `.taco` — ideally valid ≥ 5 years.

This is a **third, independent** signing path: the driver uses Azure Trusted
Signing and the Power BI connector uses a MakePQX `.pfx`. Neither can sign a
`.taco`.

Without a certificate the build still emits a working `.taco`, but Tableau loads
it only with verification disabled — Desktop:
`-DDisableVerifyConnectorPluginSignature=true`; Server: the TSM setting
`native_api.disable_verify_connector_plugin_signature`. Use that for development
only.

## Validate

Schema check (no Tableau needed) — catches invented elements/attributes:

```bash
pip install xmlschema
python connectors/tableau/validate-xml.py
```

This validates against the SDK's published XSDs. **Passing it does not mean the
connector works.** It only proves the XML is well-formed and schema-valid.

## Test for real

Nothing below is optional before shipping; the schema check cannot substitute
for any of it.

1. Install the Argus ODBC driver and start a backend
   (`docker compose -f tests/integration/docker-compose.yml up -d trino`).
2. Copy the `.taco` to `~/Documents/My Tableau Repository/Connectors` (or
   `%USERPROFILE%\Documents\My Tableau Repository\Connectors`).
3. Launch Tableau Desktop with signature verification off while iterating:
   `tableau.exe -DDisableVerifyConnectorPluginSignature=true`
4. Connect, drag in `nation` and `region`, then apply a filter, a GROUP BY and a
   top-N.
5. **Read the generated SQL** — Tableau's log (`My Tableau Repository/Logs/log.txt`)
   or Trino's `system.runtime.queries`. It must contain no `{fn`, use the right
   quote character, and fold the top-N to `LIMIT`/`OFFSET…FETCH`.
6. Run [TDVT](https://tableau.github.io/connector-plugin-sdk/docs/tdvt) against
   the same backend. TDVT is what turns "it seemed to work" into a number, and
   it is a prerequisite for a Tableau Exchange submission.
7. Load the connector **with a real signature** before calling it done — the
   signing path is the most likely thing to fail late.
