# Argus ODBC Driver — Enterprise Licensing

The **open-source driver (Apache-2.0) needs no license** and is fully functional
on its own. Licensing applies only to the **enterprise edition** — the open
driver linked with the closed `argus_ee` add-on, which lights up the enterprise
capability taps (secret/Vault resolution, OAuth token cache, statement
guardrails, multi-host failover, and enterprise-only backends). The enterprise
edition **refuses to open a connection without a valid license** and reports
SQLSTATE `08004` with the message *"Enterprise license required: …"*.

> The community driver silently ignores every setting on this page. If you are
> not running the enterprise edition, nothing here applies.

## What a license is

A license is a compact, cryptographically **signed token** (`ARGUSLIC1:…`). It is
verified **offline** against a public key baked into the enterprise binary — the
private signing key is held only by the Varga Foundation and never ships, so a
token cannot be forged even with full knowledge of the binary. A token encodes
your organization, the entitled features/backends, seat count, and an expiry.

Because verification is offline, the driver makes **no network call on the
connection path** — it works air-gapped, behind proxies, and inside BI/ETL
hosts with no outbound access. An optional, non-blocking background channel adds
revocation and seat reporting for machines that can reach the internet (see
[Online layer](#optional-online-layer)); it never delays or blocks a connection.

## Supplying the license

Sources are consulted in priority order; the **first one found wins**, and
machine-wide policy always beats a user- or DSN-supplied value:

| # | Source | Where | Best for |
|---|--------|-------|----------|
| 1 | `ARGUS_LICENSE` | Environment variable (inline token) | Containers, CI, ad-hoc override |
| 2 | `ARGUS_LICENSE_FILE` | Environment variable (path to a token file) | Containers, mounted secrets |
| 3 | Machine-wide managed store | Registry / file / plist (see below) | **MDM fleets — recommended** |
| 4 | `License=` / `LicenseKey=` | DSN or connection string | Per-application convenience |

A DSN/connection-string token (row 4) is a convenience only: it can supply a
license where none exists, but it **cannot downgrade** a valid machine-wide
policy.

### Machine-wide store (recommended for managed fleets)

| OS | Location |
|----|----------|
| Windows | Registry value `License` under `HKLM\SOFTWARE\Varga\Argus` (**64-bit view**), and/or the file `%ProgramData%\Varga\Argus\license.jwt` |
| macOS | Managed preference `License` in `/Library/Managed Preferences/org.vargafoundation.argus.plist`, and/or `/Library/Application Support/Varga/Argus/license.jwt` |
| Linux | `/etc/argus/license` |

## Deploying via MDM (Microsoft Intune)

The driver already ships as a silent [Intune Win32 app](../installer/intune/)
(NSIS `/S`, installed in **System** context). **Deliver the license separately
from the driver package** so it can rotate on its own cadence without
redeploying the driver:

- **Windows (recommended):** push the registry value
  `HKLM\SOFTWARE\Varga\Argus\License` (REG_SZ, 64-bit) via an Intune **Settings
  Catalog / OMA-URI** profile, or a **Proactive Remediation** script that keeps
  it current. Alternatively drop `%ProgramData%\Varga\Argus\license.jwt`.
- **macOS:** deliver the managed plist via a **Configuration Profile** (works
  alongside the notarized `.pkg`).
- **Linux:** place `/etc/argus/license` via your configuration management (or a
  container mount + `ARGUS_LICENSE_FILE`).

### Fleet pitfalls to avoid

- **Machine, not user:** put policy in **HKLM**, never HKCU — a user value would
  not be treated as the machine default.
- **64-bit registry view:** the 64-bit driver reads the 64-bit hive. A value
  written by 32-bit PowerShell lands in `WOW6432Node` and is invisible. Force the
  64-bit view.
- **Env vars and running services:** a newly pushed system environment variable
  is not seen by already-running BI services until they restart; prefer the
  registry/file store for changes that must take effect immediately.

## Optional online layer

Off by default (pure offline). When enabled (`ARGUS_LICENSE_ONLINE=1`, or an
`online` claim in the token) and a license server URL is configured, a
background thread — modeled on the telemetry sender, fully asynchronous and
**fail-open** — performs, best-effort and never on the connection path:

- a **heartbeat** (~every 24 h) reporting only a license id, an anonymous
  install id, and driver/OS versions (never host, user, database, or SQL); and
- a **revocation** pull; a revoked license is cached (signed) and denied
  offline thereafter.

If the server is unreachable, nothing changes — the offline token remains
authoritative until its own expiry. Typical parameters: **30-day** token,
**14-day** grace past expiry (features keep working, a warning is logged),
renewal around day 15, revocation cache TTL 7 days. Set `ARGUS_LICENSE_ONLINE=0`
to hard-disable the channel.

## Troubleshooting

- **`08004` "Enterprise license required"** on connect: no valid license was
  found for this connection. Check, in order: `ARGUS_LICENSE` /
  `ARGUS_LICENSE_FILE`, the machine-wide store for your OS (and the 64-bit
  registry view on Windows), then any `License=` on the DSN. The diagnostic
  message names the specific reason (missing, expired, wrong signature, revoked,
  or feature-not-entitled).
- **Expired token:** within the grace window the connection still succeeds and a
  warning is logged; past grace it is denied. Renew the token.
- **Token in logs:** the driver masks any `LICENSE*` value in its redacted
  connection-string logs, so tokens are not written to log files.
