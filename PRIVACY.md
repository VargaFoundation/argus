# Privacy Notice — Argus ODBC Driver

_Last updated: 2026-07-20 · Maintained by the Varga Foundation_

The Argus ODBC driver can send **anonymous usage telemetry** to a Varga
Foundation collector. This notice explains what that means for your data. The
technical detail lives in [docs/TELEMETRY.md](docs/TELEMETRY.md).

## Opt-in by design

Telemetry is **disabled by default**. The driver makes **no** connection to any
telemetry endpoint unless you explicitly opt in, per connection (`TELEMETRY=1`)
or machine-wide (`ARGUS_TELEMETRY=1`). `ARGUS_TELEMETRY=0` is a hard kill switch,
and the feature can be removed entirely at build time
(`-DARGUS_ENABLE_TELEMETRY=OFF`).

This opt-in model is deliberate: it is the norm for database drivers, it survives
distribution packaging, and — for a foundation operating in the EU — it rests on
**consent** as the GDPR lawful basis rather than the contested "legitimate
interest / opt-out" approach.

## What we collect

Only a strict whitelist of non-identifying, aggregate signals: driver and build
version, OS family/architecture/version, backend name (e.g. `trino`), connection
and statement latencies, coarse row-count buckets, error counts, and SQLSTATE
error codes. A random, resettable install id de-duplicates events.

Full field-by-field list: [docs/TELEMETRY.md](docs/TELEMETRY.md#what-is-collected).

## What we do NOT collect

We never collect, and the driver never transmits: hostnames or server addresses,
user names or credentials, database/schema/table/column names, **query text**, or
**database error messages** (only the SQLSTATE code is sent). Row counts are
bucketed, never exact.

## Source IP

As with any network request, our collector observes the source IP of the
connection. The driver cannot strip this. Our collector does **not** retain
source IPs in clear text; at most a short-lived salted hash is kept for abuse
mitigation, with coarse (country-level) geolocation. IPs are never joined to the
telemetry payload to build a profile.

## Retention

Raw events are retained briefly (target 30–90 days) and then reduced to anonymous
aggregates. Aggregates contain no identifiers.

## Your controls

- **Reset your install id:** delete `~/.config/argus/install_id` (POSIX) or
  `%APPDATA%\argus\install_id` (Windows).
- **Turn it off any time:** `ARGUS_TELEMETRY=0`, or remove `TELEMETRY=1` from your
  DSN, or build with `-DARGUS_ENABLE_TELEMETRY=OFF`.
- **Audit it:** the driver is open source. The collection code is
  [`src/odbc/telemetry.c`](src/odbc/telemetry.c); you can watch every payload with
  the reference mock collector (see docs/TELEMETRY.md).

## Contact

Questions or data requests: the Varga Foundation (see the project repository for
current contact details).
