# Telemetry

Argus can send **anonymous, aggregate usage telemetry** to help the Varga
Foundation understand which backends, platforms, and error patterns matter in
practice. It is **opt-in and off by default** — nothing is ever sent unless you
explicitly enable it.

This document describes exactly what is (and is not) collected, how to turn it
on or off, how to inspect it, and the collector API. See also [PRIVACY.md](../PRIVACY.md).

## TL;DR

- **Default: disabled.** No network connection to any telemetry endpoint is made.
- **Enable per connection:** add `TELEMETRY=1` to the DSN or connection string.
- **Enable machine-wide:** `export ARGUS_TELEMETRY=1`.
- **Hard off (overrides everything):** `export ARGUS_TELEMETRY=0`.
- **Compile it out entirely:** `cmake -DARGUS_ENABLE_TELEMETRY=OFF`.

## What is collected

Only this strict whitelist of non-identifying fields ever leaves your machine.

Envelope (once per batch):

| Field | Example | Notes |
|-------|---------|-------|
| `schema_version` | `1` | Payload format version |
| `install_id` | `efc4d961-…` | Random UUID, **not** derived from any hardware/user id; resettable (see below) |
| `driver_version` | `0.5.9` | Driver version |
| `build_id` | `2c4166d` | Short git SHA of the build |
| `os` | `linux` / `windows` / `darwin` | OS family |
| `arch` | `x86_64` / `aarch64` | CPU architecture |
| `os_version` | `10.0.22631` | OS release/build string |

Events (batched in `events[]`):

| `type` | Fields | Meaning |
|--------|--------|---------|
| `connect` | `backend`, `latency_ms`, `success`, `attempts` | A connection attempt outcome |
| `statement` | `backend`, `execute_ms`, `rows_bucket`, `errors` | A statement's timing; row count is **bucketed** (`0`, `1-9`, `10-99`, …, `1M+`), never exact |
| `error` | `backend`, `sqlstate`, `sqlclass`, `native` | An execution error — the **SQLSTATE code only** |
| `session` | `backend`, `errors` | A disconnect summary |

### What is NEVER collected

By construction, the following never enter a telemetry payload:

- Hostnames, IP addresses of your servers, ports
- User names, passwords, tokens, or any credential
- Database, schema, catalog, table, or column names
- **Query text** (not even truncated)
- **Backend error messages** — only the 5-character SQLSTATE. Error message text
  routinely contains table/column names and query fragments, so it is dropped at
  the source in `argus_telemetry_error()`.

> Note: the collector, like any HTTP server, observes the **source IP** of the
> request. Argus cannot remove that client-side; the collector is contractually
> required not to retain it — see [PRIVACY.md](../PRIVACY.md) and the collector
> spec below.

## Turning it on and off

Precedence (highest first):

1. `ARGUS_TELEMETRY=0` — environment kill switch, overrides everything.
2. `ARGUS_TELEMETRY=1` — machine-wide opt-in.
3. `TELEMETRY=1` (DSN key or connection-string keyword, alias `ENABLETELEMETRY`)
   — per-connection opt-in.
4. Otherwise: **off**.

The endpoint is configurable with `ARGUS_TELEMETRY_ENDPOINT` (defaults to the
compiled-in `ARGUS_TELEMETRY_ENDPOINT` CMake value).

## The install id

On first activation a random UUID is written to:

- POSIX: `$XDG_CONFIG_HOME/argus/install_id` (usually `~/.config/argus/install_id`)
- Windows: `%APPDATA%\argus\install_id`

It is a random value used only to de-duplicate events from the same install; it
is **not** tied to your machine, user, or hardware. Delete the file to reset it.
A one-time notice is logged (at INFO) the first time telemetry is active; the
marker `telemetry_notice_shown` in the same directory suppresses repeats.

## Inspecting what is sent

Run with debug logging to see activity, and point the driver at your own
collector to see the raw payloads:

```bash
export ARGUS_LOG_LEVEL=5           # DEBUG
export ARGUS_TELEMETRY=1
export ARGUS_TELEMETRY_ENDPOINT="http://127.0.0.1:8080/v1/events"
```

A reference mock collector that prints every payload is in
[`tests/tools/telemetry_mock_collector.py`](../tests/tools/telemetry_mock_collector.py).

## Collector API contract

`POST <endpoint>` (default path `/v1/events`), `Content-Type: application/json`.

Request body is the envelope above with an `events` array. Example:

```json
{
  "schema_version": 1,
  "install_id": "efc4d961-78f2-4947-9b8e-5050731eb772",
  "driver_version": "0.5.9",
  "build_id": "2c4166d",
  "os": "linux", "arch": "x86_64", "os_version": "5.15.0",
  "events": [
    {"type":"connect","backend":"trino","latency_ms":12.5,"success":true,"attempts":1},
    {"type":"statement","backend":"trino","execute_ms":4.2,"rows_bucket":"1k-9k","errors":0},
    {"type":"error","backend":"trino","sqlstate":"42S02","sqlclass":"42","native":1003},
    {"type":"session","backend":"trino","errors":1}
  ]
}
```

Response: `204 No Content` on success (`2xx` accepted). The driver ignores the
body and never retries beyond best-effort delivery.

### Server requirements (Varga Foundation collector)

- **HTTPS only**, valid certificate (the driver verifies peer and host).
- **Strict schema validation**: reject unknown fields (defence-in-depth against a
  client bug ever leaking something outside the whitelist).
- **Do not persist the source IP** in clear text. Drop it, or keep only a
  short-lived salted hash for abuse mitigation; coarse geo (country) at most.
- Payload size cap and per-IP rate limiting; no authentication (public ingest)
  but abuse protection.
- **Retention:** raw events kept briefly (e.g. 30–90 days), then reduced to
  anonymous aggregates (counts per backend/OS/SQLSTATE, latency histograms).
- Publish a privacy notice at the endpoint host.

## Client behaviour guarantees

- **Asynchronous & best-effort.** Events are queued and sent by a single
  background thread. Emission never blocks, slows, or fails an ODBC call.
- **Bounded.** The queue is capped (1000 events); under backpressure new events
  are dropped, never buffered without limit.
- **Silent on failure.** A slow or unreachable collector produces no error and no
  user-visible latency; the driver keeps working exactly as if telemetry were off.
