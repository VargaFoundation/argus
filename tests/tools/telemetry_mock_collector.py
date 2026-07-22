#!/usr/bin/env python3
"""Reference / test telemetry collector for the Argus ODBC driver.

Accepts POST <path> with a JSON telemetry envelope, validates it against the
documented whitelist (rejecting unknown fields — defence-in-depth), pretty-prints
each payload, and replies 204. Intended for local inspection and tests, NOT as a
production collector.

Usage:
    python3 telemetry_mock_collector.py [--port 8080] [--path /v1/events] \
                                        [--capture FILE] [--strict]

Point the driver at it:
    export ARGUS_TELEMETRY=1
    export ARGUS_TELEMETRY_ENDPOINT="http://127.0.0.1:8080/v1/events"
"""
import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

ENVELOPE_FIELDS = {
    "schema_version", "install_id", "driver_version", "build_id",
    "os", "arch", "os_version", "events",
}
EVENT_FIELDS = {
    "connect": {"type", "backend", "latency_ms", "success", "attempts"},
    "statement": {"type", "backend", "execute_ms", "rows_bucket", "errors"},
    "error": {"type", "backend", "sqlstate", "sqlclass", "native"},
    "session": {"type", "backend", "errors"},
}
# Substrings that must never appear in a payload (guards against PII leaks).
FORBIDDEN_KEYS = {"host", "hostname", "server", "user", "username", "password",
                  "token", "database", "schema", "table", "column", "query",
                  "sql", "message"}


def validate(env):
    problems = []
    extra = set(env) - ENVELOPE_FIELDS
    if extra:
        problems.append(f"unknown envelope field(s): {sorted(extra)}")
    for i, ev in enumerate(env.get("events", [])):
        t = ev.get("type")
        allowed = EVENT_FIELDS.get(t)
        if allowed is None:
            problems.append(f"event[{i}]: unknown type {t!r}")
            continue
        bad = set(ev) - allowed
        if bad:
            problems.append(f"event[{i}] ({t}): unexpected field(s): {sorted(bad)}")
    # Whole-payload forbidden-key scan.
    flat = json.dumps(env).lower()
    for k in FORBIDDEN_KEYS:
        if f'"{k}"' in flat:
            problems.append(f"forbidden key present: {k!r}")
    return problems


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n)
        try:
            env = json.loads(raw)
            problems = validate(env)
        except json.JSONDecodeError as e:
            env, problems = None, [f"invalid JSON: {e}"]

        print(f"\n=== POST {self.path} ({n} bytes) ===")
        if env is not None:
            print(json.dumps(env, indent=2))
        if problems:
            print("!! VALIDATION PROBLEMS:", *problems, sep="\n   ")
        if self.server.capture:
            with open(self.server.capture, "ab") as f:
                f.write(raw + b"\n")
        if self.server.strict and problems:
            self.send_response(400)
            self.end_headers()
            return
        self.send_response(204)
        self.end_headers()

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--path", default="/v1/events")
    ap.add_argument("--capture", default=None, help="append raw payloads to FILE")
    ap.add_argument("--strict", action="store_true",
                    help="reply 400 on any validation problem")
    args = ap.parse_args()

    srv = HTTPServer(("127.0.0.1", args.port), Handler)
    srv.capture = args.capture
    srv.strict = args.strict
    print(f"Argus telemetry mock collector on http://127.0.0.1:{args.port}{args.path}",
          file=sys.stderr)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
