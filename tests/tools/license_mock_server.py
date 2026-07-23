#!/usr/bin/env python3
"""Reference / test license server for the Argus ODBC driver enterprise edition.

Models the OPTIONAL, non-blocking online layer (heartbeat + revocation). It
accepts a heartbeat POST with a strict JSON whitelist (no PII), and replies with
a *signed* status document — itself an `ARGUSLIC1:` blob signed by the same dev
key — so the driver can verify it offline with the embedded public key and a
revocation cannot be forged or hand-edited. Intended for local inspection and
tests, NOT a production server. The real service self-hosts on your infra and
keeps the signing key offline/HSM.

Usage:
    python3 license_mock_server.py --key ./devkeys/argus_license_dev.key \
        [--port 8090] [--path /v1/heartbeat] [--revoke ARG-2026-0007 ...] \
        [--strict] [--capture FILE]

The driver's online layer (argus_ee) would POST to
    http://127.0.0.1:8090/v1/heartbeat
and verify the returned `license_status` token against its embedded key.
"""
import argparse
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gen_test_license import load_priv, sign_token          # noqa: E402

# Heartbeat envelope: strict whitelist, mirroring the telemetry posture.
ENVELOPE_FIELDS = {
    "lid", "org_id", "install_id", "driver_version", "build_id",
    "os", "arch", "os_version", "nonce", "exp",
}
FORBIDDEN_KEYS = {"host", "hostname", "server", "user", "username", "password",
                  "database", "schema", "table", "column", "query", "sql",
                  "message"}


def validate(env):
    problems = []
    if not isinstance(env, dict):
        return ["envelope is not a JSON object"]
    extra = set(env) - ENVELOPE_FIELDS
    if extra:
        problems.append(f"unknown envelope field(s): {sorted(extra)}")
    if "lid" not in env:
        problems.append("missing required field: lid")
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
            print(json.dumps(env, indent=2, sort_keys=True))
        if problems:
            print("!! VALIDATION PROBLEMS:", *problems, sep="\n   ")
        if self.server.capture:
            with open(self.server.capture, "ab") as f:
                f.write(raw + b"\n")
        if self.server.strict and problems:
            self.send_response(400)
            self.end_headers()
            return

        lid = (env or {}).get("lid", "")
        nonce = (env or {}).get("nonce")
        revoked = lid in self.server.revoked
        now = int(time.time())
        status_claims = {
            "typ": "status",
            "lid": lid,
            "status": "revoked" if revoked else "active",
            "not_after": now + 30 * 86400,
            "server_time": now,
            "nonce": nonce,               # echo the client nonce (anti-replay)
        }
        # The status doc is itself a signed ARGUSLIC1: blob → unforgeable.
        token = sign_token(self.server.priv, status_claims)
        body = json.dumps({"license_status": token}).encode("utf-8")
        print(f"-> reply: status={status_claims['status']} (signed)")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True, help="signing private key (PEM)")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--path", default="/v1/heartbeat")
    ap.add_argument("--revoke", action="append", default=[],
                    metavar="LID", help="mark a license id revoked (repeatable)")
    ap.add_argument("--strict", action="store_true",
                    help="reply 400 on any validation problem")
    ap.add_argument("--capture", default=None, help="append raw payloads to FILE")
    args = ap.parse_args()

    srv = HTTPServer(("127.0.0.1", args.port), Handler)
    srv.priv = load_priv(args.key)
    srv.revoked = set(args.revoke)
    srv.strict = args.strict
    srv.capture = args.capture
    print(f"Argus license mock server on http://127.0.0.1:{args.port}{args.path}"
          f"  (revoked: {sorted(srv.revoked) or 'none'})", file=sys.stderr)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
