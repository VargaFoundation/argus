# Security Policy

## Supported versions

Security fixes go to the latest minor release line and to the tip of `main`.
Older minor lines are not backported to; upgrading to the current minor is
the supported path.

| Version | Supported |
|---------|-----------|
| 0.6.x   | Yes       |
| < 0.6   | No        |

## What the released binaries carry

Every published artefact is built with the hardening flags the project's
`ARGUS_HARDENING` option sets — stack protector, stack-clash protection,
control-flow protection, `_FORTIFY_SOURCE=2`, full RELRO and BIND_NOW on
ELF, `/GS /guard:cf` and ASLR on Windows — and `scripts/check-hardening.sh`
fails the release if one of them is missing. Only the ODBC entry points are
exported; `scripts/check-exports.sh` pins that list.

Linux packages and the checksum manifest are GPG-signed (the public key is in
`KEYS`), and the Windows driver and installer are Authenticode-signed. Verify
a download against `SHA256SUMS` and its `.asc` before installing it.

## Reporting a vulnerability

Please **do not open a public issue** for a suspected vulnerability.

- Report privately via GitHub Security Advisories on this repository
  (*Security → Report a vulnerability*), or
- email `security@varga.foundation`.

Include the driver version (or commit), the backend involved, a reproduction
(connection string shape with secrets redacted, SQL if relevant), and the
observed impact.

We aim to acknowledge reports within 7 days. Fixes are developed privately and
credited to the reporter unless anonymity is requested.

## Scope notes

- The driver parses untrusted server responses (JSON, Thrift, MySQL wire,
  Arrow Flight); memory-safety issues in those paths are in scope and
  high-priority.
- SASL/Kerberos and TLS handshake code (`src/backend/thrift_sasl.c`, transport
  layers) is in scope.
- `SSLVerify=0` and other explicitly-documented insecure toggles are not
  vulnerabilities by themselves.
- The opt-in telemetry whitelist is documented in `docs/TELEMETRY.md` and
  `PRIVACY.md`; any data leaving the process beyond that whitelist is a
  reportable bug.
