# Security Policy

## Supported versions

Argus has not yet cut a tagged release; until the first release, only the tip
of `main` is supported. Once releases exist, the latest minor release line will
receive security fixes.

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
