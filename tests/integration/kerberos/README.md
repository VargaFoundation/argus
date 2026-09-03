# Kerberos (GSSAPI) integration validation

Validates the driver's binary-Thrift GSSAPI SASL handshake
(`argus_thrift_sasl_handshake_gssapi`) against a real MIT KDC and a Kerberized
HiveServer2. This is the Linux/GSSAPI side of Kerberos; the Windows/SSPI path
shares the same SASL wire framing and QoP negotiation and differs only in token
generation.

This stack is kept separate from the main `docker-compose.yml`: it needs a KDC,
a purpose-built keytab, and FQDN/SPN alignment that would make the standard
integration job slower and more fragile. Run it on demand.

## Stack

- `Dockerfile.kdc` / `kdc.conf` / `entrypoint-kdc.sh` — a minimal MIT KDC for
  realm `EXAMPLE.COM`. On first boot it creates `hive/hive.example.com` and
  `testuser`, and exports the Hive keytab into a shared volume.
- `krb5.conf.container` / `krb5.conf.host` — client configs (KDC via docker
  alias vs. `127.0.0.1:88`). `dns_canonicalize_hostname=false` and `rdns=false`
  are required so the SPN host is not rewritten.
- `core-site.xml` — puts Hadoop UGI in Kerberos mode (authentication only; the
  service-level authorization check crash-loops HS2 in this local setup).
- `kerberos-compose.yml` — the KDC + a Kerberized `apache/hive:4.0.0`. The
  Kerberos settings come from `SERVICE_OPTS` (not a replaced `hive-site.xml`, so
  the image's scratch-dir config stays intact). HS2 is published on **10001** so
  it coexists with the stock NOSASL hiveserver2 on 10000.
- `hiveserver2-krb-http` (compose profile `http`) — the same image in
  `transport.mode=http`, which under KERBEROS means **SPNEGO**. Published on
  **10004**. It is behind a profile because it needs the same
  `hive.example.com` network alias as the binary service, and only one
  container may hold that name.

## Run

```sh
cd tests/integration/kerberos
docker compose -f kerberos-compose.yml up -d          # KDC + Kerberized Hive
docker compose -f kerberos-compose.yml logs -f hiveserver2-krb   # wait for port 10000

# Build the driver WITH GSSAPI (krb5-gssapi.pc on PKG_CONFIG_PATH, or
# -DGSSAPI_LIB/-DGSSAPI_INCLUDE); the configure banner must say
#   GSSAPI (Kerberos) auth: ENABLED

# Obtain a ticket (password: testpass) into a FILE ccache:
kinit testuser@EXAMPLE.COM        # needs krb5.conf.host as /etc/krb5.conf

# Connect via 127.0.0.1 while keeping the hive.example.com SPN:
KRB5_CONFIG=$PWD/krb5.conf.host KRB5CCNAME=FILE:/tmp/krb5cc ARGUS_LOG_LEVEL=6 \
HIVE_HOST=127.0.0.1 HIVE_PORT=10001 KRB_HOST_FQDN=hive.example.com \
  ./build/tests/test_hive_kerberos
```

Success: `SQLDriverConnect` returns `SQL_SUCCESS`, the log shows
`Hive: SASL handshake completed successfully`, and `SELECT 1` returns `1`.

The `KrbHostFQDN` override (here `hive.example.com`) is what lets the TCP host be
`127.0.0.1` while the service principal stays `hive/hive.example.com@EXAMPLE.COM`
— the same knob production needs behind a load balancer.

## SPNEGO over HTTP

The binary path above is the driver's own SASL/GSSAPI handshake. The HTTP path
is a different mechanism: `HttpPath` selects Thrift-over-HTTP, and under
`AuthMech=KERBEROS` the transport sets `CURLAUTH_NEGOTIATE`, delegating the
Negotiate exchange to libcurl against the ambient credential cache.

**There is no `KrbHostFQDN` equivalent here.** libcurl derives the service
principal from the URL host, so the host you dial *is* the SPN host. Dialing
`127.0.0.1:10004` asks for `HTTP/127.0.0.1@EXAMPLE.COM`, which the server —
configured with `spnego.principal=HTTP/_HOST@EXAMPLE.COM`, i.e.
`HTTP/hive.example.com` — will not accept. The name has to line up on both
sides.

So run the HTTP test where `hive.example.com` resolves to the server. Either
join the compose network:

```sh
# Name the service explicitly: a bare `--profile http up` also starts the
# binary-SASL hiveserver2, and the two then fight over the hive.example.com alias.
docker compose -f kerberos-compose.yml --profile http up -d kdc hiveserver2-krb-http
docker compose -f kerberos-compose.yml logs -f hiveserver2-krb-http   # wait for 10001

# Run the test from a container on the stack's network, where Docker DNS
# resolves hive.example.com and the SPN matches with no host-level setup.
docker run --rm --network kerberos_default \
  -v "$PWD/../../..":/w -v "$PWD/krb5.conf.container":/etc/krb5.conf:ro \
  -e HIVE_HOST=hive.example.com -e HIVE_PORT=10001 \
  <image-with-the-built-driver> \
  sh -c 'echo testpass | kinit testuser@EXAMPLE.COM && ./build/tests/test_hive_kerberos_http'
```

…or add `127.0.0.1 hive.example.com` to `/etc/hosts` and run it from the host
against port 10004 with `HIVE_HOST=hive.example.com`.

Verified on 2026-09-03 against this stack: with a TGT the test passes and the
ticket cache gains `HTTP/hive.example.com@EXAMPLE.COM`, which only the SPNEGO
exchange can have fetched; with `kdestroy` first, the same test fails on
`HTTP 401`. Both halves matter -- a green run alone would not prove the server
was enforcing anything.

The KDC also registers `HTTP/127.0.0.1@EXAMPLE.COM`. It is unused by the
default config above, and only becomes usable if you override
`hive.server2.authentication.spnego.principal` to that literal name — the
escape hatch for a machine where neither Docker DNS nor `/etc/hosts` is
available.
