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
