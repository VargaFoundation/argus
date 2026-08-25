# Configuration

## Registering with unixODBC

### Automatic

```bash
sudo bash scripts/install_dsn.sh
```

### Manual

Add to `/etc/odbcinst.ini`:

```ini
[Argus]
Description = Argus ODBC Driver for Hive, Impala, and Trino
Driver = /usr/local/lib/libargus_odbc.so
Setup = /usr/local/lib/libargus_odbc.so
```

Add a DSN to `/etc/odbc.ini` or `~/.odbc.ini`:

```ini
[ArgusHive]
Description = Hive via Argus
Driver = Argus
HOST = localhost
PORT = 10000
UID = hive
PWD =
DATABASE = default
AUTHMECH = NOSASL
BACKEND = hive
```

## Registering on Windows

The [NSIS installer](../installer/) copies the driver with its bundled DLLs and
registers `Argus ODBC Driver` under `HKLM\SOFTWARE\ODBC\ODBCINST.INI`. The
driver reads DSNs from the registry: `HKCU\SOFTWARE\ODBC\ODBC.INI\<dsn>`
first (user DSNs), then `HKLM` (system DSNs).

The driver setup has no configuration dialog, so create DSNs from PowerShell:

```powershell
Add-OdbcDsn -Name MyTrino -DriverName "Argus ODBC Driver" -DsnType User -Platform 64-bit `
    -SetPropertyValue @("BACKEND=trino", "HOST=trino.example.com", "PORT=8443", "SSL=1")

Remove-OdbcDsn -Name MyTrino -DsnType User -Platform 64-bit
```

> **Note:** the Windows ODBC Manager rejects `UID`/`PWD` in a DSN's
> `-SetPropertyValue`. Supply credentials at connect time (the application's
> user/password fields) or in a DSN-less connection string instead.

DSN-less connection strings (`DRIVER={Argus ODBC Driver};...`) need no DSN at
all.

## Connection String Parameters

Use with `SQLDriverConnect`:

```
DRIVER=Argus;BACKEND=hive;HOST=hive.example.com;PORT=10000;UID=admin;PWD={p@ss};Database=analytics;AuthMech=PLAIN
```

| Parameter | Aliases | Default | Description |
|-----------|---------|---------|-------------|
| HOST | SERVER | localhost | Server hostname or IP |
| PORT | | (per backend) | Server port |
| UID | USERNAME, USER | (empty) | Username for authentication |
| PWD | PASSWORD | (empty) | Password for authentication |
| DATABASE | SCHEMA | default | Initial database/catalog to use |
| AUTHMECH | AUTH | NOSASL | Authentication mechanism |
| KRBSERVICENAME | SERVICEPRINCIPALNAME | hive/impala | Kerberos SPN service name |
| KRBHOSTFQDN | KRBHOST | (HOST) | Kerberos SPN host, if it differs from HOST |
| KRBREALM | REALM | (from krb5.conf) | Explicit Kerberos realm |
| BACKEND | DRIVER_TYPE | hive | Backend type: hive, impala, trino, phoenix, pinot, druid, bigquery, mysql, postgres, greenplum, cloudberry, flightsql, kudu |
| APPLICATIONNAME | APPNAME | (none) | Client application name reported to the backend |
| FETCHBUFFERSIZE | | (backend default) | Rows fetched per backend round-trip |
| SOCKETTIMEOUT | | 0 (none) | Socket I/O timeout in seconds |
| MAXSCROLLROWS | | (driver default) | Cap on rows a static (scrollable) cursor will materialize in memory |
| SSLMODE | | (from SSL/SSLVerify) | PostgreSQL family: libpq `sslmode`, verbatim (`prefer`, `verify-ca`, …) |
| SEARCHPATH | CURRENTSCHEMA | (server default) | PostgreSQL family: `search_path` for the session |
| SHOWPARTITIONS | | 0 | PostgreSQL family: list partition children in `SQLTables`/`SQLColumns` |
| SHOWALLDATABASES | | 0 | PostgreSQL family: list every database, not just the connected one |
| ROWVERSIONING | | 0 | PostgreSQL family: expose `xmin` to `SQLSpecialColumns(SQL_ROWVER)` |

### Default Ports by Backend

| Backend | Default Port |
|---------|-------------|
| hive | 10000 |
| impala | 21050 |
| trino | 8080 |
| mysql | 3306 |
| postgres | 5432 |
| greenplum | 5432 |
| cloudberry | 5432 |
| flightsql | 32010 |
| pinot | 8000 |
| druid | 8888 |

### Authentication Mechanisms

| Value | Description |
|-------|-------------|
| NOSASL | No authentication (development/testing) |
| PLAIN | Username/password over SASL PLAIN |

## Backend-Specific Configuration

### Hive (BACKEND=hive)

```
DRIVER=Argus;BACKEND=hive;HOST=hive.example.com;PORT=10000;UID=hive;DATABASE=default;AUTHMECH=NOSASL
```

- Protocol: Thrift TCLIService (binary)
- Default port: 10000
- Protocol version: V10
- Database set via `use:database` config in OpenSession
- **Authentication** (`AuthMech`):
  - `NOSASL` (default): no SASL layer.
  - `PLAIN` / `LDAP`: SASL PLAIN with `UID`/`PWD`.
  - `KERBEROS` / `GSSAPI`: SASL GSSAPI over binary Thrift. On Linux/macOS
    it uses the system GSSAPI (a `kinit` ticket or keytab); on **Windows**
    it uses the native SSPI Kerberos of the logged-in domain user — no
    MIT Kerberos install required. The service principal defaults to
    `hive/<host>` (`impala/<host>` for Impala), with the realm resolved
    from the Kerberos config. Override any part with:
    - `KrbServiceName` — the SPN service (default `hive`/`impala`).
    - `KrbHostFQDN` — the SPN host, when it differs from the connection
      `HOST` (e.g. connecting through a load balancer or by IP).
    - `KrbRealm` — an explicit realm, producing `service/host@REALM`
      (for cross-realm or when no `domain_realm` mapping applies).
  - Over HTTP transport (`TransportMode=HTTP`), `KERBEROS` uses SPNEGO via
    libcurl, and `JWT`/`BEARER`/`DATABRICKS` send a token from `PWD`.

### Impala (BACKEND=impala)

```
DRIVER=Argus;BACKEND=impala;HOST=impala.example.com;PORT=21050;UID=impala;DATABASE=default
```

- Protocol: Thrift TCLIService (binary)
- Default port: 21050
- Protocol version: V6
- Database set via `USE <db>` statement after connect
- Same type system as Hive
- **Authentication** (`AuthMech`): same as Hive — `NOSASL` (default),
  `PLAIN`/`LDAP`, and `KERBEROS`/`GSSAPI` (system GSSAPI on Linux/macOS,
  native SSPI on Windows). The service principal defaults to `impala/<host>`.

### Trino (BACKEND=trino)

```
DRIVER=Argus;BACKEND=trino;HOST=trino.example.com;PORT=8080;UID=analyst;DATABASE=hive
```

- Protocol: HTTP REST API (JSON)
- Default port: 8080
- DATABASE parameter maps to Trino catalog
- Catalog operations via `information_schema` queries
- Headers: X-Trino-User, X-Trino-Catalog, X-Trino-Schema
- **Authentication** (`AuthMech`):
  - `BASIC` / `LDAP` / `PLAIN` (or supplying `PWD`): HTTP Basic — requires TLS (`SSL=1`).
  - `JWT` / `BEARER`: token in `PWD`, sent as `Authorization: Bearer <token>`.
  - `OAUTH2` / `CLIENT_CREDENTIALS`: machine-to-machine OAuth2 — Argus fetches a
    token from the IdP token endpoint and uses it as the bearer. Params:
    `OAuth2TokenEndpoint` (`TokenURI`), `ClientId`, `ClientSecret`, optional `Scope`.
    The access token is **re-fetched automatically** if the server returns `401`
    (token expiry), and the request is retried transparently.
  - `GSSAPI` / `KERBEROS`: SPNEGO/Negotiate via libcurl using a `kinit` ticket.
  - `DEVICE_CODE` / `DEVICE`: OAuth2 **device authorization grant** (RFC 8628) for
    headless/no-browser logins. Argus requests a device + user code, prints the
    verification URL and code to stderr/log, then polls the token endpoint until
    the user authorizes, and uses the resulting access token as the bearer.
    Params: `OAuth2DeviceEndpoint` (`DeviceAuthURI`), `OAuth2TokenEndpoint`,
    `ClientId`, optional `Scope`.
  - `AUTH_CODE` / `BROWSER` / `SSO`: OAuth2 **authorization-code grant with PKCE**
    and a **browser + loopback redirect** — the standard interactive cloud-BI
    flow. Argus opens the system browser (honoring `$BROWSER`) to the
    authorization endpoint, listens on `127.0.0.1:<ephemeral>` for the redirect,
    exchanges the code (with the PKCE `code_verifier`) at the token endpoint, and
    uses the access token as the bearer. Params: `OAuth2AuthEndpoint` (`AuthURI`),
    `OAuth2TokenEndpoint`, `ClientId`, optional `ClientSecret`/`Scope`.
  - **OIDC discovery**: instead of giving each endpoint, set `OAuth2Issuer`
    (`Issuer`) and Argus fetches `<issuer>/.well-known/openid-configuration` to
    discover the authorization, token and device endpoints automatically. Works
    with any of the OAuth2 mechanisms above.

  ```
  DRIVER=Argus;BACKEND=trino;HOST=trino;PORT=8443;SSL=1;UID=analyst;PWD={secret};AuthMech=LDAP
  DRIVER=Argus;BACKEND=trino;HOST=trino;PORT=8443;SSL=1;AuthMech=OAUTH2;OAuth2TokenEndpoint=https://idp/token;ClientId=cid;ClientSecret=csec;Scope=trino
  ```

### MySQL-wire (BACKEND=mysql)

A single backend serves every engine that speaks the MySQL client/server
protocol, via libmariadb. This covers **StarRocks**, **Apache Doris** and
**ClickHouse** (MySQL interface) as well as MySQL/MariaDB themselves.

```
DRIVER=Argus;BACKEND=mysql;HOST=starrocks-fe;PORT=9030;UID=root;DATABASE=analytics
DRIVER=Argus;BACKEND=mysql;HOST=doris-fe;PORT=9030;UID=admin;PWD={secret};DATABASE=ods
DRIVER=Argus;BACKEND=mysql;HOST=clickhouse;PORT=9004;UID=default;DATABASE=default
```

- Protocol: MySQL client/server wire protocol (libmariadb)
- Default port: 3306 — **set PORT explicitly** for StarRocks/Doris FE (`9030`)
  and ClickHouse (`9004`)
- A database is reported as an ODBC **catalog** (`TABLE_CAT`); the schema column
  is left empty, following the MySQL Connector/ODBC convention
- Catalog operations run against `information_schema`
- `SSL=1` enables TLS (`SSLCertFile`/`SSLKeyFile`/`SSLCAFile`, `SSLVerify` honored)
- Requires a build with libmariadb (`libmariadb-dev`); auto-detected at cmake time

### PostgreSQL (BACKEND=postgres)

Native PostgreSQL over the PostgreSQL wire protocol, via libpq.

```
DRIVER=Argus;BACKEND=postgres;HOST=pg.example.com;PORT=5432;UID=analyst;PWD={secret};DATABASE=warehouse
DRIVER=Argus;BACKEND=postgres;HOST=pg.example.com;SSL=1;SSLCAFile=/etc/ssl/certs/ca.pem;UID=analyst;PWD={secret};DATABASE=warehouse
```

- Protocol: PostgreSQL wire protocol v3 (libpq)
- Default port: **5432**
- Namespace model: a **database is the ODBC catalog** and a **schema is the ODBC
  schema** — the full three-level model, unlike the MySQL backend. A PostgreSQL
  session cannot query across databases, so `SQLTables(SQL_ALL_CATALOGS)`
  reports only the connected database; `SHOWALLDATABASES=1` lists them all
  instead.
- Catalog operations read `pg_catalog` directly, with every application-supplied
  filter escaped through libpq (`PQescapeLiteral`) rather than interpolated
- **Partition and inheritance children are hidden from `SQLTables` and
  `SQLColumns`.** A table partitioned monthly over ten years is one entry in a
  BI navigator, not 120. `SHOWPARTITIONS=1` lists them.
- System schemas (`pg_catalog`, `information_schema`, `pg_toast*`, `pg_temp*`)
  and relations the user cannot `SELECT` from are hidden unless asked for by name
- **Rows are streamed, not buffered.** Memory is a function of `FetchBufferSize`,
  not of the result set, so a multi-million-row extract does not have to fit in
  client memory first
- Numeric columns take the driver's native fast path — no value→text→value
  round trip for `SQL_C_SLONG`/`SQL_C_DOUBLE` and friends
- **`SQLCancel` is a real server-side cancellation** (libpq's out-of-band cancel
  request), not a no-op
- **Server SQLSTATEs are passed through.** A missing table reports PostgreSQL's
  own `42P01` rather than a generic driver code
- `QueryTimeout` becomes a server-side `statement_timeout`, so the server stops
  doing the work rather than the client stopping to wait for it
- TLS: `SSL=0` (the default) means a genuinely plaintext session (`sslmode=disable`);
  `SSL=1` with `SSLVerify=1` (the default) is `sslmode=verify-full`;
  `SSLVerify=0` downgrades to `require`. `SSLCAFile`/`SSLCertFile`/`SSLKeyFile`
  map to `sslrootcert`/`sslcert`/`sslkey`.
- Auth: libpq negotiates **SCRAM-SHA-256** or md5 from the server's challenge with
  no configuration. `AUTHMECH=KERBEROS` uses libpq's own GSSAPI (SSPI on Windows);
  `KRBSERVICENAME` sets the SPN service name, default `postgres`
- If `DATABASE` is omitted the driver connects to `postgres`. (The ODBC layer
  substitutes the literal `default` for an absent database, which is meaningful
  for Hive and is not a database any PostgreSQL has.)
- **Real transactions.** `SQL_TXN_CAPABLE` reports `SQL_TC_ALL`;
  `SQL_ATTR_AUTOCOMMIT=SQL_AUTOCOMMIT_OFF` puts every subsequent statement in a
  transaction and `SQLEndTran` commits or rolls it back. The `BEGIN` is sent
  lazily with the first statement rather than when the attribute is set, so the
  driver never leaves a session idle-in-transaction. `SQL_ATTR_TXN_ISOLATION`
  accepts READ COMMITTED, REPEATABLE READ and SERIALIZABLE;
  `SQL_TXN_READ_UNCOMMITTED` is accepted but not advertised, because PostgreSQL
  silently upgrades it to READ COMMITTED.
- **Pooled connections are cleaned before reuse** — an open transaction is
  rolled back and `DISCARD ALL` clears session state, so the next borrower
  never inherits a search_path, a temp table or an aborted transaction. A
  connection that cannot be cleaned is discarded rather than reused.
- **Real `SQLDescribeParam`** (`SQL_DESCRIBE_PARAMETER` = `"Y"`): the statement
  is parsed and described server-side, so parameter types come from
  PostgreSQL's own inference rather than the SQL_VARCHAR/255 guess. Execution
  is unchanged — parameters are still rendered as literals. PostgreSQL's jsonb
  `?` operator is indistinguishable from a parameter marker; when the describe
  fails the driver falls back to the generic answer, and `jsonb_exists(j, 'k')`
  is the unambiguous spelling.
- **`SQLForeignKeys`, `SQLProcedures`, `SQLProcedureColumns`,
  `SQLTablePrivileges`, `SQLColumnPrivileges` and `SQLSpecialColumns` return
  real data**, read from `pg_catalog`. `SQL_BEST_ROWID` reports the primary key
  or a fully-NOT-NULL unique index; `ctid` is deliberately not offered as a
  fallback, because UPDATE and VACUUM FULL invalidate it.
  `SQL_ROWVER` reports `xmin` only under `ROWVERSIONING=1` — the counter wraps.
- **`SQLTables`' enumeration forms work**: `SQLTables("%", "", "")` lists
  catalogs and `SQLTables("", "%", "")` lists schemas, which is how Power BI's
  hierarchical navigator and Tableau's schema picker open.
- **`SQLRowCount` reports rows affected** after INSERT/UPDATE/DELETE. DDL keeps
  -1, which ODBC defines as "not available" and is not the same as 0.
- **`{call f(a)}` is translated** to `SELECT * FROM f(a)`, so `SQL_PROCEDURES`
  answers `"Y"` — the info type promises both that the engine has procedures and
  that the driver accepts the invocation syntax.
- **Domains and enums are resolved.** A column declared over
  `CREATE DOMAIN postcode AS varchar(10)` reports SQL_VARCHAR with size 10, not
  an unbounded string; an enum reports a bounded string, since PostgreSQL caps
  labels at 63 bytes. The map is built once at connect.
- Every option above is **per connection**, and the matching `ARGUS_PG_*`
  environment variable is a machine-wide fallback for flipping a behaviour
  without editing every DSN.
- Requires a build with libpq (`libpq-dev`); auto-detected at cmake time

### Greenplum and Apache Cloudberry (BACKEND=greenplum / BACKEND=cloudberry)

Both are MPP forks of PostgreSQL and reuse the whole PostgreSQL backend above —
same wire protocol, same streaming fetch, same type mapping, same dialect. They
are separate backends because everything a BI tool keys on is the backend name:
`SQL_DBMS_NAME`, the dialect entry, the Tableau connector, the Power BI backend
list.

```
DRIVER=Argus;BACKEND=greenplum;HOST=gp-coordinator;PORT=5432;UID=analyst;PWD={secret};DATABASE=warehouse
DRIVER=Argus;BACKEND=cloudberry;HOST=cbdb-coordinator;PORT=5432;UID=analyst;PWD={secret};DATABASE=warehouse
```

What they add over `BACKEND=postgres`:

- **Partition children are hidden from `SQLTables` and `SQLColumns`,** whichever
  way the server records them — Greenplum 6 partitions are inheritance children
  recorded in `pg_partition_rule`, Greenplum 7 and Cloudberry use PostgreSQL's
  declarative partitioning. This is the difference between a connection dialog
  that opens and one that enumerates tens of thousands of child relations: a
  fact table partitioned monthly over ten years, times two hundred tables, is
  ~24,000 relations a driver filtering on `relkind` alone will list.
  `SHOWPARTITIONS=1` turns the filter off.
- **`REMARKS` carries the facts that explain query cost** — the distribution
  policy (`[DISTRIBUTED BY (customer_id)]`, `[DISTRIBUTED RANDOMLY]`,
  `[DISTRIBUTED REPLICATED]`), append-optimized and column-oriented storage
  (`[AO row]`, `[AO column]`), and external-table locations
  (`[external: gpfdist]`). Tableau and Power BI both show `REMARKS` as the table
  description, so an analyst can see why a join shuffles.
- External tables (gpfdist, PXF, and the FDW form Greenplum 7 and Cloudberry
  use) are reported as `TABLE`, because that is what a BI tool can query.

Which catalogs the driver reads is decided by **probing the server at connect**,
not by the version string: one query asks whether `gp_distribution_policy`,
`pg_appendonly`, `pg_exttable` and `pg_class.relispartition` actually exist.
That means a catalog call can never fail with "relation gp_… does not exist",
and pointing `BACKEND=greenplum` at a plain PostgreSQL degrades to PostgreSQL
behaviour with a `01000` warning at connect rather than breaking `SQLTables`.

> **Verification status.** Neither Greenplum nor Cloudberry has a maintained,
> pullable public container image, so the MPP catalog SQL has **not** been run
> against a real cluster. It is exercised against a simulated Greenplum catalog
> built on PostgreSQL (`tests/integration/test_pg_mpp_sim.c`), which proves the
> SQL parses and the logic is right, and the dialect tables are inherited from
> PostgreSQL rather than independently verified — the header of
> `src/odbc/dialect.c` records exactly this. To verify against your own cluster:
> ```
> PG_BACKEND=greenplum PG_HOST=coordinator PG_PORT=5432 ./test_postgres_escapes
> BI_BACKEND=greenplum BI_HOST=coordinator BI_PORT=5432 ./test_bi_escapes
> ```

### Arrow Flight SQL (BACKEND=flightsql)

Reaches any engine exposing an Arrow Flight SQL endpoint — **Dremio**,
**InfluxDB 3.x**, **Apache Doris** and **StarRocks** — over gRPC.

```
DRIVER=Argus;BACKEND=flightsql;HOST=dremio;PORT=32010;UID=user;PWD={secret}
DRIVER=Argus;BACKEND=flightsql;HOST=influxdb3;PORT=443;SSL=1;PWD={token}
```

- Protocol: Arrow Flight SQL (gRPC); record batches are **streamed** lazily
  (one block per fetch, bounded memory). Numeric columns are kept as **native
  typed values** (no per-cell string), so SQLGetData converts straight to the
  requested C type; text/other types fall back to a string cell
- Default port: 32010 (Dremio); set PORT explicitly per engine (InfluxDB 3: 8181)
- `DATABASE` is sent as the gRPC `database` call header (how InfluxDB 3 selects
  the target database)
- Auth: UID+PWD → Flight handshake (basic token); PWD alone → `Bearer` token (JWT)
- **Validated end-to-end** against InfluxDB 3 Core (`SELECT` + `SQLTables`)
- `SSL=1` uses a TLS gRPC channel
- Requires a build with `libarrow-flight-sql-dev` (from the Apache Arrow APT repo)
  and **GCC 14+** with **C++20** — Arrow 24's headers don't compile on GCC 13.
  Auto-detected at cmake time. See `docs/FLIGHTSQL_DESIGN.md` for the exact steps.

### Apache Pinot (BACKEND=pinot)

Real-time OLAP datastore. Argus queries the Pinot **broker**'s synchronous SQL
endpoint (`POST /query/sql`) and lists tables from the **controller** (`/tables`,
default port 9000 on the same host).

```
DRIVER=Argus;BACKEND=pinot;HOST=pinot-broker;PORT=8000
DRIVER=Argus;BACKEND=pinot;HOST=pinot;PORT=8000;UID=user;PWD={secret}
```

- Protocol: HTTP/JSON (`/query/sql`); the whole result arrives in one response
- Default port: 8000 (broker); table listing uses the controller on `:9000`
- Optional HTTP Basic auth via `UID`/`PWD`; `SSL=1` for HTTPS
- `SQLTables` lists the cluster's tables; query errors surface the real Pinot
  message. **Validated end-to-end** against a Pinot QuickStart cluster
- Requires libcurl + json-glib (auto-detected at cmake time)

### Apache Druid (BACKEND=druid)

Real-time analytics database. Argus queries the broker/router's synchronous SQL
endpoint (`POST /druid/v2/sql`, `resultFormat=array`) and uses Druid's full
`INFORMATION_SCHEMA` for catalog operations (like the Trino backend).

```
DRIVER=Argus;BACKEND=druid;HOST=druid-router;PORT=8888
DRIVER=Argus;BACKEND=druid;HOST=broker;PORT=8082;UID=user;PWD={secret}
```

- Protocol: HTTP/JSON (`/druid/v2/sql`); whole result in one response
- Default port: 8888 (router); the broker is 8082
- `SQLTables`/`SQLColumns`/`SQLSchemas` via `INFORMATION_SCHEMA`; query errors
  surface the real Druid `errorMessage`
- Optional HTTP Basic auth (`UID`/`PWD`); `SSL=1` for HTTPS
- Requires libcurl + json-glib (auto-detected). Implemented on the validated
  Pinot/Trino HTTP-JSON pattern; runtime validation against a Druid cluster
  is pending (Druid's stack is multi-service)

### Google BigQuery (BACKEND=bigquery)

REST API (`bigquery/v2`) over libcurl + json-glib. **Every Google URL is
configurable**, so the driver works unchanged on sovereign-cloud
deployments — e.g. S3NS, the trusted-cloud GCP offer operated by Thales,
whose IAM and service endpoints differ from public Google — and against
the BigQuery emulator.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `Project` / `BQProject` | GCP project id (**required**) | - |
| `Database` / `Schema` | Default dataset | - |
| `BQLocation` | Job location (`EU`, `europe-west9`, ...) | - |
| `BQEndpoint` | API base URL | `https://bigquery.googleapis.com` |
| `BQTokenEndpoint` | OAuth2 token endpoint | `https://oauth2.googleapis.com/token` or the key file's `token_uri` |
| `BQAudience` | JWT `aud` claim | the token endpoint |
| `BQScope` | OAuth2 scope | `https://www.googleapis.com/auth/bigquery` |
| `BQKeyFile` | Service-account JSON key path (RS256 JWT-bearer grant; needs OpenSSL) | - |
| `AccessToken` | Pre-fetched bearer token (skips the token flow) | - |

Public GCP with a service-account key:

```
Backend=bigquery;Project=my-project;Database=my_dataset;
BQKeyFile=/etc/argus/sa-key.json;BQLocation=EU
```

Sovereign cloud (S3NS-style: custom API + IAM endpoints, audience and a
private CA). `SSLCAFile` is honoured for **both** the API and the token
endpoint — a sovereign deployment's private PKI is not in the system trust
store, so without it the TLS handshake fails:

```
Backend=bigquery;Project=my-project;
BQEndpoint=https://bigquery.s3ns.example;
BQTokenEndpoint=https://iam.s3ns.example/token;
BQAudience=https://iam.s3ns.example/token;
BQKeyFile=/etc/argus/sa-key.json;
SSL=1;SSLVerify=1;SSLCAFile=/etc/argus/s3ns-ca.pem
```

For mutual TLS, add `SSLCertFile=` / `SSLKeyFile=`.

Emulator / tests (no auth):

```
Backend=bigquery;Project=test;BQEndpoint=http://localhost:9050
```

### Apache Kudu (BACKEND=kudu) — deprecated

> **Deprecated — use the Impala backend instead.** Kudu is normally queried
> through Impala (Impala plans and executes SQL against Kudu tables natively), so
> a direct Kudu SQL backend duplicates that with a hand-rolled SQL parser. The
> deciding factor: the native C++ client (`libkudu_client`) is **not packaged for
> any Ubuntu newer than 16.04** (Cloudera's apt repo stops at `xenial`; it is not
> in Ubuntu universe, conda-forge, or vcpkg), so the backend can't even be built
> on a current OS without compiling Kudu from source.
>
> Reach Kudu tables through Impala instead:
>
> ```
> DRIVER=Argus;BACKEND=impala;HOST=impalad;PORT=21050;DATABASE=default
> ```
>
> The `kudu` backend still builds and runs where `libkudu_client` is available
> (`-DARGUS_BUILD_KUDU`, auto-detected), but is in maintenance mode and receives
> no new feature work (e.g. server-error propagation is not wired up).

## Connecting to Databricks (via the Hive backend)

Databricks SQL warehouses speak the **HiveServer2 Thrift protocol over HTTP**
with a **bearer token** (a personal access token, or an OAuth token). Reach them
with `BACKEND=hive`, `TransportMode=HTTP`, `SSL=1`, and `AuthMech=DATABRICKS`
(aliases `BEARER`/`JWT`/`TOKEN`) with the token in `PWD`:

```
DRIVER=Argus;BACKEND=hive;HOST=<workspace>.cloud.databricks.com;PORT=443;SSL=1;\
  TransportMode=HTTP;HTTPPath=/sql/1.0/warehouses/<id>;AuthMech=DATABRICKS;PWD={dapi...}
```

The same bearer-over-HTTP path also covers any token-gated HiveServer2/Spark
Thrift Server endpoint (e.g. behind Apache Knox with a JWT). The token is sent
as `Authorization: Bearer <token>`.

## Connecting to Spark and Flink (via the Hive backend)

Apache Spark (Thrift Server) and Apache Flink (SQL Gateway `hiveserver2` endpoint)
both speak the HiveServer2 protocol, so they are reached with `BACKEND=hive` — no
separate backend is required.

```
DRIVER=Argus;BACKEND=hive;HOST=spark-thrift;PORT=10000;UID=spark;AuthMech=NOSASL
DRIVER=Argus;BACKEND=hive;HOST=flink-gateway;PORT=10000;UID=flink;AuthMech=NOSASL
```

> Spark Connect (gRPC) is a DataFrame API, not a SQL wire protocol, and is not
> reachable over ODBC; use the Thrift Server.

## DSN Examples

### Hive DSN

```ini
[ArgusHive]
Description = Hive via Argus
Driver = Argus
HOST = localhost
PORT = 10000
UID = hive
PWD =
DATABASE = default
AUTHMECH = NOSASL
BACKEND = hive
```

### Impala DSN

```ini
[ArgusImpala]
Description = Impala via Argus
Driver = Argus
HOST = impala-host
PORT = 21050
UID = impala
DATABASE = default
BACKEND = impala
```

### Trino DSN

```ini
[ArgusTrino]
Description = Trino via Argus
Driver = Argus
HOST = trino-coordinator
PORT = 8080
UID = analyst
DATABASE = hive
BACKEND = trino
```

## DSN vs DSN-less Connections

### DSN Connection (SQLConnect)

```c
SQLConnect(dbc, "ArgusHive", SQL_NTS, "hive", SQL_NTS, "", SQL_NTS);
```

Uses the DSN defined in `odbc.ini`.

### DSN-less Connection (SQLDriverConnect)

```c
SQLDriverConnect(dbc, NULL,
    "DRIVER=Argus;BACKEND=impala;HOST=impala.example.com;PORT=21050;UID=impala",
    SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
```

All parameters are in the connection string.

## Testing the Connection

```bash
# Using isql (from unixODBC)
isql -v ArgusHive

# You should get a SQL prompt:
# +---------------------------------------+
# | Connected!                             |
# | sql-statement                          |
# +---------------------------------------+
# SQL> SELECT 1;
```

## Telemetry (opt-in, off by default)

Argus can send anonymous, aggregate usage telemetry to the Varga Foundation. It
is **disabled by default** and never phones home unless you opt in. Full detail:
[TELEMETRY.md](TELEMETRY.md) and [PRIVACY.md](../PRIVACY.md).

Controls (precedence high → low):

| Control | Effect |
|---------|--------|
| `ARGUS_TELEMETRY=0` (env) | Hard off — overrides every opt-in |
| `ARGUS_TELEMETRY=1` (env) | Machine-wide opt-in |
| `TELEMETRY=1` (DSN / connection string; alias `ENABLETELEMETRY`) | Per-connection opt-in |
| _unset_ | Off |

| Env var | Purpose |
|---------|---------|
| `ARGUS_TELEMETRY` | `1`/`0` machine-wide opt-in / kill switch |
| `ARGUS_TELEMETRY_ENDPOINT` | Override the collector URL (send to your own) |

Only non-identifying fields are ever sent (backend name, latencies, OS, SQLSTATE
codes; **never** hostnames, credentials, database/table names, or query text).
Build without the feature entirely: `cmake -DARGUS_ENABLE_TELEMETRY=OFF`.
