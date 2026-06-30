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
| BACKEND | DRIVER_TYPE | hive | Backend type: hive, impala, or trino |

### Default Ports by Backend

| Backend | Default Port |
|---------|-------------|
| hive | 10000 |
| impala | 21050 |
| trino | 8080 |
| mysql | 3306 |
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

### Impala (BACKEND=impala)

```
DRIVER=Argus;BACKEND=impala;HOST=impala.example.com;PORT=21050;UID=impala;DATABASE=default
```

- Protocol: Thrift TCLIService (binary)
- Default port: 21050
- Protocol version: V6
- Database set via `USE <db>` statement after connect
- Same type system as Hive

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
