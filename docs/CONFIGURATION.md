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
