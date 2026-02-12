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
Description = Argus ODBC Driver for Hive
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
```

## Connection String Parameters

Use with `SQLDriverConnect`:

```
DRIVER=Argus;HOST=hive.example.com;PORT=10000;UID=admin;PWD={p@ss};Database=analytics;AuthMech=PLAIN
```

| Parameter | Aliases | Default | Description |
|-----------|---------|---------|-------------|
| HOST | SERVER | localhost | HiveServer2 hostname or IP |
| PORT | | 10000 | HiveServer2 Thrift port |
| UID | USERNAME, USER | (empty) | Username for authentication |
| PWD | PASSWORD | (empty) | Password for authentication |
| DATABASE | SCHEMA | default | Initial database to use |
| AUTHMECH | AUTH | NOSASL | Authentication mechanism |
| BACKEND | DRIVER_TYPE | hive | Backend type (hive) |

### Authentication Mechanisms

| Value | Description |
|-------|-------------|
| NOSASL | No authentication (development/testing) |
| PLAIN | Username/password over SASL PLAIN |

## DSN vs DSN-less Connections

### DSN Connection (SQLConnect)

```c
SQLConnect(dbc, "ArgusHive", SQL_NTS, "hive", SQL_NTS, "", SQL_NTS);
```

Uses the DSN defined in `odbc.ini`.

### DSN-less Connection (SQLDriverConnect)

```c
SQLDriverConnect(dbc, NULL,
    "DRIVER=Argus;HOST=hive.example.com;PORT=10000;UID=hive;AuthMech=NOSASL",
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
