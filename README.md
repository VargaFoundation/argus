# Argus ODBC Driver

An open-source ODBC driver for Data Warehouses and Lakehouses. The first backend targets **Apache Hive** via the HiveServer2 Thrift protocol.

Argus is designed to work with BI tools like PowerBI, Tableau, and DBeaver through the **unixODBC Driver Manager**.

## Features

- Full ODBC 3.80 API surface (40+ functions)
- Apache Hive backend via HiveServer2 Thrift binary protocol
- Columnar result set parsing with batch fetching (1000 rows/batch)
- Catalog functions: `SQLTables`, `SQLColumns`, `SQLGetTypeInfo`
- Pluggable backend architecture for adding Impala, Trino, Spark, etc.
- NOSASL and PLAIN authentication

## Quick Start

### Prerequisites

```bash
# Ubuntu/Debian
sudo apt-get install -y \
    build-essential cmake pkg-config \
    unixodbc-dev \
    libglib2.0-dev \
    libthrift-c-glib-dev \
    thrift-compiler \
    libcmocka-dev
```

### Build

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Run Tests

```bash
# Unit tests
cd build && ctest --output-on-failure

# Integration tests (requires HiveServer2)
docker compose -f tests/integration/docker-compose.yml up -d
cmake .. -DBUILD_INTEGRATION_TESTS=ON && make
ctest -L integration --output-on-failure
```

### Install & Configure

```bash
sudo make install
sudo bash scripts/install_dsn.sh
```

Then connect with `isql`:

```bash
isql -v ArgusHive
```

## Connection String

```
HOST=hive.example.com;PORT=10000;UID=hive;PWD=secret;Database=analytics;AuthMech=NOSASL
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| HOST      | localhost | HiveServer2 hostname |
| PORT      | 10000    | HiveServer2 port |
| UID       |          | Username |
| PWD       |          | Password |
| DATABASE  | default  | Initial database/schema |
| AUTHMECH  | NOSASL   | Auth mechanism (NOSASL or PLAIN) |
| BACKEND   | hive     | Backend type |

## Architecture

```
BI Tool (PowerBI, etc.)
       |
[unixODBC Driver Manager]
       |
+------+------+
| Argus ODBC  |  <-- libargus_odbc.so
| API Layer   |
+------+------+
       |
+------+------+
| Backend     |  <-- Abstract vtable interface
| Abstraction |
+------+------+
       |
    [Hive]        <-- Thrift C GLib -> HiveServer2
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Adding New Backends

Argus uses a pluggable backend architecture. Adding support for Impala, Trino, or Spark requires implementing a single vtable struct with ~12 function pointers. No changes to the ODBC layer are needed.

See [docs/ADDING_BACKENDS.md](docs/ADDING_BACKENDS.md) for a step-by-step guide.

## License

Apache License 2.0 - see [LICENSE](LICENSE).
