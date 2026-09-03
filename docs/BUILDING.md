# Building Argus

## Prerequisites

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    unixodbc-dev \
    libglib2.0-dev \
    libthrift-c-glib-dev \
    thrift-compiler \
    libcmocka-dev \
    libcurl4-openssl-dev \
    libjson-glib-dev \
    libmariadb-dev \
    libpq-dev \
    libkrb5-dev
```

### Fedora/RHEL

```bash
sudo dnf install -y \
    gcc cmake pkgconfig \
    unixODBC-devel \
    glib2-devel \
    thrift-devel \
    libcmocka-devel \
    libcurl-devel \
    json-glib-devel \
    mariadb-connector-c-devel \
    libpq-devel \
    krb5-devel
```

### macOS (Homebrew)

```bash
brew install cmake unixodbc glib thrift cmocka pkg-config curl json-glib \
    libpq mariadb-connector-c
```

`libpq` and `mariadb-connector-c` are keg-only, so pkg-config cannot see them
until they are on its path. Configure with:

```bash
export PKG_CONFIG_PATH="$(brew --prefix libpq)/lib/pkgconfig:$(brew --prefix mariadb-connector-c)/lib/pkgconfig:$PKG_CONFIG_PATH"
```

Without it the PostgreSQL and MySQL-wire backends are left out of the build
(see [Choosing backends](#choosing-backends) for how to make that an error
instead).

### Windows (MSYS2/UCRT64)

Install [MSYS2](https://www.msys2.org/), then in the UCRT64 shell:

```bash
pacman -S \
    mingw-w64-ucrt-x86_64-gcc \
    mingw-w64-ucrt-x86_64-cmake \
    mingw-w64-ucrt-x86_64-ninja \
    mingw-w64-ucrt-x86_64-pkgconf \
    mingw-w64-ucrt-x86_64-glib2 \
    mingw-w64-ucrt-x86_64-curl \
    mingw-w64-ucrt-x86_64-json-glib \
    mingw-w64-ucrt-x86_64-cmocka \
    mingw-w64-ucrt-x86_64-libmariadbclient \
    mingw-w64-ucrt-x86_64-postgresql
```

MSYS2 ships the thrift compiler (`mingw-w64-ucrt-x86_64-thrift`) but not the
c_glib runtime. Build the portable subset once, then point pkg-config at it:

```bash
bash scripts/build-thrift-c-glib.sh "$PWD/thrift-c-glib-prefix" 0.23.0
PKG_CONFIG_PATH="$PWD/thrift-c-glib-prefix/lib/pkgconfig" \
    cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
```

Hive/Impala then build on Windows (they use Argus's GIO socket transport,
not thrift's POSIX sockets). For TLS over binary Thrift, install
`mingw-w64-ucrt-x86_64-glib-networking`; the installer ships its
libgioopenssl module in `gio-modules\` next to the driver, which the driver
loads automatically. Add `mingw-w64-ucrt-x86_64-nsis` to build the
installer.

## Building

### Linux / macOS

```bash
# Clone the repository
git clone https://github.com/VargaFoundation/argus.git
cd argus

# Create build directory
mkdir build && cd build

# Configure
cmake ..

# Build
make -j$(nproc)
```

### Windows (MSYS2/UCRT64)

```bash
cd argus

# Configure with Ninja generator
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build build
```

## CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTING` | ON | Build unit tests |
| `BUILD_INTEGRATION_TESTS` | OFF | Build integration tests |
| `BUILD_SHARED_LIBS` | ON | Build shared library (required for ODBC) |
| `BUILD_ADBC` | ON | Build the Arrow ADBC driver over the ODBC stack |
| `ARGUS_WITH_<BACKEND>` | AUTO | Per-backend request: `AUTO`, `ON` or `OFF` (see below) |
| `ARGUS_RELEASE` | OFF | Turn every `AUTO` a release ships with into `ON` |
| `ARGUS_ENABLE_TELEMETRY` | ON | Compile in the opt-in usage telemetry (off at runtime by default) |
| `ARGUS_HARDENING` | ON | Stack protector, `_FORTIFY_SOURCE=2` (optimising builds), full RELRO + `BIND_NOW`, DEP/ASLR/CFG on Windows |
| `ENABLE_ASAN` | OFF | AddressSanitizer + UndefinedBehaviorSanitizer |
| `ENABLE_FUZZING` | OFF | libFuzzer harnesses in `fuzz/` (requires Clang) |
| `CMAKE_BUILD_TYPE` | Release | Debug or Release |
| `CMAKE_INSTALL_PREFIX` | /usr/local | Installation prefix |

### Choosing backends

Each backend has a tri-state cache variable:

| Variable | Backends | Needs |
|----------|----------|-------|
| `ARGUS_WITH_THRIFT_BACKENDS` | Hive, Impala | thrift_c_glib ≥ 0.16 |
| `ARGUS_WITH_GSSAPI` | Kerberos auth for Hive/Impala | krb5-gssapi (Windows uses SSPI, no option) |
| `ARGUS_WITH_TRINO`, `_PHOENIX`, `_PINOT`, `_DRUID`, `_BIGQUERY` | the HTTP backends | libcurl + json-glib |
| `ARGUS_WITH_MYSQL` | MySQL-wire (MySQL, MariaDB, Doris, StarRocks…) | libmariadb / libmysqlclient |
| `ARGUS_WITH_POSTGRES` | PostgreSQL, Greenplum, Cloudberry | libpq |
| `ARGUS_WITH_KUDU` | Kudu | kudu_client (C++) |
| `ARGUS_WITH_FLIGHTSQL` | Arrow Flight SQL | Arrow Flight SQL (C++) |

- `AUTO` (default): build the backend when its dependency is found, otherwise
  print `DISABLED (... not found)` and carry on.
- `ON`: the backend is required; a missing dependency is a configure-time
  `FATAL_ERROR` naming the package to install.
- `OFF`: never build it, even if the dependency is present.

`-DARGUS_RELEASE=ON` promotes every `AUTO` for the backends a release artefact
ships with (Hive, Impala, Trino, Phoenix, Pinot, Druid, BigQuery, MySQL-wire,
PostgreSQL family) to `ON`. The CI and release workflows configure with it so a
runner missing a `-dev` package fails the build instead of silently publishing
a driver without that backend. Kudu and Flight SQL stay optional because their
C++ SDKs are not packaged everywhere. An explicit `ARGUS_WITH_<BACKEND>=OFF`
still wins under `ARGUS_RELEASE=ON`.

```bash
# Require PostgreSQL, skip Kudu, take whatever else is installed
cmake -B build -DARGUS_WITH_POSTGRES=ON -DARGUS_WITH_KUDU=OFF
```

### Debug Build

```bash
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)
```

### With Integration Tests

```bash
cmake -DBUILD_INTEGRATION_TESTS=ON ..
make -j$(nproc)
```

## Running Tests

### Unit Tests

```bash
cd build
ctest --output-on-failure -L unit
```

Or run individual tests:

```bash
./tests/test_handle
./tests/test_connect_string
./tests/test_type_convert
./tests/test_diag
./tests/test_info
./tests/test_impala_types
./tests/test_trino_types
```

### Integration Tests

Start HiveServer2:

```bash
docker compose -f tests/integration/docker-compose.yml up -d
# Wait for health check to pass (~30-60 seconds)
```

Run:

```bash
ctest -L integration --output-on-failure
```

Clean up:

```bash
docker compose -f tests/integration/docker-compose.yml down
```

## Installation

### Linux

```bash
cd build
sudo make install
```

This installs:
- `libargus_odbc.so` to `<prefix>/lib/`
- Header files to `<prefix>/include/argus/`

### Windows

Use the NSIS installer (see `installer/argus-odbc.nsi`):
1. Build the driver DLL
2. Run `makensis installer/argus-odbc.nsi` to create the installer
3. Run the installer to register the driver with Windows ODBC

## Verifying the Build

The driver embeds one line naming its version and every backend and auth
feature it was compiled with. Read it straight out of the binary — no need to
load the driver — with `strings`, or with the script the release workflow
uses, which fails when a requested token is missing:

```bash
strings -a build/src/libargus_odbc.so | grep '^argus-build '
# argus-build 0.6.1 hive impala trino phoenix mysql pinot druid bigquery postgres greenplum cloudberry gssapi openssl telemetry

scripts/check-build-manifest.sh build/src/libargus_odbc.so hive impala trino postgres
```

The same line is the first thing the driver logs at `INFO` level when it is
loaded, which is the quickest way to find out what an installed driver
actually contains.

The shared driver exports the ODBC entry points and nothing else, and it is
built with the hardening flags a distribution applies to its own packages.
`ctest -L unit` runs both checks (`check_exports`, `check_hardening`) on the
library it just built; the release workflow runs them again on each artefact.
To run them by hand:

```bash
# Every exported symbol must start with SQL; anything else is listed as LEAKED
scripts/check-exports.sh build/src/libargus_odbc.so

# RELRO + BIND_NOW, non-executable stack, stack protector; --release also
# expects the FORTIFY_SOURCE wrappers, which only exist in an optimising build
scripts/check-hardening.sh build/src/libargus_odbc.so --release
```

Both take a `.so`, `.dylib` or `.dll` and use `nm`/`readelf`/`objdump` from the
toolchain that built it (`NM`, `READELF`, `OBJDUMP` override the defaults).
The raw export list is one command away:

```bash
nm -D --defined-only build/src/libargus_odbc.so | grep ' T '

# Should show only entries like:
# T SQLAllocHandle
# T SQLConnect
# T SQLDriverConnect
# T SQLExecDirect
# ...
```
