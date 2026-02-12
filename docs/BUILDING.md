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
    libjson-glib-dev
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
    json-glib-devel
```

### macOS (Homebrew)

```bash
brew install cmake unixodbc glib thrift cmocka pkg-config curl json-glib
```

### Windows (MSYS2/UCRT64)

Install [MSYS2](https://www.msys2.org/), then in the UCRT64 shell:

```bash
pacman -S \
    mingw-w64-ucrt-x86_64-gcc \
    mingw-w64-ucrt-x86_64-cmake \
    mingw-w64-ucrt-x86_64-ninja \
    mingw-w64-ucrt-x86_64-pkg-config \
    mingw-w64-ucrt-x86_64-glib2 \
    mingw-w64-ucrt-x86_64-thrift \
    mingw-w64-ucrt-x86_64-curl \
    mingw-w64-ucrt-x86_64-json-glib \
    mingw-w64-ucrt-x86_64-cmocka
```

## Building

### Linux / macOS

```bash
# Clone the repository
git clone https://github.com/your-org/argus.git
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
| `CMAKE_BUILD_TYPE` | Release | Debug or Release |
| `CMAKE_INSTALL_PREFIX` | /usr/local | Installation prefix |

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

```bash
# Check the shared library exports (Linux)
nm -D build/libargus_odbc.so | grep SQL

# Should show entries like:
# T SQLAllocHandle
# T SQLConnect
# T SQLDriverConnect
# T SQLExecDirect
# ...
```
