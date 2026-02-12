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
    libcmocka-dev
```

### Fedora/RHEL

```bash
sudo dnf install -y \
    gcc cmake pkgconfig \
    unixODBC-devel \
    glib2-devel \
    thrift-devel \
    libcmocka-devel
```

### macOS (Homebrew)

```bash
brew install cmake unixodbc glib thrift cmocka pkg-config
```

## Building

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
ctest --output-on-failure
```

Or run individual tests:

```bash
./tests/test_handle
./tests/test_connect_string
./tests/test_type_convert
./tests/test_diag
./tests/test_info
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

```bash
cd build
sudo make install
```

This installs:
- `libargus_odbc.so` to `<prefix>/lib/`
- Header files to `<prefix>/include/argus/`

## Verifying the Build

```bash
# Check the shared library exports
nm -D build/libargus_odbc.so | grep SQL

# Should show entries like:
# T SQLAllocHandle
# T SQLConnect
# T SQLDriverConnect
# T SQLExecDirect
# ...
```
