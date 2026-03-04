#!/bin/bash
#
# Register Argus ODBC driver and a sample DSN with unixODBC on macOS.
# Run: bash scripts/install_dsn_macos.sh
#
# Prerequisites: brew install unixodbc
#
set -euo pipefail

# Detect Homebrew prefix (Apple Silicon vs Intel)
if [ -d /opt/homebrew ]; then
    HOMEBREW_PREFIX="/opt/homebrew"
elif [ -d /usr/local/Cellar ]; then
    HOMEBREW_PREFIX="/usr/local"
else
    echo "Error: Homebrew not found. Install from https://brew.sh"
    exit 1
fi

# Verify unixODBC is installed
if ! command -v odbcinst &>/dev/null; then
    echo "Error: odbcinst not found. Install unixODBC: brew install unixodbc"
    exit 1
fi

# Determine driver path
# Priority: env var > script directory > /usr/local/lib > Homebrew lib
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -n "${ARGUS_DRIVER_PATH:-}" ] && [ -f "$ARGUS_DRIVER_PATH" ]; then
    DRIVER_PATH="$ARGUS_DRIVER_PATH"
elif [ -f "$SCRIPT_DIR/../lib/libargus_odbc.dylib" ]; then
    DRIVER_PATH="$(cd "$SCRIPT_DIR/../lib" && pwd)/libargus_odbc.dylib"
elif [ -f /usr/local/lib/libargus_odbc.dylib ]; then
    DRIVER_PATH="/usr/local/lib/libargus_odbc.dylib"
elif [ -f "$HOMEBREW_PREFIX/lib/libargus_odbc.dylib" ]; then
    DRIVER_PATH="$HOMEBREW_PREFIX/lib/libargus_odbc.dylib"
else
    echo "Error: libargus_odbc.dylib not found."
    echo "Build and install first, or set ARGUS_DRIVER_PATH."
    echo "  cmake --build build && sudo cmake --install build"
    exit 1
fi

echo "Using driver: $DRIVER_PATH"
echo ""

echo "Registering Argus ODBC driver..."

# Register driver in odbcinst.ini
odbcinst -i -d -n "Argus" -f /dev/stdin <<EOF
[Argus]
Description = Argus ODBC Driver for Data Warehouses
Driver = ${DRIVER_PATH}
Setup = ${DRIVER_PATH}
EOF

echo "Driver registered."

# Create a sample DSN in system odbc.ini
HIVE_HOST="${HIVE_HOST:-localhost}"
HIVE_PORT="${HIVE_PORT:-10000}"

odbcinst -i -s -l -n "ArgusHive" -f /dev/stdin <<EOF
[ArgusHive]
Description = Hive via Argus ODBC
Driver = Argus
HOST = ${HIVE_HOST}
PORT = ${HIVE_PORT}
UID = hive
PWD =
DATABASE = default
AUTHMECH = NOSASL
EOF

echo "Sample DSN 'ArgusHive' registered."
echo ""
echo "Test with: isql -v ArgusHive"
echo ""
echo "To customize, edit /etc/odbc.ini or create ~/.odbc.ini"
