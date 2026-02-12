#!/bin/bash
#
# Register Argus ODBC driver and a sample DSN with unixODBC.
# Run as root: sudo bash scripts/install_dsn.sh
#
set -euo pipefail

DRIVER_PATH="${ARGUS_DRIVER_PATH:-/usr/local/lib/libargus_odbc.so}"
HIVE_HOST="${HIVE_HOST:-localhost}"
HIVE_PORT="${HIVE_PORT:-10000}"

if [ ! -f "$DRIVER_PATH" ]; then
    echo "Error: Driver not found at $DRIVER_PATH"
    echo "Build and install first: cd build && sudo make install"
    exit 1
fi

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
