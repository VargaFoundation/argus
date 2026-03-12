#!/bin/bash
#
# Build a .deb package for Argus ODBC driver.
# Usage: bash packaging/build-deb.sh [version]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION="${1:-$(grep 'project(argus VERSION' "$PROJECT_DIR/CMakeLists.txt" | sed 's/.*VERSION \([^ )]*\).*/\1/')}"
ARCH="amd64"
PKG_NAME="argus-odbc_${VERSION}_${ARCH}"
BUILD_DIR="${PROJECT_DIR}/build"
PKG_DIR="${PROJECT_DIR}/${PKG_NAME}"

echo "Building .deb package: ${PKG_NAME}.deb"

# Clean previous build
rm -rf "$PKG_DIR"

# Create directory structure
mkdir -p "$PKG_DIR/DEBIAN"
mkdir -p "$PKG_DIR/usr/lib/x86_64-linux-gnu"
mkdir -p "$PKG_DIR/usr/include/argus"

# Copy files
cp "$BUILD_DIR/src/libargus_odbc.so" "$PKG_DIR/usr/lib/x86_64-linux-gnu/"
cp "$PROJECT_DIR/include/argus/"*.h "$PKG_DIR/usr/include/argus/"

# Create control file
cat > "$PKG_DIR/DEBIAN/control" <<EOF
Package: argus-odbc
Version: ${VERSION}
Section: libs
Priority: optional
Architecture: ${ARCH}
Depends: unixodbc, libglib2.0-0, libcurl4, libjson-glib-1.0-0
Maintainer: Varga <contact@varga.co>
Description: Argus ODBC Driver for Data Warehouses
 Argus is a universal ODBC driver supporting Hive, Impala,
 Trino, Phoenix, and Kudu backends.
EOF

# Create postinst script
cat > "$PKG_DIR/DEBIAN/postinst" <<'POSTINST'
#!/bin/bash
set -e

DRIVER_PATH="/usr/lib/x86_64-linux-gnu/libargus_odbc.so"

if command -v odbcinst >/dev/null 2>&1; then
    odbcinst -i -d -n "Argus" -f /dev/stdin <<EOF
[Argus]
Description = Argus ODBC Driver for Data Warehouses
Driver = ${DRIVER_PATH}
Setup = ${DRIVER_PATH}
EOF
    echo "Argus ODBC driver registered."
fi

ldconfig
POSTINST
chmod 755 "$PKG_DIR/DEBIAN/postinst"

# Create prerm script
cat > "$PKG_DIR/DEBIAN/prerm" <<'PRERM'
#!/bin/bash
set -e

if command -v odbcinst >/dev/null 2>&1; then
    odbcinst -u -d -n "Argus" 2>/dev/null || true
    echo "Argus ODBC driver unregistered."
fi
PRERM
chmod 755 "$PKG_DIR/DEBIAN/prerm"

# Build the .deb
dpkg-deb --build "$PKG_DIR"

# Clean up
rm -rf "$PKG_DIR"

echo "Package built: ${PKG_NAME}.deb"
dpkg-deb --info "${PROJECT_DIR}/${PKG_NAME}.deb"
