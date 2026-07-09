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

# Compute the runtime library dependencies from the built .so so the package
# declares exactly what it needs (thrift_c_glib, mariadb, krb5, … for the
# backends that were compiled in). dpkg-shlibdeps also handles the t64 library
# renames automatically. Fall back to an explicit superset if it is unavailable
# or the .so links non-packaged libraries (e.g. a local dev build).
LIB_DEPS=""
if command -v dpkg-shlibdeps >/dev/null 2>&1; then
    SHLIBDIR="$(mktemp -d)"
    mkdir -p "$SHLIBDIR/debian"
    printf 'Source: argus-odbc\n\nPackage: argus-odbc\nArchitecture: %s\nDepends: ${shlibs:Depends}\n' \
        "$ARCH" > "$SHLIBDIR/debian/control"
    : > "$SHLIBDIR/debian/substvars"
    LIB_DEPS="$( (cd "$SHLIBDIR" && dpkg-shlibdeps -O \
        "$PKG_DIR/usr/lib/x86_64-linux-gnu/libargus_odbc.so" 2>/dev/null) \
        | sed -n 's/^shlibs:Depends=//p')"
    rm -rf "$SHLIBDIR"
fi
if [ -z "$LIB_DEPS" ]; then
    echo "dpkg-shlibdeps unavailable — using the explicit dependency superset"
    LIB_DEPS="libglib2.0-0, libcurl4, libjson-glib-1.0-0, \
libthrift-c-glib0t64 | libthrift-c-glib0, libmariadb3, libgssapi-krb5-2"
fi

# Create control file (unixodbc is a runtime requirement not seen by ldd)
cat > "$PKG_DIR/DEBIAN/control" <<EOF
Package: argus-odbc
Version: ${VERSION}
Section: libs
Priority: optional
Architecture: ${ARCH}
Depends: unixodbc, ${LIB_DEPS}
Maintainer: Varga <contact@varga.co>
Description: Argus ODBC Driver for Data Warehouses
 Argus is a universal ODBC driver supporting Hive, Impala, Trino, Phoenix,
 Pinot, Druid, BigQuery, MySQL-wire (StarRocks/Doris/ClickHouse) and Kudu.
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
