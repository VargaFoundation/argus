#!/bin/bash
#
# Build a macOS .pkg installer for Argus ODBC driver.
# Usage: bash packaging/build-macos-pkg.sh [version]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION="${1:-$(grep 'project(argus VERSION' "$PROJECT_DIR/CMakeLists.txt" | sed 's/.*VERSION \([^ )]*\).*/\1/')}"
ARCH="$(uname -m)"
BUILD_DIR="${PROJECT_DIR}/build"
PKG_ROOT="${PROJECT_DIR}/_pkgroot"
PKG_SCRIPTS="${PROJECT_DIR}/_pkgscripts"
PKG_OUTPUT="${PROJECT_DIR}/argus-odbc-${VERSION}-macos-${ARCH}.pkg"

echo "Building macOS .pkg: argus-odbc-${VERSION}-macos-${ARCH}.pkg"

# Clean previous build
rm -rf "$PKG_ROOT" "$PKG_SCRIPTS"

# Create payload directory structure
mkdir -p "$PKG_ROOT/usr/local/lib"
mkdir -p "$PKG_ROOT/usr/local/include/argus"

# Copy files
cp "$BUILD_DIR/src/libargus_odbc.dylib" "$PKG_ROOT/usr/local/lib/"
cp "$PROJECT_DIR/include/argus/"*.h "$PKG_ROOT/usr/local/include/argus/"

# Create postinstall script
mkdir -p "$PKG_SCRIPTS"
cat > "$PKG_SCRIPTS/postinstall" <<'POSTINSTALL'
#!/bin/bash
set -e

DRIVER_PATH="/usr/local/lib/libargus_odbc.dylib"

if command -v odbcinst >/dev/null 2>&1; then
    odbcinst -i -d -n "Argus" -f /dev/stdin <<EOF
[Argus]
Description = Argus ODBC Driver for Data Warehouses
Driver = ${DRIVER_PATH}
Setup = ${DRIVER_PATH}
EOF
    echo "Argus ODBC driver registered."
fi
POSTINSTALL
chmod 755 "$PKG_SCRIPTS/postinstall"

# Build component package
pkgbuild \
    --root "$PKG_ROOT" \
    --scripts "$PKG_SCRIPTS" \
    --identifier "org.varga.argus-odbc" \
    --version "$VERSION" \
    --install-location "/" \
    "$PKG_OUTPUT"

# Clean up
rm -rf "$PKG_ROOT" "$PKG_SCRIPTS"

echo "Package built: $PKG_OUTPUT"
