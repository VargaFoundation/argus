#!/bin/bash
#
# Build an .rpm package for Argus ODBC driver.
# Usage: bash packaging/build-rpm.sh [version]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION="${1:-$(grep 'project(argus VERSION' "$PROJECT_DIR/CMakeLists.txt" | sed 's/.*VERSION \([^ )]*\).*/\1/')}"
BUILD_DIR="${PROJECT_DIR}/build"
RPMBUILD_DIR="${PROJECT_DIR}/_rpmbuild"

echo "Building .rpm package: argus-odbc-${VERSION}"

# Clean previous build
rm -rf "$RPMBUILD_DIR"

# Create rpmbuild directory structure
mkdir -p "$RPMBUILD_DIR"/{BUILD,RPMS,SOURCES,SPECS,SRPMS,BUILDROOT}

# Create tarball for rpmbuild
TARBALL_DIR="argus-odbc-${VERSION}"
mkdir -p "/tmp/${TARBALL_DIR}"
mkdir -p "/tmp/${TARBALL_DIR}/lib"
mkdir -p "/tmp/${TARBALL_DIR}/include/argus"
cp "$BUILD_DIR/src/libargus_odbc.so" "/tmp/${TARBALL_DIR}/lib/"
cp "$PROJECT_DIR/include/argus/"*.h "/tmp/${TARBALL_DIR}/include/argus/"
tar -czf "$RPMBUILD_DIR/SOURCES/argus-odbc-${VERSION}.tar.gz" -C /tmp "$TARBALL_DIR"
rm -rf "/tmp/${TARBALL_DIR}"

# Copy spec file
cp "$SCRIPT_DIR/argus-odbc.spec" "$RPMBUILD_DIR/SPECS/"

# Build RPM
rpmbuild \
    --define "_topdir $RPMBUILD_DIR" \
    --define "version $VERSION" \
    -bb "$RPMBUILD_DIR/SPECS/argus-odbc.spec"

# Copy RPM to project directory
cp "$RPMBUILD_DIR/RPMS/"*/*.rpm "$PROJECT_DIR/"

# Clean up
rm -rf "$RPMBUILD_DIR"

echo "RPM package built:"
ls -la "$PROJECT_DIR/"argus-odbc-*.rpm
