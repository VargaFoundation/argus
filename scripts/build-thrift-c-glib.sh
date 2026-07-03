#!/bin/bash
# Build the portable subset of the thrift c_glib runtime as a static
# library, for platforms whose package manager does not ship it (MSYS2).
#
# thrift_c_glib's socket transports are raw-POSIX and do not compile on
# Windows, but Argus does not use them: Hive/Impala speak through the
# GIO transport (src/backend/thrift_gio_transport.c). What the backends
# and the generated TCLIService code actually need is the protocol
# layer, the transport base classes and the buffered/framed/memory
# transports — all portable C, modulo three trivial header shims for
# htonl/ntohl.
#
# Usage: build-thrift-c-glib.sh <install-prefix> [thrift-version]
# Env:   CC, AR (default cc/ar), PKG_CONFIG (default pkg-config)

set -euo pipefail

PREFIX="${1:?usage: build-thrift-c-glib.sh <install-prefix> [version]}"
VERSION="${2:-0.23.0}"
CC="${CC:-cc}"
AR="${AR:-ar}"
PKG_CONFIG="${PKG_CONFIG:-pkg-config}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

TARBALL="thrift-$VERSION.tar.gz"
if [ ! -f "$WORK/$TARBALL" ]; then
    curl -fsSL --retry 3 -o "$WORK/$TARBALL" \
        "https://archive.apache.org/dist/thrift/$VERSION/$TARBALL"
fi
tar -xzf "$WORK/$TARBALL" -C "$WORK"
SRC="$WORK/thrift-$VERSION/lib/c_glib/src"

# Shim headers: the buffered/framed/memory transports include POSIX
# socket headers only for htonl/ntohl. Map them to winsock on Windows.
SHIM="$WORK/shim"
if "$CC" -dumpmachine | grep -qiE "mingw|windows"; then
    mkdir -p "$SHIM/sys" "$SHIM/netinet"
    printf '#include <winsock2.h>\n'  > "$SHIM/sys/socket.h"
    printf '#include <winsock2.h>\n'  > "$SHIM/netinet/in.h"
    printf '#include <ws2tcpip.h>\n'  > "$SHIM/netdb.h"
fi

FILES="
thrift/c_glib/thrift.c
thrift/c_glib/thrift_struct.c
thrift/c_glib/thrift_application_exception.c
thrift/c_glib/thrift_configuration.c
thrift/c_glib/protocol/thrift_protocol.c
thrift/c_glib/protocol/thrift_protocol_factory.c
thrift/c_glib/protocol/thrift_binary_protocol.c
thrift/c_glib/protocol/thrift_binary_protocol_factory.c
thrift/c_glib/transport/thrift_transport.c
thrift/c_glib/transport/thrift_transport_factory.c
thrift/c_glib/transport/thrift_buffered_transport.c
thrift/c_glib/transport/thrift_buffered_transport_factory.c
thrift/c_glib/transport/thrift_framed_transport.c
thrift/c_glib/transport/thrift_framed_transport_factory.c
thrift/c_glib/transport/thrift_memory_buffer.c
thrift/c_glib/processor/thrift_processor.c
thrift/c_glib/processor/thrift_dispatch_processor.c
"

GLIB_CFLAGS="$($PKG_CONFIG --cflags glib-2.0 gobject-2.0)"

mkdir -p "$WORK/obj"
OBJS=""
for f in $FILES; do
    o="$WORK/obj/$(basename "$f" .c).o"
    # shellcheck disable=SC2086
    "$CC" -O2 -c "$SRC/$f" -o "$o" \
        -I"$SRC" ${SHIM:+-I"$SHIM"} $GLIB_CFLAGS
    OBJS="$OBJS $o"
done

mkdir -p "$PREFIX/lib/pkgconfig" "$PREFIX/include"
# shellcheck disable=SC2086
"$AR" rcs "$PREFIX/lib/libthrift_c_glib.a" $OBJS

# Headers: keep the <thrift/c_glib/...> include layout.
(cd "$SRC" && find thrift -name '*.h' -exec install -D {} "$PREFIX/include/{}" \;)

cat > "$PREFIX/lib/pkgconfig/thrift_c_glib.pc" <<EOF
prefix=$PREFIX
libdir=\${prefix}/lib
includedir=\${prefix}/include

Name: thrift_c_glib
Description: Thrift c_glib runtime (portable subset, static)
Version: $VERSION
Requires: glib-2.0 gobject-2.0
Libs: -L\${libdir} -lthrift_c_glib
Cflags: -I\${includedir}
EOF

echo "thrift_c_glib $VERSION (portable subset) installed into $PREFIX"
