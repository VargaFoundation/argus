#!/bin/sh
# Assert that a built driver carries the backends it is supposed to ship with.
#
# The driver embeds one line, "argus-build <version> <feature>...", listing
# every backend and auth feature compiled in (src/backend/backend.c). This
# script pulls that line out of the binary with `strings` and checks each
# requested token against it, so a release runner missing a -dev package fails
# the job instead of publishing a driver that quietly lacks a backend, which is
# how v0.6.0 shipped without PostgreSQL and MySQL-wire.
#
# Usage: scripts/check-build-manifest.sh <driver-binary> <token>...
#   e.g. scripts/check-build-manifest.sh build/src/libargus_odbc.so \
#            hive impala trino phoenix pinot druid bigquery mysql postgres
# Env:   STRINGS (default: strings)

set -u

if [ $# -lt 2 ]; then
    echo "usage: $0 <driver-binary> <token>..." >&2
    exit 2
fi

binary="$1"
shift

STRINGS="${STRINGS:-strings}"

if [ ! -f "$binary" ]; then
    echo "error: $binary does not exist" >&2
    exit 2
fi

# -a scans the whole file, so the result does not depend on which sections a
# given binutils/llvm/cctools `strings` considers loadable.
manifest="$("$STRINGS" -a -- "$binary" | grep '^argus-build ' | head -n 1)"
if [ -z "$manifest" ]; then
    echo "error: no 'argus-build' manifest line in $binary" >&2
    exit 1
fi
echo "$manifest"

missing=0
for token in "$@"; do
    case " $manifest " in
        *" $token "*) ;;
        *)
            echo "MISSING: $token" >&2
            missing=1
            ;;
    esac
done

if [ "$missing" -ne 0 ]; then
    echo "error: $binary was built without the features listed above" >&2
    exit 1
fi
echo "ok: every requested feature is compiled in"
