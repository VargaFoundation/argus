#!/bin/sh
# Assert that a built driver exports nothing but its ODBC entry points.
#
# Every symbol the driver exports is a name another library in the same
# process can bind to. Before this check the Linux driver exported 385
# symbols, 281 of them Thrift-generated GObject types: two Thrift c_glib
# drivers loaded into one BI tool resolved each other's copies. The driver's
# own sources are compiled with hidden visibility and only SQL* carries
# ARGUS_EXPORT (include/argus/odbc_api.h); this script reads the dynamic
# symbol table back and fails on anything else, so the property cannot
# silently regress when a target or a generated library is added.
#
# Usage: scripts/check-exports.sh <driver-binary> [<allowed-prefix>]
#   The prefix defaults to SQL. The format is picked from the extension:
#   .dll is PE (objdump), .dylib is Mach-O (nm -gU), anything else is ELF
#   (nm -D). Set NM/OBJDUMP to point at a specific binutils. A .dll may
#   also export the ODBC installer's setup entry points (ConfigDSN,
#   ConfigDriver, ConfigTranslator): the Driver Manager looks them up by
#   name in the driver DLL itself, so they cannot carry the SQL prefix.

set -u

if [ $# -lt 1 ]; then
    echo "usage: $0 <driver-binary> [<allowed-prefix>]" >&2
    exit 2
fi

binary="$1"
prefix="${2:-SQL}"
NM="${NM:-nm}"
OBJDUMP="${OBJDUMP:-objdump}"
setup_exports=''

if [ ! -f "$binary" ]; then
    echo "error: $binary does not exist" >&2
    exit 2
fi

case "$binary" in
    *.dll)
        # The [Ordinal/Name Pointer] Table of the export directory: one line
        # per exported name, the name last. binutils prints it either as
        # "[  12] SQLConnect" or, with newer binutils, as
        # "[  12] +base[  13]  000c SQLConnect".
        exports="$("$OBJDUMP" -p -- "$binary" \
            | sed -n '/\[Ordinal\/Name Pointer\] Table/,/^$/p' \
            | awk '/^[[:space:]]*\[[[:space:]]*[0-9]+\]/ { print $NF }')"
        setup_exports='ConfigDSN|ConfigDriver|ConfigTranslator'
        ;;
    *.dylib)
        # Global, defined. Mach-O prepends an underscore to every C symbol.
        exports="$("$NM" -gU -- "$binary" | awk 'NF >= 3 { sub(/^_/, "", $3); print $3 }')"
        ;;
    *)
        # Every defined entry of the dynamic symbol table, whatever its type:
        # a leaked data object is as much a collision as a leaked function.
        # _init/_fini and the linker-defined section markers are not ours.
        exports="$("$NM" -D --defined-only -- "$binary" \
            | awk 'NF >= 3 { print $3 }' \
            | grep -vxE '_init|_fini|_edata|_end|__bss_start')"
        ;;
esac

if [ -z "$exports" ]; then
    echo "error: no exported symbols found in $binary" >&2
    exit 1
fi

total=$(printf '%s\n' "$exports" | wc -l | tr -d ' ')
wanted=$(printf '%s\n' "$exports" | grep -c "^$prefix")
leaked=$(printf '%s\n' "$exports" | grep -v "^$prefix" || true)
if [ -n "$setup_exports" ] && [ -n "$leaked" ]; then
    setup=$(printf '%s\n' "$leaked" | grep -xE "$setup_exports" || true)
    leaked=$(printf '%s\n' "$leaked" | grep -vxE "$setup_exports" || true)
    [ -n "$setup" ] && echo "setup entry points: $(printf '%s' "$setup" | tr '\n' ' ')"
fi

echo "$binary: $total exported symbols, $wanted starting with $prefix"
if [ -n "$leaked" ]; then
    echo "$leaked" | sed 's/^/LEAKED: /' >&2
    echo "error: $binary exports symbols outside the $prefix* API" >&2
    exit 1
fi
if [ "$wanted" -eq 0 ]; then
    echo "error: $binary exports no $prefix* symbol at all" >&2
    exit 1
fi
if [ -n "${setup:-}" ]; then
    echo "ok: only $prefix* symbols and the setup entry points are exported"
else
    echo "ok: only $prefix* symbols are exported"
fi
