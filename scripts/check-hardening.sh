#!/bin/sh
# Assert that a built driver carries the hardening the build asked for.
#
# CMakeLists.txt (ARGUS_HARDENING) adds stack canaries, FORTIFY_SOURCE,
# RELRO + BIND_NOW and a non-executable stack on ELF, DEP/ASLR/high-entropy
# ASLR on PE. Flags are easy to lose -- a toolchain that ignores one, a
# target that forgets to inherit them -- so this script reads the properties
# back out of the binary, the way a distribution's hardening-check does.
#
# Usage: scripts/check-hardening.sh <driver-binary> [--release]
#   --release additionally requires fortified libc calls, which only exist
#   in an optimised build (FORTIFY_SOURCE is off at -O0 and under the
#   sanitizers). The format is picked from the extension: .dll is PE,
#   .dylib is Mach-O, anything else is ELF. Set NM/OBJDUMP/READELF to point
#   at a specific binutils.

set -u

if [ $# -lt 1 ]; then
    echo "usage: $0 <driver-binary> [--release]" >&2
    exit 2
fi

binary="$1"
release=0
[ "${2:-}" = "--release" ] && release=1
NM="${NM:-nm}"
OBJDUMP="${OBJDUMP:-objdump}"
READELF="${READELF:-readelf}"

if [ ! -f "$binary" ]; then
    echo "error: $binary does not exist" >&2
    exit 2
fi

failed=0
check() {
    # check <label> <status>   status: 0 = present
    if [ "$2" -eq 0 ]; then
        echo "  ok      $1"
    else
        echo "  MISSING $1" >&2
        failed=1
    fi
}

echo "$binary:"
case "$binary" in
    *.dll)
        chars="$("$OBJDUMP" -p -- "$binary" | awk '/DllCharacteristics/ { print $2; exit }')"
        if [ -z "$chars" ]; then
            echo "error: no DllCharacteristics in $binary" >&2
            exit 1
        fi
        bits=$((0x$chars))
        # IMAGE_DLLCHARACTERISTICS_* from winnt.h
        check "high-entropy ASLR (0x0020)" $(( (bits & 0x0020) == 0 ))
        check "ASLR / DYNAMIC_BASE (0x0040)" $(( (bits & 0x0040) == 0 ))
        check "DEP / NX_COMPAT (0x0100)" $(( (bits & 0x0100) == 0 ))
        # __stack_chk_fail is either defined in the DLL (the mingw-w64 CRT
        # provides it statically) or imported from libssp-0.dll: nm sees both
        # while the COFF symbol table is there, the import table survives a
        # strip when it was linked from the DLL.
        { "$NM" -- "$binary" 2>/dev/null; "$OBJDUMP" -p -- "$binary"; } | grep -q '__stack_chk_fail'
        check "stack protector (__stack_chk_fail referenced)" $?
        ;;
    *.dylib)
        "$NM" -u -- "$binary" | grep -q '___stack_chk_fail'
        check "stack protector (___stack_chk_fail imported)" $?
        if [ "$release" -eq 1 ]; then
            "$NM" -u -- "$binary" | grep -qE '(^|[[:space:]])___[a-z_0-9]+_chk$'
            check "FORTIFY_SOURCE (___*_chk imported)" $?
        fi
        ;;
    *)
        dyn="$("$READELF" -d -- "$binary")"
        printf '%s\n' "$dyn" | grep -qE '\(FLAGS(_1)?\).*(BIND_NOW|NOW)'
        check "BIND_NOW" $?
        "$READELF" -lW -- "$binary" | grep -q 'GNU_RELRO'
        check "RELRO segment" $?
        stack="$("$READELF" -lW -- "$binary" | awk '/GNU_STACK/ { print $7 }')"
        [ -n "$stack" ] && [ "$stack" = "RW" ]
        check "non-executable stack (GNU_STACK ${stack:-absent})" $?
        undef="$("$NM" -D --undefined-only -- "$binary")"
        printf '%s\n' "$undef" | grep -q '__stack_chk_fail'
        check "stack protector (__stack_chk_fail imported)" $?
        if [ "$release" -eq 1 ]; then
            printf '%s\n' "$undef" | grep -qE '__[a-z_0-9]+_chk(@|$)'
            check "FORTIFY_SOURCE (__*_chk imported)" $?
        fi
        ;;
esac

if [ "$failed" -ne 0 ]; then
    echo "error: $binary is missing hardening listed above" >&2
    exit 1
fi
echo "ok: hardening present"
