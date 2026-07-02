#!/bin/bash
# Bundle the transitive MinGW DLL dependencies of the driver into the
# installer staging directory, and fail if any non-system dependency
# cannot be found. This replaces a hardcoded DLL list, which silently
# drifted whenever MSYS2 renamed a DLL or a package grew a new
# dependency (zlib1.dll vs libz1.dll, curl's HTTP/3 ngtcp2 DLLs, ...):
# the resulting installer loaded fine on the build runner but failed
# with "DLL not found" on clean machines.
#
# Usage: bundle_windows_dlls.sh <source-bin-dir> <staging-dir> <seed>...
# Env:   OBJDUMP (default: objdump)

set -u

if [ $# -lt 3 ]; then
    echo "usage: $0 <source-bin-dir> <staging-dir> <seed-binary>..." >&2
    exit 2
fi

source_bin="$1"
staging="$2"
shift 2

OBJDUMP="${OBJDUMP:-objdump}"

# DLLs provided by Windows itself (never bundled). Matched
# case-insensitively against the import table entries.
system_re='^(kernel32|user32|gdi32|advapi32|shell32|ole32|oleaut32|shlwapi|comdlg32|comctl32|ws2_32|msvcrt|ucrtbase|vcruntime[0-9]*|api-ms-win-.*|ext-ms-.*|bcrypt|bcryptprimitives|crypt32|ncrypt|rpcrt4|sechost|secur32|iphlpapi|dnsapi|winmm|version|setupapi|cfgmgr32|userenv|wldap32|imm32|odbc32|odbccp32|odbcint|winhttp|wininet|normaliz|dbghelp|wintrust|msimg32|usp10|powrprof|profapi|ntdll)\.dll$'

queue=("$@")
seen=" "
missing=0
copied=0

while [ ${#queue[@]} -gt 0 ]; do
    bin="${queue[0]}"
    queue=("${queue[@]:1}")

    deps=$("$OBJDUMP" -p "$bin" 2>/dev/null | awk '/DLL Name:/{print $3}')
    if [ -z "$deps" ]; then
        echo "warning: no import table read from $bin" >&2
        continue
    fi

    for dep in $deps; do
        dep_lc=$(echo "$dep" | tr 'A-Z' 'a-z')
        case "$seen" in *" $dep_lc "*) continue ;; esac
        seen="$seen$dep_lc "

        if echo "$dep_lc" | grep -qE "$system_re"; then
            continue
        fi

        if [ -f "$staging/$dep" ] || [ -f "$staging/$dep_lc" ]; then
            queue+=("$staging/$dep")
            continue
        fi

        if [ -f "$source_bin/$dep" ]; then
            src="$source_bin/$dep"
        elif [ -f "$source_bin/$dep_lc" ]; then
            src="$source_bin/$dep_lc"
        else
            echo "MISSING: $dep (imported by $(basename "$bin"))" >&2
            missing=1
            continue
        fi

        cp "$src" "$staging/"
        copied=$((copied + 1))
        queue+=("$src")
    done
done

if [ "$missing" -ne 0 ]; then
    echo "Bundled DLL set is incomplete; the driver would not load on a clean machine." >&2
    exit 1
fi

echo "DLL closure complete: $copied dependencies bundled into $staging"
