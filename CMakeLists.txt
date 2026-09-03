cmake_minimum_required(VERSION 3.16)
project(argus VERSION 0.6.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Windows/MinGW ODBC headers require compiler extensions (gnu11 not c11)
if(WIN32)
    set(CMAKE_C_EXTENSIONS ON)
else()
    set(CMAKE_C_EXTENSIONS OFF)
endif()

# Build options
option(BUILD_TESTING "Build unit tests" ON)
option(BUILD_INTEGRATION_TESTS "Build integration tests" OFF)
option(BUILD_SHARED_LIBS "Build shared libraries" ON)
option(BUILD_ADBC "Build the Arrow ADBC driver (over the ODBC stack)" ON)
option(ENABLE_ASAN "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)
option(ARGUS_ENABLE_TELEMETRY "Compile in anonymous opt-in usage telemetry (off at runtime by default)" ON)
set(ARGUS_TELEMETRY_ENDPOINT "https://telemetry.vargafoundation.org/v1/events"
    CACHE STRING "Default telemetry collector endpoint (overridable at runtime via ARGUS_TELEMETRY_ENDPOINT)")

option(ENABLE_FUZZING "Build libFuzzer harnesses in fuzz/ (requires Clang)" OFF)

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer
                        -fno-sanitize-recover=undefined -g)
    add_link_options(-fsanitize=address,undefined)
    message(STATUS "Sanitizers: ENABLED (ASan + UBSan)")
endif()

if(ENABLE_FUZZING)
    if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "ENABLE_FUZZING requires Clang (libFuzzer): "
                            "configure with CC=clang CXX=clang++")
    endif()
    # Instrument the whole driver for coverage-guided fuzzing; the harnesses
    # themselves add -fsanitize=fuzzer to get the libFuzzer main.
    add_compile_options(-fsanitize=fuzzer-no-link,address
                        -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address)
    message(STATUS "Fuzzing: ENABLED (libFuzzer + ASan)")
endif()

# Append our cmake module path. PROJECT_SOURCE_DIR (not CMAKE_SOURCE_DIR) so
# the driver also builds when embedded via add_subdirectory in a super-project.
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Find dependencies
find_package(PkgConfig REQUIRED)
find_package(ODBC REQUIRED)

pkg_check_modules(GLIB2 REQUIRED glib-2.0>=2.56)
pkg_check_modules(GOBJECT2 REQUIRED gobject-2.0>=2.56)
pkg_check_modules(GIO2 REQUIRED gio-2.0>=2.56)

# ---------------------------------------------------------------------------
# Backend selection
#
# Every backend is a tri-state request, ARGUS_WITH_<NAME>:
#   AUTO  build it when its dependencies are found (the default)
#   ON    require it: configuring fails when a dependency is missing
#   OFF   leave it out even when the dependencies are present
# The decision lands in ARGUS_BUILD_<NAME> (ON/OFF), which src/ and tests/ read.
#
# ARGUS_RELEASE=ON promotes every backend a release artefact ships with from
# AUTO to ON. The v0.6.0 Linux and macOS artefacts went out without PostgreSQL
# and MySQL-wire (and the Windows installer without PostgreSQL) because the
# release runners lacked the client libraries and the auto-detection only
# printed a STATUS line; a release configure now fails on that runner instead.
# Kudu and Flight SQL stay optional: their client libraries are not packaged
# for the release platforms.
# ---------------------------------------------------------------------------
option(ARGUS_RELEASE "Require every backend a release artefact ships with" OFF)
set(ARGUS_RELEASE_BACKENDS THRIFT_BACKENDS TRINO PHOENIX PINOT DRUID BIGQUERY MYSQL POSTGRES)

function(argus_backend_request name doc)
    set(ARGUS_WITH_${name} AUTO CACHE STRING "${doc}: AUTO, ON or OFF")
    set_property(CACHE ARGUS_WITH_${name} PROPERTY STRINGS AUTO ON OFF)
endfunction()

argus_backend_request(THRIFT_BACKENDS "Hive/Impala backends (thrift_c_glib)")
argus_backend_request(GSSAPI "Kerberos auth for Hive/Impala (krb5-gssapi)")
argus_backend_request(TRINO "Trino backend (libcurl + json-glib)")
argus_backend_request(PHOENIX "Phoenix backend (libcurl + json-glib)")
argus_backend_request(PINOT "Pinot backend (libcurl + json-glib)")
argus_backend_request(DRUID "Druid backend (libcurl + json-glib)")
argus_backend_request(BIGQUERY "BigQuery backend (libcurl + json-glib)")
argus_backend_request(KUDU "Kudu backend (libkudu_client)")
argus_backend_request(MYSQL "MySQL-wire backend (libmariadb)")
argus_backend_request(POSTGRES "PostgreSQL/Greenplum/Cloudberry backends (libpq)")
argus_backend_request(FLIGHTSQL "Arrow Flight SQL backend (arrow-flight-sql)")

# argus_resolve_backend(<name> <result-var> <label> <found> <missing> <install> [<detail>])
# Combines the ARGUS_WITH_<name> request with the probe result <found> into
# <result-var> (ON/OFF). A backend that is required but whose dependency
# <missing> was not found stops the configure step with the <install> hint.
function(argus_resolve_backend name result label found missing install)
    string(TOUPPER "${ARGUS_WITH_${name}}" request)
    if(request MATCHES "^(1|ON|YES|TRUE|Y)$")
        set(request ON)
    elseif(request MATCHES "^(0|OFF|NO|FALSE|N|IGNORE)?$")
        set(request OFF)
    elseif(NOT request STREQUAL "AUTO")
        message(FATAL_ERROR "ARGUS_WITH_${name} must be AUTO, ON or OFF (got '${ARGUS_WITH_${name}}')")
    endif()
    set(why "ARGUS_WITH_${name}=${request}")
    if(request STREQUAL "AUTO" AND ARGUS_RELEASE AND name IN_LIST ARGUS_RELEASE_BACKENDS)
        set(request ON)
        set(why "ARGUS_RELEASE=ON")
    endif()

    if(request STREQUAL "OFF")
        set(${result} OFF PARENT_SCOPE)
        message(STATUS "${label}: DISABLED (${why})")
    elseif(found)
        set(${result} ON PARENT_SCOPE)
        message(STATUS "${label}: ENABLED${ARGN}")
    elseif(request STREQUAL "ON")
        message(FATAL_ERROR "${label}: required by ${why}, but ${missing} was not found: ${install}.")
    else()
        set(${result} OFF PARENT_SCOPE)
        message(STATUS "${label}: DISABLED (${missing} not found; ${install}, or ARGUS_WITH_${name}=ON to make this an error)")
    endif()
endfunction()

# Hive/Impala backends (Thrift C GLib)
pkg_check_modules(THRIFT_C_GLIB thrift_c_glib>=0.16)
argus_resolve_backend(THRIFT_BACKENDS ARGUS_BUILD_THRIFT_BACKENDS "Hive/Impala backends"
    "${THRIFT_C_GLIB_FOUND}" "thrift_c_glib" "install libthrift-c-glib-dev and thrift-compiler"
    " (thrift_c_glib ${THRIFT_C_GLIB_VERSION})")

if(ARGUS_BUILD_THRIFT_BACKENDS)
    # GSSAPI (Kerberos) for the Thrift transports. Windows uses SSPI instead
    # (see src/CMakeLists.txt), so GSSAPI is never required there by default.
    pkg_check_modules(KRB5_GSSAPI krb5-gssapi)
    set(GSSAPI_PROBE_FOUND ${KRB5_GSSAPI_FOUND})
    if(NOT KRB5_GSSAPI_FOUND)
        # Fallback: search directly for the library
        find_library(GSSAPI_LIB gssapi_krb5)
        find_path(GSSAPI_INCLUDE gssapi/gssapi.h)
        if(GSSAPI_LIB AND GSSAPI_INCLUDE)
            set(GSSAPI_PROBE_FOUND TRUE)
        endif()
    endif()
    argus_resolve_backend(GSSAPI ARGUS_HAS_GSSAPI "  GSSAPI (Kerberos) auth"
        "${GSSAPI_PROBE_FOUND}" "krb5-gssapi" "install libkrb5-dev")
else()
    set(ARGUS_HAS_GSSAPI OFF)
    string(TOUPPER "${ARGUS_WITH_GSSAPI}" gssapi_request)
    if(gssapi_request MATCHES "^(1|ON|YES|TRUE|Y)$")
        message(FATAL_ERROR "ARGUS_WITH_GSSAPI=ON needs the Hive/Impala backends (ARGUS_WITH_THRIFT_BACKENDS)")
    endif()
endif()

# HTTP/JSON backends: Trino, Phoenix, Pinot, Druid and BigQuery all speak
# through libcurl + json-glib. OpenSSL additionally enables BigQuery
# service-account key authentication.
pkg_check_modules(LIBCURL libcurl>=7.28)
pkg_check_modules(JSON_GLIB json-glib-1.0>=1.2)
pkg_check_modules(OPENSSL openssl)
set(HTTP_JSON_FOUND FALSE)
if(LIBCURL_FOUND AND JSON_GLIB_FOUND)
    set(HTTP_JSON_FOUND TRUE)
endif()
set(HTTP_JSON_MISSING "libcurl or json-glib")
set(HTTP_JSON_INSTALL "install libcurl4-openssl-dev and libjson-glib-dev")

argus_resolve_backend(TRINO ARGUS_BUILD_TRINO "Trino backend" "${HTTP_JSON_FOUND}"
    "${HTTP_JSON_MISSING}" "${HTTP_JSON_INSTALL}")
argus_resolve_backend(PHOENIX ARGUS_BUILD_PHOENIX "Phoenix backend" "${HTTP_JSON_FOUND}"
    "${HTTP_JSON_MISSING}" "${HTTP_JSON_INSTALL}")
argus_resolve_backend(PINOT ARGUS_BUILD_PINOT "Pinot backend" "${HTTP_JSON_FOUND}"
    "${HTTP_JSON_MISSING}" "${HTTP_JSON_INSTALL}")
argus_resolve_backend(DRUID ARGUS_BUILD_DRUID "Druid backend" "${HTTP_JSON_FOUND}"
    "${HTTP_JSON_MISSING}" "${HTTP_JSON_INSTALL}")
if(OPENSSL_FOUND)
    set(BIGQUERY_DETAIL " (service-account auth via OpenSSL)")
else()
    set(BIGQUERY_DETAIL " (AccessToken auth only; install libssl-dev for key files)")
endif()
argus_resolve_backend(BIGQUERY ARGUS_BUILD_BIGQUERY "BigQuery backend" "${HTTP_JSON_FOUND}"
    "${HTTP_JSON_MISSING}" "${HTTP_JSON_INSTALL}" "${BIGQUERY_DETAIL}")

# Kudu backend (libkudu_client C++ library)
find_library(KUDU_CLIENT_LIB kudu_client)
find_path(KUDU_CLIENT_INCLUDE kudu/client/client.h)
set(KUDU_FOUND FALSE)
if(KUDU_CLIENT_LIB AND KUDU_CLIENT_INCLUDE)
    set(KUDU_FOUND TRUE)
endif()
argus_resolve_backend(KUDU ARGUS_BUILD_KUDU "Kudu backend" "${KUDU_FOUND}"
    "libkudu_client" "install libkudu-client-dev")

# MySQL-wire backend (StarRocks/Doris/ClickHouse/MySQL/MariaDB)
pkg_check_modules(LIBMARIADB libmariadb)
if(NOT LIBMARIADB_FOUND)
    pkg_check_modules(LIBMARIADB mariadb)
endif()
if(NOT LIBMARIADB_FOUND)
    pkg_check_modules(LIBMARIADB libmysqlclient)
endif()
argus_resolve_backend(MYSQL ARGUS_BUILD_MYSQL "MySQL-wire backend" "${LIBMARIADB_FOUND}"
    "libmariadb" "install libmariadb-dev" " (libmariadb ${LIBMARIADB_VERSION})")

# PostgreSQL-family backends (PostgreSQL / Greenplum / Apache Cloudberry)
# One libpq-based core serves all three; they differ in catalog and dialect.
pkg_check_modules(LIBPQ libpq)
if(NOT LIBPQ_FOUND)
    # Homebrew's libpq is keg-only and Windows/MSYS2 layouts vary, so fall back
    # to a direct search before giving up on the backend.
    find_library(PQ_LIB NAMES pq libpq)
    find_path(PQ_INC NAMES libpq-fe.h PATH_SUFFIXES postgresql)
    if(PQ_LIB AND PQ_INC)
        set(LIBPQ_FOUND TRUE)
        set(LIBPQ_LIBRARIES ${PQ_LIB})
        set(LIBPQ_INCLUDE_DIRS ${PQ_INC})
        set(LIBPQ_VERSION "unknown")
    endif()
endif()
argus_resolve_backend(POSTGRES ARGUS_BUILD_POSTGRES "PostgreSQL/Greenplum/Cloudberry backends"
    "${LIBPQ_FOUND}" "libpq" "install libpq-dev; Homebrew's keg-only libpq needs its lib/pkgconfig on PKG_CONFIG_PATH"
    " (libpq ${LIBPQ_VERSION})")

# Arrow Flight SQL backend (Dremio / InfluxDB 3 / Doris / StarRocks)
pkg_check_modules(ARROW_FLIGHT_SQL arrow-flight-sql)
argus_resolve_backend(FLIGHTSQL ARGUS_BUILD_FLIGHTSQL "Arrow Flight SQL backend" "${ARROW_FLIGHT_SQL_FOUND}"
    "arrow-flight-sql" "install libarrow-flight-sql-dev"
    " (arrow-flight-sql ${ARROW_FLIGHT_SQL_VERSION})")

if(NOT ARGUS_BUILD_THRIFT_BACKENDS AND NOT ARGUS_BUILD_TRINO AND NOT ARGUS_BUILD_PHOENIX AND NOT ARGUS_BUILD_KUDU AND NOT ARGUS_BUILD_MYSQL AND NOT ARGUS_BUILD_FLIGHTSQL AND NOT ARGUS_BUILD_PINOT AND NOT ARGUS_BUILD_DRUID AND NOT ARGUS_BUILD_BIGQUERY AND NOT ARGUS_BUILD_POSTGRES)
    message(FATAL_ERROR "No backends available. Install thrift_c_glib (for Hive/Impala) or libcurl+json-glib (for Trino/Phoenix) or libkudu_client (for Kudu) or libmariadb-dev (for MySQL-wire: StarRocks/Doris/ClickHouse) or libpq-dev (for PostgreSQL/Greenplum/Cloudberry) or libarrow-flight-sql-dev (for Arrow Flight SQL: Dremio/InfluxDB 3).")
endif()

# Anonymous opt-in telemetry (requires libcurl for the HTTPS transport)
if(ARGUS_ENABLE_TELEMETRY AND LIBCURL_FOUND)
    set(ARGUS_BUILD_TELEMETRY ON)
    message(STATUS "Telemetry: ENABLED at build (opt-in; off by default at runtime)")
elseif(ARGUS_ENABLE_TELEMETRY)
    set(ARGUS_BUILD_TELEMETRY OFF)
    message(STATUS "Telemetry: DISABLED (requires libcurl-dev)")
else()
    set(ARGUS_BUILD_TELEMETRY OFF)
    message(STATUS "Telemetry: DISABLED (ARGUS_ENABLE_TELEMETRY=OFF)")
endif()

# Kudu and Flight SQL are C++ backends. Enable CXX here at the top-level scope:
# calling enable_language(CXX) from a subdirectory breaks CMake's regenerate step
# ("required internal CMake variable not set").
if(ARGUS_BUILD_KUDU OR ARGUS_BUILD_FLIGHTSQL)
    enable_language(CXX)
endif()

# POSIX extensions needed for strdup/strndup in C11 mode
if(NOT WIN32)
    add_compile_definitions(_GNU_SOURCE)
endif()

# MinGW ODBC headers (sqltypes.h) require Windows type definitions from
# windows.h to be available. Force-include it before any source file.
# WIN32_LEAN_AND_MEAN keeps winsock.h out of windows.h so that the
# winsock2 users (Trino OAuth2 loopback) do not hit the classic
# winsock/winsock2 redefinition clash.
if(WIN32)
    add_compile_definitions(WIN32_LEAN_AND_MEAN NOMINMAX)
    if(MSVC)
        add_compile_options(/FIwindows.h)
    else()
        add_compile_options(-include windows.h)
    endif()
endif()

# Global compiler flags
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Werror=implicit-function-declaration)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(MSVC)
        add_compile_options(/Zi /Od)
    else()
        add_compile_options(-g -O0)
    endif()
else()
    if(MSVC)
        add_compile_options(/O2)
    else()
        add_compile_options(-O2)
    endif()
endif()

# ---------------------------------------------------------------------------
# Hardening. The driver is loaded into Excel, Tableau and whatever else a
# user points at a DSN, and it parses bytes from the network before the
# connection is authenticated, so it gets the floor a distribution applies
# to its own packages: stack canaries, fortified libc calls, read-only
# relocations bound at load time, non-executable stack, and DEP/ASLR on
# Windows. scripts/check-hardening.sh reads the result back out of the
# built driver in CI. ARGUS_HARDENING=OFF is for profiling and for a
# toolchain that rejects one of the flags.
# ---------------------------------------------------------------------------
option(ARGUS_HARDENING "Stack protector, FORTIFY_SOURCE, RELRO/BIND_NOW, DEP/ASLR" ON)
if(ARGUS_HARDENING)
    if(MSVC)
        add_compile_options(/GS /guard:cf)
        add_link_options(/DYNAMICBASE /HIGHENTROPYVA /NXCOMPAT /guard:cf)
    else()
        include(CheckCCompilerFlag)
        # Probed rather than assumed: -fcf-protection is x86-only and Apple's
        # clang rejects -fstack-clash-protection on arm64.
        foreach(flag IN ITEMS -fstack-protector-strong -fstack-clash-protection -fcf-protection=full)
            string(MAKE_C_IDENTIFIER "ARGUS_CC_HAS${flag}" have)
            check_c_compiler_flag("${flag}" ${have})
            if(${have})
                add_compile_options("${flag}")
            endif()
        endforeach()
        # FORTIFY needs an optimising build (glibc warns at -O0), and its
        # checked wrappers would bypass the sanitizer interceptors. tests/
        # reads ARGUS_FORTIFY to know whether to expect the *_chk imports.
        if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug" AND NOT ENABLE_ASAN AND NOT ENABLE_FUZZING)
            add_compile_options(-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2)
            set(ARGUS_FORTIFY ON)
        endif()
        if(WIN32)
            # MinGW: GCC pulls libssp in only when the protector flag is also
            # on the link line. binutils has defaulted the three PE flags on
            # since 2.36; stating them keeps older toolchains honest.
            if(ARGUS_CC_HAS_fstack_protector_strong)
                add_link_options(-fstack-protector-strong)
            endif()
            add_link_options(-Wl,--dynamicbase -Wl,--high-entropy-va -Wl,--nxcompat)
        elseif(NOT APPLE)
            add_link_options(-Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack)
        endif()
    endif()
    message(STATUS "Hardening: ENABLED")
else()
    message(STATUS "Hardening: DISABLED (ARGUS_HARDENING=OFF)")
endif()

# SQL_DRIVER_NAME is the driver's own file name, which differs per platform.
# It lives here rather than in src/ because tests/ asserts on it too, and a
# second copy of the rule is exactly how the two drifted apart before.
if(WIN32)
    set(ARGUS_DRIVER_FILE_NAME "argus_odbc.dll")
elseif(APPLE)
    set(ARGUS_DRIVER_FILE_NAME "libargus_odbc.dylib")
else()
    set(ARGUS_DRIVER_FILE_NAME "libargus_odbc.so")
endif()

# Thrift generated code (only if Hive/Impala backends enabled)
if(ARGUS_BUILD_THRIFT_BACKENDS)
    add_subdirectory(thrift)
endif()

# Main library
add_subdirectory(src)

# Tests
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

# libFuzzer harnesses (need the static lib from BUILD_TESTING)
if(ENABLE_FUZZING AND BUILD_TESTING)
    add_subdirectory(fuzz)
endif()
