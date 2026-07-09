cmake_minimum_required(VERSION 3.16)
project(argus VERSION 0.5.9 LANGUAGES C)

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

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer
                        -fno-sanitize-recover=undefined -g)
    add_link_options(-fsanitize=address,undefined)
    message(STATUS "Sanitizers: ENABLED (ASan + UBSan)")
endif()

# Append our cmake module path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Find dependencies
find_package(PkgConfig REQUIRED)
find_package(ODBC REQUIRED)

pkg_check_modules(GLIB2 REQUIRED glib-2.0>=2.56)
pkg_check_modules(GOBJECT2 REQUIRED gobject-2.0>=2.56)
pkg_check_modules(GIO2 REQUIRED gio-2.0>=2.56)

# Optional: Hive/Impala backend dependencies (Thrift C GLib)
pkg_check_modules(THRIFT_C_GLIB thrift_c_glib>=0.16)
if(THRIFT_C_GLIB_FOUND)
    set(ARGUS_BUILD_THRIFT_BACKENDS ON)
    message(STATUS "Hive/Impala backends: ENABLED (thrift_c_glib found)")

    # Optional: GSSAPI (Kerberos) support for Thrift backends
    pkg_check_modules(KRB5_GSSAPI krb5-gssapi)
    if(NOT KRB5_GSSAPI_FOUND)
        # Fallback: search directly for the library
        find_library(GSSAPI_LIB gssapi_krb5)
        find_path(GSSAPI_INCLUDE gssapi/gssapi.h)
        if(GSSAPI_LIB AND GSSAPI_INCLUDE)
            set(ARGUS_HAS_GSSAPI ON)
        else()
            set(ARGUS_HAS_GSSAPI OFF)
        endif()
    else()
        set(ARGUS_HAS_GSSAPI ON)
    endif()
    if(ARGUS_HAS_GSSAPI)
        message(STATUS "  GSSAPI (Kerberos) auth: ENABLED")
    else()
        message(STATUS "  GSSAPI (Kerberos) auth: DISABLED (install krb5-devel to enable)")
    endif()
else()
    set(ARGUS_BUILD_THRIFT_BACKENDS OFF)
    message(STATUS "Hive/Impala backends: DISABLED (thrift_c_glib not found)")
endif()

# Optional: Trino backend dependencies (libcurl + json-glib)
pkg_check_modules(LIBCURL libcurl>=7.28)
pkg_check_modules(JSON_GLIB json-glib-1.0>=1.2)

if(LIBCURL_FOUND AND JSON_GLIB_FOUND)
    set(ARGUS_BUILD_TRINO ON)
    message(STATUS "Trino backend: ENABLED (libcurl + json-glib found)")
else()
    set(ARGUS_BUILD_TRINO OFF)
    message(STATUS "Trino backend: DISABLED (install libcurl-dev and libjson-glib-dev to enable)")
endif()

# Optional: Phoenix backend dependencies (same as Trino: libcurl + json-glib)
if(LIBCURL_FOUND AND JSON_GLIB_FOUND)
    set(ARGUS_BUILD_PHOENIX ON)
    message(STATUS "Phoenix backend: ENABLED (libcurl + json-glib found)")
else()
    set(ARGUS_BUILD_PHOENIX OFF)
    message(STATUS "Phoenix backend: DISABLED (install libcurl-dev and libjson-glib-dev to enable)")
endif()

# Optional: Pinot backend (HTTP/JSON SQL; same deps as Trino)
if(LIBCURL_FOUND AND JSON_GLIB_FOUND)
    set(ARGUS_BUILD_PINOT ON)
    message(STATUS "Pinot backend: ENABLED (libcurl + json-glib found)")
else()
    set(ARGUS_BUILD_PINOT OFF)
    message(STATUS "Pinot backend: DISABLED (install libcurl-dev and libjson-glib-dev to enable)")
endif()

# Optional: Druid backend (HTTP/JSON SQL; same deps as Trino)
if(LIBCURL_FOUND AND JSON_GLIB_FOUND)
    set(ARGUS_BUILD_DRUID ON)
    message(STATUS "Druid backend: ENABLED (libcurl + json-glib found)")
else()
    set(ARGUS_BUILD_DRUID OFF)
    message(STATUS "Druid backend: DISABLED (install libcurl-dev and libjson-glib-dev to enable)")
endif()

# Optional: BigQuery backend (REST/JSON; libcurl + json-glib; OpenSSL enables
# service-account key authentication)
pkg_check_modules(OPENSSL openssl)
if(LIBCURL_FOUND AND JSON_GLIB_FOUND)
    set(ARGUS_BUILD_BIGQUERY ON)
    if(OPENSSL_FOUND)
        message(STATUS "BigQuery backend: ENABLED (service-account auth via OpenSSL)")
    else()
        message(STATUS "BigQuery backend: ENABLED (AccessToken auth only; install libssl-dev for key files)")
    endif()
else()
    set(ARGUS_BUILD_BIGQUERY OFF)
    message(STATUS "BigQuery backend: DISABLED (install libcurl-dev and libjson-glib-dev to enable)")
endif()

# Optional: Kudu backend dependencies (libkudu_client C++ library)
find_library(KUDU_CLIENT_LIB kudu_client)
find_path(KUDU_CLIENT_INCLUDE kudu/client/client.h)

if(KUDU_CLIENT_LIB AND KUDU_CLIENT_INCLUDE)
    set(ARGUS_BUILD_KUDU ON)
    message(STATUS "Kudu backend: ENABLED (libkudu_client found)")
else()
    set(ARGUS_BUILD_KUDU OFF)
    message(STATUS "Kudu backend: DISABLED (install libkudu-client-dev to enable)")
endif()

# Optional: MySQL-wire backend (StarRocks/Doris/ClickHouse/MySQL/MariaDB)
pkg_check_modules(LIBMARIADB libmariadb)
if(NOT LIBMARIADB_FOUND)
    pkg_check_modules(LIBMARIADB mariadb)
endif()
if(NOT LIBMARIADB_FOUND)
    pkg_check_modules(LIBMARIADB libmysqlclient)
endif()

if(LIBMARIADB_FOUND)
    set(ARGUS_BUILD_MYSQL ON)
    message(STATUS "MySQL-wire backend: ENABLED (libmariadb/mysqlclient found)")
else()
    set(ARGUS_BUILD_MYSQL OFF)
    message(STATUS "MySQL-wire backend: DISABLED (install libmariadb-dev to enable)")
endif()

# Optional: Arrow Flight SQL backend (Dremio / InfluxDB 3 / Doris / StarRocks)
pkg_check_modules(ARROW_FLIGHT_SQL arrow-flight-sql)
if(ARROW_FLIGHT_SQL_FOUND)
    set(ARGUS_BUILD_FLIGHTSQL ON)
    message(STATUS "Arrow Flight SQL backend: ENABLED (arrow-flight-sql ${ARROW_FLIGHT_SQL_VERSION})")
else()
    set(ARGUS_BUILD_FLIGHTSQL OFF)
    message(STATUS "Arrow Flight SQL backend: DISABLED (install libarrow-flight-sql-dev to enable)")
endif()

if(NOT ARGUS_BUILD_THRIFT_BACKENDS AND NOT ARGUS_BUILD_TRINO AND NOT ARGUS_BUILD_PHOENIX AND NOT ARGUS_BUILD_KUDU AND NOT ARGUS_BUILD_MYSQL AND NOT ARGUS_BUILD_FLIGHTSQL AND NOT ARGUS_BUILD_PINOT AND NOT ARGUS_BUILD_DRUID AND NOT ARGUS_BUILD_BIGQUERY)
    message(FATAL_ERROR "No backends available. Install thrift_c_glib (for Hive/Impala) or libcurl+json-glib (for Trino/Phoenix) or libkudu_client (for Kudu) or libmariadb-dev (for MySQL-wire: StarRocks/Doris/ClickHouse) or libarrow-flight-sql-dev (for Arrow Flight SQL: Dremio/InfluxDB 3).")
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
