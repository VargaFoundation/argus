cmake_minimum_required(VERSION 3.16)
project(argus VERSION 0.1.0 LANGUAGES C)

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

if(NOT ARGUS_BUILD_THRIFT_BACKENDS AND NOT ARGUS_BUILD_TRINO)
    message(FATAL_ERROR "No backends available. Install thrift_c_glib (for Hive/Impala) or libcurl+json-glib (for Trino).")
endif()

# POSIX extensions needed for strdup/strndup in C11 mode
if(NOT WIN32)
    add_compile_definitions(_GNU_SOURCE)
endif()

# MinGW ODBC headers (sqltypes.h) require Windows type definitions from
# windows.h to be available. Force-include it before any source file.
if(WIN32)
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
