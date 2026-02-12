cmake_minimum_required(VERSION 3.16)
project(argus VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

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
pkg_check_modules(THRIFT_C_GLIB REQUIRED thrift_c_glib>=0.16)

# POSIX extensions needed for strdup/strndup in C11 mode
add_compile_definitions(_GNU_SOURCE)

# Global compiler flags
add_compile_options(-Wall -Wextra -Wpedantic -Werror=implicit-function-declaration)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0)
else()
    add_compile_options(-O2)
endif()

# Thrift generated code
add_subdirectory(thrift)

# Main library
add_subdirectory(src)

# Tests
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()
