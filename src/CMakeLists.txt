# Argus ODBC driver shared library

set(ARGUS_SOURCES
    odbc/api_entry.c
    odbc/handle.c
    odbc/connect.c
    odbc/execute.c
    odbc/fetch.c
    odbc/catalog.c
    odbc/info.c
    odbc/diag.c
    odbc/attr.c
    odbc/util.c
    odbc/log.c
    odbc/unicode.c
    odbc/desc.c
    odbc/dsn.c
    odbc/metadata_cache.c
    odbc/pool.c
    backend/backend.c
)

set(ARGUS_PRIVATE_INCLUDE_DIRS
    ${ODBC_INCLUDE_DIRS}
    ${GLIB2_INCLUDE_DIRS}
    ${GOBJECT2_INCLUDE_DIRS}
    ${GIO2_INCLUDE_DIRS}
)

set(ARGUS_LINK_LIBS
    ${ODBC_LIBRARIES}
    ${GLIB2_LIBRARIES}
    ${GOBJECT2_LIBRARIES}
    ${GIO2_LIBRARIES}
)

set(ARGUS_LINK_DIRS
    ${GLIB2_LIBRARY_DIRS}
    ${GOBJECT2_LIBRARY_DIRS}
    ${GIO2_LIBRARY_DIRS}
)

set(ARGUS_COMPILE_DEFS ARGUS_BUILD_DLL SQL_NOUNICODEMAP)

# Windows: the installer setup entry points (ConfigDSN), winsock for the
# Trino OAuth2 loopback, odbccp32 for the installer API and advapi32 for
# registry DSN lookup. Added to ARGUS_LINK_LIBS so the static library
# (unit tests) gets them too, not just the shared driver target.
if(WIN32)
    list(APPEND ARGUS_SOURCES odbc/setup.c)
    list(APPEND ARGUS_LINK_LIBS ws2_32 odbccp32 advapi32)
endif()

# Conditionally add Hive/Impala backends (require Thrift C GLib)
if(ARGUS_BUILD_THRIFT_BACKENDS)
    list(APPEND ARGUS_SOURCES
        backend/thrift_sasl.c
        backend/hive/hive_backend.c
        backend/hive/hive_session.c
        backend/hive/hive_query.c
        backend/hive/hive_fetch.c
        backend/hive/hive_metadata.c
        backend/hive/hive_types.c
        backend/impala/impala_backend.c
        backend/impala/impala_session.c
        backend/impala/impala_query.c
        backend/impala/impala_fetch.c
        backend/impala/impala_metadata.c
        backend/impala/impala_types.c
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/hive
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/impala
        ${THRIFT_C_GLIB_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS
        argus_thrift_generated
        ${THRIFT_C_GLIB_LIBRARIES}
    )
    list(APPEND ARGUS_LINK_DIRS
        ${THRIFT_C_GLIB_LIBRARY_DIRS}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_THRIFT_BACKENDS)
    # Optional: HTTP transport for Hive (requires libcurl)
    if(LIBCURL_FOUND)
        list(APPEND ARGUS_SOURCES backend/hive/thrift_http_transport.c)
        list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS ${LIBCURL_INCLUDE_DIRS})
        list(APPEND ARGUS_LINK_LIBS ${LIBCURL_LIBRARIES})
        list(APPEND ARGUS_LINK_DIRS ${LIBCURL_LIBRARY_DIRS})
        list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_CURL)
        message(STATUS "  Hive HTTP transport: ENABLED")
    else()
        message(STATUS "  Hive HTTP transport: DISABLED (install libcurl-dev)")
    endif()
    if(ARGUS_HAS_GSSAPI)
        list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_GSSAPI)
        if(KRB5_GSSAPI_FOUND)
            list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS ${KRB5_GSSAPI_INCLUDE_DIRS})
            list(APPEND ARGUS_LINK_LIBS ${KRB5_GSSAPI_LIBRARIES})
            list(APPEND ARGUS_LINK_DIRS ${KRB5_GSSAPI_LIBRARY_DIRS})
        else()
            list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS ${GSSAPI_INCLUDE})
            list(APPEND ARGUS_LINK_LIBS ${GSSAPI_LIB})
        endif()
    endif()
endif()

# Conditionally add Trino backend (requires libcurl + json-glib)
if(ARGUS_BUILD_TRINO)
    list(APPEND ARGUS_SOURCES
        backend/trino/trino_backend.c
        backend/trino/trino_session.c
        backend/trino/trino_query.c
        backend/trino/trino_fetch.c
        backend/trino/trino_metadata.c
        backend/trino/trino_types.c
        backend/trino/trino_spooling.c
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/trino
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS
        ${LIBCURL_LIBRARIES}
        ${JSON_GLIB_LIBRARIES}
    )
    list(APPEND ARGUS_LINK_DIRS
        ${LIBCURL_LIBRARY_DIRS}
        ${JSON_GLIB_LIBRARY_DIRS}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_TRINO)
endif()

# Conditionally add Phoenix backend (requires libcurl + json-glib, same as Trino)
if(ARGUS_BUILD_PHOENIX)
    list(APPEND ARGUS_SOURCES
        backend/phoenix/phoenix_backend.c
        backend/phoenix/phoenix_session.c
        backend/phoenix/phoenix_query.c
        backend/phoenix/phoenix_fetch.c
        backend/phoenix/phoenix_metadata.c
        backend/phoenix/phoenix_types.c
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/phoenix
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS
        ${LIBCURL_LIBRARIES}
        ${JSON_GLIB_LIBRARIES}
    )
    list(APPEND ARGUS_LINK_DIRS
        ${LIBCURL_LIBRARY_DIRS}
        ${JSON_GLIB_LIBRARY_DIRS}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_PHOENIX)
endif()

# Conditionally add Pinot backend (HTTP/JSON SQL; libcurl + json-glib)
if(ARGUS_BUILD_PINOT)
    list(APPEND ARGUS_SOURCES
        backend/pinot/pinot_backend.c
        backend/pinot/pinot_types.c
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/pinot
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS
        ${LIBCURL_LIBRARIES}
        ${JSON_GLIB_LIBRARIES}
    )
    list(APPEND ARGUS_LINK_DIRS
        ${LIBCURL_LIBRARY_DIRS}
        ${JSON_GLIB_LIBRARY_DIRS}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_PINOT)
endif()

# Conditionally add Druid backend (HTTP/JSON SQL; libcurl + json-glib)
if(ARGUS_BUILD_DRUID)
    list(APPEND ARGUS_SOURCES
        backend/druid/druid_backend.c
        backend/druid/druid_types.c
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/druid
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS ${LIBCURL_LIBRARIES} ${JSON_GLIB_LIBRARIES})
    list(APPEND ARGUS_LINK_DIRS ${LIBCURL_LIBRARY_DIRS} ${JSON_GLIB_LIBRARY_DIRS})
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_DRUID)
endif()

# Conditionally add Kudu backend (requires libkudu_client C++ library)
if(ARGUS_BUILD_KUDU)
    enable_language(CXX)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    list(APPEND ARGUS_SOURCES
        backend/kudu/kudu_backend.c
        backend/kudu/kudu_types.c
        backend/kudu/kudu_sql_parser.c
        backend/kudu/kudu_fetch.c
        backend/kudu/kudu_session.cpp
        backend/kudu/kudu_query.cpp
        backend/kudu/kudu_metadata.cpp
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/kudu
        ${KUDU_CLIENT_INCLUDE}
    )
    list(APPEND ARGUS_LINK_LIBS
        ${KUDU_CLIENT_LIB}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_KUDU)
endif()

# Conditionally add MySQL-wire backend (StarRocks/Doris/ClickHouse/MySQL)
if(ARGUS_BUILD_MYSQL)
    list(APPEND ARGUS_SOURCES
        backend/mysql/mywire_backend.c
        backend/mysql/mywire_metadata.c
        backend/mysql/mywire_types.c
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/mysql
        ${LIBMARIADB_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS
        ${LIBMARIADB_LIBRARIES}
    )
    list(APPEND ARGUS_LINK_DIRS
        ${LIBMARIADB_LIBRARY_DIRS}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_MYSQL)
endif()

# Conditionally add Arrow Flight SQL backend (Dremio/InfluxDB 3/Doris/StarRocks)
if(ARGUS_BUILD_FLIGHTSQL)
    # CXX is enabled at the top-level scope (see top CMakeLists.txt). Arrow's
    # modern headers require C++17 (Arrow >= 18 needs C++20); use C++20.
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    list(APPEND ARGUS_SOURCES
        backend/flightsql/flightsql_backend.cpp
        backend/flightsql/flightsql_convert.cpp
    )
    list(APPEND ARGUS_PRIVATE_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/flightsql
        ${ARROW_FLIGHT_SQL_INCLUDE_DIRS}
    )
    list(APPEND ARGUS_LINK_LIBS
        ${ARROW_FLIGHT_SQL_LIBRARIES}
    )
    list(APPEND ARGUS_LINK_DIRS
        ${ARROW_FLIGHT_SQL_LIBRARY_DIRS}
    )
    list(APPEND ARGUS_COMPILE_DEFS ARGUS_HAS_FLIGHTSQL)
endif()

# Windows version resource (embeds version/company info in the DLL)
if(WIN32)
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/version.rc.in"
        "${CMAKE_CURRENT_BINARY_DIR}/version.rc"
        @ONLY
    )
    list(APPEND ARGUS_SOURCES "${CMAKE_CURRENT_BINARY_DIR}/version.rc")
endif()

add_library(argus_odbc ${ARGUS_SOURCES})

target_include_directories(argus_odbc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${ARGUS_PRIVATE_INCLUDE_DIRS}
)

target_link_libraries(argus_odbc
    PRIVATE
        ${ARGUS_LINK_LIBS}
)

target_link_directories(argus_odbc
    PRIVATE
        ${ARGUS_LINK_DIRS}
)

target_compile_definitions(argus_odbc PRIVATE ${ARGUS_COMPILE_DEFS})

if(WIN32)
    # Trino's OAuth2 browser-SSO loopback uses winsock.
    target_link_libraries(argus_odbc PRIVATE ws2_32)
endif()

set_target_properties(argus_odbc PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    OUTPUT_NAME argus_odbc
    C_VISIBILITY_PRESET hidden
)

# Static library for unit tests (all symbols visible)
if(BUILD_TESTING)
    add_library(argus_odbc_static STATIC ${ARGUS_SOURCES})
    target_include_directories(argus_odbc_static
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        PRIVATE
            ${ARGUS_PRIVATE_INCLUDE_DIRS}
    )
    target_link_libraries(argus_odbc_static
        PUBLIC
            ${ARGUS_LINK_LIBS}
    )
    target_link_directories(argus_odbc_static
        PUBLIC
            ${ARGUS_LINK_DIRS}
    )
    target_compile_definitions(argus_odbc_static PRIVATE ${ARGUS_COMPILE_DEFS})
    set_target_properties(argus_odbc_static PROPERTIES
        POSITION_INDEPENDENT_CODE ON
    )
endif()

# Optional ADBC driver (Arrow-native surface over the ODBC stack)
if(BUILD_ADBC)
    add_library(argus_adbc SHARED adbc/argus_adbc.c)
    target_include_directories(argus_adbc
        PUBLIC  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        PRIVATE ${ODBC_INCLUDE_DIRS})
    target_link_libraries(argus_adbc PRIVATE argus_odbc)
    set_target_properties(argus_adbc PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
        OUTPUT_NAME argus_adbc)
    install(TARGETS argus_adbc LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
endif()

# Install rules
install(TARGETS argus_odbc
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/argus
    DESTINATION include
)
