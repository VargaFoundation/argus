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
    backend/backend.c
)

set(ARGUS_PRIVATE_INCLUDE_DIRS
    ${ODBC_INCLUDE_DIRS}
    ${GLIB2_INCLUDE_DIRS}
    ${GOBJECT2_INCLUDE_DIRS}
)

set(ARGUS_LINK_LIBS
    ${ODBC_LIBRARIES}
    ${GLIB2_LIBRARIES}
    ${GOBJECT2_LIBRARIES}
)

set(ARGUS_LINK_DIRS
    ${GLIB2_LIBRARY_DIRS}
    ${GOBJECT2_LIBRARY_DIRS}
)

set(ARGUS_COMPILE_DEFS ARGUS_BUILD_DLL)

# Conditionally add Hive/Impala backends (require Thrift C GLib)
if(ARGUS_BUILD_THRIFT_BACKENDS)
    list(APPEND ARGUS_SOURCES
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

# Install rules
install(TARGETS argus_odbc
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/argus
    DESTINATION include
)
