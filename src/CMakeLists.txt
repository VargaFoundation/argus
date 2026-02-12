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
    backend/hive/hive_backend.c
    backend/hive/hive_session.c
    backend/hive/hive_query.c
    backend/hive/hive_fetch.c
    backend/hive/hive_metadata.c
    backend/hive/hive_types.c
)

add_library(argus_odbc ${ARGUS_SOURCES})

target_include_directories(argus_odbc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/backend/hive
        ${ODBC_INCLUDE_DIRS}
        ${GLIB2_INCLUDE_DIRS}
        ${GOBJECT2_INCLUDE_DIRS}
        ${THRIFT_C_GLIB_INCLUDE_DIRS}
)

target_link_libraries(argus_odbc
    PRIVATE
        argus_thrift_generated
        ${ODBC_LIBRARIES}
        ${GLIB2_LIBRARIES}
        ${GOBJECT2_LIBRARIES}
        ${THRIFT_C_GLIB_LIBRARIES}
)

target_link_directories(argus_odbc
    PRIVATE
        ${GLIB2_LIBRARY_DIRS}
        ${GOBJECT2_LIBRARY_DIRS}
        ${THRIFT_C_GLIB_LIBRARY_DIRS}
)

target_compile_definitions(argus_odbc PRIVATE ARGUS_BUILD_DLL)

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
            ${CMAKE_CURRENT_SOURCE_DIR}/backend/hive
            ${ODBC_INCLUDE_DIRS}
            ${GLIB2_INCLUDE_DIRS}
            ${GOBJECT2_INCLUDE_DIRS}
            ${THRIFT_C_GLIB_INCLUDE_DIRS}
    )
    target_link_libraries(argus_odbc_static
        PUBLIC
            argus_thrift_generated
            ${ODBC_LIBRARIES}
            ${GLIB2_LIBRARIES}
            ${GOBJECT2_LIBRARIES}
            ${THRIFT_C_GLIB_LIBRARIES}
    )
    target_link_directories(argus_odbc_static
        PUBLIC
            ${GLIB2_LIBRARY_DIRS}
            ${GOBJECT2_LIBRARY_DIRS}
            ${THRIFT_C_GLIB_LIBRARY_DIRS}
    )
    target_compile_definitions(argus_odbc_static PRIVATE ARGUS_BUILD_DLL)
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
