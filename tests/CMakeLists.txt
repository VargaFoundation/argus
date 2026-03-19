# Argus tests

find_package(PkgConfig REQUIRED)
pkg_check_modules(CMOCKA REQUIRED cmocka>=1.1)

# Helper function to add a unit test
function(argus_add_unit_test name source)
    add_executable(${name} ${source})
    target_include_directories(${name} PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${ODBC_INCLUDE_DIRS}
        ${CMOCKA_INCLUDE_DIRS}
        ${GLIB2_INCLUDE_DIRS}
    )
    target_link_libraries(${name} PRIVATE
        argus_odbc_static
        ${CMOCKA_LIBRARIES}
    )
    target_link_directories(${name} PRIVATE
        ${CMOCKA_LIBRARY_DIRS}
    )
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES LABELS "unit")
endfunction()

# Unit tests
argus_add_unit_test(test_handle unit/test_handle.c)
argus_add_unit_test(test_connect_string unit/test_connect_string.c)
argus_add_unit_test(test_diag unit/test_diag.c)
argus_add_unit_test(test_info unit/test_info.c)
argus_add_unit_test(test_bind_parameter unit/test_bind_parameter.c)
argus_add_unit_test(test_unicode unit/test_unicode.c)
argus_add_unit_test(test_descriptor unit/test_descriptor.c)
argus_add_unit_test(test_pool unit/test_pool.c)
argus_add_unit_test(test_catalog unit/test_catalog.c)
argus_add_unit_test(test_odbc2_compat unit/test_odbc2_compat.c)
argus_add_unit_test(test_fetch_features unit/test_fetch_features.c)
argus_add_unit_test(test_getdata_multi unit/test_getdata_multi.c)
argus_add_unit_test(test_getinfo_exhaustive unit/test_getinfo_exhaustive.c)
argus_add_unit_test(test_colattribute_meta unit/test_colattribute_meta.c)
argus_add_unit_test(test_bi_connect_sequence unit/test_bi_connect_sequence.c)
if(ARGUS_BUILD_THRIFT_BACKENDS)
    argus_add_unit_test(test_type_convert unit/test_type_convert.c)
    argus_add_unit_test(test_impala_types unit/test_impala_types.c)
endif()

if(ARGUS_BUILD_TRINO)
    argus_add_unit_test(test_trino_types unit/test_trino_types.c)
endif()

if(ARGUS_BUILD_PHOENIX)
    argus_add_unit_test(test_phoenix_types unit/test_phoenix_types.c)
endif()

if(ARGUS_BUILD_KUDU)
    argus_add_unit_test(test_kudu_types unit/test_kudu_types.c)
    argus_add_unit_test(test_kudu_sql_parser unit/test_kudu_sql_parser.c)
endif()

# Integration tests (optional)
if(BUILD_INTEGRATION_TESTS)
    function(argus_add_integration_test name source)
        add_executable(${name} ${source})
        target_include_directories(${name} PRIVATE
            ${CMAKE_SOURCE_DIR}/include
            ${ODBC_INCLUDE_DIRS}
            ${CMOCKA_INCLUDE_DIRS}
        )
        target_link_libraries(${name} PRIVATE
            argus_odbc
            ${CMOCKA_LIBRARIES}
        )
        target_link_directories(${name} PRIVATE
            ${CMOCKA_LIBRARY_DIRS}
        )
        add_test(NAME ${name} COMMAND ${name})
        set_tests_properties(${name} PROPERTIES LABELS "integration")
    endfunction()

    argus_add_integration_test(test_hive_connect integration/test_hive_connect.c)
    argus_add_integration_test(test_hive_query integration/test_hive_query.c)

    if(ARGUS_BUILD_TRINO)
        argus_add_integration_test(test_trino_connect integration/test_trino_connect.c)
        argus_add_integration_test(test_trino_query integration/test_trino_query.c)
        argus_add_integration_test(test_trino_ssl integration/test_trino_ssl.c)
    endif()

    if(ARGUS_BUILD_PHOENIX)
        argus_add_integration_test(test_phoenix_connect integration/test_phoenix_connect.c)
        argus_add_integration_test(test_phoenix_query integration/test_phoenix_query.c)
    endif()

    if(ARGUS_BUILD_KUDU)
        argus_add_integration_test(test_kudu_connect integration/test_kudu_connect.c)
        argus_add_integration_test(test_kudu_query integration/test_kudu_query.c)
    endif()
endif()
