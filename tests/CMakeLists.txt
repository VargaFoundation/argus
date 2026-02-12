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
argus_add_unit_test(test_type_convert unit/test_type_convert.c)
argus_add_unit_test(test_diag unit/test_diag.c)
argus_add_unit_test(test_info unit/test_info.c)
argus_add_unit_test(test_impala_types unit/test_impala_types.c)

if(ARGUS_BUILD_TRINO)
    argus_add_unit_test(test_trino_types unit/test_trino_types.c)
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
endif()
