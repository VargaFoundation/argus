# Argus tests

find_package(PkgConfig REQUIRED)
pkg_check_modules(CMOCKA REQUIRED cmocka>=1.1)

# Helper function to add a unit test
function(argus_add_unit_test name source)
    add_executable(${name} ${source})
    target_include_directories(${name} PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${ODBC_INCLUDE_DIRS}
        ${CMOCKA_INCLUDE_DIRS}
        ${GLIB2_INCLUDE_DIRS}
    )
    target_link_libraries(${name} PRIVATE
        argus_odbc_static
        ${CMOCKA_LIBRARIES}
        stdc++
    )
    target_link_directories(${name} PRIVATE
        ${CMOCKA_LIBRARY_DIRS}
    )
    target_compile_definitions(${name} PRIVATE
        ARGUS_STATIC
        ARGUS_DRIVER_NAME="${ARGUS_DRIVER_FILE_NAME}"
    )
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES LABELS "unit")
endfunction()

# Unit tests
argus_add_unit_test(test_handle unit/test_handle.c)
argus_add_unit_test(test_connect_string unit/test_connect_string.c)
argus_add_unit_test(test_driver_connect unit/test_driver_connect.c)
argus_add_unit_test(test_dm_pooling unit/test_dm_pooling.c)
argus_add_unit_test(test_sql_quote unit/test_sql_quote.c)
argus_add_unit_test(test_diag unit/test_diag.c)
argus_add_unit_test(test_thread_safety unit/test_thread_safety.c)
argus_add_unit_test(test_info unit/test_info.c)
argus_add_unit_test(test_backend_caps unit/test_backend_caps.c)
argus_add_unit_test(test_dialect unit/test_dialect.c)
argus_add_unit_test(test_escape unit/test_escape.c)
argus_add_unit_test(test_bind_parameter unit/test_bind_parameter.c)
argus_add_unit_test(test_unicode unit/test_unicode.c)
argus_add_unit_test(test_descriptor unit/test_descriptor.c)
argus_add_unit_test(test_catalog unit/test_catalog.c)
argus_add_unit_test(test_odbc2_compat unit/test_odbc2_compat.c)
argus_add_unit_test(test_fetch_features unit/test_fetch_features.c)
argus_add_unit_test(test_fetch_errors unit/test_fetch_errors.c)
argus_add_unit_test(test_getdata_multi unit/test_getdata_multi.c)
argus_add_unit_test(test_getinfo_exhaustive unit/test_getinfo_exhaustive.c)
argus_add_unit_test(test_colattribute_meta unit/test_colattribute_meta.c)
argus_add_unit_test(test_bi_connect_sequence unit/test_bi_connect_sequence.c)
argus_add_unit_test(test_p2_features unit/test_p2_features.c)
argus_add_unit_test(test_dsn unit/test_dsn.c)
argus_add_unit_test(test_wide_api unit/test_wide_api.c)
argus_add_unit_test(test_locale_numbers unit/test_locale_numbers.c)
if(ARGUS_BUILD_TELEMETRY)
    argus_add_unit_test(test_telemetry unit/test_telemetry.c)
    # The header only exposes the real emitters when this is defined; without
    # it the no-op stubs would make the assertions vacuous.
    target_compile_definitions(test_telemetry PRIVATE ARGUS_HAS_TELEMETRY)
endif()
if(ARGUS_BUILD_THRIFT_BACKENDS)
    argus_add_unit_test(test_type_convert unit/test_type_convert.c)
    argus_add_unit_test(test_impala_types unit/test_impala_types.c)
    # Drives the real hive/impala fetch functions through a TCLIServiceClient
    # fed from a memory buffer, so it needs the backend internals and Thrift.
    argus_add_unit_test(test_hs2_fetch unit/test_hs2_fetch.c)
    target_include_directories(test_hs2_fetch PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend
        ${GOBJECT2_INCLUDE_DIRS}
        ${THRIFT_C_GLIB_INCLUDE_DIRS}
    )
    # SASL negotiation framing and libthrift size caps over a memory buffer.
    argus_add_unit_test(test_thrift_sasl unit/test_thrift_sasl.c)
    target_include_directories(test_thrift_sasl PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend
        ${GOBJECT2_INCLUDE_DIRS}
        ${THRIFT_C_GLIB_INCLUDE_DIRS}
    )
endif()

if(ARGUS_BUILD_TRINO)
    argus_add_unit_test(test_trino_types unit/test_trino_types.c)
    # Browser launcher behind the authorization-code SSO flow.
    argus_add_unit_test(test_browser unit/test_browser.c)
    target_include_directories(test_browser PRIVATE ${PROJECT_SOURCE_DIR}/src/backend)
    # Which headers reach the origin vs. a spooled-segment store.
    argus_add_unit_test(test_trino_http unit/test_trino_http.c)
    target_include_directories(test_trino_http PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend/trino
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
    # information_schema queries built from ODBC search patterns.
    argus_add_unit_test(test_trino_catalog_sql unit/test_trino_catalog_sql.c)
    target_include_directories(test_trino_catalog_sql PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend/trino
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
endif()

if(LIBCURL_FOUND)
    argus_add_unit_test(test_curl_common unit/test_curl_common.c)
    target_include_directories(test_curl_common PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend
        ${LIBCURL_INCLUDE_DIRS}
    )
endif()

if(ARGUS_BUILD_PHOENIX)
    argus_add_unit_test(test_phoenix_types unit/test_phoenix_types.c)
endif()

if(ARGUS_BUILD_PINOT)
    argus_add_unit_test(test_pinot_types unit/test_pinot_types.c)
endif()

if(ARGUS_BUILD_DRUID)
    argus_add_unit_test(test_druid_types unit/test_druid_types.c)
    argus_add_unit_test(test_druid_catalog_sql unit/test_druid_catalog_sql.c)
    target_include_directories(test_druid_catalog_sql PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend/druid
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
endif()

if(ARGUS_BUILD_MYSQL)
    argus_add_unit_test(test_mywire_catalog_sql unit/test_mywire_catalog_sql.c)
    target_include_directories(test_mywire_catalog_sql PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend/mysql
        ${LIBMARIADB_INCLUDE_DIRS}
    )
endif()

if(ARGUS_BUILD_BIGQUERY)
    argus_add_unit_test(test_bigquery_types unit/test_bigquery_types.c)
    target_include_directories(test_bigquery_types PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend/bigquery
        ${LIBCURL_INCLUDE_DIRS}
        ${JSON_GLIB_INCLUDE_DIRS}
    )
endif()

if(ARGUS_BUILD_POSTGRES)
    argus_add_unit_test(test_postgres_types unit/test_postgres_types.c)
    target_include_directories(test_postgres_types PRIVATE
        ${PROJECT_SOURCE_DIR}/src/backend/pgcommon
        ${LIBPQ_INCLUDE_DIRS}
    )
endif()

if(ARGUS_BUILD_KUDU)
    argus_add_unit_test(test_kudu_types unit/test_kudu_types.c)
    argus_add_unit_test(test_kudu_sql_parser unit/test_kudu_sql_parser.c)
endif()

# Flight SQL Arrow->ODBC conversion (plain libarrow, no live endpoint needed)
if(ARGUS_BUILD_FLIGHTSQL)
    add_executable(test_flightsql_convert unit/test_flightsql_convert.cpp)
    set_target_properties(test_flightsql_convert PROPERTIES
        CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
    target_include_directories(test_flightsql_convert PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/backend/flightsql
        ${ARROW_FLIGHT_SQL_INCLUDE_DIRS}
    )
    target_link_libraries(test_flightsql_convert PRIVATE
        argus_odbc_static
        ${ARROW_FLIGHT_SQL_LIBRARIES}
        stdc++
    )
    target_link_directories(test_flightsql_convert PRIVATE
        ${ARROW_FLIGHT_SQL_LIBRARY_DIRS}
    )
    add_test(NAME test_flightsql_convert COMMAND test_flightsql_convert)
    set_tests_properties(test_flightsql_convert PROPERTIES LABELS "unit")
endif()

# Integration tests (optional)
if(BUILD_INTEGRATION_TESTS)
    function(argus_add_integration_test name source)
        add_executable(${name} ${source})
        target_include_directories(${name} PRIVATE
            ${PROJECT_SOURCE_DIR}/include
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
    argus_add_integration_test(test_hive_kerberos integration/test_hive_kerberos.c)
    argus_add_integration_test(test_hive_kerberos_http integration/test_hive_kerberos_http.c)
    argus_add_integration_test(test_impala_connect integration/test_impala_connect.c)
    argus_add_integration_test(test_impala_query integration/test_impala_query.c)
    argus_add_integration_test(test_hive_http integration/test_hive_http.c)
    argus_add_integration_test(test_bi_folding integration/test_bi_folding.c)
    argus_add_integration_test(test_bi_escapes integration/test_bi_escapes.c)
    argus_add_integration_test(test_block_cursor integration/test_block_cursor.c)
    argus_add_integration_test(test_async integration/test_async.c)

    # Spark (Thrift Server) and Flink (SQL Gateway hiveserver2 endpoint) speak the
    # HiveServer2 protocol and are exercised through the Hive backend.
    if(ARGUS_BUILD_THRIFT_BACKENDS)
        argus_add_integration_test(test_spark_connect integration/test_spark_connect.c)
        argus_add_integration_test(test_spark_query integration/test_spark_query.c)
        argus_add_integration_test(test_flink_connect integration/test_flink_connect.c)
        argus_add_integration_test(test_flink_query integration/test_flink_query.c)
    endif()

    if(ARGUS_BUILD_TRINO)
        argus_add_integration_test(test_trino_connect integration/test_trino_connect.c)
        argus_add_integration_test(test_trino_query integration/test_trino_query.c)
        argus_add_integration_test(test_trino_ssl integration/test_trino_ssl.c)
        argus_add_integration_test(test_failover integration/test_failover.c)
        argus_add_integration_test(test_scroll_cap integration/test_scroll_cap.c)
    endif()

    if(ARGUS_BUILD_PHOENIX)
        argus_add_integration_test(test_phoenix_connect integration/test_phoenix_connect.c)
        argus_add_integration_test(test_phoenix_query integration/test_phoenix_query.c)
    endif()

    if(ARGUS_BUILD_BIGQUERY)
        argus_add_integration_test(test_bigquery_connect integration/test_bigquery_connect.c)
        argus_add_integration_test(test_bigquery_query integration/test_bigquery_query.c)
    endif()

    if(ARGUS_BUILD_KUDU)
        argus_add_integration_test(test_kudu_connect integration/test_kudu_connect.c)
        argus_add_integration_test(test_kudu_query integration/test_kudu_query.c)
    endif()

    # MySQL-wire backend (MariaDB service; also covers StarRocks/Doris/ClickHouse)
    if(ARGUS_BUILD_MYSQL)
        argus_add_integration_test(test_mysql_connect integration/test_mysql_connect.c)
        argus_add_integration_test(test_mysql_query integration/test_mysql_query.c)
    endif()

    # PostgreSQL family. Only the `postgres` service is started by default;
    # the greenplum/cloudberry compose services are behind profiles, so their
    # tests are opt-in via PG_HOST/PG_PORT pointed at a real cluster.
    if(ARGUS_BUILD_POSTGRES)
        argus_add_integration_test(test_postgres_connect integration/test_postgres_connect.c)
        argus_add_integration_test(test_postgres_query integration/test_postgres_query.c)
        argus_add_integration_test(test_postgres_escapes integration/test_postgres_escapes.c)
        argus_add_integration_test(test_pg_transactions integration/test_pg_transactions.c)
        argus_add_integration_test(test_pg_catalog_ext integration/test_pg_catalog_ext.c)
        argus_add_integration_test(test_pg_describe_param integration/test_pg_describe_param.c)
        argus_add_integration_test(test_pg_options integration/test_pg_options.c)

        # The Greenplum/Cloudberry catalog SQL, exercised against a simulated
        # Greenplum catalog built on plain PostgreSQL. Links the static library
        # (and libpq directly) because it calls the fragment builders, which
        # the shared driver does not export.
        add_executable(test_pg_mpp_sim integration/test_pg_mpp_sim.c)
        target_include_directories(test_pg_mpp_sim PRIVATE
            ${PROJECT_SOURCE_DIR}/include
            ${PROJECT_SOURCE_DIR}/src/backend/pgcommon
            ${ODBC_INCLUDE_DIRS}
            ${CMOCKA_INCLUDE_DIRS}
            ${GLIB2_INCLUDE_DIRS}
            ${LIBPQ_INCLUDE_DIRS}
        )
        target_link_libraries(test_pg_mpp_sim PRIVATE
            argus_odbc_static ${CMOCKA_LIBRARIES} ${LIBPQ_LIBRARIES} stdc++)
        target_link_directories(test_pg_mpp_sim PRIVATE
            ${CMOCKA_LIBRARY_DIRS} ${LIBPQ_LIBRARY_DIRS})
        target_compile_definitions(test_pg_mpp_sim PRIVATE ARGUS_STATIC)
        add_test(NAME test_pg_mpp_sim COMMAND test_pg_mpp_sim)
        set_tests_properties(test_pg_mpp_sim PROPERTIES LABELS "integration")
    endif()

    # Arrow Flight SQL backend (InfluxDB 3 service; also covers Dremio/Doris)
    if(ARGUS_BUILD_FLIGHTSQL)
        argus_add_integration_test(test_flightsql_connect integration/test_flightsql_connect.c)
        argus_add_integration_test(test_flightsql_query integration/test_flightsql_query.c)
    endif()

    # Pinot backend (Pinot QuickStart service: sample table baseballStats)
    if(ARGUS_BUILD_PINOT)
        argus_add_integration_test(test_pinot_connect integration/test_pinot_connect.c)
        argus_add_integration_test(test_pinot_query integration/test_pinot_query.c)
    endif()

    # ADBC driver integration test (C++; needs Arrow + C++20, guaranteed when the
    # Flight SQL backend is enabled). Runs against a live Trino.
    if(BUILD_ADBC AND ARGUS_BUILD_FLIGHTSQL)
        add_executable(test_adbc integration/test_adbc.cpp)
        target_include_directories(test_adbc PRIVATE
            ${PROJECT_SOURCE_DIR}/include ${ODBC_INCLUDE_DIRS}
            ${ARROW_FLIGHT_SQL_INCLUDE_DIRS})
        target_link_libraries(test_adbc PRIVATE argus_adbc ${ARROW_FLIGHT_SQL_LIBRARIES})
        target_link_directories(test_adbc PRIVATE ${ARROW_FLIGHT_SQL_LIBRARY_DIRS})
        set_target_properties(test_adbc PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
        add_test(NAME test_adbc COMMAND test_adbc)
        set_tests_properties(test_adbc PROPERTIES LABELS "integration")
    endif()

    # Fetch-path micro-benchmark tool (manual; needs a live backend)
    add_executable(argus_bench bench/bench.c)
    target_include_directories(argus_bench PRIVATE
        ${PROJECT_SOURCE_DIR}/include ${ODBC_INCLUDE_DIRS})
    target_link_libraries(argus_bench PRIVATE argus_odbc)
endif()
