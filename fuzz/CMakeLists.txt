# libFuzzer harnesses over the driver's hand-written parsers. Built only with
# ENABLE_FUZZING=ON under Clang (see the root CMakeLists.txt, which already
# instruments every object with -fsanitize=fuzzer-no-link,address).

set(ARGUS_FUZZERS
    fuzz_escape
    fuzz_connstr
)

pkg_check_modules(GLIB_FUZZ REQUIRED glib-2.0)

foreach(f ${ARGUS_FUZZERS})
    add_executable(${f} ${f}.c)
    target_include_directories(${f} PRIVATE ${GLIB_FUZZ_INCLUDE_DIRS})
    target_link_libraries(${f} PRIVATE argus_odbc_static ${GLIB_FUZZ_LIBRARIES})
    target_compile_options(${f} PRIVATE -fsanitize=fuzzer,address)
    target_link_options(${f} PRIVATE -fsanitize=fuzzer,address)
endforeach()
