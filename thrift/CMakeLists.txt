# Thrift code generation for TCLIService

find_program(THRIFT_COMPILER thrift REQUIRED)

set(THRIFT_IDL "${CMAKE_CURRENT_SOURCE_DIR}/TCLIService.thrift")
set(THRIFT_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/gen-c_glib")

# Generate C GLib code from the Thrift IDL
add_custom_command(
    OUTPUT
        "${THRIFT_GEN_DIR}/t_c_l_i_service.c"
        "${THRIFT_GEN_DIR}/t_c_l_i_service.h"
        "${THRIFT_GEN_DIR}/t_c_l_i_service_types.c"
        "${THRIFT_GEN_DIR}/t_c_l_i_service_types.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${THRIFT_GEN_DIR}"
    COMMAND ${THRIFT_COMPILER}
        --gen c_glib
        -out "${THRIFT_GEN_DIR}"
        "${THRIFT_IDL}"
    DEPENDS "${THRIFT_IDL}"
    COMMENT "Generating Thrift C GLib code from TCLIService.thrift"
)

# Collect all generated source files
add_library(argus_thrift_generated STATIC
    "${THRIFT_GEN_DIR}/t_c_l_i_service.c"
    "${THRIFT_GEN_DIR}/t_c_l_i_service_types.c"
)

target_include_directories(argus_thrift_generated
    PUBLIC
        "${CMAKE_CURRENT_BINARY_DIR}"
    PRIVATE
        ${GLIB2_INCLUDE_DIRS}
        ${GOBJECT2_INCLUDE_DIRS}
        ${THRIFT_C_GLIB_INCLUDE_DIRS}
)

target_link_libraries(argus_thrift_generated
    PRIVATE
        ${GLIB2_LIBRARIES}
        ${GOBJECT2_LIBRARIES}
        ${THRIFT_C_GLIB_LIBRARIES}
)

target_link_directories(argus_thrift_generated
    PRIVATE
        ${GLIB2_LIBRARY_DIRS}
        ${GOBJECT2_LIBRARY_DIRS}
        ${THRIFT_C_GLIB_LIBRARY_DIRS}
)

# PIC required since this static lib is linked into a shared library
set_target_properties(argus_thrift_generated PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Suppress warnings in generated code
if(NOT MSVC)
    target_compile_options(argus_thrift_generated PRIVATE
        -Wno-unused-parameter
        -Wno-pedantic
        -Wno-sign-compare
        -Wno-missing-field-initializers
        -Wno-cast-function-type
    )
endif()
