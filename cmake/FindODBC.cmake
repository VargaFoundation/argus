# FindODBC.cmake - Find ODBC headers and library
#
# Sets:
#   ODBC_FOUND       - True if ODBC was found
#   ODBC_INCLUDE_DIRS - Include directories
#   ODBC_LIBRARIES   - Libraries to link

if(WIN32)
    # On Windows, ODBC is always available via the Windows SDK
    find_path(ODBC_INCLUDE_DIR
        NAMES sql.h sqlext.h
        PATHS
            "$ENV{WindowsSdkDir}/Include"
            "C:/Program Files (x86)/Windows Kits/10/Include"
            # MSYS2/MinGW paths
            "$ENV{MINGW_PREFIX}/include"
            "/ucrt64/include"
            "/mingw64/include"
    )

    find_library(ODBC_LIBRARY
        NAMES odbc32 odbc
        PATHS
            "$ENV{WindowsSdkDir}/Lib"
            "C:/Program Files (x86)/Windows Kits/10/Lib"
            # MSYS2/MinGW paths
            "$ENV{MINGW_PREFIX}/lib"
            "/ucrt64/lib"
            "/mingw64/lib"
    )
else()
    find_path(ODBC_INCLUDE_DIR
        NAMES sql.h sqlext.h
        PATHS
            /usr/include
            /usr/local/include
            /usr/include/odbc
            /usr/local/include/odbc
    )

    find_library(ODBC_LIBRARY
        NAMES odbc
        PATHS
            /usr/lib
            /usr/local/lib
            /usr/lib/x86_64-linux-gnu
    )
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ODBC
    REQUIRED_VARS ODBC_LIBRARY ODBC_INCLUDE_DIR
)

if(ODBC_FOUND)
    set(ODBC_INCLUDE_DIRS ${ODBC_INCLUDE_DIR})
    set(ODBC_LIBRARIES ${ODBC_LIBRARY})

    if(NOT TARGET ODBC::ODBC)
        add_library(ODBC::ODBC UNKNOWN IMPORTED)
        set_target_properties(ODBC::ODBC PROPERTIES
            IMPORTED_LOCATION "${ODBC_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${ODBC_INCLUDE_DIR}"
        )
    endif()
endif()

mark_as_advanced(ODBC_INCLUDE_DIR ODBC_LIBRARY)
