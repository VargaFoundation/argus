/*
 * api_entry.c - ODBC API entry points for Argus driver.
 *
 * This file exists as the unified entry point registry. All actual
 * implementations are in their respective files (handle.c, connect.c, etc.)
 * and are exported directly from the shared library.
 *
 * This file ensures backends and logging are initialized on library load.
 */

#include "argus/odbc_api.h"
#include "argus/backend.h"
#include "argus/log.h"
#include "argus/telemetry.h"

#ifdef ARGUS_HAS_CURL
#include <curl/curl.h>
#endif

/* One-time process-wide startup/teardown shared by the Windows DllMain and the
 * POSIX constructor/destructor. curl_global_init() is not thread-safe, so it
 * belongs here at load time (before any backend or the telemetry sender spins
 * up its own easy handles) rather than being called ad hoc per backend. */
static void argus_library_load(void)
{
#ifdef ARGUS_HAS_CURL
    curl_global_init(CURL_GLOBAL_DEFAULT);
#endif
    argus_log_init();
    argus_backends_init();
    argus_telemetry_init();
}

static void argus_library_unload(void)
{
    argus_telemetry_shutdown();
    argus_log_cleanup();
#ifdef ARGUS_HAS_CURL
    curl_global_cleanup();
#endif
}

#ifdef _WIN32
#include <windows.h>

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    (void)hinstDLL;
    (void)lpvReserved;

    if (fdwReason == DLL_PROCESS_ATTACH) {
        argus_library_load();
    } else if (fdwReason == DLL_PROCESS_DETACH) {
        argus_library_unload();
    }
    return TRUE;
}

#else

/* Constructor: initialize logging, backends and telemetry on library load */
__attribute__((constructor))
static void argus_init(void)
{
    argus_library_load();
}

/* Destructor: flush telemetry and clean up when the library is unloaded */
__attribute__((destructor))
static void argus_cleanup(void)
{
    argus_library_unload();
}

#endif
