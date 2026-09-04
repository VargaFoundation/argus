/* SPDX-License-Identifier: Apache-2.0 */
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
#include "argus/lifecycle.h"
#include "argus/log.h"
#include "argus/obs_hooks.h"
#include "argus/telemetry.h"

#include <stdbool.h>

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

/* Stop everything that runs on a thread of its own: the telemetry sender and
 * whatever a tap provider started. Returns true once none of them is left.
 * `may_wait` is false only from DllMain, where the loader lock is held and a
 * thread cannot exit until it is released, so waiting there would deadlock;
 * that path signals the threads and leaves them to finish on their own. */
static bool argus_library_stop_threads(bool may_wait)
{
    bool clean = argus_obs_hook_unload(may_wait ? 1 : 0) != 0;
    if (!argus_telemetry_stop(may_wait))
        clean = false;
    return clean;
}

void argus_library_quiesce(void)
{
    argus_library_stop_threads(true);
}

static void argus_library_unload(bool may_wait)
{
    /* A thread that is still running -- which only happens when a host
     * unloads the driver without freeing its environment handle first -- may
     * be inside libcurl or the log; tearing those down under it would trade
     * a leak for a crash, so the teardown is skipped altogether. */
    if (!argus_library_stop_threads(may_wait))
        return;
    argus_telemetry_shutdown();
    argus_log_cleanup();
#ifdef ARGUS_HAS_CURL
    curl_global_cleanup();
#endif
}

#ifdef _WIN32
#include <windows.h>

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved);

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    (void)hinstDLL;

    if (fdwReason == DLL_PROCESS_ATTACH) {
        /*
         * Pin the module for the life of the process. The driver runs threads
         * of its own -- the telemetry sender, whatever an observability tap
         * started -- and the DLL_PROCESS_DETACH path cannot wait for them,
         * because it holds the loader lock and a thread cannot exit while it
         * is held. Without a pin, a host that calls FreeLibrary while one of
         * those threads is between instructions unmaps the code under it.
         * GET_MODULE_HANDLE_EX_FLAG_PIN adds a reference that is never
         * released, so FreeLibrary stops at the driver and the threads keep
         * running on mapped code; the process exit path frees it.
         *
         * The cost is that the DLL stays mapped after the last FreeLibrary,
         * which is what the driver already assumes -- argus_library_unload
         * declines to tear anything down while a thread is still running.
         */
        HMODULE pinned = NULL;
        GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_PIN |
                           GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
                           (LPCWSTR)(void *)&DllMain, &pinned);
        argus_library_load();
    } else if (fdwReason == DLL_PROCESS_DETACH) {
        /* A non-NULL lpvReserved means the process is exiting: every other
         * thread has already been terminated and the OS reclaims everything,
         * so nothing may block or free here -- a mutex one of those threads
         * held would never be released. On FreeLibrary the loader lock is
         * held, hence may_wait = false. */
        if (lpvReserved == NULL)
            argus_library_unload(false);
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
    argus_library_unload(true);
}

#endif
