/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ARGUS_LIFECYCLE_H
#define ARGUS_LIFECYCLE_H

/*
 * lifecycle.h - Process-wide driver lifecycle (implemented in api_entry.c).
 *
 * Load-time initialisation runs from the library constructor (DllMain on
 * Windows). Teardown happens in two stages because of how Driver Managers
 * unload a driver: they free the last environment handle first and
 * dlclose() / FreeLibrary() afterwards. Every thread the driver started must
 * be gone by the time the code it runs is unmapped, and a Windows DllMain
 * cannot wait for a thread (it holds the loader lock that an exiting thread
 * also needs), so the last SQLFreeHandle(SQL_HANDLE_ENV) is where the driver
 * quiesces; the destructor only does what is left.
 */

/*
 * Stop every background thread the driver -- or a tap provider -- started,
 * waiting a bounded time for each. Called by argus_free_env() when the last
 * environment handle goes away. Idempotent; everything restarts lazily if the
 * application allocates a new environment afterwards.
 */
void argus_library_quiesce(void);

#endif /* ARGUS_LIFECYCLE_H */
