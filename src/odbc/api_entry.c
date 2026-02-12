/*
 * api_entry.c - ODBC API entry points for Argus driver.
 *
 * This file exists as the unified entry point registry. All actual
 * implementations are in their respective files (handle.c, connect.c, etc.)
 * and are exported directly from the shared library.
 *
 * This file ensures backends are initialized on library load.
 */

#include "argus/odbc_api.h"
#include "argus/backend.h"

/* Constructor: initialize backends when the library is loaded */
__attribute__((constructor))
static void argus_init(void)
{
    argus_backends_init();
}
