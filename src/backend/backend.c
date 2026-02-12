#include "argus/backend.h"
#include <string.h>
#include <strings.h>

/* Backend registry */
static const argus_backend_t *registry[ARGUS_MAX_BACKENDS];
static int registry_count = 0;

/* Hive backend registration (defined in hive/hive_backend.c) */
extern const argus_backend_t *argus_hive_backend_get(void);

void argus_backend_register(const argus_backend_t *backend)
{
    if (registry_count < ARGUS_MAX_BACKENDS && backend) {
        registry[registry_count++] = backend;
    }
}

const argus_backend_t *argus_backend_find(const char *name)
{
    if (!name) return NULL;

    for (int i = 0; i < registry_count; i++) {
        if (strcasecmp(registry[i]->name, name) == 0) {
            return registry[i];
        }
    }
    return NULL;
}

void argus_backends_init(void)
{
    /* Register all built-in backends */
    argus_backend_register(argus_hive_backend_get());
}
