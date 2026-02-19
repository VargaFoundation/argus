#include "argus/backend.h"
#include "argus/compat.h"
#include <string.h>

/* Backend registry */
static const argus_backend_t *registry[ARGUS_MAX_BACKENDS];
static int registry_count = 0;

/* Backend registration (defined in respective backend files) */
#ifdef ARGUS_HAS_THRIFT_BACKENDS
extern const argus_backend_t *argus_hive_backend_get(void);
extern const argus_backend_t *argus_impala_backend_get(void);
#endif
#ifdef ARGUS_HAS_TRINO
extern const argus_backend_t *argus_trino_backend_get(void);
#endif
#ifdef ARGUS_HAS_PHOENIX
extern const argus_backend_t *argus_phoenix_backend_get(void);
#endif
#ifdef ARGUS_HAS_KUDU
extern const argus_backend_t *argus_kudu_backend_get(void);
#endif

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
    /* Register all available backends */
#ifdef ARGUS_HAS_THRIFT_BACKENDS
    argus_backend_register(argus_hive_backend_get());
    argus_backend_register(argus_impala_backend_get());
#endif
#ifdef ARGUS_HAS_TRINO
    argus_backend_register(argus_trino_backend_get());
#endif
#ifdef ARGUS_HAS_PHOENIX
    argus_backend_register(argus_phoenix_backend_get());
#endif
#ifdef ARGUS_HAS_KUDU
    argus_backend_register(argus_kudu_backend_get());
#endif
}
