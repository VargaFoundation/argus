#include "argus/backend.h"
#include "argus/compat.h"
#include "argus/log.h"
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
#ifdef ARGUS_HAS_MYSQL
extern const argus_backend_t *argus_mysql_backend_get(void);
#endif
#ifdef ARGUS_HAS_FLIGHTSQL
extern const argus_backend_t *argus_flightsql_backend_get(void);
#endif
#ifdef ARGUS_HAS_PINOT
extern const argus_backend_t *argus_pinot_backend_get(void);
#endif
#ifdef ARGUS_HAS_DRUID
extern const argus_backend_t *argus_druid_backend_get(void);
#endif
#ifdef ARGUS_HAS_BIGQUERY
extern const argus_backend_t *argus_bigquery_backend_get(void);
#endif
#ifdef ARGUS_HAS_POSTGRES
extern const argus_backend_t *argus_postgres_backend_get(void);
extern const argus_backend_t *argus_greenplum_backend_get(void);
extern const argus_backend_t *argus_cloudberry_backend_get(void);
#endif

void argus_backend_register(const argus_backend_t *backend)
{
    if (!backend) return;

    if (registry_count >= ARGUS_MAX_BACKENDS) {
        /* Silently dropping a backend here would surface much later as
         * "Unknown backend: <name>" from do_connect(), with nothing to
         * connect it to the real cause. */
        ARGUS_LOG_ERROR("Backend registry full (%d): '%s' not registered. "
                        "Raise ARGUS_MAX_BACKENDS.",
                        ARGUS_MAX_BACKENDS, backend->name);
        return;
    }
    registry[registry_count++] = backend;
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

size_t argus_backend_count(void)
{
    return (size_t)registry_count;
}

const argus_backend_t *argus_backend_at(size_t index)
{
    return (index < (size_t)registry_count) ? registry[index] : NULL;
}

void argus_backends_init(void)
{
    /* Idempotent: several entry points call this, and registering twice would
     * fill the registry with duplicates. */
    if (registry_count > 0) return;

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
#ifdef ARGUS_HAS_MYSQL
    argus_backend_register(argus_mysql_backend_get());
#endif
#ifdef ARGUS_HAS_FLIGHTSQL
    argus_backend_register(argus_flightsql_backend_get());
#endif
#ifdef ARGUS_HAS_PINOT
    argus_backend_register(argus_pinot_backend_get());
#endif
#ifdef ARGUS_HAS_DRUID
    argus_backend_register(argus_druid_backend_get());
#endif
#ifdef ARGUS_HAS_BIGQUERY
    argus_backend_register(argus_bigquery_backend_get());
#endif
#ifdef ARGUS_HAS_POSTGRES
    argus_backend_register(argus_postgres_backend_get());
    argus_backend_register(argus_greenplum_backend_get());
    argus_backend_register(argus_cloudberry_backend_get());
#endif
}
