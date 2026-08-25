#include "argus/caps.h"
#include "argus/handle.h"

/*
 * Capability defaulting.
 *
 * The two most consequential defaults are free: a zero-initialised descriptor
 * already says SQL_TC_NONE and SQL_OSC_MINIMUM, which is what info.c returned
 * for every backend before capabilities existed. That is load-bearing — if
 * either constant were ever non-zero, a backend that declares a caps struct
 * without mentioning transactions would start claiming some other level — so
 * it is asserted at compile time rather than trusted.
 */
_Static_assert(SQL_TC_NONE == 0,
               "a zero-initialised caps struct must mean 'no transactions'");
_Static_assert(SQL_OSC_MINIMUM == 0,
               "a zero-initialised caps struct must mean 'minimum SQL conformance'");

/* Every field zero: the pre-capabilities behaviour, exactly. */
static const argus_backend_caps_t argus_legacy_caps = { 0 };

const argus_backend_caps_t *argus_caps_for(const argus_dbc_t *dbc)
{
    if (dbc && dbc->backend && dbc->backend->caps)
        return dbc->backend->caps;
    return &argus_legacy_caps;
}

const char *argus_caps_str(const char *value, const char *dflt)
{
    return (value && *value) ? value : dflt;
}

SQLUSMALLINT argus_caps_u16(SQLUSMALLINT value, SQLUSMALLINT dflt)
{
    return value ? value : dflt;
}
