#ifndef ARGUS_NUMTEXT_H
#define ARGUS_NUMTEXT_H

/*
 * Locale-independent number <-> text.
 *
 * SQL text and every wire format the backends speak write the decimal point
 * as '.', whatever the process locale says. Desktop applications (Excel,
 * Tableau, Power BI Desktop) call setlocale(LC_ALL, "") at startup, after
 * which printf("%g", 1.5) yields "1,5" and strtod("1.5") stops at the '.'.
 * Both silently corrupt data: a bound parameter is sent as "1,5", a fetched
 * "1.5" comes back as 1.0. Nothing in the driver may pass a floating-point
 * value through printf or strtod; it goes through these helpers instead.
 */

#include <glib.h>
#include <stddef.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Render `v` with printf's "%.<precision>g" in the C locale. Returns the
 * length written (0 when buf is too small; `buf` is always terminated).
 */
static inline size_t argus_dtoa(char *buf, size_t size, int precision, double v)
{
    char fmt[16];
    if (!buf || size == 0) return 0;
    if (precision < 1) precision = 1;
    if (precision > 40) precision = 40;
    g_snprintf(fmt, sizeof(fmt), "%%.%dg", precision);
    g_ascii_formatd(buf, (gint)size, fmt, v);
    return strlen(buf);
}

/* strtod() as in the C locale: accepts "1.5", "1e3", "inf", "nan";
 * sets errno to ERANGE on overflow/underflow like strtod(). */
static inline double argus_strtod(const char *s, char **end)
{
    return g_ascii_strtod(s, end);
}

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_NUMTEXT_H */
