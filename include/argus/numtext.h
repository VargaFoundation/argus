/* SPDX-License-Identifier: Apache-2.0 */
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

/*
 * The shortest "%g" rendering that reads back as the same value: 15
 * significant digits when they round-trip (0.1 stays "0.1"), the 17 a
 * double can need otherwise (0.1 + 0.2 is "0.30000000000000004", not the
 * "0.3" a fixed %.15g would print). Floats likewise try 7 digits, then 9.
 */
static inline size_t argus_dtoa_shortest(char *buf, size_t size, double v)
{
    size_t n = argus_dtoa(buf, size, 15, v);
    if (n && argus_strtod(buf, NULL) != v)
        n = argus_dtoa(buf, size, 17, v);
    return n;
}

static inline size_t argus_ftoa_shortest(char *buf, size_t size, float v)
{
    size_t n = argus_dtoa(buf, size, 7, (double)v);
    if (n && (float)argus_strtod(buf, NULL) != v)
        n = argus_dtoa(buf, size, 9, (double)v);
    return n;
}

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_NUMTEXT_H */
