#ifndef ARGUS_COMPAT_H
#define ARGUS_COMPAT_H

/*
 * Platform compatibility macros for Windows/POSIX portability.
 * Include this header in any file that uses POSIX-specific functions.
 */

#ifdef _WIN32

#include <string.h>
#include <stdlib.h>
#include <windows.h>

/* Case-insensitive string comparison */
#define strcasecmp  _stricmp
#define strncasecmp _strnicmp

/* strdup is POSIX, Windows has _strdup */
#define strdup _strdup

/* strtok_r -> strtok_s on Windows */
#define strtok_r strtok_s

/* strndup: MSVC's CRT lacks it; modern mingw-w64 declares it in
 * <string.h>, so defining it there would clash with the CRT. */
#if !defined(__MINGW32__) && !defined(__MINGW64__)
static inline char *strndup(const char *s, size_t n)
{
    size_t len = strlen(s);
    if (len > n) len = n;
    char *copy = (char *)malloc(len + 1);
    if (copy) {
        memcpy(copy, s, len);
        copy[len] = '\0';
    }
    return copy;
}
#endif

/* Secure memory zeroing before free (for credentials) */
static inline void argus_secure_free(char *p)
{
    if (p) {
        SecureZeroMemory(p, strlen(p));
        free(p);
    }
}

#else /* POSIX */

#include <stdlib.h>
#include <strings.h>
#include <string.h>

/* Secure memory zeroing before free (for credentials).
 * explicit_bzero is glibc/BSD-only (macOS lacks it); store through a
 * volatile pointer instead, which the compiler may not elide. */
static inline void argus_secure_free(char *p)
{
    if (p) {
        volatile char *vp = p;
        size_t n = strlen(p);
        while (n--) *vp++ = '\0';
        free(p);
    }
}

#endif /* _WIN32 */

#endif /* ARGUS_COMPAT_H */
