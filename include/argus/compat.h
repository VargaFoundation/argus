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

/* strndup is not available on Windows */
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

/* Secure memory zeroing before free (for credentials) */
static inline void argus_secure_free(char *p)
{
    if (p) {
        SecureZeroMemory(p, strlen(p));
        free(p);
    }
}

#else /* POSIX */

#include <strings.h>
#include <string.h>

/* Secure memory zeroing before free (for credentials) */
static inline void argus_secure_free(char *p)
{
    if (p) {
        explicit_bzero(p, strlen(p));
        free(p);
    }
}

#endif /* _WIN32 */

#endif /* ARGUS_COMPAT_H */
