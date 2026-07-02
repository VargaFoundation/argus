/*
 * ODBC installer setup entry points (Windows only).
 *
 * The ODBCINST.INI "Setup" key of the driver points at this DLL, so the
 * installer library (odbccp32) loads it to create, modify and remove
 * DSNs. This implementation is UI-less: it handles the attribute list
 * it is given (which is how PowerShell Add-OdbcDsn, odbcconf and other
 * scripted installs drive it) and fails requests that would require a
 * configuration dialog, i.e. an interactive Add in odbcad32.exe with no
 * attributes.
 *
 * lpszAttributes is a list of "KEY=value" entries, each terminated by a
 * null byte, the list itself terminated by a double null.
 */
#ifdef _WIN32

#include <windows.h>
#include <odbcinst.h>
#include <string.h>
#include <stdlib.h>

#include "argus/compat.h"

#define ARGUS_SETUP_EXPORT __declspec(dllexport)

/* ── Attribute-list helpers ──────────────────────────────────── */

/* Find "key=" (case-insensitive) in the null-separated list; returns a
 * pointer to the value, or NULL. */
static const char *setup_find_attr(const char *attrs, const char *key)
{
    if (!attrs)
        return NULL;
    size_t key_len = strlen(key);
    for (const char *p = attrs; *p; p += strlen(p) + 1) {
        if (strncasecmp(p, key, key_len) == 0 && p[key_len] == '=')
            return p + key_len + 1;
    }
    return NULL;
}

/* Write every attribute except DSN and Driver into the DSN section. */
static BOOL setup_write_attrs(const char *dsn, const char *attrs)
{
    for (const char *p = attrs; *p; p += strlen(p) + 1) {
        const char *eq = strchr(p, '=');
        if (!eq || eq == p)
            continue;

        size_t key_len = (size_t)(eq - p);
        char key[256];
        if (key_len >= sizeof(key))
            continue;
        memcpy(key, p, key_len);
        key[key_len] = '\0';

        if (strcasecmp(key, "DSN") == 0 || strcasecmp(key, "Driver") == 0)
            continue;

        if (!SQLWritePrivateProfileString(dsn, key, eq + 1, "ODBC.INI"))
            return FALSE;
    }
    return TRUE;
}

/* ── ConfigDSN: create / configure / remove a data source ───────── */

ARGUS_SETUP_EXPORT BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD fRequest,
                                          LPCSTR lpszDriver,
                                          LPCSTR lpszAttributes)
{
    (void)hwndParent;

    const char *dsn = setup_find_attr(lpszAttributes, "DSN");
    if (!dsn || !*dsn || !SQLValidDSN(dsn)) {
        /* No usable DSN in the attributes. Without a configuration
         * dialog there is nothing we can do interactively. */
        SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE,
                              "The Argus ODBC setup has no configuration "
                              "dialog; pass a DSN= attribute (e.g. via "
                              "PowerShell Add-OdbcDsn or odbcconf).");
        return FALSE;
    }

    switch (fRequest) {
    case ODBC_ADD_DSN:
    case ODBC_CONFIG_DSN:
        if (!SQLWriteDSNToIni(dsn, lpszDriver))
            return FALSE;
        return setup_write_attrs(dsn, lpszAttributes);

    case ODBC_REMOVE_DSN:
        return SQLRemoveDSNFromIni(dsn);

    default:
        SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED,
                              "Unsupported ConfigDSN request");
        return FALSE;
    }
}

/* ── ConfigDriver: driver-level install/remove hook ─────────────── */

ARGUS_SETUP_EXPORT BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest,
                                             LPCSTR lpszDriver,
                                             LPCSTR lpszArgs,
                                             LPSTR lpszMsg, WORD cbMsgMax,
                                             WORD *pcbMsgOut)
{
    (void)hwndParent;
    (void)lpszDriver;
    (void)lpszArgs;

    if (lpszMsg && cbMsgMax > 0)
        lpszMsg[0] = '\0';
    if (pcbMsgOut)
        *pcbMsgOut = 0;

    switch (fRequest) {
    case ODBC_INSTALL_DRIVER:
    case ODBC_REMOVE_DRIVER:
        /* No driver-specific post-install work is needed. */
        return TRUE;
    default:
        return FALSE;
    }
}

#endif /* _WIN32 */
