/* SPDX-License-Identifier: Apache-2.0 */
#include "browser.h"

#include <glib.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <shellapi.h>
#endif

/* Loopback hosts for which a plain http:// authorization endpoint is allowed. */
static bool is_loopback_host(const char *host_start)
{
    static const char *const hosts[] = { "localhost", "127.0.0.1", "[::1]" };
    for (size_t i = 0; i < G_N_ELEMENTS(hosts); i++) {
        size_t n = strlen(hosts[i]);
        if (g_ascii_strncasecmp(host_start, hosts[i], n) != 0) continue;
        char next = host_start[n];
        if (next == '\0' || next == ':' || next == '/' || next == '?' || next == '#')
            return true;
    }
    return false;
}

bool argus_browser_url_ok(const char *url)
{
    if (!url) return false;

    const char *rest;
    if (g_ascii_strncasecmp(url, "https://", 8) == 0) {
        rest = url + 8;
    } else if (g_ascii_strncasecmp(url, "http://", 7) == 0) {
        rest = url + 7;
        if (!is_loopback_host(rest)) return false;
    } else {
        return false;
    }
    if (*rest == '\0' || *rest == '/' || *rest == '?' || *rest == '#') return false;

    /* RFC 3986 unreserved + reserved characters and '%', minus the quotes a
     * real endpoint never contains. Anything else (spaces, control bytes,
     * backslashes, UTF-8) is refused rather than escaped. */
    for (const unsigned char *p = (const unsigned char *)url; *p; p++) {
        if (g_ascii_isalnum(*p)) continue;
        if (strchr("-._~:/?#[]@!$&()*+,;=%", *p)) continue;
        return false;
    }
    return true;
}

#ifdef _WIN32

int argus_browser_open(const char *url, char *errmsg, size_t errmsg_size)
{
    if (!argus_browser_url_ok(url)) {
        snprintf(errmsg, errmsg_size, "refusing to open '%.80s': not an https:// URL", url ? url : "");
        return -1;
    }

    gunichar2 *wurl = g_utf8_to_utf16(url, -1, NULL, NULL, NULL);
    if (!wurl) {
        snprintf(errmsg, errmsg_size, "cannot convert the URL to UTF-16");
        return -1;
    }

    const char *browser = g_getenv("BROWSER");
    HINSTANCE h;
    if (browser && *browser) {
        gunichar2 *wbrowser = g_utf8_to_utf16(browser, -1, NULL, NULL, NULL);
        /* The URL is a single parameter: it was validated to contain neither
         * spaces nor quotes, so it needs no quoting. */
        h = ShellExecuteW(NULL, NULL, (LPCWSTR)wbrowser, (LPCWSTR)wurl, NULL, SW_SHOWNORMAL);
        g_free(wbrowser);
    } else {
        h = ShellExecuteW(NULL, L"open", (LPCWSTR)wurl, NULL, NULL, SW_SHOWNORMAL);
    }
    g_free(wurl);

    if ((INT_PTR)h <= 32) {
        snprintf(errmsg, errmsg_size, "ShellExecute failed (code %ld)", (long)(INT_PTR)h);
        return -1;
    }
    return 0;
}

#else /* POSIX */

/* Spawn argv detached, stdout/stderr to /dev/null. GLib reaps the child. */
static bool spawn_quiet(char **argv)
{
    GError *err = NULL;
    gboolean ok = g_spawn_async(NULL, argv, NULL,
                                G_SPAWN_SEARCH_PATH | G_SPAWN_STDOUT_TO_DEV_NULL |
                                G_SPAWN_STDERR_TO_DEV_NULL,
                                NULL, NULL, NULL, &err);
    g_clear_error(&err);
    return ok;
}

/*
 * $BROWSER is split with shell quoting rules but never given to a shell:
 * `BROWSER="firefox --new-window"` becomes {"firefox", "--new-window", url}.
 * A `%s` argument is replaced by the URL (the convention Python's webbrowser
 * and xdg-open follow); otherwise the URL is appended.
 */
static bool spawn_env_browser(const char *browser, const char *url)
{
    gint argc = 0;
    gchar **words = NULL;
    if (!g_shell_parse_argv(browser, &argc, &words, NULL) || argc == 0) {
        g_strfreev(words);
        return false;
    }

    GPtrArray *argv = g_ptr_array_new_with_free_func(g_free);
    bool placed = false;
    for (gint i = 0; i < argc; i++) {
        if (strcmp(words[i], "%s") == 0) {
            g_ptr_array_add(argv, g_strdup(url));
            placed = true;
        } else {
            g_ptr_array_add(argv, g_strdup(words[i]));
        }
    }
    if (!placed) g_ptr_array_add(argv, g_strdup(url));
    g_ptr_array_add(argv, NULL);
    g_strfreev(words);

    bool ok = spawn_quiet((char **)argv->pdata);
    g_ptr_array_free(argv, TRUE);
    return ok;
}

int argus_browser_open(const char *url, char *errmsg, size_t errmsg_size)
{
    if (!argus_browser_url_ok(url)) {
        snprintf(errmsg, errmsg_size, "refusing to open '%.80s': not an https:// URL", url ? url : "");
        return -1;
    }

    const char *browser = g_getenv("BROWSER");
    if (browser && *browser && spawn_env_browser(browser, url)) return 0;

    char *xdg[] = { "xdg-open", (char *)url, NULL };
    if (spawn_quiet(xdg)) return 0;

    char *open_cmd[] = { "open", (char *)url, NULL };
    if (spawn_quiet(open_cmd)) return 0;

    snprintf(errmsg, errmsg_size, "no browser launcher found (tried %sxdg-open and open)",
             browser && *browser ? "$BROWSER, " : "");
    return -1;
}

#endif
