/* SPDX-License-Identifier: Apache-2.0 */
/*
 * The browser launcher behind Trino's authorization-code SSO. The URL it
 * receives is assembled from connection-string material, so two things are
 * checked here: which URLs it refuses outright, and that whatever it starts
 * receives the URL as a plain argument — never through a shell.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <string.h>

#include "browser.h"

/* ── URL validation ──────────────────────────────────────────── */

static void test_accepts_https_and_loopback_http(void **state)
{
    (void)state;
    assert_true(argus_browser_url_ok("https://idp.example.com/oauth2/authorize"));
    assert_true(argus_browser_url_ok("https://idp.example.com:8443/authorize?p=1&q=a%20b#frag"));
    assert_true(argus_browser_url_ok("HTTPS://IDP.EXAMPLE.COM/authorize"));
    assert_true(argus_browser_url_ok("https://login.microsoftonline.com/common/oauth2/v2.0/authorize"));

    /* A development IdP on the local machine may stay on plain http. */
    assert_true(argus_browser_url_ok("http://localhost:8080/realms/dev/protocol/openid-connect/auth"));
    assert_true(argus_browser_url_ok("http://127.0.0.1/auth"));
    assert_true(argus_browser_url_ok("http://[::1]:9000/auth"));
    assert_true(argus_browser_url_ok("http://localhost"));
}

static void test_refuses_other_schemes_and_remote_http(void **state)
{
    (void)state;
    assert_false(argus_browser_url_ok(NULL));
    assert_false(argus_browser_url_ok(""));
    assert_false(argus_browser_url_ok("https://"));
    assert_false(argus_browser_url_ok("https:///authorize"));
    assert_false(argus_browser_url_ok("http://idp.example.com/authorize"));
    assert_false(argus_browser_url_ok("http://localhost.evil.com/authorize"));
    assert_false(argus_browser_url_ok("http://127.0.0.1.evil.com/"));
    assert_false(argus_browser_url_ok("ftp://idp.example.com/"));
    assert_false(argus_browser_url_ok("file:///etc/passwd"));
    assert_false(argus_browser_url_ok("javascript:alert(1)"));
    assert_false(argus_browser_url_ok("idp.example.com/authorize"));
    assert_false(argus_browser_url_ok(" https://idp.example.com/"));
}

static void test_refuses_shell_material(void **state)
{
    (void)state;
    /* The payload the old system() launcher would have executed. */
    assert_false(argus_browser_url_ok("https://x/'; touch /tmp/pwned; '"));
    assert_false(argus_browser_url_ok("https://x/\"; touch /tmp/pwned; \""));
    assert_false(argus_browser_url_ok("https://x/`id`"));
    assert_false(argus_browser_url_ok("https://x/a b"));
    assert_false(argus_browser_url_ok("https://x/a\tb"));
    assert_false(argus_browser_url_ok("https://x/a\nb"));
    assert_false(argus_browser_url_ok("https://x/a\rb"));
    assert_false(argus_browser_url_ok("https://x/a\\b"));
    assert_false(argus_browser_url_ok("https://x/a<b>"));
    assert_false(argus_browser_url_ok("https://x/a|b"));
    assert_false(argus_browser_url_ok("https://x/{a}"));
    assert_false(argus_browser_url_ok("https://x/\x7f"));
    assert_false(argus_browser_url_ok("https://x/caf\xc3\xa9"));
    assert_false(argus_browser_url_ok("https://x/'"));
}

#ifndef _WIN32

/* ── Launch: the URL must arrive as one argv element ─────────── */

typedef struct {
    char *dir;
    char *script;   /* records its argv, one per line, into `out` */
    char *out;
} launcher_t;

static int launcher_setup(void **state)
{
    launcher_t *l = g_new0(launcher_t, 1);
    l->dir = g_dir_make_tmp("argus-browser-XXXXXX", NULL);
    assert_non_null(l->dir);
    l->script = g_build_filename(l->dir, "browser.sh", NULL);
    l->out = g_build_filename(l->dir, "argv.txt", NULL);

    char *body = g_strdup_printf("#!/bin/sh\nfor a in \"$@\"; do printf '%%s\\n' \"$a\"; done > '%s'\n",
                                 l->out);
    assert_true(g_file_set_contents(l->script, body, -1, NULL));
    g_free(body);
    assert_int_equal(g_chmod(l->script, 0700), 0);

    *state = l;
    return 0;
}

static int launcher_teardown(void **state)
{
    launcher_t *l = *state;
    g_unsetenv("BROWSER");
    g_unlink(l->out);
    g_unlink(l->script);
    g_rmdir(l->dir);
    g_free(l->out); g_free(l->script); g_free(l->dir);
    g_free(l);
    return 0;
}

/* The launcher is spawned detached: poll for its output. */
static char *wait_for_argv(launcher_t *l)
{
    for (int i = 0; i < 200; i++) {
        char *content = NULL;
        if (g_file_get_contents(l->out, &content, NULL, NULL) && content && *content)
            return content;
        g_free(content);
        g_usleep(25 * 1000);
    }
    return NULL;
}

static void test_browser_env_receives_the_url_verbatim(void **state)
{
    launcher_t *l = *state;
    g_setenv("BROWSER", l->script, TRUE);

    /* URL-legal but shell-significant throughout; none of it may be interpreted. */
    const char *url = "https://idp.example.com/auth?x=$(id)&y=$HOME;touch$IFS/tmp/argus-pwned&z=a*b!";
    char err[256] = {0};
    assert_int_equal(argus_browser_open(url, err, sizeof(err)), 0);

    char *argv = wait_for_argv(l);
    assert_non_null(argv);
    char *expected = g_strdup_printf("%s\n", url);
    assert_string_equal(argv, expected);
    assert_false(g_file_test("/tmp/argus-pwned", G_FILE_TEST_EXISTS));
    g_free(expected);
    g_free(argv);
}

static void test_browser_env_is_split_without_a_shell(void **state)
{
    launcher_t *l = *state;
    char *value = g_strdup_printf("%s --new-window '%%s' \"--profile=dir with space\"", l->script);
    g_setenv("BROWSER", value, TRUE);
    g_free(value);

    char err[256] = {0};
    assert_int_equal(argus_browser_open("https://idp.example.com/auth", err, sizeof(err)), 0);

    char *argv = wait_for_argv(l);
    assert_non_null(argv);
    assert_string_equal(argv, "--new-window\nhttps://idp.example.com/auth\n--profile=dir with space\n");
    g_free(argv);
}

static void test_unsafe_url_never_reaches_the_launcher(void **state)
{
    launcher_t *l = *state;
    g_setenv("BROWSER", l->script, TRUE);

    char err[256] = {0};
    assert_int_equal(argus_browser_open("https://x/'; touch /tmp/pwned; '", err, sizeof(err)), -1);
    assert_non_null(strstr(err, "https://"));

    g_usleep(200 * 1000);
    assert_false(g_file_test(l->out, G_FILE_TEST_EXISTS));
}

#endif /* !_WIN32 */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_accepts_https_and_loopback_http),
        cmocka_unit_test(test_refuses_other_schemes_and_remote_http),
        cmocka_unit_test(test_refuses_shell_material),
#ifndef _WIN32
        cmocka_unit_test_setup_teardown(test_browser_env_receives_the_url_verbatim,
                                        launcher_setup, launcher_teardown),
        cmocka_unit_test_setup_teardown(test_browser_env_is_split_without_a_shell,
                                        launcher_setup, launcher_teardown),
        cmocka_unit_test_setup_teardown(test_unsafe_url_never_reaches_the_launcher,
                                        launcher_setup, launcher_teardown),
#endif
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
