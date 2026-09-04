/* SPDX-License-Identifier: Apache-2.0 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "argus/backend.h"

/*
 * The build manifest is what the release job greps out of every artefact to
 * prove which backends a binary ships with (scripts/check-build-manifest.sh).
 * A manifest that drifted from the registry would either fail good releases
 * or, worse, certify a driver that cannot actually serve a backend, so these
 * tests pin the two together in both directions.
 */

/* Feature tokens that are not backend names. */
static const char *const feature_tokens[] = {
    "gssapi", "sspi", "openssl", "telemetry", NULL
};

static bool is_feature_token(const char *tok)
{
    for (int i = 0; feature_tokens[i]; i++)
        if (strcmp(tok, feature_tokens[i]) == 0) return true;
    return false;
}

/* Copy of the manifest split into tokens; caller frees `copy`. */
static char *tokenize(const char *manifest, char **tokens, int max, int *count)
{
    size_t len = strlen(manifest);
    char *copy = malloc(len + 1);
    assert_non_null(copy);
    memcpy(copy, manifest, len + 1);
    *count = 0;
    for (char *tok = strtok(copy, " "); tok; tok = strtok(NULL, " ")) {
        assert_true(*count < max);
        tokens[(*count)++] = tok;
    }
    return copy;
}

static void test_manifest_names_the_version(void **state)
{
    (void)state;
    const char *manifest = argus_build_manifest();
    assert_non_null(manifest);
    assert_memory_equal(manifest, "argus-build ", strlen("argus-build "));

    int major = -1, minor = -1, patch = -1;
    char trailing = 0;
    int n = sscanf(manifest + strlen("argus-build "), "%d.%d.%d%c",
                   &major, &minor, &patch, &trailing);
    assert_int_equal(n, 4);
    assert_true(major >= 0 && minor >= 0 && patch >= 0);
    assert_int_equal(trailing, ' ');
}

static void test_every_registered_backend_is_listed(void **state)
{
    (void)state;
    argus_backends_init();
    const char *manifest = argus_build_manifest();

    size_t count = argus_backend_count();
    assert_true(count > 0);
    for (size_t i = 0; i < count; i++) {
        const char *name = argus_backend_at(i)->name;
        char needle[64];
        snprintf(needle, sizeof(needle), " %s ", name);

        /* Pad the manifest so the last token matches the same way. */
        size_t len = strlen(manifest);
        char *padded = malloc(len + 2);
        assert_non_null(padded);
        memcpy(padded, manifest, len);
        padded[len] = ' ';
        padded[len + 1] = '\0';
        if (!strstr(padded, needle))
            fail_msg("registered backend '%s' is missing from \"%s\"", name, manifest);
        free(padded);
    }
}

static void test_every_listed_backend_is_registered(void **state)
{
    (void)state;
    argus_backends_init();

    char *tokens[64];
    int ntok = 0;
    char *copy = tokenize(argus_build_manifest(), tokens, 64, &ntok);
    assert_true(ntok >= 3); /* "argus-build", version, at least one backend */

    size_t backends_listed = 0;
    for (int i = 2; i < ntok; i++) {
        if (is_feature_token(tokens[i])) continue;
        const argus_backend_t *b = argus_backend_find(tokens[i]);
        if (!b) fail_msg("manifest lists '%s' but no such backend is registered", tokens[i]);
        /* Names are exact, not merely case-insensitive matches. */
        assert_string_equal(b->name, tokens[i]);
        backends_listed++;
    }
    assert_int_equal(backends_listed, argus_backend_count());
    free(copy);
}

static void test_no_token_is_repeated(void **state)
{
    (void)state;
    char *tokens[64];
    int ntok = 0;
    char *copy = tokenize(argus_build_manifest(), tokens, 64, &ntok);
    for (int i = 0; i < ntok; i++)
        for (int j = i + 1; j < ntok; j++)
            assert_string_not_equal(tokens[i], tokens[j]);
    free(copy);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_manifest_names_the_version),
        cmocka_unit_test(test_every_registered_backend_is_listed),
        cmocka_unit_test(test_every_listed_backend_is_registered),
        cmocka_unit_test(test_no_token_is_repeated),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
