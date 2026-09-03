/*
 * Switch the process to a locale whose decimal separator is a comma, the
 * way Excel, Tableau and Power BI leave it after setlocale(LC_ALL, "") on a
 * French or German desktop. Tests use it to prove that numbers still cross
 * the driver with a '.' regardless of LC_NUMERIC.
 */
#ifndef ARGUS_TEST_LOCALE_HELPER_H
#define ARGUS_TEST_LOCALE_HELPER_H

#include <locale.h>
#include <stdlib.h>
#include <string.h>

/* Returns the name of the locale now in force for LC_NUMERIC, or NULL when
 * none of the candidates is installed. */
static inline const char *argus_test_use_comma_locale(void)
{
    static const char *const candidates[] = {
        "fr_FR.UTF-8", "fr_FR.utf8", "de_DE.UTF-8", "de_DE.utf8",
        "fr_FR", "de_DE",
#ifdef _WIN32
        "fr-FR", "French_France.1252", "de-DE", "German_Germany.1252",
#endif
        NULL
    };
    for (int i = 0; candidates[i]; i++) {
        if (setlocale(LC_NUMERIC, candidates[i]) &&
            strcmp(localeconv()->decimal_point, ",") == 0)
            return candidates[i];
    }
    setlocale(LC_NUMERIC, "C");
    return NULL;
}

static inline void argus_test_restore_c_locale(void)
{
    setlocale(LC_NUMERIC, "C");
}

/* CI sets ARGUS_TEST_REQUIRE_LOCALE=1 so a missing locale fails instead of
 * silently skipping the whole point of the test. */
static inline int argus_test_locale_required(void)
{
    const char *v = getenv("ARGUS_TEST_REQUIRE_LOCALE");
    return v && *v && strcmp(v, "0") != 0;
}

#endif /* ARGUS_TEST_LOCALE_HELPER_H */
