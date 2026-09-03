/*
 * The INFORMATION_SCHEMA queries behind SQLTables/SQLColumns/SQLSchemas on
 * Druid: every application-supplied pattern is a quoted literal.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <glib.h>
#include "druid_internal.h"

static void test_tables_query(void **state)
{
    (void)state;
    char *q = druid_build_tables_query("druid", "dr%", "wiki'", "TABLE");
    assert_non_null(q);
    assert_non_null(strstr(q, " AND TABLE_CATALOG = 'druid'"));
    assert_non_null(strstr(q, " AND TABLE_SCHEMA LIKE 'dr%'"));
    assert_non_null(strstr(q, " AND TABLE_NAME LIKE 'wiki'''"));
    assert_non_null(strstr(q, " AND TABLE_TYPE IN ('TABLE','BASE TABLE')"));
    assert_non_null(strstr(q, " ORDER BY TABLE_SCHEMA, TABLE_NAME"));
    g_free(q);

    /* The type list is inspected, never copied into the query. */
    q = druid_build_tables_query(NULL, NULL, NULL, "TABLE') OR ('1'='1");
    assert_null(strstr(q, "OR ("));
    g_free(q);
}

static void test_columns_and_schemas_escape(void **state)
{
    (void)state;
    char *q = druid_build_columns_query(NULL, NULL, "t",
                                        "c' OR 1=1 --");
    assert_non_null(strstr(q, " AND COLUMN_NAME LIKE 'c'' OR 1=1 --'"));
    assert_null(strstr(q, "c' OR"));
    g_free(q);

    q = druid_build_schemas_query("c'", "s\\'");
    assert_non_null(strstr(q, " AND CATALOG_NAME = 'c''' AND SCHEMA_NAME LIKE 's\\'''"));
    g_free(q);
}

static void test_long_pattern_not_truncated(void **state)
{
    (void)state;
    char name[4000];
    memset(name, 'n', sizeof(name) - 1);
    name[sizeof(name) - 1] = '\0';
    char *q = druid_build_columns_query("c", "s", name, "col");
    assert_non_null(q);
    assert_non_null(strstr(q, " AND COLUMN_NAME LIKE 'col'"));
    assert_non_null(strstr(q, " ORDER BY "));
    g_free(q);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_tables_query),
        cmocka_unit_test(test_columns_and_schemas_escape),
        cmocka_unit_test(test_long_pattern_not_truncated),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
