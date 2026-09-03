/*
 * The information_schema queries behind SQLTables/SQLColumns/... on Trino,
 * built from application-supplied search patterns: every pattern is a
 * quoted literal and the type list no longer lives in a fixed buffer.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <glib.h>
#include "trino_internal.h"

static void test_tables_query_quotes_patterns(void **state)
{
    (void)state;
    char *q = trino_build_tables_query("hive", "def%", "ord_%", NULL);
    assert_non_null(q);
    assert_non_null(strstr(q, " AND table_catalog = 'hive'"));
    assert_non_null(strstr(q, " AND table_schema LIKE 'def%'"));
    assert_non_null(strstr(q, " AND table_name LIKE 'ord_%'"));
    assert_non_null(strstr(q, " ORDER BY table_catalog, table_schema, table_name"));
    g_free(q);

    /* Empty and NULL filters add nothing. */
    q = trino_build_tables_query("", NULL, "", "");
    assert_null(strstr(q, " AND "));
    g_free(q);
}

static void test_tables_query_escapes_injection(void **state)
{
    (void)state;
    char *q = trino_build_tables_query(NULL, NULL,
                                       "x' OR 1=1 --", NULL);
    assert_non_null(q);
    assert_non_null(strstr(q, " AND table_name LIKE 'x'' OR 1=1 --'"));
    /* The single quote never closes the literal early. */
    assert_null(strstr(q, "x' OR"));
    g_free(q);

    /* A backslash before the quote must not neutralise the doubling:
     * Trino literals are ANSI, the backslash is an ordinary character. */
    q = trino_build_columns_query(NULL, NULL, "t", "c\\'; DROP TABLE t; --");
    assert_non_null(strstr(q, "column_name LIKE 'c\\''; DROP TABLE t; --'"));
    g_free(q);
}

static void test_table_types_are_quoted_and_unbounded(void **state)
{
    (void)state;
    char *q = trino_build_tables_query(NULL, NULL, NULL,
                                       "'TABLE', \"VIEW\" , SYSTEM TABLE");
    assert_non_null(strstr(q, " AND table_type IN ('BASE TABLE','VIEW','SYSTEM TABLE')"));
    g_free(q);

    q = trino_build_tables_query(NULL, NULL, NULL, "TABLE') OR ('1'='1");
    assert_non_null(strstr(q, " AND table_type IN ('TABLE'') OR (''1''=''1')"));
    g_free(q);

    /* Previously accumulated into char[256] with an unchecked offset. */
    GString *big = g_string_new(NULL);
    for (int i = 0; i < 200; i++) g_string_append_printf(big, "TYPE%03d,", i);
    q = trino_build_tables_query(NULL, NULL, NULL, big->str);
    assert_non_null(q);
    assert_non_null(strstr(q, "'TYPE000','TYPE001'"));
    assert_non_null(strstr(q, "'TYPE199')"));
    assert_non_null(strstr(q, " ORDER BY "));
    g_free(q);
    g_string_free(big, TRUE);

    /* An empty list after trimming adds no clause. */
    q = trino_build_tables_query(NULL, NULL, NULL, " , '' ");
    assert_null(strstr(q, "table_type IN"));
    g_free(q);
}

static void test_long_patterns_are_not_truncated(void **state)
{
    (void)state;
    char name[4000];
    memset(name, 'n', sizeof(name) - 1);
    name[sizeof(name) - 1] = '\0';
    char *q = trino_build_columns_query("c", "s", name, "col");
    assert_non_null(q);
    assert_true(strlen(q) > sizeof(name));
    assert_non_null(strstr(q, " AND column_name LIKE 'col'"));
    assert_non_null(strstr(q, " ORDER BY "));
    g_free(q);
}

static void test_schemas_and_primary_keys(void **state)
{
    (void)state;
    char *q = trino_build_schemas_query("c'at", "s%");
    assert_non_null(strstr(q, " AND catalog_name = 'c''at' AND schema_name LIKE 's%'"));
    assert_non_null(strstr(q, " ORDER BY catalog_name, schema_name"));
    g_free(q);

    q = trino_build_primary_keys_query("c", "s", "t'");
    assert_non_null(strstr(q, " AND table_cat = 'c' AND table_schem = 's' AND table_name = 't'''"));
    g_free(q);
}

static void test_statistics_quotes_identifiers(void **state)
{
    (void)state;
    char *q = trino_build_statistics_query("hive", "sales", "orders");
    assert_string_equal(q, "SHOW STATS FOR \"hive\".\"sales\".\"orders\"");
    g_free(q);

    q = trino_build_statistics_query(NULL, "sales", "orders");
    assert_string_equal(q, "SHOW STATS FOR \"sales\".\"orders\"");
    g_free(q);

    /* Catalog without schema is not a valid 3-part name: catalog ignored. */
    q = trino_build_statistics_query("hive", NULL, "orders");
    assert_string_equal(q, "SHOW STATS FOR \"orders\"");
    g_free(q);

    q = trino_build_statistics_query(NULL, NULL, "o\"; DROP TABLE x; --");
    assert_string_equal(q, "SHOW STATS FOR \"o\"\"; DROP TABLE x; --\"");
    g_free(q);

    assert_null(trino_build_statistics_query("c", "s", NULL));
    assert_null(trino_build_statistics_query("c", "s", ""));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_tables_query_quotes_patterns),
        cmocka_unit_test(test_tables_query_escapes_injection),
        cmocka_unit_test(test_table_types_are_quoted_and_unbounded),
        cmocka_unit_test(test_long_patterns_are_not_truncated),
        cmocka_unit_test(test_schemas_and_primary_keys),
        cmocka_unit_test(test_statistics_quotes_identifiers),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
