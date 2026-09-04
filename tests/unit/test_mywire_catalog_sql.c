/* SPDX-License-Identifier: Apache-2.0 */
/*
 * The information_schema queries behind SQLTables/SQLColumns/SQLPrimaryKeys
 * on the MySQL wire backend: every application-supplied pattern goes
 * through mysql_real_escape_string() on the connection's handle. An
 * initialised, unconnected handle is enough for the escaping.
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <glib.h>
#include "mywire_internal.h"

static MYSQL *g_mysql;

static int group_setup(void **state)
{
    (void)state;
    g_mysql = mysql_init(NULL);
    return g_mysql ? 0 : -1;
}

static int group_teardown(void **state)
{
    (void)state;
    mysql_close(g_mysql);
    return 0;
}

static void test_tables_query_escapes_patterns(void **state)
{
    (void)state;
    char *q = mywire_build_tables_query(g_mysql, "shop", NULL, "ord_%", NULL);
    assert_non_null(q);
    assert_non_null(strstr(q, " AND table_schema = 'shop'"));
    assert_non_null(strstr(q, " AND table_name LIKE 'ord_%' ESCAPE '\\\\'"));
    assert_non_null(strstr(q, " ORDER BY table_schema, table_name"));
    g_free(q);

    /* Schema is the fallback filter when no catalog is given. */
    q = mywire_build_tables_query(g_mysql, NULL, "sh%", NULL, NULL);
    assert_non_null(strstr(q, " AND table_schema LIKE 'sh%' ESCAPE '\\\\'"));
    g_free(q);

    /* MySQL escapes with a backslash: quote and backslash both survive
     * as data, and the clause cannot be closed early. */
    q = mywire_build_tables_query(g_mysql, NULL, NULL, "x' OR 1=1 --", NULL);
    assert_non_null(strstr(q, " AND table_name = 'x\\' OR 1=1 --'"));
    g_free(q);

    q = mywire_build_columns_query(g_mysql, NULL, NULL, "t", "c\\'");
    /* The backslash is the advertised search-pattern escape, so it makes the
     * quote literal and is consumed; what reaches MySQL is the one quote,
     * escaped for the literal by mysql_real_escape_string. */
    assert_non_null(strstr(q, " AND column_name = 'c\\''"));
    g_free(q);
}

static void test_table_types_quoted_and_unbounded(void **state)
{
    (void)state;
    char *q = mywire_build_tables_query(g_mysql, NULL, NULL, NULL,
                                        "'TABLE', VIEW");
    assert_non_null(strstr(q, " AND table_type IN ('BASE TABLE','VIEW')"));
    g_free(q);

    q = mywire_build_tables_query(g_mysql, NULL, NULL, NULL,
                                  "TABLE') OR ('1'='1");
    assert_non_null(strstr(q, " AND table_type IN ('TABLE\\') OR (\\'1\\'=\\'1')"));
    g_free(q);

    GString *big = g_string_new(NULL);
    for (int i = 0; i < 200; i++) g_string_append_printf(big, "TYPE%03d,", i);
    q = mywire_build_tables_query(g_mysql, NULL, NULL, NULL, big->str);
    assert_non_null(q);
    assert_non_null(strstr(q, "'TYPE199')"));
    assert_non_null(strstr(q, " ORDER BY "));
    g_free(q);
    g_string_free(big, TRUE);
}

static void test_primary_keys_and_long_names(void **state)
{
    (void)state;
    char *q = mywire_build_primary_keys_query(g_mysql, NULL, "db", "t'");
    assert_non_null(strstr(q, " AND table_schema = 'db' AND table_name = 't\\''"));
    g_free(q);

    char name[4000];
    memset(name, 'n', sizeof(name) - 1);
    name[sizeof(name) - 1] = '\0';
    q = mywire_build_columns_query(g_mysql, "c", NULL, name, "col");
    assert_non_null(q);
    assert_non_null(strstr(q, " AND column_name = 'col'"));
    assert_non_null(strstr(q, " ORDER BY "));
    g_free(q);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_tables_query_escapes_patterns),
        cmocka_unit_test(test_table_types_quoted_and_unbounded),
        cmocka_unit_test(test_primary_keys_and_long_names),
    };
    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
