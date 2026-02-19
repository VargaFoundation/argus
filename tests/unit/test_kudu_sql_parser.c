#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>

/* SQL parser API (defined in kudu_sql_parser.c) */
typedef struct kudu_predicate {
    char *column;
    int   op;
    char *value;
    char **in_values;
    int    num_in_values;
} kudu_predicate_t;

typedef struct kudu_parsed_query {
    char  *table_name;
    char **columns;
    int    num_columns;
    kudu_predicate_t *predicates;
    int    num_predicates;
    int    limit;
} kudu_parsed_query_t;

int kudu_sql_parse(const char *sql, kudu_parsed_query_t *query,
                   const char **error_msg);
void kudu_parsed_query_free(kudu_parsed_query_t *query);

#define KUDU_OP_EQ          0
#define KUDU_OP_LT          1
#define KUDU_OP_LE          2
#define KUDU_OP_GT          3
#define KUDU_OP_GE          4
#define KUDU_OP_NE          5
#define KUDU_OP_IN          6
#define KUDU_OP_IS_NULL     7
#define KUDU_OP_IS_NOT_NULL 8

/* ── Test: SELECT * FROM table ───────────────────────────────── */

static void test_select_star(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(kudu_sql_parse("SELECT * FROM my_table", &q, &err), 0);
    assert_string_equal(q.table_name, "my_table");
    assert_null(q.columns);
    assert_int_equal(q.num_columns, 0);
    assert_int_equal(q.num_predicates, 0);
    assert_int_equal(q.limit, -1);
    kudu_parsed_query_free(&q);
}

/* ── Test: SELECT specific columns ───────────────────────────── */

static void test_select_columns(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse("SELECT col1, col2, col3 FROM tbl", &q, &err), 0);
    assert_string_equal(q.table_name, "tbl");
    assert_int_equal(q.num_columns, 3);
    assert_string_equal(q.columns[0], "col1");
    assert_string_equal(q.columns[1], "col2");
    assert_string_equal(q.columns[2], "col3");
    kudu_parsed_query_free(&q);
}

/* ── Test: WHERE with comparison operators ───────────────────── */

static void test_where_comparisons(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE x = 42", &q, &err), 0);
    assert_int_equal(q.num_predicates, 1);
    assert_string_equal(q.predicates[0].column, "x");
    assert_int_equal(q.predicates[0].op, KUDU_OP_EQ);
    assert_string_equal(q.predicates[0].value, "42");
    kudu_parsed_query_free(&q);

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE y < 10", &q, &err), 0);
    assert_int_equal(q.predicates[0].op, KUDU_OP_LT);
    kudu_parsed_query_free(&q);

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE z >= 100", &q, &err), 0);
    assert_int_equal(q.predicates[0].op, KUDU_OP_GE);
    kudu_parsed_query_free(&q);

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE a != 0", &q, &err), 0);
    assert_int_equal(q.predicates[0].op, KUDU_OP_NE);
    kudu_parsed_query_free(&q);

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE b <> 0", &q, &err), 0);
    assert_int_equal(q.predicates[0].op, KUDU_OP_NE);
    kudu_parsed_query_free(&q);
}

/* ── Test: WHERE with multiple AND predicates ────────────────── */

static void test_where_multiple_and(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse(
            "SELECT * FROM t WHERE x = 1 AND y > 2 AND z <= 3",
            &q, &err), 0);
    assert_int_equal(q.num_predicates, 3);
    assert_string_equal(q.predicates[0].column, "x");
    assert_int_equal(q.predicates[0].op, KUDU_OP_EQ);
    assert_string_equal(q.predicates[1].column, "y");
    assert_int_equal(q.predicates[1].op, KUDU_OP_GT);
    assert_string_equal(q.predicates[2].column, "z");
    assert_int_equal(q.predicates[2].op, KUDU_OP_LE);
    kudu_parsed_query_free(&q);
}

/* ── Test: WHERE with IN list ────────────────────────────────── */

static void test_where_in_list(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse(
            "SELECT * FROM t WHERE status IN ('active', 'pending', 'new')",
            &q, &err), 0);
    assert_int_equal(q.num_predicates, 1);
    assert_int_equal(q.predicates[0].op, KUDU_OP_IN);
    assert_string_equal(q.predicates[0].column, "status");
    assert_int_equal(q.predicates[0].num_in_values, 3);
    assert_string_equal(q.predicates[0].in_values[0], "active");
    assert_string_equal(q.predicates[0].in_values[1], "pending");
    assert_string_equal(q.predicates[0].in_values[2], "new");
    kudu_parsed_query_free(&q);
}

/* ── Test: WHERE with IS NULL / IS NOT NULL ──────────────────── */

static void test_where_null_checks(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE x IS NULL", &q, &err), 0);
    assert_int_equal(q.num_predicates, 1);
    assert_int_equal(q.predicates[0].op, KUDU_OP_IS_NULL);
    assert_string_equal(q.predicates[0].column, "x");
    kudu_parsed_query_free(&q);

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t WHERE y IS NOT NULL", &q, &err), 0);
    assert_int_equal(q.predicates[0].op, KUDU_OP_IS_NOT_NULL);
    kudu_parsed_query_free(&q);
}

/* ── Test: LIMIT clause ──────────────────────────────────────── */

static void test_limit(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM t LIMIT 100", &q, &err), 0);
    assert_int_equal(q.limit, 100);
    kudu_parsed_query_free(&q);

    assert_int_equal(
        kudu_sql_parse(
            "SELECT col1 FROM t WHERE x = 1 LIMIT 50",
            &q, &err), 0);
    assert_int_equal(q.limit, 50);
    assert_int_equal(q.num_columns, 1);
    assert_int_equal(q.num_predicates, 1);
    kudu_parsed_query_free(&q);
}

/* ── Test: String values in WHERE ────────────────────────────── */

static void test_string_values(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse(
            "SELECT * FROM t WHERE name = 'hello world'",
            &q, &err), 0);
    assert_int_equal(q.num_predicates, 1);
    assert_string_equal(q.predicates[0].value, "hello world");
    kudu_parsed_query_free(&q);
}

/* ── Test: Case insensitive keywords ─────────────────────────── */

static void test_case_insensitive_keywords(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse("select * from my_table where x = 1 limit 10",
                       &q, &err), 0);
    assert_string_equal(q.table_name, "my_table");
    assert_int_equal(q.num_predicates, 1);
    assert_int_equal(q.limit, 10);
    kudu_parsed_query_free(&q);
}

/* ── Test: Unsupported statements rejected ───────────────────── */

static void test_unsupported_rejected(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_not_equal(
        kudu_sql_parse("INSERT INTO t VALUES (1)", &q, &err), 0);
    assert_non_null(err);

    assert_int_not_equal(
        kudu_sql_parse("UPDATE t SET x = 1", &q, &err), 0);

    assert_int_not_equal(
        kudu_sql_parse("DELETE FROM t WHERE x = 1", &q, &err), 0);

    assert_int_not_equal(
        kudu_sql_parse("CREATE TABLE t (x INT)", &q, &err), 0);

    assert_int_not_equal(
        kudu_sql_parse("DROP TABLE t", &q, &err), 0);
}

/* ── Test: Unsupported SELECT clauses rejected ───────────────── */

static void test_unsupported_clauses(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_not_equal(
        kudu_sql_parse(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
            &q, &err), 0);

    assert_int_not_equal(
        kudu_sql_parse(
            "SELECT x, COUNT(*) FROM t GROUP BY x",
            &q, &err), 0);

    assert_int_not_equal(
        kudu_sql_parse(
            "SELECT * FROM t ORDER BY x",
            &q, &err), 0);
}

/* ── Test: NULL input ────────────────────────────────────────── */

static void test_null_input(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_not_equal(kudu_sql_parse(NULL, &q, &err), 0);
    assert_int_not_equal(kudu_sql_parse("SELECT * FROM t", NULL, &err), 0);
}

/* ── Test: Semicolon handling ────────────────────────────────── */

static void test_semicolon(void **state)
{
    (void)state;
    kudu_parsed_query_t q;
    const char *err = NULL;

    assert_int_equal(
        kudu_sql_parse("SELECT * FROM my_table;", &q, &err), 0);
    assert_string_equal(q.table_name, "my_table");
    kudu_parsed_query_free(&q);
}

/* ── Main ─────────────────────────────────────────────────────── */

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_select_star),
        cmocka_unit_test(test_select_columns),
        cmocka_unit_test(test_where_comparisons),
        cmocka_unit_test(test_where_multiple_and),
        cmocka_unit_test(test_where_in_list),
        cmocka_unit_test(test_where_null_checks),
        cmocka_unit_test(test_limit),
        cmocka_unit_test(test_string_values),
        cmocka_unit_test(test_case_insensitive_keywords),
        cmocka_unit_test(test_unsupported_rejected),
        cmocka_unit_test(test_unsupported_clauses),
        cmocka_unit_test(test_null_input),
        cmocka_unit_test(test_semicolon),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
