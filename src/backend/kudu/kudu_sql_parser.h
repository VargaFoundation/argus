#ifndef ARGUS_KUDU_SQL_PARSER_H
#define ARGUS_KUDU_SQL_PARSER_H

#ifdef __cplusplus
extern "C" {
#endif

/* Predicate operators */
#define KUDU_OP_EQ          0
#define KUDU_OP_LT          1
#define KUDU_OP_LE          2
#define KUDU_OP_GT          3
#define KUDU_OP_GE          4
#define KUDU_OP_NE          5
#define KUDU_OP_IN          6
#define KUDU_OP_IS_NULL     7
#define KUDU_OP_IS_NOT_NULL 8

/* A single WHERE predicate */
typedef struct kudu_predicate {
    char *column;
    int   op;               /* KUDU_OP_* constant */
    char *value;            /* string representation (for comparison ops) */
    char **in_values;       /* for IN lists */
    int    num_in_values;
} kudu_predicate_t;

/* Parsed SELECT query */
typedef struct kudu_parsed_query {
    char  *table_name;
    char **columns;         /* NULL = SELECT * */
    int    num_columns;
    kudu_predicate_t *predicates;
    int    num_predicates;
    int    limit;           /* -1 = no limit */
} kudu_parsed_query_t;

/* Parse a SQL query string into a kudu_parsed_query_t.
 * Returns 0 on success, -1 on error (unsupported syntax).
 * On error, *error_msg is set to a static string describing the issue. */
int kudu_sql_parse(const char *sql, kudu_parsed_query_t *query,
                   const char **error_msg);

/* Free all memory allocated by kudu_sql_parse */
void kudu_parsed_query_free(kudu_parsed_query_t *query);

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_KUDU_SQL_PARSER_H */
