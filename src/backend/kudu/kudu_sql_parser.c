#include "kudu_sql_parser.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>

/*
 * Minimal SQL parser for Kudu backend.
 *
 * Supported:
 *   SELECT col1, col2 FROM table [WHERE ...] [LIMIT n]
 *   SELECT * FROM table [WHERE ...] [LIMIT n]
 *
 * WHERE clause predicates:
 *   col = value, col < value, col > value, col <= value, col >= value
 *   col != value, col <> value
 *   col IN (v1, v2, ...)
 *   col IS NULL, col IS NOT NULL
 *
 * Unsupported (returns error):
 *   JOIN, GROUP BY, ORDER BY, HAVING, subqueries, UNION,
 *   INSERT, UPDATE, DELETE, CREATE, DROP, ALTER
 */

/* ── Token types ─────────────────────────────────────────────── */

typedef enum {
    TOK_WORD,       /* identifier or keyword */
    TOK_STRING,     /* 'quoted string' */
    TOK_NUMBER,     /* numeric literal */
    TOK_COMMA,      /* , */
    TOK_STAR,       /* * */
    TOK_LPAREN,     /* ( */
    TOK_RPAREN,     /* ) */
    TOK_EQ,         /* = */
    TOK_LT,         /* < */
    TOK_GT,         /* > */
    TOK_LE,         /* <= */
    TOK_GE,         /* >= */
    TOK_NE,         /* != or <> */
    TOK_END,        /* end of input */
} token_type_t;

typedef struct {
    token_type_t type;
    char *text;     /* heap-allocated copy */
} token_t;

typedef struct {
    token_t *tokens;
    int count;
    int capacity;
    int pos;        /* current read position */
} tokenizer_t;

/* ── Tokenizer ───────────────────────────────────────────────── */

static void token_add(tokenizer_t *t, token_type_t type, const char *text,
                      int len)
{
    if (t->count >= t->capacity) {
        t->capacity = t->capacity ? t->capacity * 2 : 32;
        t->tokens = realloc(t->tokens, (size_t)t->capacity * sizeof(token_t));
    }
    t->tokens[t->count].type = type;
    t->tokens[t->count].text = strndup(text, (size_t)len);
    t->count++;
}

static void tokenizer_free(tokenizer_t *t)
{
    for (int i = 0; i < t->count; i++)
        free(t->tokens[i].text);
    free(t->tokens);
    memset(t, 0, sizeof(*t));
}

static int tokenize(const char *sql, tokenizer_t *t)
{
    memset(t, 0, sizeof(*t));
    const char *p = sql;

    while (*p) {
        /* Skip whitespace */
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        /* Single-quoted string literal */
        if (*p == '\'') {
            p++;
            const char *start = p;
            while (*p && *p != '\'') {
                if (*p == '\\' && *(p + 1)) p++; /* skip escaped char */
                p++;
            }
            token_add(t, TOK_STRING, start, (int)(p - start));
            if (*p == '\'') p++;
            continue;
        }

        /* Two-character operators */
        if (*p == '<' && *(p + 1) == '=') { token_add(t, TOK_LE, "<=", 2); p += 2; continue; }
        if (*p == '>' && *(p + 1) == '=') { token_add(t, TOK_GE, ">=", 2); p += 2; continue; }
        if (*p == '!' && *(p + 1) == '=') { token_add(t, TOK_NE, "!=", 2); p += 2; continue; }
        if (*p == '<' && *(p + 1) == '>') { token_add(t, TOK_NE, "<>", 2); p += 2; continue; }

        /* Single-character operators */
        if (*p == ',') { token_add(t, TOK_COMMA, ",", 1); p++; continue; }
        if (*p == '*') { token_add(t, TOK_STAR, "*", 1); p++; continue; }
        if (*p == '(') { token_add(t, TOK_LPAREN, "(", 1); p++; continue; }
        if (*p == ')') { token_add(t, TOK_RPAREN, ")", 1); p++; continue; }
        if (*p == '=') { token_add(t, TOK_EQ, "=", 1); p++; continue; }
        if (*p == '<') { token_add(t, TOK_LT, "<", 1); p++; continue; }
        if (*p == '>') { token_add(t, TOK_GT, ">", 1); p++; continue; }

        /* Semi-colon: treat as end */
        if (*p == ';') { p++; continue; }

        /* Number */
        if (isdigit((unsigned char)*p) ||
            (*p == '-' && isdigit((unsigned char)*(p + 1)))) {
            const char *start = p;
            if (*p == '-') p++;
            while (*p && (isdigit((unsigned char)*p) || *p == '.')) p++;
            token_add(t, TOK_NUMBER, start, (int)(p - start));
            continue;
        }

        /* Word (identifier or keyword) */
        if (isalpha((unsigned char)*p) || *p == '_') {
            const char *start = p;
            while (*p && (isalnum((unsigned char)*p) || *p == '_' || *p == '.'))
                p++;
            token_add(t, TOK_WORD, start, (int)(p - start));
            continue;
        }

        /* Unknown character, skip */
        p++;
    }

    token_add(t, TOK_END, "", 0);
    t->pos = 0;
    return 0;
}

/* ── Parser helpers ──────────────────────────────────────────── */

static token_t *peek(tokenizer_t *t)
{
    return &t->tokens[t->pos];
}

static token_t *advance(tokenizer_t *t)
{
    token_t *tok = &t->tokens[t->pos];
    if (tok->type != TOK_END)
        t->pos++;
    return tok;
}

static bool is_keyword(token_t *tok, const char *kw)
{
    if (tok->type != TOK_WORD) return false;
    return strcasecmp(tok->text, kw) == 0;
}

static bool is_unsupported_keyword(token_t *tok)
{
    if (tok->type != TOK_WORD) return false;
    const char *unsupported[] = {
        "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "FULL",
        "GROUP", "ORDER", "HAVING", "UNION", "INTERSECT", "EXCEPT",
        "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
        "WITH", "EXPLAIN", NULL
    };
    for (int i = 0; unsupported[i]; i++) {
        if (strcasecmp(tok->text, unsupported[i]) == 0) return true;
    }
    return false;
}

/* ── Parse value from token ──────────────────────────────────── */

static char *parse_value(tokenizer_t *t)
{
    token_t *tok = advance(t);
    if (tok->type == TOK_STRING || tok->type == TOK_NUMBER ||
        tok->type == TOK_WORD) {
        return strdup(tok->text);
    }
    return NULL;
}

/* ── Parse WHERE predicate ───────────────────────────────────── */

static int parse_predicate(tokenizer_t *t, kudu_predicate_t *pred)
{
    memset(pred, 0, sizeof(*pred));

    /* Column name */
    token_t *col_tok = advance(t);
    if (col_tok->type != TOK_WORD) return -1;
    pred->column = strdup(col_tok->text);

    token_t *op_tok = peek(t);

    /* IS NULL / IS NOT NULL */
    if (is_keyword(op_tok, "IS")) {
        advance(t);
        token_t *next = peek(t);
        if (is_keyword(next, "NOT")) {
            advance(t);
            token_t *null_tok = advance(t);
            if (!is_keyword(null_tok, "NULL")) return -1;
            pred->op = KUDU_OP_IS_NOT_NULL;
        } else if (is_keyword(next, "NULL")) {
            advance(t);
            pred->op = KUDU_OP_IS_NULL;
        } else {
            return -1;
        }
        return 0;
    }

    /* IN (v1, v2, ...) */
    if (is_keyword(op_tok, "IN")) {
        advance(t);
        pred->op = KUDU_OP_IN;

        if (peek(t)->type != TOK_LPAREN) return -1;
        advance(t); /* skip ( */

        int cap = 8;
        pred->in_values = malloc((size_t)cap * sizeof(char *));
        pred->num_in_values = 0;

        while (peek(t)->type != TOK_RPAREN && peek(t)->type != TOK_END) {
            char *val = parse_value(t);
            if (!val) return -1;

            if (pred->num_in_values >= cap) {
                cap *= 2;
                pred->in_values = realloc(pred->in_values,
                                          (size_t)cap * sizeof(char *));
            }
            pred->in_values[pred->num_in_values++] = val;

            if (peek(t)->type == TOK_COMMA)
                advance(t);
        }

        if (peek(t)->type == TOK_RPAREN)
            advance(t);

        return 0;
    }

    /* Comparison operators */
    op_tok = advance(t);
    switch (op_tok->type) {
    case TOK_EQ: pred->op = KUDU_OP_EQ; break;
    case TOK_LT: pred->op = KUDU_OP_LT; break;
    case TOK_GT: pred->op = KUDU_OP_GT; break;
    case TOK_LE: pred->op = KUDU_OP_LE; break;
    case TOK_GE: pred->op = KUDU_OP_GE; break;
    case TOK_NE: pred->op = KUDU_OP_NE; break;
    default: return -1;
    }

    pred->value = parse_value(t);
    if (!pred->value) return -1;

    return 0;
}

/* ── Main parser ─────────────────────────────────────────────── */

int kudu_sql_parse(const char *sql, kudu_parsed_query_t *query,
                   const char **error_msg)
{
    if (!sql || !query) {
        if (error_msg) *error_msg = "NULL input";
        return -1;
    }

    memset(query, 0, sizeof(*query));
    query->limit = -1;

    tokenizer_t t;
    if (tokenize(sql, &t) != 0) {
        if (error_msg) *error_msg = "tokenization failed";
        return -1;
    }

    /* Check for unsupported statements */
    token_t *first = peek(&t);
    if (first->type == TOK_WORD && !is_keyword(first, "SELECT")) {
        if (error_msg)
            *error_msg = "only SELECT statements are supported";
        tokenizer_free(&t);
        return -1;
    }

    /* Expect SELECT */
    if (!is_keyword(peek(&t), "SELECT")) {
        if (error_msg) *error_msg = "expected SELECT";
        tokenizer_free(&t);
        return -1;
    }
    advance(&t);

    /* Check for unsupported keywords early */
    for (int i = t.pos; i < t.count; i++) {
        if (is_unsupported_keyword(&t.tokens[i])) {
            if (error_msg) {
                static char err_buf[128];
                snprintf(err_buf, sizeof(err_buf),
                         "unsupported keyword: %s", t.tokens[i].text);
                *error_msg = err_buf;
            }
            tokenizer_free(&t);
            return -1;
        }
    }

    /* Parse column list */
    if (peek(&t)->type == TOK_STAR) {
        advance(&t);
        query->columns = NULL;
        query->num_columns = 0;
    } else {
        int cap = 8;
        query->columns = malloc((size_t)cap * sizeof(char *));
        query->num_columns = 0;

        while (peek(&t)->type != TOK_END && !is_keyword(peek(&t), "FROM")) {
            token_t *col = advance(&t);
            if (col->type != TOK_WORD) {
                if (error_msg) *error_msg = "expected column name";
                kudu_parsed_query_free(query);
                tokenizer_free(&t);
                return -1;
            }

            if (query->num_columns >= cap) {
                cap *= 2;
                query->columns = realloc(query->columns,
                                         (size_t)cap * sizeof(char *));
            }
            query->columns[query->num_columns++] = strdup(col->text);

            if (peek(&t)->type == TOK_COMMA)
                advance(&t);
        }
    }

    /* Expect FROM */
    if (!is_keyword(peek(&t), "FROM")) {
        if (error_msg) *error_msg = "expected FROM";
        kudu_parsed_query_free(query);
        tokenizer_free(&t);
        return -1;
    }
    advance(&t);

    /* Table name */
    token_t *table_tok = advance(&t);
    if (table_tok->type != TOK_WORD) {
        if (error_msg) *error_msg = "expected table name";
        kudu_parsed_query_free(query);
        tokenizer_free(&t);
        return -1;
    }
    query->table_name = strdup(table_tok->text);

    /* Optional WHERE clause */
    if (is_keyword(peek(&t), "WHERE")) {
        advance(&t);

        int cap = 4;
        query->predicates = malloc((size_t)cap * sizeof(kudu_predicate_t));
        query->num_predicates = 0;

        while (peek(&t)->type != TOK_END &&
               !is_keyword(peek(&t), "LIMIT")) {
            if (is_keyword(peek(&t), "AND")) {
                advance(&t);
                continue;
            }

            if (query->num_predicates >= cap) {
                cap *= 2;
                query->predicates = realloc(query->predicates,
                                            (size_t)cap *
                                            sizeof(kudu_predicate_t));
            }

            if (parse_predicate(&t,
                                &query->predicates[query->num_predicates])
                != 0) {
                if (error_msg) *error_msg = "failed to parse WHERE predicate";
                kudu_parsed_query_free(query);
                tokenizer_free(&t);
                return -1;
            }
            query->num_predicates++;
        }
    }

    /* Optional LIMIT clause */
    if (is_keyword(peek(&t), "LIMIT")) {
        advance(&t);
        token_t *limit_tok = advance(&t);
        if (limit_tok->type == TOK_NUMBER) {
            query->limit = atoi(limit_tok->text);
        } else {
            if (error_msg) *error_msg = "expected numeric LIMIT value";
            kudu_parsed_query_free(query);
            tokenizer_free(&t);
            return -1;
        }
    }

    tokenizer_free(&t);
    return 0;
}

/* ── Free parsed query ───────────────────────────────────────── */

void kudu_parsed_query_free(kudu_parsed_query_t *query)
{
    if (!query) return;

    free(query->table_name);
    query->table_name = NULL;

    if (query->columns) {
        for (int i = 0; i < query->num_columns; i++)
            free(query->columns[i]);
        free(query->columns);
        query->columns = NULL;
    }

    if (query->predicates) {
        for (int i = 0; i < query->num_predicates; i++) {
            free(query->predicates[i].column);
            free(query->predicates[i].value);
            if (query->predicates[i].in_values) {
                for (int j = 0; j < query->predicates[i].num_in_values; j++)
                    free(query->predicates[i].in_values[j]);
                free(query->predicates[i].in_values);
            }
        }
        free(query->predicates);
        query->predicates = NULL;
    }

    query->num_columns = 0;
    query->num_predicates = 0;
}
