/*
 * ODBC escape sequence translation.
 *
 * "The escape sequence is recognized and parsed by drivers, which replace the
 * escape sequences with DBMS-specific grammar." — ODBC spec, Escape Sequences
 * in ODBC. Translating them is the driver's job, not the driver manager's, and
 * SQLGetInfo is where the driver declares which ones it can handle.
 *
 * Power Query builds SQL from its own AST and never emits these, which is why
 * the Power BI connector worked without this file. Tableau, Excel, Qlik and
 * Alteryx all read SQLGetInfo and then emit {fn UCASE(x)}, {ts '...'} and
 * {oj ...}; passing those through unchanged is what makes a server reject the
 * query (see ClickHouse/clickhouse-odbc#141 for the same bug).
 *
 * Anything this file cannot render is an error, never a pass-through: the
 * dialect table is the single source of what SQLGetInfo advertised, so an
 * unmappable escape means the application was misled, and a clear 42000 beats
 * an opaque server-side syntax error.
 */

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <glib.h>
#include "argus/dialect.h"
#include "argus/handle.h"

/* Parser state threaded through the recursive descent. */
typedef struct {
    const argus_dialect_t *dialect;
    argus_diag_t          *diag;
    bool                   failed;
} escape_ctx_t;

static void escape_fail(escape_ctx_t *ctx, const char *sqlstate, const char *fmt, ...)
    G_GNUC_PRINTF(3, 4);

static void escape_fail(escape_ctx_t *ctx, const char *sqlstate, const char *fmt, ...)
{
    char    msg[ARGUS_MAX_MESSAGE_LEN];
    va_list ap;

    if (ctx->failed) return;   /* keep the first, most specific error */
    ctx->failed = true;

    va_start(ap, fmt);
    g_vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    if (ctx->diag) argus_diag_push(ctx->diag, sqlstate, msg, 0);
}

static void skip_ws(const char **p)
{
    while (**p && g_ascii_isspace(**p)) (*p)++;
}

/*
 * Copy one quoted run verbatim, starting at the opening quote. Handles the SQL
 * doubling convention ('' inside '...', "" inside "..."), which is what keeps
 * a brace inside a string literal from being mistaken for an escape.
 * Returns false if the quote is never closed.
 */
static bool copy_quoted(const char **p, GString *out)
{
    char quote = **p;

    g_string_append_c(out, quote);
    (*p)++;

    while (**p) {
        if (**p == quote) {
            if (*(*p + 1) == quote) {          /* doubled: literal quote char */
                g_string_append_c(out, quote);
                g_string_append_c(out, quote);
                *p += 2;
                continue;
            }
            g_string_append_c(out, quote);
            (*p)++;
            return true;
        }
        g_string_append_c(out, **p);
        (*p)++;
    }
    return false;   /* unterminated */
}

static void scan_escape(escape_ctx_t *ctx, const char **p, GString *out);

/*
 * Copy an expression, translating any nested escapes, until a top-level ')' or
 * (when stop_at_comma) ','. Nesting of () and [] is tracked so that commas
 * inside a call argument don't split the enclosing argument list. The
 * terminator itself is left unconsumed.
 */
static void scan_expr(escape_ctx_t *ctx, const char **p, GString *out,
                      bool stop_at_comma)
{
    int depth = 0;

    while (**p && !ctx->failed) {
        char c = **p;

        if (c == '\'' || c == '"' || c == '`') {
            if (!copy_quoted(p, out))
                escape_fail(ctx, "42000", "[Argus] Unterminated string literal");
            continue;
        }
        if (c == '{') {
            (*p)++;
            scan_escape(ctx, p, out);
            continue;
        }
        if (c == '(' || c == '[') {
            depth++;
        } else if (c == ')' || c == ']') {
            if (depth == 0 && c == ')') return;   /* caller's closing paren */
            if (depth > 0) depth--;
        } else if (c == ',' && depth == 0 && stop_at_comma) {
            return;
        }

        g_string_append_c(out, c);
        (*p)++;
    }
}

/* Read an identifier ([A-Za-z_][A-Za-z0-9_]*) into a caller-owned buffer. */
static bool read_word(const char **p, char *buf, size_t buflen)
{
    size_t n = 0;

    if (!(g_ascii_isalpha(**p) || **p == '_')) return false;

    while ((g_ascii_isalnum(**p) || **p == '_') && n + 1 < buflen) {
        buf[n++] = **p;
        (*p)++;
    }
    buf[n] = '\0';
    return n > 0;
}

/* Expand a dialect template ("strpos($2, $1)") against parsed arguments. */
static void expand_template(escape_ctx_t *ctx, const char *tmpl,
                            GPtrArray *args, GString *out)
{
    for (const char *t = tmpl; *t; t++) {
        if (*t != '$') {
            g_string_append_c(out, *t);
            continue;
        }

        t++;
        if (*t == '*') {
            for (guint i = 0; i < args->len; i++) {
                if (i) g_string_append(out, ", ");
                g_string_append(out, ((GString *)args->pdata[i])->str);
            }
        } else if (*t >= '1' && *t <= '9') {
            guint idx = (guint)(*t - '1');
            if (idx < args->len) {
                g_string_append(out, ((GString *)args->pdata[idx])->str);
            } else {
                /* A template referencing an argument the arity check should
                 * have guaranteed — a bug in the dialect table, not in input. */
                escape_fail(ctx, "HY000",
                            "[Argus] Dialect template '%s' references $%c "
                            "but only %u argument(s) were given",
                            tmpl, *t, args->len);
                return;
            }
        } else if (*t == '\0') {
            g_string_append_c(out, '$');
            return;
        } else {
            g_string_append_c(out, '$');
            g_string_append_c(out, *t);
        }
    }
}

static void free_args(GPtrArray *args)
{
    for (guint i = 0; i < args->len; i++)
        g_string_free((GString *)args->pdata[i], TRUE);
    g_ptr_array_free(args, TRUE);
}

/* {fn NAME(args)} — *p is just past "fn". */
static void scan_fn(escape_ctx_t *ctx, const char **p, GString *out)
{
    char       name[64];
    GPtrArray *args;

    skip_ws(p);
    if (!read_word(p, name, sizeof(name))) {
        escape_fail(ctx, "42000", "[Argus] Malformed {fn ...} escape: expected a function name");
        return;
    }

    const argus_fn_entry_t *fn = argus_dialect_find_fn(ctx->dialect, name);
    if (!fn) {
        /* SQLGetInfo never advertised this one, so the application should not
         * have sent it. Say so plainly rather than let the server guess. */
        escape_fail(ctx, "42000",
                    "[Argus] Scalar function '%s' is not supported by the %s dialect",
                    name, ctx->dialect->name);
        return;
    }

    skip_ws(p);
    if (**p != '(') {
        escape_fail(ctx, "42000",
                    "[Argus] Malformed {fn %s ...} escape: expected '('", name);
        return;
    }
    (*p)++;

    args = g_ptr_array_new();

    skip_ws(p);
    if (**p == ')') {
        (*p)++;   /* zero arguments, e.g. {fn NOW()} */
    } else {
        for (;;) {
            GString *arg = g_string_new(NULL);
            scan_expr(ctx, p, arg, true);
            /* g_strstrip works in place; resync the GString's length to the
             * NUL it just moved, or ->len stays stale. */
            g_strstrip(arg->str);
            g_string_set_size(arg, strlen(arg->str));
            g_ptr_array_add(args, arg);

            if (ctx->failed) { free_args(args); return; }

            if (**p == ',') { (*p)++; continue; }
            if (**p == ')') { (*p)++; break; }

            escape_fail(ctx, "42000",
                        "[Argus] Malformed {fn %s ...} escape: expected ',' or ')'", name);
            free_args(args);
            return;
        }
    }

    if ((int)args->len < fn->min_args ||
        (fn->max_args != ARGUS_FN_VARIADIC && (int)args->len > fn->max_args)) {
        if (fn->max_args == ARGUS_FN_VARIADIC) {
            escape_fail(ctx, "42000",
                        "[Argus] {fn %s} takes at least %d argument(s), got %u",
                        name, fn->min_args, args->len);
        } else if (fn->min_args == fn->max_args) {
            escape_fail(ctx, "42000",
                        "[Argus] {fn %s} takes %d argument(s), got %u",
                        name, fn->min_args, args->len);
        } else {
            escape_fail(ctx, "42000",
                        "[Argus] {fn %s} takes %d to %d argument(s), got %u",
                        name, fn->min_args, fn->max_args, args->len);
        }
        free_args(args);
        return;
    }

    expand_template(ctx, fn->tmpl, args, out);
    free_args(args);

    skip_ws(p);
    if (**p != '}') {
        escape_fail(ctx, "42000",
                    "[Argus] Malformed {fn %s ...} escape: expected '}'", name);
        return;
    }
    (*p)++;
}

/* {d '...'} / {t '...'} / {ts '...'} */
static void scan_literal(escape_ctx_t *ctx, const char **p, GString *out,
                         const char *kind, const char *sql_type)
{
    GString *lit;

    skip_ws(p);
    if (**p != '\'') {
        escape_fail(ctx, "42000",
                    "[Argus] Malformed {%s ...} escape: expected a quoted literal", kind);
        return;
    }

    lit = g_string_new(NULL);
    if (!copy_quoted(p, lit)) {
        escape_fail(ctx, "42000", "[Argus] Unterminated literal in {%s ...} escape", kind);
        g_string_free(lit, TRUE);
        return;
    }

    switch (ctx->dialect->literals) {
    case ARGUS_LIT_CAST:
        g_string_append_printf(out, "CAST(%s AS %s)", lit->str, sql_type);
        break;
    case ARGUS_LIT_STRING:
        g_string_append(out, lit->str);
        break;
    case ARGUS_LIT_ANSI:
    default:
        g_string_append_printf(out, "%s %s", sql_type, lit->str);
        break;
    }
    g_string_free(lit, TRUE);

    skip_ws(p);
    if (**p != '}') {
        escape_fail(ctx, "42000", "[Argus] Malformed {%s ...} escape: expected '}'", kind);
        return;
    }
    (*p)++;
}

/* Copy the escape body through to its matching '}', translating nested
 * escapes. Used by {oj ...} and {interval ...}, whose bodies are already
 * standard SQL — the escape only marks them. */
static void scan_body(escape_ctx_t *ctx, const char **p, GString *out,
                      const char *kind)
{
    int depth = 0;

    skip_ws(p);
    while (**p && !ctx->failed) {
        char c = **p;

        if (c == '\'' || c == '"' || c == '`') {
            if (!copy_quoted(p, out))
                escape_fail(ctx, "42000", "[Argus] Unterminated string literal");
            continue;
        }
        if (c == '}' && depth == 0) { (*p)++; return; }
        if (c == '{') {
            (*p)++;
            scan_escape(ctx, p, out);
            continue;
        }
        if (c == '(') depth++;
        else if (c == ')') depth--;

        g_string_append_c(out, c);
        (*p)++;
    }

    if (!ctx->failed)
        escape_fail(ctx, "42000", "[Argus] Unterminated {%s ...} escape", kind);
}

/* Dispatch on the escape keyword. *p is just past the opening '{'. */
static void scan_escape(escape_ctx_t *ctx, const char **p, GString *out)
{
    char word[32];

    skip_ws(p);

    /* {?= call proc(...)} — the only escape that doesn't start with a word. */
    if (**p == '?') {
        escape_fail(ctx, "HYC00",
                    "[Argus] Procedure call escapes are not supported "
                    "(this driver reports no procedures)");
        return;
    }

    if (!read_word(p, word, sizeof(word))) {
        escape_fail(ctx, "42000", "[Argus] Malformed escape sequence");
        return;
    }

    if (g_ascii_strcasecmp(word, "fn") == 0) {
        scan_fn(ctx, p, out);
    } else if (g_ascii_strcasecmp(word, "d") == 0) {
        scan_literal(ctx, p, out, "d", "DATE");
    } else if (g_ascii_strcasecmp(word, "t") == 0) {
        scan_literal(ctx, p, out, "t", "TIME");
    } else if (g_ascii_strcasecmp(word, "ts") == 0) {
        scan_literal(ctx, p, out, "ts", "TIMESTAMP");
    } else if (g_ascii_strcasecmp(word, "escape") == 0) {
        g_string_append(out, "ESCAPE ");
        scan_body(ctx, p, out, "escape");
    } else if (g_ascii_strcasecmp(word, "oj") == 0) {
        if (!ctx->dialect->supports_oj) {
            escape_fail(ctx, "42000",
                        "[Argus] Outer join escapes are not supported by the %s dialect",
                        ctx->dialect->name);
            return;
        }
        scan_body(ctx, p, out, "oj");
    } else if (g_ascii_strcasecmp(word, "interval") == 0) {
        g_string_append(out, "INTERVAL ");
        scan_body(ctx, p, out, "interval");
    } else if (g_ascii_strcasecmp(word, "call") == 0) {
        escape_fail(ctx, "HYC00",
                    "[Argus] Procedure call escapes are not supported "
                    "(this driver reports no procedures)");
    } else {
        escape_fail(ctx, "42000", "[Argus] Unknown ODBC escape sequence '{%s ...}'", word);
    }
}

argus_escape_result_t argus_escape_translate(const argus_dialect_t *dialect,
                                             const char *sql,
                                             char **out,
                                             argus_diag_t *diag)
{
    escape_ctx_t ctx = { dialect, diag, false };
    GString     *buf;
    const char  *p;

    if (!sql || !out || !dialect) return ARGUS_ESCAPE_NONE;

    /* Fast path: no brace anywhere means no escape, so don't touch the string.
     * A brace inside a string literal is a false positive and merely costs one
     * scan — the real parse below leaves quoted runs alone. */
    if (!strchr(sql, '{')) return ARGUS_ESCAPE_NONE;

    buf = g_string_sized_new(strlen(sql) + 32);
    p   = sql;

    while (*p && !ctx.failed) {
        char c = *p;

        if (c == '\'' || c == '"' || c == '`') {
            if (!copy_quoted(&p, buf))
                escape_fail(&ctx, "42000", "[Argus] Unterminated string literal");
            continue;
        }
        if (c == '{') {
            p++;
            scan_escape(&ctx, &p, buf);
            continue;
        }

        g_string_append_c(buf, c);
        p++;
    }

    if (ctx.failed) {
        g_string_free(buf, TRUE);
        return ARGUS_ESCAPE_ERROR;
    }

    *out = g_string_free(buf, FALSE);
    return ARGUS_ESCAPE_OK;
}
