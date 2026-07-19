#ifndef ARGUS_DIALECT_H
#define ARGUS_DIALECT_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>
#include "argus/backend.h"
#include "argus/error.h"

/*
 * Per-backend SQL dialect description.
 *
 * ODBC makes SQLGetInfo a *contract*: an application that sees SQL_FN_STR_UCASE
 * in SQL_STRING_FUNCTIONS is entitled to send {fn UCASE(x)} to SQLExecDirect and
 * expect the driver to translate it ("The escape sequence is recognized and
 * parsed by drivers, which replace the escape sequences with DBMS-specific
 * grammar" — ODBC spec, Escape Sequences in ODBC). Power Query never exercises
 * this because it builds SQL from its own AST, but Tableau, Excel, Qlik and
 * Alteryx all do.
 *
 * So the scalar-function bitmaps reported by SQLGetInfo are NOT stored here:
 * argus_dialect_fn_bitmap() derives them from fn_map. A backend therefore cannot
 * advertise a function it has no translation for — the two can't drift apart,
 * because there is only one source of truth.
 */

/* Which SQLGetInfo bitmap an entry belongs to. Bit values collide across groups
 * (SQL_FN_STR_CONCAT == SQL_FN_NUM_ABS == 1), so the group disambiguates. */
typedef enum argus_fn_group {
    ARGUS_FN_GROUP_STRING = 0,   /* SQL_STRING_FUNCTIONS   */
    ARGUS_FN_GROUP_NUMERIC,      /* SQL_NUMERIC_FUNCTIONS  */
    ARGUS_FN_GROUP_TIMEDATE,     /* SQL_TIMEDATE_FUNCTIONS */
    ARGUS_FN_GROUP_SYSTEM        /* SQL_SYSTEM_FUNCTIONS   */
} argus_fn_group_t;

/* max_args value meaning "any number of arguments" (e.g. CONCAT). */
#define ARGUS_FN_VARIADIC (-1)

/*
 * One ODBC scalar function and how to render it natively.
 *
 * tmpl is a template where:
 *   $1..$9  expand to the nth argument (as written by the application)
 *   $*      expands to every argument, comma-separated
 * Anything else is copied verbatim. This covers plain renames
 * ("upper($1)"), argument reordering ("strpos($2, $1)"), keyword forms with
 * no parentheses ("current_date") and semantic fix-ups.
 */
typedef struct argus_fn_entry {
    const char      *odbc_name;   /* uppercase ODBC name, e.g. "UCASE" */
    argus_fn_group_t group;
    SQLUINTEGER      bit;         /* SQL_FN_STR_UCASE, ... */
    int              min_args;
    int              max_args;    /* ARGUS_FN_VARIADIC for unbounded */
    const char      *tmpl;
} argus_fn_entry_t;

/* How {d '...'} / {t '...'} / {ts '...'} are rendered. */
typedef enum argus_literal_style {
    ARGUS_LIT_ANSI = 0,   /* DATE '..' / TIME '..' / TIMESTAMP '..' */
    ARGUS_LIT_CAST,       /* CAST('..' AS DATE) / ... AS TIMESTAMP)  */
    ARGUS_LIT_STRING      /* '..' — engine coerces strings to temporal types */
} argus_literal_style_t;

typedef struct argus_dialect {
    const char             *name;        /* matches argus_backend_t.name */
    const char             *quote_char;  /* SQL_IDENTIFIER_QUOTE_CHAR */
    argus_literal_style_t   literals;
    bool                    supports_oj; /* {oj ...} / SQL_OJ_CAPABILITIES */
    const argus_fn_entry_t *fn_map;      /* terminated by odbc_name == NULL */
} argus_dialect_t;

/* Look up a dialect by backend name. Never returns NULL: an unknown or absent
 * name yields the conservative ANSI dialect. Deliberately keyed by name rather
 * than by the backend vtable, so it works for backends that were not compiled
 * in (and so unit tests need no live backend). */
const argus_dialect_t *argus_dialect_by_name(const char *backend_name);

/* Dialect for a connection (its backend's, or ANSI if not connected). */
const argus_dialect_t *argus_dialect_for(const argus_dbc_t *dbc);

/* Find a scalar function by ODBC name (case-insensitive). NULL if the dialect
 * cannot express it — the caller must then reject {fn NAME(...)} rather than
 * pass it through, because SQLGetInfo never advertised it. */
const argus_fn_entry_t *argus_dialect_find_fn(const argus_dialect_t *dialect,
                                              const char *odbc_name);

/* The SQLGetInfo bitmap for a group, derived from fn_map. */
SQLUINTEGER argus_dialect_fn_bitmap(const argus_dialect_t *dialect,
                                    argus_fn_group_t group);

/* Iteration over every dialect, for tests and diagnostics. */
size_t argus_dialect_count(void);
const argus_dialect_t *argus_dialect_at(size_t index);

/* ── ODBC escape sequence translation (src/odbc/escape.c) ──────── */

typedef enum argus_escape_result {
    ARGUS_ESCAPE_NONE = 0, /* no escape sequences; *out untouched */
    ARGUS_ESCAPE_OK,       /* *out = translated SQL, caller g_free()s it */
    ARGUS_ESCAPE_ERROR     /* diag holds the reason */
} argus_escape_result_t;

/*
 * Replace ODBC escape sequences in `sql` with `dialect`'s native grammar:
 * {fn ...}, {d/t/ts '...'}, {escape '...'}, {oj ...} and {interval ...}.
 * {call ...} is rejected with HYC00; an unknown escape, or a function this
 * dialect has no mapping for, is rejected with 42000 — never passed through,
 * since SQLGetInfo only ever advertised what fn_map can render.
 *
 * Returning ARGUS_ESCAPE_NONE for escape-free SQL (the common case) lets
 * callers skip the copy entirely.
 */
argus_escape_result_t argus_escape_translate(const argus_dialect_t *dialect,
                                             const char *sql,
                                             char **out,
                                             argus_diag_t *diag);

#endif /* ARGUS_DIALECT_H */
