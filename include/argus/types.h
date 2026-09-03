#ifndef ARGUS_TYPES_H
#define ARGUS_TYPES_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

/* Maximum column name length */
#define ARGUS_MAX_COLUMN_NAME 256

/* Maximum number of bound columns */
#define ARGUS_MAX_COLUMNS 1024

/* Default fetch batch size */
#define ARGUS_DEFAULT_BATCH_SIZE 1000

/* Default cap on how many rows a static (scrollable) cursor will materialise in
 * memory. A scrollable cursor buffers the whole result set, so an unbounded
 * SELECT would OOM the process; past this cap the driver fails cleanly and tells
 * the application to use a forward-only cursor. Override with MaxScrollRows. */
#define ARGUS_DEFAULT_MAX_SCROLL_ROWS 5000000L

/* Column descriptor - describes a result column */
typedef struct argus_column_desc {
    SQLCHAR      name[ARGUS_MAX_COLUMN_NAME];
    SQLSMALLINT  name_len;
    SQLSMALLINT  sql_type;      /* SQL_INTEGER, SQL_VARCHAR, etc. */
    SQLULEN      column_size;   /* precision / display size */
    SQLSMALLINT  decimal_digits;
    SQLSMALLINT  nullable;      /* SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLABLE_UNKNOWN */
    SQLCHAR      table_name[ARGUS_MAX_COLUMN_NAME];
    SQLCHAR      schema_name[ARGUS_MAX_COLUMN_NAME];
    SQLCHAR      catalog_name[ARGUS_MAX_COLUMN_NAME];
} argus_column_desc_t;

/* Column binding - application's buffer for SQLBindCol */
typedef struct argus_col_binding {
    SQLSMALLINT  target_type;       /* C type the app wants */
    SQLPOINTER   target_value;      /* app buffer pointer */
    SQLLEN       buffer_length;     /* size of app buffer */
    SQLLEN      *str_len_or_ind;    /* output length/indicator */
    bool         bound;             /* whether this column is bound */
} argus_col_binding_t;

/* Native value kind for a cell (zero = none, so calloc'd cells default to the
 * text representation and existing backends are unaffected). */
typedef enum argus_native_kind {
    ARGUS_NATIVE_NONE = 0,  /* value lives in `data` (string) */
    ARGUS_NATIVE_I64,       /* value lives in `native.i64` */
    ARGUS_NATIVE_F64,       /* value lives in `native.f64` */
    ARGUS_NATIVE_BINARY     /* `data` holds data_len raw bytes, not text */
} argus_native_kind_t;

/* A single cell value in our row cache.
 *
 * A cell may carry a native (typed) value to avoid the value->text->value
 * round-trip on numeric columns. When `native_kind` is non-zero, numeric
 * SQLGetData targets read the native value directly; text targets fall back to
 * `data` (or format the native value on demand if `data` is NULL). Backends
 * that only produce text leave `native_kind` at ARGUS_NATIVE_NONE.
 *
 * A binary column's value is the bytes themselves (ARGUS_NATIVE_BINARY):
 * `data` then holds data_len raw bytes, which may include NULs, and no text
 * form. The wire encodings the engines use for those bytes — hex on Hive,
 * Impala and Pinot, base64 on Trino, BigQuery and Avatica, \x-hex on
 * PostgreSQL — are decoded by the backend (argus_cell_decode_hex /
 * argus_cell_decode_base64), never guessed from the content at SQLGetData
 * time. SQL_C_BINARY returns the bytes; character targets get them as hex. */
typedef struct argus_cell {
    char   *data;       /* string representation of the value (owned, may be NULL) */
    size_t  data_len;   /* length of data (not including NUL) */
    bool    is_null;    /* whether this cell is NULL */
    uint8_t native_kind;            /* argus_native_kind_t */
    union {
        int64_t i64;
        double  f64;
    } native;
} argus_cell_t;

/* A row in the row cache.
 *
 * `block` marks a single-allocation row: the cells array and every cell's
 * data payload live in ONE malloc'd block (see argus_row_alloc_block), so the
 * whole row frees with a single free(cells) and a 300k x 9 result costs 300k
 * allocations instead of ~3M. Ownership-transfer semantics (the scroll cache
 * moving rows out of the batch cache) are unchanged — the flag travels with
 * the struct. Rows built the classic way (calloc'd cells + malloc per cell)
 * leave `block` false and keep per-cell frees. */
typedef struct argus_row {
    argus_cell_t *cells;    /* array of num_cols cells */
    bool          block;    /* cells + payloads are one allocation */
} argus_row_t;

/* Allocate a row as one contiguous block: a num_cols cell array (zeroed)
 * followed by payload_bytes of string storage. Returns the payload cursor
 * (NULL on allocation failure); the caller copies cell data sequentially and
 * points cells[i].data into it. Rows built this way MUST NOT have their
 * cell data free()d individually. */
char *argus_row_alloc_block(argus_row_t *row, int num_cols,
                            size_t payload_bytes);

/* Free one row, honouring both layouts. */
void argus_row_free(argus_row_t *row, int num_cols);

/* Turn a text cell holding an encoded binary value into the raw bytes, in
 * place: decoding only shrinks, so the cell's storage (a block row's payload
 * or its own allocation) is reused. On success the cell is ARGUS_NATIVE_BINARY
 * with data_len set to the byte count and a NUL kept after the bytes. Returns
 * 0, or -1 when the text is not valid hex (an optional 0x / \x prefix, then
 * pairs of hex digits) or base64 — the cell is then left as it was, so the
 * application still sees the engine's text rather than nothing. NULL cells
 * and cells that are already binary succeed without change. */
int argus_cell_decode_hex(argus_cell_t *cell);
int argus_cell_decode_base64(argus_cell_t *cell);

/* Row cache - batch of fetched rows */
typedef struct argus_row_cache {
    argus_row_t *rows;          /* array of fetched rows */
    size_t       num_rows;      /* number of rows in cache */
    size_t       capacity;      /* allocated capacity */
    size_t       current_row;   /* current position (0-based) */
    int          num_cols;      /* number of columns */
    bool         exhausted;     /* backend has no more rows */
} argus_row_cache_t;

/* Initialize a row cache */
void argus_row_cache_init(argus_row_cache_t *cache);

/* Free all memory in a row cache */
void argus_row_cache_free(argus_row_cache_t *cache);

/* Clear cache contents but keep allocated memory */
void argus_row_cache_clear(argus_row_cache_t *cache);

/* Maximum number of bound parameters */
#define ARGUS_MAX_PARAMS 256

/* Parameter binding for SQLBindParameter */
typedef struct argus_param_binding {
    SQLSMALLINT  io_type;
    SQLSMALLINT  value_type;
    SQLSMALLINT  param_type;
    SQLULEN      column_size;
    SQLSMALLINT  decimal_digits;
    SQLPOINTER   value;
    SQLLEN       buffer_length;
    SQLLEN      *str_len_or_ind;
    bool         bound;
} argus_param_binding_t;

/* Connection string key-value pair */
typedef struct argus_conn_param {
    char *key;
    char *value;
} argus_conn_param_t;

/* Parsed connection string */
typedef struct argus_conn_params {
    argus_conn_param_t *params;
    int count;
    int capacity;
} argus_conn_params_t;

/* Connection string parsing */
void argus_conn_params_init(argus_conn_params_t *params);
void argus_conn_params_free(argus_conn_params_t *params);
int  argus_conn_params_parse(argus_conn_params_t *params, const char *conn_str);
const char *argus_conn_params_get(const argus_conn_params_t *params, const char *key);

/* Connection-string keys whose value is a credential (PWD, *SECRET, *TOKEN,
 * AuditKey, ...). The one list every redaction in the driver uses. */
bool  argus_connstr_key_is_secret(const char *key);

/* Copy of `conn_str` with every secret value replaced by "***", parsed with
 * the same rules as argus_conn_params_parse (whitespace around keys and
 * values, {braced} values). Returns NULL only for NULL input or OOM; the
 * caller frees the result with free(). */
char *argus_connstr_redact(const char *conn_str);

/* ── ODBC string arguments ───────────────────────────────────── */
/* A length argument of a narrow entry point is a byte count or SQL_NTS.
 * Any other negative value is a caller bug that ODBC reports as HY090
 * ("Invalid string or buffer length"); the entry point checks it before
 * the buffer is read, because cast to size_t it is a multi-gigabyte
 * memcpy. */
static inline bool argus_odbc_len_valid(SQLLEN len)
{
    return len >= 0 || len == SQL_NTS;
}

/* Copy of `str`: `len` bytes, or up to the NUL for SQL_NTS. NULL for a NULL
 * `str`, for a length that argus_odbc_len_valid rejects, and on OOM; an entry
 * point that must tell those apart checks the length first. Caller frees
 * with free(). */
char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len);
char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len);

/* ── SQL text helpers ────────────────────────────────────────── */
/* Shared by the parameter renderer and every backend that builds catalog
 * queries from ODBC search patterns: a value that reaches a query string
 * goes through one of these, never through snprintf("'%s'"). */

/* `value` (len bytes) as a quoted SQL string literal: quotes doubled, and
 * backslashes doubled when the dialect treats them as escapes. NULL when
 * the value carries an embedded NUL byte (which would truncate the query)
 * or on OOM. Caller frees with free(). */
char *argus_sql_quote_literal_n(const char *value, size_t len,
                                bool backslash_escapes);
char *argus_sql_quote_literal(const char *value, bool backslash_escapes);

/* `name` as a delimited identifier: wrapped in `quote` with embedded quote
 * characters doubled. NULL on embedded NUL or OOM. Caller frees with free(). */
char *argus_sql_quote_ident(const char *name, char quote);

#endif /* ARGUS_TYPES_H */
