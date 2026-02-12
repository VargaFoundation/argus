#ifndef ARGUS_TYPES_H
#define ARGUS_TYPES_H

#include <sql.h>
#include <sqlext.h>
#include <stddef.h>
#include <stdbool.h>

/* Maximum column name length */
#define ARGUS_MAX_COLUMN_NAME 256

/* Maximum number of bound columns */
#define ARGUS_MAX_COLUMNS 1024

/* Default fetch batch size */
#define ARGUS_DEFAULT_BATCH_SIZE 1000

/* Column descriptor - describes a result column */
typedef struct argus_column_desc {
    SQLCHAR      name[ARGUS_MAX_COLUMN_NAME];
    SQLSMALLINT  name_len;
    SQLSMALLINT  sql_type;      /* SQL_INTEGER, SQL_VARCHAR, etc. */
    SQLULEN      column_size;   /* precision / display size */
    SQLSMALLINT  decimal_digits;
    SQLSMALLINT  nullable;      /* SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLABLE_UNKNOWN */
} argus_column_desc_t;

/* Column binding - application's buffer for SQLBindCol */
typedef struct argus_col_binding {
    SQLSMALLINT  target_type;       /* C type the app wants */
    SQLPOINTER   target_value;      /* app buffer pointer */
    SQLLEN       buffer_length;     /* size of app buffer */
    SQLLEN      *str_len_or_ind;    /* output length/indicator */
    bool         bound;             /* whether this column is bound */
} argus_col_binding_t;

/* A single cell value in our row cache */
typedef struct argus_cell {
    char   *data;       /* string representation of the value (owned) */
    size_t  data_len;   /* length of data (not including NUL) */
    bool    is_null;    /* whether this cell is NULL */
} argus_cell_t;

/* A row in the row cache */
typedef struct argus_row {
    argus_cell_t *cells;    /* array of num_cols cells */
} argus_row_t;

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

/* Parameter binding for SQLBindParameter (future use) */
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

#endif /* ARGUS_TYPES_H */
