#ifndef ARGUS_HANDLE_H
#define ARGUS_HANDLE_H

#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>
#include "argus/error.h"
#include "argus/types.h"
#include "argus/backend.h"

/* Handle type signatures for runtime type checking */
#define ARGUS_ENV_SIGNATURE  0x41524745U  /* 'ARGE' */
#define ARGUS_DBC_SIGNATURE  0x41524744U  /* 'ARGD' */
#define ARGUS_STMT_SIGNATURE 0x41524753U  /* 'ARGS' */

/* Environment handle */
typedef struct argus_env {
    unsigned int        signature;
    argus_diag_t        diag;
    SQLINTEGER          odbc_version;   /* SQL_OV_ODBC3, SQL_OV_ODBC3_80 */
    SQLINTEGER          connection_pooling;
} argus_env_t;

/* Connection handle */
struct argus_dbc {
    unsigned int            signature;
    argus_diag_t            diag;
    argus_env_t            *env;
    const argus_backend_t  *backend;
    argus_backend_conn_t    backend_conn;
    bool                    connected;

    /* Connection attributes */
    SQLUINTEGER  login_timeout;
    SQLUINTEGER  connection_timeout;
    SQLUINTEGER  access_mode;   /* SQL_MODE_READ_ONLY / SQL_MODE_READ_WRITE */
    SQLUINTEGER  autocommit;    /* SQL_AUTOCOMMIT_ON / SQL_AUTOCOMMIT_OFF */
    char        *current_catalog;

    /* Connection parameters (parsed from conn string) */
    char        *host;
    int          port;
    char        *username;
    char        *password;
    char        *database;
    char        *auth_mechanism; /* "NOSASL" or "PLAIN" */
    char        *backend_name;  /* "hive", "impala", etc. */
};

/* Statement handle */
struct argus_stmt {
    unsigned int            signature;
    argus_diag_t            diag;
    argus_dbc_t            *dbc;

    /* Query state */
    char                   *query;
    bool                    prepared;
    bool                    executed;

    /* Backend operation handle */
    argus_backend_op_t      op;

    /* Result metadata */
    argus_column_desc_t     columns[ARGUS_MAX_COLUMNS];
    int                     num_cols;
    bool                    metadata_fetched;

    /* Row cache and fetch state */
    argus_row_cache_t       row_cache;
    bool                    fetch_started;
    SQLLEN                  row_count;   /* -1 if unknown */

    /* Column bindings */
    argus_col_binding_t     bindings[ARGUS_MAX_COLUMNS];

    /* Statement attributes */
    SQLULEN                 max_rows;
    SQLULEN                 query_timeout;
    SQLULEN                 row_array_size;
    SQLULEN                *rows_fetched_ptr;
    SQLUSMALLINT           *row_status_ptr;
};

/* Handle validation */
static inline bool argus_valid_env(SQLHANDLE h) {
    return h && ((argus_env_t *)h)->signature == ARGUS_ENV_SIGNATURE;
}
static inline bool argus_valid_dbc(SQLHANDLE h) {
    return h && ((argus_dbc_t *)h)->signature == ARGUS_DBC_SIGNATURE;
}
static inline bool argus_valid_stmt(SQLHANDLE h) {
    return h && ((argus_stmt_t *)h)->signature == ARGUS_STMT_SIGNATURE;
}

/* Handle allocation/deallocation */
SQLRETURN argus_alloc_env(argus_env_t **out);
SQLRETURN argus_alloc_dbc(argus_env_t *env, argus_dbc_t **out);
SQLRETURN argus_alloc_stmt(argus_dbc_t *dbc, argus_stmt_t **out);

SQLRETURN argus_free_env(argus_env_t *env);
SQLRETURN argus_free_dbc(argus_dbc_t *dbc);
SQLRETURN argus_free_stmt(argus_stmt_t *stmt);
void argus_stmt_reset(argus_stmt_t *stmt);

#endif /* ARGUS_HANDLE_H */
