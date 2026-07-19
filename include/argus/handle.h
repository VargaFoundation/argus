#ifndef ARGUS_HANDLE_H
#define ARGUS_HANDLE_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>
#include <glib.h>
#include "argus/error.h"
#include "argus/types.h"
#include "argus/backend.h"

/* Driver-specific attribute IDs for metrics (base > 65536 to avoid ODBC range) */
#define ARGUS_ATTR_CONNECT_TIME_MS  65537
#define ARGUS_ATTR_EXECUTE_TIME_MS  65538
#define ARGUS_ATTR_ROWS_FETCHED     65539
#define ARGUS_ATTR_ERRORS_TOTAL     65540

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
    GMutex                  mutex;        /* thread safety */
    const argus_backend_t  *backend;
    argus_backend_conn_t    backend_conn;
    bool                    connected;
    bool                    pooled;     /* true if connection came from pool */

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

    /* Kerberos SPN overrides (Hive/Impala GSSAPI/SSPI). Default: service is
     * the backend name ("hive"/"impala") and the host is the connection HOST;
     * realm comes from krb5.conf. These let the SPN differ from the connection
     * host (load balancers) or pin a cross-realm principal. */
    char        *krb_service_name; /* overrides "hive"/"impala" */
    char        *krb_host_fqdn;    /* overrides HOST in the SPN */
    char        *krb_realm;        /* explicit realm → service/host@REALM */
    char        *backend_name;  /* "hive", "impala", etc. */

    /* SSL/TLS settings */
    bool         ssl_enabled;
    char        *ssl_cert_file;
    char        *ssl_key_file;
    char        *ssl_ca_file;
    bool         ssl_verify;

    /* Additional connection parameters */
    char        *app_name;
    int          fetch_buffer_size;
    long         max_scroll_rows;  /* cap for static-cursor materialization
                                    * (0 = ARGUS_DEFAULT_MAX_SCROLL_ROWS) */
    int          retry_count;
    int          retry_delay_sec;
    int          socket_timeout_sec;
    int          connect_timeout_sec;
    int          query_timeout_sec;
    char        *http_path;
    int          trino_protocol_version;  /* 1 = v1 (default), 2 = v2 spooling */
    int          log_level;
    char        *log_file;

    /* OAuth2 client-credentials (M2M) — used by Trino when AuthMech=OAUTH2 */
    char        *oauth_token_url;     /* IdP token endpoint */
    char        *oauth_client_id;
    char        *oauth_client_secret;
    char        *oauth_scope;         /* optional */
    char        *oauth_device_url;    /* device authorization endpoint (DEVICE_CODE) */
    char        *oauth_auth_url;      /* authorization endpoint (AUTH_CODE browser SSO) */
    char        *oauth_issuer;        /* OIDC issuer for .well-known discovery */

    /* BigQuery (BACKEND=bigquery). Every Google URL is overridable so the
     * driver works on sovereign clouds (S3NS) and against the emulator. */
    char        *bq_project;          /* GCP project id (required) */
    char        *bq_location;         /* job location, e.g. EU (optional) */
    char        *bq_endpoint;         /* API base URL override */
    char        *bq_token_url;        /* OAuth2 token endpoint override */
    char        *bq_audience;         /* JWT aud claim override */
    char        *bq_scope;            /* OAuth2 scope override */
    char        *bq_key_file;         /* service-account JSON key path */
    char        *bq_access_token;     /* pre-fetched bearer token */

    /* SQLBrowseConnect accumulated keywords */
    char        *browse_buf;

    /* Metadata cache (GHashTable*, lazily initialized) */
    void        *metadata_cache;

    /* Metrics */
    double       connect_time_ms;       /* last connect duration */
    unsigned long errors_total;         /* total error count */
};

/* Async execution states */
typedef enum {
    ARGUS_ASYNC_IDLE       = 0,
    ARGUS_ASYNC_SUBMITTED  = 1,
    ARGUS_ASYNC_RUNNING    = 2,
    ARGUS_ASYNC_DONE       = 3,
    ARGUS_ASYNC_ERROR      = 4
} argus_async_state_t;

/* Data-at-execution states */
typedef enum {
    ARGUS_DAE_IDLE       = 0,
    ARGUS_DAE_NEED_DATA  = 1,
    ARGUS_DAE_PUTTING    = 2
} argus_dae_state_t;

/* ── Descriptors ──────────────────────────────────────────────────
 * ODBC requires the four descriptors (ARD/APD/IRD/IPD) to be real, distinct
 * handles that SQLAllocHandle(SQL_HANDLE_DESC) can also allocate explicitly.
 * A descriptor is a thin object: an implicit one is a typed view onto its
 * statement's data, an explicit one carries its own binding-record array. */
#define ARGUS_DESC_SIGNATURE 0x41524758U  /* 'ARGX' */

typedef enum {
    ARGUS_DESC_ARD = 0,  /* Application Row Descriptor      */
    ARGUS_DESC_APD,      /* Application Parameter Descriptor */
    ARGUS_DESC_IRD,      /* Implementation Row Descriptor    */
    ARGUS_DESC_IPD       /* Implementation Parameter Descriptor */
} argus_desc_type_t;

typedef struct argus_desc {
    unsigned int         signature;
    argus_desc_type_t    type;
    bool                 is_explicit;   /* allocated via SQLAllocHandle */
    argus_stmt_t        *stmt;          /* implicit: owner; explicit: associated stmt or NULL */
    argus_dbc_t         *dbc;           /* explicit: owning connection */
    /* Explicit descriptors carry their own records; implicit ones leave these
     * NULL and route through the statement. */
    argus_col_binding_t *records;
    int                  record_capacity;
    argus_diag_t         diag;
} argus_desc_t;

static inline bool argus_valid_desc(SQLHANDLE h) {
    return h && ((argus_desc_t *)h)->signature == ARGUS_DESC_SIGNATURE;
}

/* Statement handle */
struct argus_stmt {
    unsigned int            signature;
    argus_diag_t            diag;
    argus_dbc_t            *dbc;
    GMutex                  mutex;        /* thread safety */

    /* Query state */
    char                   *query;
    bool                    prepared;
    bool                    executed;

    /* Backend operation handle */
    argus_backend_op_t      op;

    /* Result metadata (dynamically allocated) */
    argus_column_desc_t    *columns;
    int                     num_cols;
    int                     columns_capacity;
    bool                    metadata_fetched;

    /* Row cache and fetch state */
    argus_row_cache_t       row_cache;
    bool                    fetch_started;
    SQLLEN                  row_count;   /* -1 if unknown */

    /* Column bindings. `bindings` is the ACTIVE application row descriptor's
     * record array — normally the implicit one below, but swapped to point at
     * an explicitly-associated ARD's records by SQLSetStmtAttr. The whole fetch
     * path reads `bindings`, so association just re-points it. */
    argus_col_binding_t    *bindings;
    int                     bindings_capacity;
    /* The implicit ARD's own array, owned by the stmt and freed with it. Equals
     * `bindings` unless an explicit ARD is associated. */
    argus_col_binding_t    *implicit_bindings;
    int                     implicit_bindings_capacity;

    /* The four descriptors, embedded so their handles are stable and distinct.
     * active_ard is &desc_ard by default, or an explicit descriptor once one is
     * associated as the application row descriptor. */
    argus_desc_t            desc_ard;
    argus_desc_t            desc_apd;
    argus_desc_t            desc_ird;
    argus_desc_t            desc_ipd;
    argus_desc_t           *active_ard;

    /* Parameter bindings */
    argus_param_binding_t   param_bindings[ARGUS_MAX_PARAMS];
    int                     num_param_bindings;

    /* SQLGetData multi-call state */
    SQLUSMALLINT            getdata_col;     /* 1-based column of last GetData, 0=none */
    size_t                  getdata_offset;  /* byte offset for next GetData call */

    /* Statement attributes */
    SQLULEN                 max_rows;
    SQLULEN                 query_timeout;
    SQLULEN                 row_array_size;
    SQLULEN                 row_bind_type;   /* SQL_BIND_BY_COLUMN (default), or
                                              * the size of the application's row
                                              * struct for row-wise binding */
    SQLULEN                *rows_fetched_ptr;
    SQLUSMALLINT           *row_status_ptr;
    SQLULEN                 metadata_id;     /* SQL_TRUE or SQL_FALSE */
    SQLULEN                 use_bookmarks;   /* SQL_UB_OFF (default) */
    SQLULEN                 noscan;          /* SQL_NOSCAN_OFF (default): the
                                              * driver scans for and translates
                                              * ODBC escape sequences */

    /* Async execution state */
    bool                    async_enabled;
    argus_async_state_t     async_state;
    char                   *async_query;    /* query saved for async polling */

    /* Data-at-execution state */
    argus_dae_state_t       dae_state;
    int                     dae_current_param;  /* 0-based index */
    GByteArray             *dae_buffer;         /* accumulated PutData bytes */

    /* Cursor type for scrollable cursors */
    SQLULEN                 cursor_type;        /* SQL_CURSOR_FORWARD_ONLY or SQL_CURSOR_STATIC */
    size_t                  scroll_position;    /* absolute position in full cache */
    size_t                  scroll_rowset_start; /* abs. start of current rowset (for SQLSetPos) */
    argus_row_t            *scroll_rows;        /* full result set cache */
    size_t                  scroll_row_count;   /* number of rows in scroll cache */
    bool                    scroll_cached;      /* true if full cache built */

    /* Batch parameter attributes */
    SQLULEN                 paramset_size;       /* SQL_ATTR_PARAMSET_SIZE (default 1) */
    SQLULEN                 param_bind_type;     /* SQL_ATTR_PARAM_BIND_TYPE */
    SQLULEN                *params_processed_ptr; /* SQL_ATTR_PARAMS_PROCESSED_PTR */
    SQLUSMALLINT           *param_status_ptr;    /* SQL_ATTR_PARAM_STATUS_PTR */

    /* Metrics */
    double                  execute_time_ms;    /* last execute duration */
    unsigned long           rows_fetched_total; /* cumulative rows fetched */
    unsigned long           errors_total;       /* total errors on this stmt */
};

/* Handle locking macros for thread safety */
#define ARGUS_DBC_LOCK(dbc)    g_mutex_lock(&(dbc)->mutex)
#define ARGUS_DBC_UNLOCK(dbc)  g_mutex_unlock(&(dbc)->mutex)
#define ARGUS_STMT_LOCK(stmt)  g_mutex_lock(&(stmt)->mutex)
#define ARGUS_STMT_UNLOCK(stmt) g_mutex_unlock(&(stmt)->mutex)

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

/* Ensure stmt has room for at least ncols columns/bindings */
int argus_stmt_ensure_columns(argus_stmt_t *stmt, int ncols);
int argus_stmt_ensure_bindings(argus_stmt_t *stmt, int ncols);

SQLRETURN argus_free_env(argus_env_t *env);
SQLRETURN argus_free_dbc(argus_dbc_t *dbc);
SQLRETURN argus_free_stmt(argus_stmt_t *stmt);
void argus_stmt_reset(argus_stmt_t *stmt);

/* Explicit descriptor handles (SQLAllocHandle/SQLFreeHandle SQL_HANDLE_DESC). */
SQLRETURN argus_alloc_desc(argus_dbc_t *dbc, argus_desc_t **out);
SQLRETURN argus_free_desc(argus_desc_t *desc);

/* The statement a descriptor operates on (its owner for implicit ones, its
 * associated stmt for explicit ones), or NULL if an explicit descriptor is not
 * associated. Descriptor API entry points accept a real descriptor handle or,
 * for Driver-Manager backward compatibility, a raw statement handle. */
argus_stmt_t *argus_desc_stmt(SQLHANDLE handle);

/* Connection pool */
argus_backend_conn_t argus_pool_acquire(
    const char *host, int port,
    const char *backend_name,
    const char *username,
    const argus_backend_t **out_backend);

void argus_pool_release(
    const char *host, int port,
    const char *backend_name,
    const char *username,
    const argus_backend_t *backend,
    argus_backend_conn_t conn);

void argus_pool_cleanup(void);
void argus_pool_evict_idle(int max_idle_sec);
void argus_pool_configure(int max_per_key, int max_total,
                           int idle_timeout_sec, int ttl_sec);
void argus_pool_get_config(int *max_per_key, int *max_total,
                            int *idle_timeout_sec, int *ttl_sec);

/* Metadata cache */
void argus_metadata_cache_init(argus_dbc_t *dbc);
void argus_metadata_cache_free(argus_dbc_t *dbc);
void argus_metadata_cache_clear(argus_dbc_t *dbc);
bool argus_metadata_cache_lookup(argus_dbc_t *dbc, argus_stmt_t *stmt,
                                  const char *func,
                                  const char *a1, const char *a2,
                                  const char *a3, const char *a4);
void argus_metadata_cache_store(argus_dbc_t *dbc, argus_stmt_t *stmt,
                                 const char *func,
                                 const char *a1, const char *a2,
                                 const char *a3, const char *a4);

#endif /* ARGUS_HANDLE_H */
