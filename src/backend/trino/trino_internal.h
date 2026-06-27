#ifndef ARGUS_TRINO_INTERNAL_H
#define ARGUS_TRINO_INTERNAL_H

#include <curl/curl.h>
#include <json-glib/json-glib.h>

#include "argus/types.h"
#include "argus/backend.h"

/* Authentication mode (derived from AuthMech + credentials) */
typedef enum {
    TRINO_AUTH_NONE = 0,    /* X-Trino-User only (no credentials) */
    TRINO_AUTH_BASIC,       /* HTTP Basic (LDAP / password) — requires TLS */
    TRINO_AUTH_BEARER,      /* JWT / OAuth2 access token in Authorization header */
    TRINO_AUTH_NEGOTIATE    /* Kerberos / SPNEGO via libcurl Negotiate */
} trino_auth_mode_t;

/* Trino connection state */
typedef struct trino_conn {
    CURL               *curl;
    char               *base_url;       /* e.g. "http://host:port" or "https://..." */
    char               *user;
    char               *password;       /* Basic password or Bearer token (NULL if none) */
    trino_auth_mode_t   auth_mode;
    char               *catalog;
    char               *schema;
    char               *app_name;       /* X-Trino-Source (NULL if unset) */
    struct curl_slist   *default_headers;

    /* OAuth2 client-credentials (M2M) params, retained so the access token can
     * be transparently re-fetched when the server returns 401 (token expiry). */
    bool                oauth_m2m;
    char               *oauth_token_url;
    char               *oauth_client_id;
    char               *oauth_client_secret;
    char               *oauth_scope;

    /* SSL/TLS settings (from DBC) */
    bool                ssl_enabled;
    char               *ssl_cert_file;
    char               *ssl_key_file;
    char               *ssl_ca_file;
    bool                ssl_verify;

    /* Timeout settings */
    int                 connect_timeout_sec;
    int                 query_timeout_sec;

    /* Protocol version: 1 = v1, 2 = v2 spooling */
    int                 protocol_version;

    /* Message of the most recent server query error (empty if none). Trino
     * runs queries asynchronously, so the error appears while polling. */
    char                last_error[512];
} trino_conn_t;

/* Trino operation state */
typedef struct trino_operation {
    char               *query_id;
    char               *next_uri;       /* URL to poll for next batch */
    bool                has_result_set;
    bool                metadata_fetched;
    bool                finished;
    bool                spooling_active;  /* true if server responded with v2 data format */

    /* Cached column metadata */
    argus_column_desc_t *columns;
    int                  num_cols;
} trino_operation_t;

/* Type mapping helpers */
SQLSMALLINT trino_type_to_sql_type(const char *trino_type);
SQLULEN     trino_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT trino_type_decimal_digits(SQLSMALLINT sql_type);

/* Helper to create/free operations */
trino_operation_t *trino_operation_new(void);
void trino_operation_free(trino_operation_t *op);

/* Store a server error message (from a response's "error" member) on the
 * connection so the ODBC layer can surface it; clears it when absent. */
void trino_capture_error(trino_conn_t *conn, JsonObject *obj);

/* CURL response buffer */
typedef struct trino_response {
    char   *data;
    size_t  size;
} trino_response_t;

/* CURL write callback */
size_t trino_curl_write_cb(void *contents, size_t size, size_t nmemb, void *userp);

/* HTTP request helpers */
int trino_http_post(trino_conn_t *conn, const char *url, const char *body,
                    trino_response_t *resp);
int trino_http_get(trino_conn_t *conn, const char *url,
                   trino_response_t *resp);
int trino_http_delete(trino_conn_t *conn, const char *url);

/* Query operations */
int trino_cancel(argus_backend_conn_t conn, argus_backend_op_t op);

/* Parse column metadata from Trino JSON response */
int trino_parse_columns(JsonNode *columns_node,
                        argus_column_desc_t *columns,
                        int *num_cols);

/* Parse data rows from Trino JSON response into row cache */
int trino_parse_data(JsonNode *data_node,
                     argus_row_cache_t *cache,
                     int num_cols);

/* v2 spooling: parse segments from data object */
int trino_parse_spooled_data(trino_conn_t *conn, JsonObject *data_obj,
                             argus_row_cache_t *cache, int num_cols);

/* v2 spooling: fetch a spooled segment by URI */
int trino_fetch_segment(trino_conn_t *conn, const char *uri,
                        trino_response_t *resp);

/* v2 spooling: acknowledge a spooled segment */
void trino_ack_segment(trino_conn_t *conn, const char *ack_uri);

/* Base64 decode helper */
unsigned char *trino_base64_decode(const char *input, size_t *out_len);

#endif /* ARGUS_TRINO_INTERNAL_H */
