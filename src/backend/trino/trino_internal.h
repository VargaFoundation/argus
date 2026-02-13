#ifndef ARGUS_TRINO_INTERNAL_H
#define ARGUS_TRINO_INTERNAL_H

#include <curl/curl.h>
#include <json-glib/json-glib.h>

#include "argus/types.h"
#include "argus/backend.h"

/* Trino connection state */
typedef struct trino_conn {
    CURL               *curl;
    char               *base_url;       /* e.g. "http://host:port" or "https://..." */
    char               *user;
    char               *catalog;
    char               *schema;
    struct curl_slist   *default_headers;

    /* SSL/TLS settings (from DBC) */
    bool                ssl_enabled;
    char               *ssl_cert_file;
    char               *ssl_key_file;
    char               *ssl_ca_file;
    bool                ssl_verify;

    /* Timeout settings */
    int                 connect_timeout_sec;
    int                 query_timeout_sec;
} trino_conn_t;

/* Trino operation state */
typedef struct trino_operation {
    char               *query_id;
    char               *next_uri;       /* URL to poll for next batch */
    bool                has_result_set;
    bool                metadata_fetched;
    bool                finished;

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

#endif /* ARGUS_TRINO_INTERNAL_H */
