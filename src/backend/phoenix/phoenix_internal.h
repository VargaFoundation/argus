#ifndef ARGUS_PHOENIX_INTERNAL_H
#define ARGUS_PHOENIX_INTERNAL_H

#include <curl/curl.h>
#include <json-glib/json-glib.h>

#include "argus/types.h"
#include "argus/backend.h"

/* Phoenix Query Server connection state (Avatica protocol) */
typedef struct phoenix_conn {
    CURL               *curl;
    char               *base_url;        /* e.g. "http://host:8765" */
    char               *connection_id;   /* UUID from openConnection */
    char               *user;
    char               *database;        /* schema for Phoenix/HBase */
    struct curl_slist   *default_headers;
    int                 next_statement_id;

    /* SSL/TLS settings (from DBC) */
    bool                ssl_enabled;
    char               *ssl_cert_file;
    char               *ssl_key_file;
    char               *ssl_ca_file;
    bool                ssl_verify;

    /* Timeout settings */
    int                 connect_timeout_sec;
    int                 query_timeout_sec;
} phoenix_conn_t;

/* Phoenix operation state */
typedef struct phoenix_operation {
    int                 statement_id;
    char               *connection_id;
    bool                has_result_set;
    bool                metadata_fetched;
    bool                finished;
    int                 offset;          /* fetch offset for Avatica fetch */

    /* Cached column metadata */
    argus_column_desc_t *columns;
    int                  num_cols;
} phoenix_operation_t;

/* Type mapping helpers */
SQLSMALLINT phoenix_type_to_sql_type(const char *phoenix_type);
SQLULEN     phoenix_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT phoenix_type_decimal_digits(SQLSMALLINT sql_type);

/* Helper to create/free operations */
phoenix_operation_t *phoenix_operation_new(void);
void phoenix_operation_free(phoenix_operation_t *op);

/* CURL response buffer */
typedef struct phoenix_response {
    char   *data;
    size_t  size;
} phoenix_response_t;

/* CURL write callback */
size_t phoenix_curl_write_cb(void *contents, size_t size, size_t nmemb,
                              void *userp);

/* HTTP request helpers */
int phoenix_http_post(phoenix_conn_t *conn, const char *url,
                      const char *body, phoenix_response_t *resp);

/* Avatica RPC helper: send a JSON request and get parsed response */
int phoenix_avatica_request(phoenix_conn_t *conn, const char *request_type,
                            JsonBuilder *params, JsonParser **out_parser);

/* Query operations */
int phoenix_cancel(argus_backend_conn_t conn, argus_backend_op_t op);

/* Parse column metadata from Avatica signature */
int phoenix_parse_columns(JsonObject *signature,
                          argus_column_desc_t *columns,
                          int *num_cols);

/* Parse data rows from Avatica frame into row cache */
int phoenix_parse_frame(JsonObject *frame,
                        argus_row_cache_t *cache,
                        int num_cols);

#endif /* ARGUS_PHOENIX_INTERNAL_H */
