#ifndef ARGUS_DRUID_INTERNAL_H
#define ARGUS_DRUID_INTERNAL_H

#include <curl/curl.h>
#include <json-glib/json-glib.h>

#include "argus/types.h"
#include "argus/backend.h"

/*
 * Apache Druid backend.
 *
 * Druid exposes a synchronous SQL endpoint on the broker/router:
 * POST /druid/v2/sql with {"query": "...", "resultFormat":"array",
 * "header":true, "sqlTypesHeader":true} returns a JSON array whose first row is
 * the column names, second row the SQL types, and the rest the data. Druid also
 * exposes a full INFORMATION_SCHEMA, so catalog operations are plain SQL (like
 * the Trino backend). Implemented over libcurl + json-glib.
 */

typedef struct druid_conn {
    CURL              *curl;
    char              *base_url;        /* http://host:port */
    char              *user;
    char              *password;        /* optional HTTP Basic */
    struct curl_slist *headers;

    bool               ssl_enabled;
    bool               ssl_verify;
    int                connect_timeout_sec;

    char               last_error[512];
} druid_conn_t;

typedef struct druid_op {
    argus_column_desc_t *columns;
    int                  num_cols;
    argus_row_cache_t    cache;
    bool                 delivered;
} druid_op_t;

typedef struct druid_response {
    char   *data;
    size_t  size;
    long    http_code;
} druid_response_t;

/* druid_types.c */
SQLSMALLINT druid_type_to_sql_type(const char *druid_sql_type);
SQLULEN     druid_type_column_size(SQLSMALLINT sql_type);

/* druid_backend.c (shared by the catalog helpers) */
int druid_execute(argus_backend_conn_t conn, const char *query,
                  argus_backend_op_t *out_op);

#endif /* ARGUS_DRUID_INTERNAL_H */
