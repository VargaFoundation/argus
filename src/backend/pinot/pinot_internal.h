#ifndef ARGUS_PINOT_INTERNAL_H
#define ARGUS_PINOT_INTERNAL_H

#include <curl/curl.h>
#include <json-glib/json-glib.h>

#include "argus/types.h"
#include "argus/backend.h"

/*
 * Apache Pinot backend.
 *
 * Pinot exposes a synchronous SQL endpoint on the broker: POST /query/sql with
 * {"sql": "..."} returns the whole result in one JSON document
 * (resultTable.dataSchema + resultTable.rows). Table listing comes from the
 * controller's /tables endpoint. Implemented over libcurl + json-glib, like
 * the Trino backend but without async polling.
 */

typedef struct pinot_conn {
    CURL              *curl;
    char              *broker_url;      /* http://host:port (queries) */
    char              *controller_url;  /* http://host:9000 (table listing) */
    char              *user;
    char              *password;        /* optional HTTP Basic */
    struct curl_slist *headers;

    bool               ssl_enabled;
    bool               ssl_verify;
    int                connect_timeout_sec;

    char               last_error[512];
} pinot_conn_t;

typedef struct pinot_op {
    argus_column_desc_t *columns;       /* owned */
    int                  num_cols;
    argus_row_cache_t    cache;         /* fully materialized result */
    bool                 delivered;     /* cache handed to ODBC layer once */
} pinot_op_t;

typedef struct pinot_response {
    char   *data;
    size_t  size;
} pinot_response_t;

/* pinot_types.c */
SQLSMALLINT pinot_type_to_sql_type(const char *pinot_type);
SQLULEN     pinot_type_column_size(SQLSMALLINT sql_type);

#endif /* ARGUS_PINOT_INTERNAL_H */
