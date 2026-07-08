#ifndef ARGUS_BIGQUERY_INTERNAL_H
#define ARGUS_BIGQUERY_INTERNAL_H

#include <curl/curl.h>
#include <json-glib/json-glib.h>
#include <stdbool.h>
#include <time.h>

#include "argus/types.h"
#include "argus/backend.h"

/*
 * Google BigQuery backend (REST bigquery/v2 over libcurl + json-glib).
 *
 * Every Google endpoint is configurable so that the driver also works
 * against sovereign-cloud deployments (e.g. S3NS, the GCP offer operated
 * by Thales) and against the BigQuery emulator, where the API base URL,
 * the OAuth token endpoint and the JWT audience all differ from
 * public GCP:
 *
 *   BQEndpoint      API base   (default https://bigquery.googleapis.com)
 *   BQTokenEndpoint OAuth2 token URL (default https://oauth2.googleapis.com/token,
 *                   or the token_uri of the service-account key file)
 *   BQAudience      JWT "aud" claim  (default = token endpoint)
 *   BQScope         OAuth2 scope (default https://www.googleapis.com/auth/bigquery)
 *
 * Authentication, in order of precedence:
 *   1. AccessToken=...   pre-fetched bearer token (also: no auth if empty,
 *                        which is what the emulator wants)
 *   2. BQKeyFile=...     service-account JSON key: RS256 JWT-bearer grant
 *                        against the (configurable) token endpoint. Requires
 *                        OpenSSL (ARGUS_HAS_OPENSSL).
 *
 * Queries run through jobs.query + getQueryResults with pageToken
 * pagination; results stream page by page into the row cache.
 */

typedef struct bq_conn {
    CURL              *curl;
    char              *base_url;      /* API base, no trailing slash */
    char              *project;       /* GCP project id */
    char              *dataset;       /* default dataset (optional) */
    char              *location;      /* job location (optional) */

    /* auth */
    char              *access_token;  /* current bearer (owned) */
    time_t             token_expiry;  /* 0 = static token, never refresh */
    char              *token_url;     /* OAuth2 token endpoint */
    char              *audience;      /* JWT aud claim */
    char              *scope;         /* OAuth2 scope */
    char              *sa_email;      /* service-account client_email */
    char              *sa_private_key;/* PEM private key from the key file */

    struct curl_slist *headers;       /* Content-Type/Accept + Authorization */

    bool               ssl_verify;
    char              *ssl_ca_file;    /* private CA bundle (S3NS et al.) */
    char              *ssl_cert_file;  /* client cert for mTLS (optional) */
    char              *ssl_key_file;   /* client key for mTLS (optional) */
    int                connect_timeout_sec;
    int                query_timeout_sec;
    int                fetch_buffer_size;

    char               last_error[1024];
} bq_conn_t;

typedef struct bq_op {
    argus_column_desc_t *columns;     /* owned */
    int                  num_cols;
    argus_row_cache_t    cache;       /* current, not-yet-delivered page */
    bool                 page_ready;  /* cache holds an undelivered page */

    char                *job_id;      /* for getQueryResults pagination */
    char                *location;
    char                *page_token;  /* next page, NULL when done */
} bq_op_t;

typedef struct bq_response {
    char   *data;
    size_t  size;
} bq_response_t;

/* bigquery_backend.c */
int bq_http(bq_conn_t *conn, const char *url, const char *post_body,
            bq_response_t *resp, long *http_code);
void bq_apply_tls(bq_conn_t *conn, CURL *curl);

/* bigquery_auth.c */
int  bq_auth_ensure(bq_conn_t *conn);   /* refresh bearer if needed */
void bq_auth_set_headers(bq_conn_t *conn);

/* bigquery_types.c */
SQLSMALLINT bq_type_to_sql_type(const char *bq_type);
SQLULEN     bq_type_column_size(const char *bq_type);
SQLSMALLINT bq_type_decimal_digits(const char *bq_type);
/* Convert a raw REST cell value ("v") for the given BigQuery type into the
 * cell (native i64/f64 for integers/floats, formatted text for timestamps,
 * plain text otherwise). */
void bq_fill_cell(argus_cell_t *cell, const char *bq_type, const char *value);

#endif /* ARGUS_BIGQUERY_INTERNAL_H */
