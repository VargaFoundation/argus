#ifndef ARGUS_KUDU_INTERNAL_H
#define ARGUS_KUDU_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stdbool.h>
#include "argus/types.h"
#include "argus/backend.h"
#include "kudu_sql_parser.h"

/* Kudu connection state
 * client is an opaque pointer to KuduClient* (C++ object) */
typedef struct kudu_conn {
    void       *client;             /* KuduClient* */
    char       *master_addresses;   /* "host1:7051,host2:7051" */
    char       *database;           /* used as table prefix */
    int         connect_timeout_sec;
    int         query_timeout_sec;
} kudu_conn_t;

/* Kudu operation state
 * scanner and batch are opaque pointers to C++ objects */
typedef struct kudu_operation {
    void       *scanner;            /* KuduScanner* */
    void       *batch;              /* KuduScanBatch* (owned by scanner) */
    bool        has_result_set;
    bool        finished;
    int         batch_offset;       /* current row in batch */
    bool        metadata_fetched;

    /* Cached column metadata */
    argus_column_desc_t *columns;
    int         num_cols;

    /* For synthetic result sets (metadata queries) */
    argus_row_cache_t *synthetic_cache;
    bool        is_synthetic;
} kudu_operation_t;

/* Kudu type mapping (integer type IDs matching Kudu C++ API) */
#define KUDU_TYPE_INT8               0
#define KUDU_TYPE_INT16              1
#define KUDU_TYPE_INT32              2
#define KUDU_TYPE_INT64              3
#define KUDU_TYPE_STRING             4
#define KUDU_TYPE_BOOL               5
#define KUDU_TYPE_FLOAT              6
#define KUDU_TYPE_DOUBLE             7
#define KUDU_TYPE_BINARY             8
#define KUDU_TYPE_UNIXTIME_MICROS    9
#define KUDU_TYPE_DECIMAL            10
#define KUDU_TYPE_VARCHAR            11
#define KUDU_TYPE_DATE               12

/* Type mapping helpers */
SQLSMALLINT kudu_type_to_sql_type(int kudu_type);
SQLSMALLINT kudu_type_name_to_sql_type(const char *type_name);
SQLULEN     kudu_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT kudu_type_decimal_digits(SQLSMALLINT sql_type);
const char *kudu_type_id_to_name(int kudu_type);

/* Helper to create/free operations */
kudu_operation_t *kudu_operation_new(void);
void kudu_operation_free(kudu_operation_t *op);

/*
 * C++ implementation functions (defined in .cpp files, exposed with C linkage).
 * These wrap the Kudu C++ client API.
 */
int kudu_cpp_client_create(const char *master_addresses,
                           int timeout_sec,
                           void **out_client);
void kudu_cpp_client_destroy(void *client);

int kudu_cpp_execute_scan(void *client, const kudu_parsed_query_t *query,
                          const char *table_prefix, int timeout_sec,
                          kudu_operation_t *op);
int kudu_cpp_fetch_batch(kudu_operation_t *op,
                         argus_row_cache_t *cache, int max_rows);
void kudu_cpp_close_scanner(kudu_operation_t *op);

int kudu_cpp_list_tables(void *client, const char *table_prefix,
                         const char *filter, char ***out_tables,
                         int *out_count);
int kudu_cpp_get_table_schema(void *client, const char *table_name,
                              argus_column_desc_t *columns, int *num_cols,
                              int **out_kudu_types);

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_KUDU_INTERNAL_H */
