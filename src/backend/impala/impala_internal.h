#ifndef ARGUS_IMPALA_INTERNAL_H
#define ARGUS_IMPALA_INTERNAL_H

#include <glib-object.h>
#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/transport/thrift_socket.h>
#include <thrift/c_glib/transport/thrift_buffered_transport.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>

/* SSL support (requires OpenSSL) */
#if __has_include(<openssl/ssl.h>)
#include <thrift/c_glib/transport/thrift_ssl_socket.h>
#define ARGUS_HAS_THRIFT_SSL 1
#endif

#include "argus/types.h"
#include "argus/backend.h"

/* Forward declare the generated Thrift types */
#include "gen-c_glib/t_c_l_i_service.h"
#include "gen-c_glib/t_c_l_i_service_types.h"

/* Impala connection state (same Thrift stack as Hive) */
typedef struct impala_conn {
    ThriftSocket           *socket;
    ThriftTransport        *transport;
    ThriftProtocol         *protocol;
    TCLIServiceIf          *client;
    TSessionHandle         *session_handle;
    char                   *database;
} impala_conn_t;

/* Impala operation state */
typedef struct impala_operation {
    TOperationHandle       *op_handle;
    bool                    has_result_set;
    bool                    metadata_fetched;
    argus_column_desc_t    *columns;
    int                     num_cols;
} impala_operation_t;

/* Type mapping helpers */
SQLSMALLINT impala_type_to_sql_type(const char *impala_type);
SQLULEN     impala_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT impala_type_decimal_digits(SQLSMALLINT sql_type);

/* Helper to create/free operations */
impala_operation_t *impala_operation_new(void);
void impala_operation_free(impala_operation_t *op);

/* Query operations */
int impala_cancel(argus_backend_conn_t conn, argus_backend_op_t op);

#endif /* ARGUS_IMPALA_INTERNAL_H */
