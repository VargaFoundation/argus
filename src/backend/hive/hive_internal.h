#ifndef ARGUS_HIVE_INTERNAL_H
#define ARGUS_HIVE_INTERNAL_H

#include <glib-object.h>
#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/transport/thrift_socket.h>
#include <thrift/c_glib/transport/thrift_buffered_transport.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>

#include "argus/types.h"
#include "argus/backend.h"

/* Forward declare the generated Thrift types */
#include "gen-c_glib/t_c_l_i_service.h"
#include "gen-c_glib/t_c_l_i_service_types.h"

/* Hive connection state */
typedef struct hive_conn {
    ThriftSocket           *socket;
    ThriftTransport        *transport;
    ThriftProtocol         *protocol;
    TCLIServiceIf          *client;
    TSessionHandle         *session_handle;
    char                   *database;
} hive_conn_t;

/* Hive operation state */
typedef struct hive_operation {
    TOperationHandle       *op_handle;
    bool                    has_result_set;
    bool                    metadata_fetched;
    argus_column_desc_t    *columns;
    int                     num_cols;
} hive_operation_t;

/* Type mapping helper */
SQLSMALLINT hive_type_to_sql_type(const char *hive_type);
SQLULEN     hive_type_column_size(SQLSMALLINT sql_type);
SQLSMALLINT hive_type_decimal_digits(SQLSMALLINT sql_type);

/* Helper to create a new operation */
hive_operation_t *hive_operation_new(void);
void hive_operation_free(hive_operation_t *op);

#endif /* ARGUS_HIVE_INTERNAL_H */
