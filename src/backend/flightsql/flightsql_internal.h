#ifndef ARGUS_FLIGHTSQL_INTERNAL_H
#define ARGUS_FLIGHTSQL_INTERNAL_H

/*
 * Internal state for the Arrow Flight SQL backend.
 *
 * Reaches any Flight SQL endpoint (Dremio, InfluxDB 3.x, Apache Doris,
 * StarRocks). Implemented in C++ over the Arrow Flight SQL client; the public
 * registry accessor argus_flightsql_backend_get() has C linkage so the C
 * backend registry can reference it.
 */

#include <memory>
#include <string>
#include <vector>

#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>

#include "argus/types.h"

/* A connected Flight SQL session. */
struct flightsql_conn {
    std::unique_ptr<arrow::flight::sql::FlightSqlClient> client;
    arrow::flight::FlightCallOptions call_options;  /* carries auth headers */
    std::string host;
    int         port = 0;
    std::string last_error;   /* message from the most recent failed RPC */
};

/* One executed statement: the FlightInfo plus a lazily-materialized result. */
struct flightsql_op {
    std::unique_ptr<arrow::flight::FlightInfo> info;
    bool                 fetched = false;          /* result drained into cache */
    bool                 metadata_fetched = false;
    argus_column_desc_t* columns = nullptr;
    int                  num_cols = 0;
};

#endif /* ARGUS_FLIGHTSQL_INTERNAL_H */
