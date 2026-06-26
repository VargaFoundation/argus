#ifndef ARGUS_FLIGHTSQL_CONVERT_H
#define ARGUS_FLIGHTSQL_CONVERT_H

/*
 * Arrow → ODBC conversion for the Flight SQL backend.
 *
 * This translation unit depends only on plain libarrow (Schema / Array /
 * RecordBatch), not on Flight or Flight SQL, so it can be compiled and unit
 * tested independently of a live Flight SQL endpoint.
 */

#include <memory>
#include <arrow/type_fwd.h>
#include "argus/types.h"

/*
 * unixODBC's <sql.h> (pulled in via argus/types.h) does `#define BOOL int`,
 * which collides with arrow::Type::BOOL and corrupts the Arrow type headers.
 * Drop the macro for the Flight SQL translation units; nothing here relies on
 * it (the ODBC C type is SQL_C_BIT, not BOOL).
 */
#ifdef BOOL
#undef BOOL
#endif

/* Map an Arrow logical type id to an ODBC SQL type. */
SQLSMALLINT flightsql_arrow_to_sql_type(int arrow_type_id);

/* Populate one ODBC column descriptor from an Arrow field. */
void flightsql_field_to_column(const std::shared_ptr<arrow::Field>& field,
                               argus_column_desc_t* col);

/* Fill column descriptors from an Arrow schema (capped at ARGUS_MAX_COLUMNS).
 * Returns the number of columns written. */
int flightsql_schema_to_columns(const std::shared_ptr<arrow::Schema>& schema,
                                argus_column_desc_t* columns);

/* Append every row of a RecordBatch to the row cache as text cells, growing
 * the cache from its current num_rows. Returns 0 on success, -1 on failure. */
int flightsql_append_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                           argus_row_cache_t* cache);

#endif /* ARGUS_FLIGHTSQL_CONVERT_H */
