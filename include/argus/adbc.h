/*
 * Minimal ADBC + Arrow C Data Interface subset for the Argus ADBC driver.
 *
 * This vendors only the structures Argus implements today (Apache Arrow ADBC is
 * Apache-2.0). The Arrow C Data Interface (ArrowSchema/ArrowArray/
 * ArrowArrayStream) is a stable, dependency-free ABI, so a producer needs no
 * Arrow library to emit it. See https://arrow.apache.org/docs/format/
 * {CDataInterface,CStreamInterface}.html and the ADBC spec.
 */
#ifndef ARGUS_ADBC_H
#define ARGUS_ADBC_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Arrow C Data Interface ──────────────────────────────────── */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_NULLABLE 2

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t     flags;
    int64_t     n_children;
    struct ArrowSchema** children;
    struct ArrowSchema*  dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray*  dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/* ── ADBC (subset) ───────────────────────────────────────────── */

typedef uint8_t AdbcStatusCode;
#define ADBC_STATUS_OK              0
#define ADBC_STATUS_UNKNOWN         1
#define ADBC_STATUS_NOT_IMPLEMENTED 2
#define ADBC_STATUS_INVALID_STATE   5
#define ADBC_STATUS_INVALID_ARGUMENT 6
#define ADBC_STATUS_IO             10

struct AdbcError {
    char*   message;
    int32_t vendor_code;
    char    sqlstate[5];
    void (*release)(struct AdbcError* error);
};

struct AdbcDatabase {
    void* private_data;
    void* private_driver;
};

struct AdbcConnection {
    void* private_data;
    void* private_driver;
};

struct AdbcStatement {
    void* private_data;
    void* private_driver;
};

/* Database */
AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error);
AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error);
AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error);
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database, struct AdbcError* error);

/* Connection */
AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection, struct AdbcError* error);
AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database, struct AdbcError* error);
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection, struct AdbcError* error);

/* Statement */
AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement, struct AdbcError* error);
AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error);
AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected, struct AdbcError* error);
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement, struct AdbcError* error);

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement, struct ArrowArray* values,
                                 struct ArrowSchema* schema, struct AdbcError* error);
AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement, struct AdbcError* error);

/* Connection metadata */
AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name, struct ArrowSchema* schema,
                                            struct AdbcError* error);
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* out, struct AdbcError* error);
AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name, struct ArrowArrayStream* out,
                                        struct AdbcError* error);

/* ── Driver-manager entry point (ADBC 1.0.0 vtable) ──────────── */

#define ADBC_VERSION_1_0_0 1000000

struct AdbcPartitions {
    size_t          num_partitions;
    const uint8_t** partitions;
    const size_t*   partition_lengths;
    void*           private_data;
    void (*release)(struct AdbcPartitions* partitions);
};

/* The field order is the stable ADBC 1.0.0 ABI; the driver manager copies it
 * field-by-field, so it must match the upstream layout exactly. */
struct AdbcDriver {
    void* private_data;
    void* private_manager;
    AdbcStatusCode (*release)(struct AdbcDriver* driver, struct AdbcError* error);

    AdbcStatusCode (*DatabaseInit)(struct AdbcDatabase*, struct AdbcError*);
    AdbcStatusCode (*DatabaseNew)(struct AdbcDatabase*, struct AdbcError*);
    AdbcStatusCode (*DatabaseSetOption)(struct AdbcDatabase*, const char*, const char*, struct AdbcError*);
    AdbcStatusCode (*DatabaseRelease)(struct AdbcDatabase*, struct AdbcError*);

    AdbcStatusCode (*ConnectionCommit)(struct AdbcConnection*, struct AdbcError*);
    AdbcStatusCode (*ConnectionGetInfo)(struct AdbcConnection*, uint32_t*, size_t, struct ArrowArrayStream*, struct AdbcError*);
    AdbcStatusCode (*ConnectionGetObjects)(struct AdbcConnection*, int, const char*, const char*, const char*, const char**, const char*, struct ArrowArrayStream*, struct AdbcError*);
    AdbcStatusCode (*ConnectionGetTableSchema)(struct AdbcConnection*, const char*, const char*, const char*, struct ArrowSchema*, struct AdbcError*);
    AdbcStatusCode (*ConnectionGetTableTypes)(struct AdbcConnection*, struct ArrowArrayStream*, struct AdbcError*);
    AdbcStatusCode (*ConnectionInit)(struct AdbcConnection*, struct AdbcDatabase*, struct AdbcError*);
    AdbcStatusCode (*ConnectionNew)(struct AdbcConnection*, struct AdbcError*);
    AdbcStatusCode (*ConnectionSetOption)(struct AdbcConnection*, const char*, const char*, struct AdbcError*);
    AdbcStatusCode (*ConnectionReadPartition)(struct AdbcConnection*, const uint8_t*, size_t, struct ArrowArrayStream*, struct AdbcError*);
    AdbcStatusCode (*ConnectionRelease)(struct AdbcConnection*, struct AdbcError*);
    AdbcStatusCode (*ConnectionRollback)(struct AdbcConnection*, struct AdbcError*);

    AdbcStatusCode (*StatementBind)(struct AdbcStatement*, struct ArrowArray*, struct ArrowSchema*, struct AdbcError*);
    AdbcStatusCode (*StatementBindStream)(struct AdbcStatement*, struct ArrowArrayStream*, struct AdbcError*);
    AdbcStatusCode (*StatementExecuteQuery)(struct AdbcStatement*, struct ArrowArrayStream*, int64_t*, struct AdbcError*);
    AdbcStatusCode (*StatementExecutePartitions)(struct AdbcStatement*, struct ArrowSchema*, struct AdbcPartitions*, int64_t*, struct AdbcError*);
    AdbcStatusCode (*StatementGetParameterSchema)(struct AdbcStatement*, struct ArrowSchema*, struct AdbcError*);
    AdbcStatusCode (*StatementNew)(struct AdbcConnection*, struct AdbcStatement*, struct AdbcError*);
    AdbcStatusCode (*StatementPrepare)(struct AdbcStatement*, struct AdbcError*);
    AdbcStatusCode (*StatementRelease)(struct AdbcStatement*, struct AdbcError*);
    AdbcStatusCode (*StatementSetOption)(struct AdbcStatement*, const char*, const char*, struct AdbcError*);
    AdbcStatusCode (*StatementSetSqlQuery)(struct AdbcStatement*, const char*, struct AdbcError*);
    AdbcStatusCode (*StatementSetSubstraitPlan)(struct AdbcStatement*, const uint8_t*, size_t, struct AdbcError*);
};

/* Driver entry point: a driver manager loads the shared library and calls this
 * to populate the vtable above. */
AdbcStatusCode AdbcDriverInit(int version, void* driver, struct AdbcError* error);

#ifdef __cplusplus
}
#endif

#endif /* ARGUS_ADBC_H */
