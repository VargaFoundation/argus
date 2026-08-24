#ifndef ARGUS_ERROR_H
#define ARGUS_ERROR_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stddef.h>

/* Maximum number of diagnostic records per handle */
#define ARGUS_MAX_DIAG_RECORDS 64
#define ARGUS_MAX_SQLSTATE_LEN 6
#define ARGUS_MAX_MESSAGE_LEN  1024

/* A single ODBC diagnostic record */
typedef struct argus_diag_record {
    SQLCHAR  sqlstate[ARGUS_MAX_SQLSTATE_LEN];
    SQLCHAR  message[ARGUS_MAX_MESSAGE_LEN];
    SQLINTEGER native_error;
} argus_diag_record_t;

/* Collection of diagnostic records for a handle.
 *
 * `records` is allocated lazily on the first push and grown geometrically up
 * to ARGUS_MAX_DIAG_RECORDS. Embedding the full 64-record array by value cost
 * ~66 KB per diag — and a statement embeds five diags (its own plus four
 * descriptors'), ~345 KB per SQLAllocHandle(STMT) for diagnostics that are
 * almost always empty. Handles are calloc'd, so the zero state is valid. */
typedef struct argus_diag {
    argus_diag_record_t *records;    /* NULL until the first push */
    int                 capacity;    /* allocated slots */
    int                 count;
    SQLCHAR             header_sqlstate[ARGUS_MAX_SQLSTATE_LEN];
    SQLRETURN           return_code;
} argus_diag_t;

/* Clear all diagnostic records (keeps the allocation for reuse) */
void argus_diag_clear(argus_diag_t *diag);

/* Release the record storage (handle destruction) */
void argus_diag_dispose(argus_diag_t *diag);

/* Push a new diagnostic record */
void argus_diag_push(argus_diag_t *diag,
                     const char *sqlstate,
                     const char *message,
                     SQLINTEGER native_error);

/* Retrieve a diagnostic record (1-indexed) */
SQLRETURN argus_diag_get_rec(const argus_diag_t *diag,
                             SQLSMALLINT rec_number,
                             SQLCHAR *sqlstate,
                             SQLINTEGER *native_error,
                             SQLCHAR *message,
                             SQLSMALLINT buffer_length,
                             SQLSMALLINT *text_length);

/* Convenience: set error and return SQL_ERROR */
SQLRETURN argus_set_error(argus_diag_t *diag,
                          const char *sqlstate,
                          const char *message,
                          SQLINTEGER native_error);

/* Convenience: set "function not supported" */
SQLRETURN argus_set_not_implemented(argus_diag_t *diag,
                                    const char *func_name);

#endif /* ARGUS_ERROR_H */
