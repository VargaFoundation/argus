/*
 * Argus ADBC driver (first increment).
 *
 * Exposes an Apache Arrow ADBC surface over the existing, validated Argus ODBC
 * stack: a connection is an ODBC connection (SQLDriverConnect with the Argus
 * connection string), and AdbcStatementExecuteQuery runs the SQL through
 * SQLExecDirect/SQLFetch and emits the result as an Arrow C Data Interface
 * stream of typed columns (int64 / double / utf8). This reuses every backend,
 * the streaming fetch and the native typed cells unchanged.
 *
 * Scope today: forward-only SELECT to a single materialized record batch with
 * int64/double/utf8 columns (other SQL types are surfaced as utf8). The driver-
 * manager AdbcDriverInit vtable and richer type/stream coverage are follow-ups.
 */
#include "argus/adbc.h"

#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

/* ── Error helpers ───────────────────────────────────────────── */

static void set_error(struct AdbcError* error, const char* msg)
{
    if (!error) return;
    error->message = msg ? strdup(msg) : NULL;
    error->vendor_code = 0;
    memset(error->sqlstate, 0, sizeof(error->sqlstate));
    error->release = NULL;
}

/* ── Private handle state ────────────────────────────────────── */

typedef struct { char* conn_str; } adbc_db_t;
typedef struct { SQLHENV env; SQLHDBC dbc; } adbc_conn_t;
typedef struct { SQLHDBC dbc; char* query; } adbc_stmt_t;

/* One result column collected into final Arrow buffers. */
typedef struct {
    int      kind;          /* 0 = int64, 1 = double, 2 = utf8 */
    char*    name;
    int64_t* i64;           /* kind 0 */
    double*  f64;           /* kind 1 */
    int32_t* offsets;       /* kind 2 (length nrows+1) */
    char*    chars;         /* kind 2 */
    size_t   chars_len, chars_cap;
    uint8_t* valid;         /* 1 byte/row, 1 = valid */
    int64_t  null_count;
    int64_t  cap;
} adbc_col_t;

typedef struct {
    adbc_col_t* cols;
    int         ncols;
    int64_t     nrows;
    int         delivered;
    char        err[256];
} adbc_stream_t;

/* ── Database ────────────────────────────────────────────────── */

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error)
{
    if (!database) { set_error(error, "null database"); return ADBC_STATUS_INVALID_ARGUMENT; }
    adbc_db_t* db = calloc(1, sizeof(*db));
    if (!db) { set_error(error, "out of memory"); return ADBC_STATUS_IO; }
    database->private_data = db;
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error)
{
    if (!database || !database->private_data) { set_error(error, "invalid database"); return ADBC_STATUS_INVALID_STATE; }
    adbc_db_t* db = database->private_data;
    /* Accept the standard "uri" key and an Argus-specific alias for the full
     * ODBC-style connection string (BACKEND=...;HOST=...;...). */
    if (key && (strcmp(key, "uri") == 0 ||
                strcmp(key, "argus.connection_string") == 0)) {
        free(db->conn_str);
        db->conn_str = value ? strdup(value) : NULL;
        return ADBC_STATUS_OK;
    }
    set_error(error, "unknown option");
    return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error)
{
    if (!database || !database->private_data) { set_error(error, "invalid database"); return ADBC_STATUS_INVALID_STATE; }
    adbc_db_t* db = database->private_data;
    if (!db->conn_str) { set_error(error, "missing connection string (set option \"uri\")"); return ADBC_STATUS_INVALID_STATE; }
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database, struct AdbcError* error)
{
    (void)error;
    if (database && database->private_data) {
        adbc_db_t* db = database->private_data;
        free(db->conn_str);
        free(db);
        database->private_data = NULL;
    }
    return ADBC_STATUS_OK;
}

/* ── Connection ──────────────────────────────────────────────── */

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection, struct AdbcError* error)
{
    if (!connection) { set_error(error, "null connection"); return ADBC_STATUS_INVALID_ARGUMENT; }
    adbc_conn_t* c = calloc(1, sizeof(*c));
    if (!c) { set_error(error, "out of memory"); return ADBC_STATUS_IO; }
    connection->private_data = c;
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database, struct AdbcError* error)
{
    if (!connection || !connection->private_data || !database || !database->private_data) {
        set_error(error, "invalid handle"); return ADBC_STATUS_INVALID_STATE;
    }
    adbc_conn_t* c = connection->private_data;
    adbc_db_t* db = database->private_data;

    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &c->env) != SQL_SUCCESS) {
        set_error(error, "SQLAllocHandle(ENV) failed"); return ADBC_STATUS_IO;
    }
    SQLSetEnvAttr(c->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (SQLAllocHandle(SQL_HANDLE_DBC, c->env, &c->dbc) != SQL_SUCCESS) {
        set_error(error, "SQLAllocHandle(DBC) failed"); return ADBC_STATUS_IO;
    }
    SQLRETURN r = SQLDriverConnect(c->dbc, NULL, (SQLCHAR*)db->conn_str, SQL_NTS,
                                   NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(r)) {
        SQLCHAR st[6], msg[300]; SQLINTEGER nat; SQLSMALLINT len;
        char buf[360] = "connect failed";
        if (SQLGetDiagRec(SQL_HANDLE_DBC, c->dbc, 1, st, &nat, msg, sizeof(msg), &len) == SQL_SUCCESS)
            snprintf(buf, sizeof(buf), "connect failed: %s", (char*)msg);
        set_error(error, buf);
        return ADBC_STATUS_IO;
    }
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection, struct AdbcError* error)
{
    (void)error;
    if (connection && connection->private_data) {
        adbc_conn_t* c = connection->private_data;
        if (c->dbc) { SQLDisconnect(c->dbc); SQLFreeHandle(SQL_HANDLE_DBC, c->dbc); }
        if (c->env) SQLFreeHandle(SQL_HANDLE_ENV, c->env);
        free(c);
        connection->private_data = NULL;
    }
    return ADBC_STATUS_OK;
}

/* ── Arrow C Data Interface emission ─────────────────────────── */

static void child_array_release(struct ArrowArray* a)
{
    for (int64_t i = 0; i < a->n_buffers; i++)
        free((void*)a->buffers[i]);   /* free(NULL) is safe */
    free((void*)a->buffers);
    a->release = NULL;
}

static void root_array_release(struct ArrowArray* a)
{
    for (int64_t i = 0; i < a->n_children; i++) {
        if (a->children[i]->release) a->children[i]->release(a->children[i]);
        free(a->children[i]);
    }
    free(a->children);
    free((void*)a->buffers);
    a->release = NULL;
}

static void child_schema_release(struct ArrowSchema* s)
{
    free((void*)s->name);
    s->release = NULL;
}

static void root_schema_release(struct ArrowSchema* s)
{
    for (int64_t i = 0; i < s->n_children; i++) {
        if (s->children[i]->release) s->children[i]->release(s->children[i]);
        free(s->children[i]);
    }
    free(s->children);
    s->release = NULL;
}

/* Build a validity bitmap (1 bit/row, LSB-first) from per-row bytes, or NULL
 * when the column has no nulls. */
static uint8_t* build_validity(const uint8_t* bytes, int64_t n, int64_t null_count)
{
    if (null_count == 0) return NULL;
    int64_t nbytes = (n + 7) / 8;
    uint8_t* bm = calloc((size_t)nbytes, 1);
    if (!bm) return NULL;
    for (int64_t i = 0; i < n; i++)
        if (bytes[i]) bm[i / 8] |= (uint8_t)(1u << (i % 8));
    return bm;
}

static int build_child(const adbc_col_t* col, int64_t nrows, struct ArrowArray* out)
{
    memset(out, 0, sizeof(*out));
    out->length = nrows;
    out->null_count = col->null_count;
    uint8_t* validity = build_validity(col->valid, nrows, col->null_count);

    if (col->kind == 2) {            /* utf8: validity, offsets, data */
        const void** bufs = calloc(3, sizeof(void*));
        if (!bufs) { free(validity); return -1; }
        int32_t* offs = malloc(sizeof(int32_t) * (size_t)(nrows + 1));
        char* data = malloc(col->chars_len ? col->chars_len : 1);
        if (!offs || !data) { free(validity); free(bufs); free(offs); free(data); return -1; }
        memcpy(offs, col->offsets, sizeof(int32_t) * (size_t)(nrows + 1));
        if (col->chars_len) memcpy(data, col->chars, col->chars_len);
        bufs[0] = validity; bufs[1] = offs; bufs[2] = data;
        out->n_buffers = 3; out->buffers = bufs;
    } else {                          /* int64 / double: validity, data */
        const void** bufs = calloc(2, sizeof(void*));
        if (!bufs) { free(validity); return -1; }
        size_t esz = (col->kind == 0) ? sizeof(int64_t) : sizeof(double);
        void* data = malloc(esz * (size_t)(nrows ? nrows : 1));
        if (!data) { free(validity); free(bufs); return -1; }
        if (col->kind == 0) memcpy(data, col->i64, sizeof(int64_t) * (size_t)nrows);
        else                memcpy(data, col->f64, sizeof(double) * (size_t)nrows);
        bufs[0] = validity; bufs[1] = data;
        out->n_buffers = 2; out->buffers = bufs;
    }
    out->release = child_array_release;
    return 0;
}

static int stream_get_schema(struct ArrowArrayStream* self, struct ArrowSchema* out)
{
    adbc_stream_t* s = self->private_data;
    memset(out, 0, sizeof(*out));
    out->format = "+s";
    out->n_children = s->ncols;
    out->children = calloc((size_t)s->ncols, sizeof(struct ArrowSchema*));
    if (!out->children) return ENOMEM;
    for (int i = 0; i < s->ncols; i++) {
        struct ArrowSchema* ch = calloc(1, sizeof(struct ArrowSchema));
        ch->format = (s->cols[i].kind == 0) ? "l" : (s->cols[i].kind == 1) ? "g" : "u";
        ch->name = strdup(s->cols[i].name ? s->cols[i].name : "");
        ch->flags = ARROW_FLAG_NULLABLE;
        ch->release = child_schema_release;
        out->children[i] = ch;
    }
    out->release = root_schema_release;
    return 0;
}

static int stream_get_next(struct ArrowArrayStream* self, struct ArrowArray* out)
{
    adbc_stream_t* s = self->private_data;
    memset(out, 0, sizeof(*out));
    if (s->delivered) return 0;     /* end of stream: released array */
    s->delivered = 1;

    out->length = s->nrows;
    out->n_children = s->ncols;
    out->children = calloc((size_t)s->ncols, sizeof(struct ArrowArray*));
    const void** root_bufs = calloc(1, sizeof(void*));  /* struct validity = NULL */
    if (!out->children || !root_bufs) { free(out->children); free(root_bufs); return ENOMEM; }
    out->n_buffers = 1; out->buffers = root_bufs;
    for (int i = 0; i < s->ncols; i++) {
        struct ArrowArray* ch = calloc(1, sizeof(struct ArrowArray));
        if (!ch || build_child(&s->cols[i], s->nrows, ch) != 0) { free(ch); return ENOMEM; }
        out->children[i] = ch;
    }
    out->release = root_array_release;
    return 0;
}

static const char* stream_get_last_error(struct ArrowArrayStream* self)
{
    adbc_stream_t* s = self->private_data;
    return (s && s->err[0]) ? s->err : NULL;
}

static void stream_release(struct ArrowArrayStream* self)
{
    adbc_stream_t* s = self->private_data;
    if (s) {
        for (int i = 0; i < s->ncols; i++) {
            free(s->cols[i].name); free(s->cols[i].i64); free(s->cols[i].f64);
            free(s->cols[i].offsets); free(s->cols[i].chars); free(s->cols[i].valid);
        }
        free(s->cols);
        free(s);
    }
    self->private_data = NULL;
    self->release = NULL;
}

/* ── Statement ───────────────────────────────────────────────── */

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement, struct AdbcError* error)
{
    if (!connection || !connection->private_data || !statement) {
        set_error(error, "invalid handle"); return ADBC_STATUS_INVALID_STATE;
    }
    adbc_conn_t* c = connection->private_data;
    adbc_stmt_t* st = calloc(1, sizeof(*st));
    if (!st) { set_error(error, "out of memory"); return ADBC_STATUS_IO; }
    st->dbc = c->dbc;
    statement->private_data = st;
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error)
{
    if (!statement || !statement->private_data) { set_error(error, "invalid statement"); return ADBC_STATUS_INVALID_STATE; }
    adbc_stmt_t* st = statement->private_data;
    free(st->query);
    st->query = query ? strdup(query) : NULL;
    return ADBC_STATUS_OK;
}

static void col_reserve(adbc_col_t* col, int64_t nrows)
{
    if (nrows <= col->cap) return;
    int64_t cap = col->cap ? col->cap * 2 : 256;
    while (cap < nrows) cap *= 2;
    col->valid = realloc(col->valid, (size_t)cap);
    if (col->kind == 0) col->i64 = realloc(col->i64, sizeof(int64_t) * (size_t)cap);
    else if (col->kind == 1) col->f64 = realloc(col->f64, sizeof(double) * (size_t)cap);
    else col->offsets = realloc(col->offsets, sizeof(int32_t) * (size_t)(cap + 1));
    col->cap = cap;
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected, struct AdbcError* error)
{
    if (!statement || !statement->private_data || !out) { set_error(error, "invalid args"); return ADBC_STATUS_INVALID_ARGUMENT; }
    adbc_stmt_t* st = statement->private_data;
    if (!st->query) { set_error(error, "no query set"); return ADBC_STATUS_INVALID_STATE; }

    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, st->dbc, &stmt) != SQL_SUCCESS) { set_error(error, "alloc stmt failed"); return ADBC_STATUS_IO; }
    if (!SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR*)st->query, SQL_NTS))) {
        SQLCHAR sst[6], msg[300]; SQLINTEGER nat; SQLSMALLINT len; char buf[360] = "execute failed";
        if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sst, &nat, msg, sizeof(msg), &len) == SQL_SUCCESS)
            snprintf(buf, sizeof(buf), "%s", (char*)msg);
        set_error(error, buf);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return ADBC_STATUS_IO;
    }

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);

    adbc_stream_t* s = calloc(1, sizeof(*s));
    s->ncols = ncols;
    s->cols = calloc((size_t)(ncols > 0 ? ncols : 1), sizeof(adbc_col_t));
    for (int c = 0; c < ncols; c++) {
        SQLCHAR cname[256]; SQLSMALLINT cnl = 0, ctype = 0, cdd = 0, cnull = 0; SQLULEN csz = 0;
        SQLDescribeCol(stmt, (SQLUSMALLINT)(c + 1), cname, sizeof(cname), &cnl, &ctype, &csz, &cdd, &cnull);
        s->cols[c].name = strdup((char*)cname);
        switch (ctype) {
        case SQL_TINYINT: case SQL_SMALLINT: case SQL_INTEGER:
        case SQL_BIGINT:  case SQL_BIT:
            s->cols[c].kind = 0; break;
        case SQL_REAL: case SQL_FLOAT: case SQL_DOUBLE:
            s->cols[c].kind = 1; break;
        default:
            s->cols[c].kind = 2; s->cols[c].offsets = NULL; break;
        }
    }

    int64_t nrows = 0;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        for (int c = 0; c < ncols; c++) {
            adbc_col_t* col = &s->cols[c];
            col_reserve(col, nrows + 1);
            SQLLEN ind = 0;
            if (col->kind == 0) {
                SQLBIGINT v = 0;
                SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_SBIGINT, &v, 0, &ind);
                int valid = (ind != SQL_NULL_DATA);
                col->i64[nrows] = valid ? (int64_t)v : 0;
                col->valid[nrows] = (uint8_t)valid;
                if (!valid) col->null_count++;
            } else if (col->kind == 1) {
                double v = 0;
                SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_DOUBLE, &v, 0, &ind);
                int valid = (ind != SQL_NULL_DATA);
                col->f64[nrows] = valid ? v : 0;
                col->valid[nrows] = (uint8_t)valid;
                if (!valid) col->null_count++;
            } else {
                char vb[4096]; vb[0] = '\0';
                SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_CHAR, vb, sizeof(vb), &ind);
                int valid = (ind != SQL_NULL_DATA);
                size_t vlen = valid ? strlen(vb) : 0;
                if (nrows == 0) col->offsets[0] = 0;
                if (col->chars_len + vlen + 1 > col->chars_cap) {
                    col->chars_cap = (col->chars_cap ? col->chars_cap : 4096);
                    while (col->chars_len + vlen + 1 > col->chars_cap) col->chars_cap *= 2;
                    col->chars = realloc(col->chars, col->chars_cap);
                }
                if (vlen) memcpy(col->chars + col->chars_len, vb, vlen);
                col->chars_len += vlen;
                col->offsets[nrows + 1] = (int32_t)col->chars_len;
                col->valid[nrows] = (uint8_t)valid;
                if (!valid) col->null_count++;
            }
        }
        nrows++;
    }
    s->nrows = nrows;
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (rows_affected) *rows_affected = -1;   /* unknown for SELECT */

    memset(out, 0, sizeof(*out));
    out->get_schema = stream_get_schema;
    out->get_next = stream_get_next;
    out->get_last_error = stream_get_last_error;
    out->release = stream_release;
    out->private_data = s;
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement, struct AdbcError* error)
{
    (void)error;
    if (statement && statement->private_data) {
        adbc_stmt_t* st = statement->private_data;
        free(st->query);
        free(st);
        statement->private_data = NULL;
    }
    return ADBC_STATUS_OK;
}
