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
typedef struct {
    SQLHDBC dbc;
    char*   query;
    /* Bound parameters captured from an Arrow array (one record = one row of
     * params), materialized to text and applied per-row on execute. */
    char**  params;
    int     nparams;
} adbc_stmt_t;

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
    SQLHSTMT    stmt;        /* open result; rows pulled lazily per get_next */
    adbc_col_t* cols;        /* template: kind + name per column */
    int         ncols;
    int64_t     batch_size;  /* max rows per emitted Arrow batch */
    int         done;
    char        err[256];
} adbc_stream_t;

#define ADBC_BATCH_ROWS 4096

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

/* Days since 1970-01-01 for a proleptic-Gregorian date (Howard Hinnant's
 * algorithm; no library needed). */
static int64_t days_from_civil(int y, unsigned m, unsigned d)
{
    y -= (m <= 2);
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153u * (m + (m > 2 ? -3u : 9u)) + 2u) / 5u + d - 1u;
    unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

/* ODBC SQL type -> column kind. */
static int sqltype_to_kind(SQLSMALLINT t)
{
    switch (t) {
    case SQL_TINYINT: case SQL_SMALLINT: case SQL_INTEGER: case SQL_BIGINT: return 0;
    case SQL_REAL: case SQL_FLOAT: case SQL_DOUBLE: return 1;
    case SQL_BIT: return 3;
    case SQL_TYPE_DATE: case SQL_DATE: return 4;
    case SQL_TYPE_TIMESTAMP: case SQL_TIMESTAMP: return 5;
    default: return 2;
    }
}

/* Column kind -> Arrow C Data Interface format string. */
static const char* kind_format(int kind)
{
    switch (kind) {
    case 0:  return "l";      /* int64                      */
    case 1:  return "g";      /* double                     */
    case 3:  return "b";      /* boolean (bitmap)           */
    case 4:  return "tdD";    /* date32 (days)              */
    case 5:  return "tsu:";   /* timestamp, microseconds    */
    default: return "u";      /* utf8                       */
    }
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
        if (col->offsets && nrows > 0)
            memcpy(offs, col->offsets, sizeof(int32_t) * (size_t)(nrows + 1));
        else
            offs[0] = 0;   /* empty column: single zero offset */
        if (col->chars_len) memcpy(data, col->chars, col->chars_len);
        bufs[0] = validity; bufs[1] = offs; bufs[2] = data;
        out->n_buffers = 3; out->buffers = bufs;
    } else {                          /* fixed-width: validity, data */
        const void** bufs = calloc(2, sizeof(void*));
        if (!bufs) { free(validity); return -1; }
        void* data = NULL;
        size_t n = (size_t)(nrows ? nrows : 1);
        if (col->kind == 1) {                         /* double */
            data = malloc(sizeof(double) * n);
            if (data) memcpy(data, col->f64, sizeof(double) * (size_t)nrows);
        } else if (col->kind == 4) {                  /* date32: int32 days */
            int32_t* d = malloc(sizeof(int32_t) * n);
            data = d;
            for (int64_t i = 0; d && i < nrows; i++) d[i] = (int32_t)col->i64[i];
        } else if (col->kind == 3) {                  /* boolean: bitmap */
            size_t nb = (size_t)((nrows + 7) / 8);
            uint8_t* bm = calloc(nb ? nb : 1, 1);
            data = bm;
            for (int64_t i = 0; bm && i < nrows; i++)
                if (col->i64[i]) bm[i / 8] |= (uint8_t)(1u << (i % 8));
        } else {                                      /* int64 / timestamp(us) */
            data = malloc(sizeof(int64_t) * n);
            if (data) memcpy(data, col->i64, sizeof(int64_t) * (size_t)nrows);
        }
        if (!data) { free(validity); free(bufs); return -1; }
        bufs[0] = validity; bufs[1] = data;
        out->n_buffers = 2; out->buffers = bufs;
    }
    out->release = child_array_release;
    return 0;
}

static void col_reserve(adbc_col_t* col, int64_t nrows)
{
    if (nrows <= col->cap) return;
    int64_t cap = col->cap ? col->cap * 2 : 256;
    while (cap < nrows) cap *= 2;
    col->valid = realloc(col->valid, (size_t)cap);
    if (col->kind == 1) col->f64 = realloc(col->f64, sizeof(double) * (size_t)cap);
    else if (col->kind == 2) col->offsets = realloc(col->offsets, sizeof(int32_t) * (size_t)(cap + 1));
    else col->i64 = realloc(col->i64, sizeof(int64_t) * (size_t)cap);   /* int64/bool/date/ts */
    col->cap = cap;
}

static void free_col_buffers(adbc_col_t* col)
{
    free(col->i64); free(col->f64); free(col->offsets);
    free(col->chars); free(col->valid);
}

/* Read one result cell from the open statement into the collection column. */
static void collect_cell(SQLHSTMT stmt, adbc_col_t* col, int c, int64_t row)
{
    col_reserve(col, row + 1);
    SQLLEN ind = 0;
    if (col->kind == 1) {
        double v = 0;
        SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_DOUBLE, &v, 0, &ind);
        int valid = (ind != SQL_NULL_DATA);
        col->f64[row] = valid ? v : 0;
        col->valid[row] = (uint8_t)valid;
        if (!valid) col->null_count++;
    } else if (col->kind != 2) {
        int64_t outv = 0; int valid = 1;
        if (col->kind == 0) {
            SQLBIGINT v = 0;
            SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_SBIGINT, &v, 0, &ind);
            valid = (ind != SQL_NULL_DATA); outv = (int64_t)v;
        } else if (col->kind == 3) {
            unsigned char v = 0;
            SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_BIT, &v, 0, &ind);
            valid = (ind != SQL_NULL_DATA); outv = v ? 1 : 0;
        } else if (col->kind == 4) {
            SQL_DATE_STRUCT v; memset(&v, 0, sizeof(v));
            SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_TYPE_DATE, &v, 0, &ind);
            valid = (ind != SQL_NULL_DATA);
            if (valid) outv = days_from_civil(v.year, v.month, v.day);
        } else {
            SQL_TIMESTAMP_STRUCT v; memset(&v, 0, sizeof(v));
            SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_TYPE_TIMESTAMP, &v, 0, &ind);
            valid = (ind != SQL_NULL_DATA);
            if (valid) {
                int64_t days = days_from_civil(v.year, v.month, v.day);
                outv = days * 86400LL * 1000000LL
                     + ((int64_t)v.hour * 3600 + v.minute * 60 + v.second) * 1000000LL
                     + (int64_t)v.fraction / 1000;
            }
        }
        col->i64[row] = valid ? outv : 0;
        col->valid[row] = (uint8_t)valid;
        if (!valid) col->null_count++;
    } else {
        char vb[4096]; vb[0] = '\0';
        SQLGetData(stmt, (SQLUSMALLINT)(c + 1), SQL_C_CHAR, vb, sizeof(vb), &ind);
        int valid = (ind != SQL_NULL_DATA);
        size_t vlen = valid ? strlen(vb) : 0;
        if (row == 0) col->offsets[0] = 0;
        if (col->chars_len + vlen + 1 > col->chars_cap) {
            col->chars_cap = (col->chars_cap ? col->chars_cap : 4096);
            while (col->chars_len + vlen + 1 > col->chars_cap) col->chars_cap *= 2;
            col->chars = realloc(col->chars, col->chars_cap);
        }
        if (vlen) memcpy(col->chars + col->chars_len, vb, vlen);
        col->chars_len += vlen;
        col->offsets[row + 1] = (int32_t)col->chars_len;
        col->valid[row] = (uint8_t)valid;
        if (!valid) col->null_count++;
    }
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
        ch->format = kind_format(s->cols[i].kind);
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
    if (s->done) return 0;          /* end of stream: released array */

    /* Pull up to batch_size rows from the open statement into temp columns. */
    adbc_col_t* tc = calloc((size_t)s->ncols, sizeof(adbc_col_t));
    if (!tc) return ENOMEM;
    for (int i = 0; i < s->ncols; i++) tc[i].kind = s->cols[i].kind;

    int64_t nrows = 0;
    while (nrows < s->batch_size && SQLFetch(s->stmt) == SQL_SUCCESS) {
        for (int c = 0; c < s->ncols; c++) collect_cell(s->stmt, &tc[c], c, nrows);
        nrows++;
    }

    if (nrows == 0) {                /* result exhausted */
        for (int i = 0; i < s->ncols; i++) free_col_buffers(&tc[i]);
        free(tc);
        if (s->stmt) { SQLFreeHandle(SQL_HANDLE_STMT, s->stmt); s->stmt = NULL; }
        s->done = 1;
        return 0;
    }

    out->length = nrows;
    out->n_children = s->ncols;
    out->children = calloc((size_t)s->ncols, sizeof(struct ArrowArray*));
    const void** root_bufs = calloc(1, sizeof(void*));  /* struct validity = NULL */
    if (!out->children || !root_bufs) { free(out->children); free(root_bufs); return ENOMEM; }
    out->n_buffers = 1; out->buffers = root_bufs;
    for (int i = 0; i < s->ncols; i++) {
        struct ArrowArray* ch = calloc(1, sizeof(struct ArrowArray));
        if (!ch || build_child(&tc[i], nrows, ch) != 0) { free(ch); return ENOMEM; }
        out->children[i] = ch;
    }
    out->release = root_array_release;

    for (int i = 0; i < s->ncols; i++) free_col_buffers(&tc[i]);
    free(tc);
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
        if (s->stmt) SQLFreeHandle(SQL_HANDLE_STMT, s->stmt);   /* abandoned early */
        for (int i = 0; i < s->ncols; i++) free(s->cols[i].name);
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

/* Replace each top-level '?' marker with its bound literal. */
static char* substitute_params(const char* q, char** params, int n)
{
    size_t cap = strlen(q) + 1;
    for (int i = 0; i < n; i++) cap += (params[i] ? strlen(params[i]) : 4);
    char* out = malloc(cap);
    if (!out) return NULL;
    char* p = out; int pi = 0;
    for (const char* c = q; *c; c++) {
        if (*c == '?' && pi < n) {
            const char* v = params[pi] ? params[pi] : "NULL";
            size_t l = strlen(v); memcpy(p, v, l); p += l; pi++;
        } else {
            *p++ = *c;
        }
    }
    *p = '\0';
    return out;
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
    char* subst = (st->nparams > 0) ? substitute_params(st->query, st->params, st->nparams) : NULL;
    const char* exec_q = subst ? subst : st->query;
    SQLRETURN er = SQLExecDirect(stmt, (SQLCHAR*)exec_q, SQL_NTS);
    free(subst);
    if (!SQL_SUCCEEDED(er)) {
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
        s->cols[c].kind = sqltype_to_kind(ctype);
    }

    /* Keep the statement open; rows are pulled one Arrow batch at a time in
     * stream_get_next (bounded memory). */
    s->stmt = stmt;
    s->batch_size = ADBC_BATCH_ROWS;
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
        free(st->params);
        free(st);
        statement->private_data = NULL;
    }
    return ADBC_STATUS_OK;
}

/* Prepare is a no-op: queries execute directly via SQLExecDirect. */
AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement, struct AdbcError* error)
{
    if (!statement || !statement->private_data) { set_error(error, "invalid statement"); return ADBC_STATUS_INVALID_STATE; }
    return ADBC_STATUS_OK;
}

/* ── Metadata + bind (implemented incrementally) ─────────────── */

/* Render the row-0 value of one Arrow parameter array as a SQL literal. */
static char* param_to_literal(const char* fmt, struct ArrowArray* arr)
{
    int64_t row = arr->offset;   /* first element */
    const uint8_t* validity = (arr->n_buffers > 0) ? arr->buffers[0] : NULL;
    if (validity && !((validity[row / 8] >> (row % 8)) & 1)) return strdup("NULL");

    char buf[64];
    switch (fmt[0]) {
    case 'l': case 'i': case 's': case 'c': {   /* int family */
        const int64_t* d64 = (fmt[0] == 'l') ? (const int64_t*)arr->buffers[1] : NULL;
        long long v = d64 ? d64[row] : (long long)((const int32_t*)arr->buffers[1])[row];
        snprintf(buf, sizeof(buf), "%lld", v); return strdup(buf);
    }
    case 'g': { const double* d = arr->buffers[1]; snprintf(buf, sizeof(buf), "%.17g", d[row]); return strdup(buf); }
    case 'f': { const float* d = arr->buffers[1]; snprintf(buf, sizeof(buf), "%.9g", (double)d[row]); return strdup(buf); }
    case 'b': { const uint8_t* bm = arr->buffers[1]; return strdup(((bm[row / 8] >> (row % 8)) & 1) ? "TRUE" : "FALSE"); }
    case 'u': case 'U': {                        /* utf8: quote + escape */
        const int32_t* off = arr->buffers[1];
        const char* data = arr->buffers[2];
        int32_t a = off[row], b = off[row + 1];
        size_t len = (size_t)(b - a);
        char* out = malloc(len * 2 + 3);
        if (!out) return strdup("NULL");
        char* p = out; *p++ = '\'';
        for (size_t i = 0; i < len; i++) { if (data[a + i] == '\'') *p++ = '\''; *p++ = data[a + i]; }
        *p++ = '\''; *p = '\0';
        return out;
    }
    default: return strdup("NULL");
    }
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement, struct ArrowArray* values,
                                 struct ArrowSchema* schema, struct AdbcError* error)
{
    if (!statement || !statement->private_data || !values || !schema) {
        set_error(error, "invalid bind args"); return ADBC_STATUS_INVALID_ARGUMENT;
    }
    adbc_stmt_t* st = statement->private_data;
    /* The bind set is a struct array: one child per parameter, row 0 used. */
    int n = (int)schema->n_children;
    for (int i = 0; i < st->nparams; i++) free(st->params[i]);
    free(st->params);
    st->params = (n > 0) ? calloc((size_t)n, sizeof(char*)) : NULL;
    st->nparams = n;
    for (int i = 0; i < n; i++)
        st->params[i] = param_to_literal(schema->children[i]->format, values->children[i]);

    if (values->release) values->release(values);   /* ADBC: bind takes ownership */
    if (schema->release) schema->release(schema);
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name, struct ArrowSchema* schema,
                                            struct AdbcError* error)
{
    if (!connection || !connection->private_data || !table_name || !schema) {
        set_error(error, "invalid GetTableSchema args"); return ADBC_STATUS_INVALID_ARGUMENT;
    }
    adbc_conn_t* c = connection->private_data;
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, c->dbc, &stmt) != SQL_SUCCESS) {
        set_error(error, "alloc stmt failed"); return ADBC_STATUS_IO;
    }
    SQLRETURN r = SQLColumns(stmt,
        (SQLCHAR*)catalog,   catalog   ? SQL_NTS : 0,
        (SQLCHAR*)db_schema, db_schema ? SQL_NTS : 0,
        (SQLCHAR*)table_name, SQL_NTS,
        NULL, 0);
    if (!SQL_SUCCEEDED(r)) { set_error(error, "SQLColumns failed"); SQLFreeHandle(SQL_HANDLE_STMT, stmt); return ADBC_STATUS_IO; }

    int cap = 16, n = 0;
    char** names = malloc((size_t)cap * sizeof(char*));
    int*   kinds = malloc((size_t)cap * sizeof(int));
    char nm[256]; SQLLEN ind;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLSMALLINT datatype = 0;
        SQLGetData(stmt, 4, SQL_C_CHAR, nm, sizeof(nm), &ind);   /* COLUMN_NAME */
        SQLGetData(stmt, 5, SQL_C_SSHORT, &datatype, 0, &ind);   /* DATA_TYPE   */
        if (n == cap) { cap *= 2; names = realloc(names, (size_t)cap * sizeof(char*)); kinds = realloc(kinds, (size_t)cap * sizeof(int)); }
        names[n] = strdup(nm); kinds[n] = sqltype_to_kind(datatype); n++;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    memset(schema, 0, sizeof(*schema));
    schema->format = "+s";
    schema->n_children = n;
    schema->children = calloc((size_t)(n > 0 ? n : 1), sizeof(struct ArrowSchema*));
    for (int i = 0; i < n; i++) {
        struct ArrowSchema* ch = calloc(1, sizeof(struct ArrowSchema));
        ch->format = kind_format(kinds[i]);
        ch->name = names[i];                 /* ownership transferred */
        ch->flags = ARROW_FLAG_NULLABLE;
        ch->release = child_schema_release;
        schema->children[i] = ch;
    }
    schema->release = root_schema_release;
    free(names);
    free(kinds);
    return ADBC_STATUS_OK;
}

/* ── One-shot stream: emit a single pre-built batch, then end ── */

typedef struct { struct ArrowSchema schema; struct ArrowArray array; int array_done; } oneshot_t;

static int oneshot_get_schema(struct ArrowArrayStream* self, struct ArrowSchema* out)
{
    oneshot_t* o = self->private_data;
    *out = o->schema; memset(&o->schema, 0, sizeof(o->schema));   /* move out */
    return 0;
}
static int oneshot_get_next(struct ArrowArrayStream* self, struct ArrowArray* out)
{
    oneshot_t* o = self->private_data;
    memset(out, 0, sizeof(*out));
    if (o->array_done) return 0;
    *out = o->array; memset(&o->array, 0, sizeof(o->array)); o->array_done = 1;
    return 0;
}
static const char* oneshot_get_last_error(struct ArrowArrayStream* self) { (void)self; return NULL; }
static void oneshot_release(struct ArrowArrayStream* self)
{
    oneshot_t* o = self->private_data;
    if (o) {
        if (o->schema.release) o->schema.release(&o->schema);
        if (o->array.release) o->array.release(&o->array);
        free(o);
    }
    self->private_data = NULL; self->release = NULL;
}

/* Build a stream of one utf8 column from C strings. */
static void make_utf8_stream(struct ArrowArrayStream* out, const char* col,
                             const char** vals, int n)
{
    oneshot_t* o = calloc(1, sizeof(*o));
    o->schema.format = "+s";
    o->schema.n_children = 1;
    o->schema.children = calloc(1, sizeof(struct ArrowSchema*));
    struct ArrowSchema* sch = calloc(1, sizeof(struct ArrowSchema));
    sch->format = "u"; sch->name = strdup(col); sch->flags = ARROW_FLAG_NULLABLE;
    sch->release = child_schema_release;
    o->schema.children[0] = sch;
    o->schema.release = root_schema_release;

    adbc_col_t tc; memset(&tc, 0, sizeof(tc)); tc.kind = 2;
    for (int i = 0; i < n; i++) {
        col_reserve(&tc, i + 1);
        if (i == 0) tc.offsets[0] = 0;
        size_t l = strlen(vals[i]);
        if (tc.chars_len + l + 1 > tc.chars_cap) {
            tc.chars_cap = tc.chars_cap ? tc.chars_cap : 64;
            while (tc.chars_len + l + 1 > tc.chars_cap) tc.chars_cap *= 2;
            tc.chars = realloc(tc.chars, tc.chars_cap);
        }
        memcpy(tc.chars + tc.chars_len, vals[i], l);
        tc.chars_len += l;
        tc.offsets[i + 1] = (int32_t)tc.chars_len;
        tc.valid[i] = 1;
    }
    o->array.length = n;
    o->array.n_children = 1;
    o->array.children = calloc(1, sizeof(struct ArrowArray*));
    const void** rb = calloc(1, sizeof(void*));
    o->array.n_buffers = 1; o->array.buffers = rb;
    struct ArrowArray* ach = calloc(1, sizeof(struct ArrowArray));
    build_child(&tc, n, ach);
    o->array.children[0] = ach;
    o->array.release = root_array_release;
    free_col_buffers(&tc);

    memset(out, 0, sizeof(*out));
    out->get_schema = oneshot_get_schema;
    out->get_next = oneshot_get_next;
    out->get_last_error = oneshot_get_last_error;
    out->release = oneshot_release;
    out->private_data = o;
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* out, struct AdbcError* error)
{
    if (!connection || !out) { set_error(error, "invalid args"); return ADBC_STATUS_INVALID_ARGUMENT; }
    const char* types[] = { "TABLE", "VIEW" };
    make_utf8_stream(out, "table_type", types, 2);
    return ADBC_STATUS_OK;
}

/* ── GetObjects: build the nested catalog hierarchy schema ────── */

static struct ArrowSchema* sch_leaf(const char* name, const char* fmt)
{
    struct ArrowSchema* s = calloc(1, sizeof(*s));
    s->format = fmt; s->name = strdup(name); s->flags = ARROW_FLAG_NULLABLE;
    s->release = child_schema_release;
    return s;
}

static struct ArrowSchema* sch_struct(const char* name, struct ArrowSchema** kids, int n)
{
    struct ArrowSchema* s = calloc(1, sizeof(*s));
    s->format = "+s"; s->name = strdup(name); s->flags = ARROW_FLAG_NULLABLE;
    s->n_children = n; s->children = calloc((size_t)n, sizeof(struct ArrowSchema*));
    for (int i = 0; i < n; i++) s->children[i] = kids[i];
    s->release = root_schema_release;
    return s;
}

static struct ArrowSchema* sch_list(const char* name, struct ArrowSchema* item)
{
    struct ArrowSchema* s = calloc(1, sizeof(*s));
    s->format = "+l"; s->name = strdup(name); s->flags = ARROW_FLAG_NULLABLE;
    if (!item->name) item->name = strdup("item");
    s->n_children = 1; s->children = calloc(1, sizeof(struct ArrowSchema*));
    s->children[0] = item;
    s->release = root_schema_release;
    return s;
}

/* The ADBC GetObjects schema (column detail kept to the common fields). */
static void build_objects_schema(struct ArrowSchema* out)
{
    struct ArrowSchema* col_kids[2] = {
        sch_leaf("column_name", "u"), sch_leaf("ordinal_position", "i") };
    struct ArrowSchema* col = sch_struct("item", col_kids, 2);
    struct ArrowSchema* tbl_kids[3] = {
        sch_leaf("table_name", "u"), sch_leaf("table_type", "u"),
        sch_list("table_columns", col) };
    struct ArrowSchema* tbl = sch_struct("item", tbl_kids, 3);
    struct ArrowSchema* sch_kids[2] = {
        sch_leaf("db_schema_name", "u"), sch_list("db_schema_tables", tbl) };
    struct ArrowSchema* sch = sch_struct("item", sch_kids, 2);
    struct ArrowSchema* root_kids[2] = {
        sch_leaf("catalog_name", "u"), sch_list("catalog_db_schemas", sch) };

    memset(out, 0, sizeof(*out));
    out->format = "+s"; out->n_children = 2;
    out->children = calloc(2, sizeof(struct ArrowSchema*));
    out->children[0] = root_kids[0];
    out->children[1] = root_kids[1];
    out->release = root_schema_release;
}

/* Build a length-0 array matching a schema (recursively). */
static struct ArrowArray* empty_array(const struct ArrowSchema* s)
{
    struct ArrowArray* a = calloc(1, sizeof(*a));
    a->release = child_array_release;
    if (s->format[0] == '+' && s->format[1] == 's') {        /* struct */
        const void** b = calloc(1, sizeof(void*)); a->n_buffers = 1; a->buffers = b;
        a->n_children = s->n_children;
        a->children = calloc((size_t)s->n_children, sizeof(struct ArrowArray*));
        for (int i = 0; i < s->n_children; i++) a->children[i] = empty_array(s->children[i]);
        a->release = root_array_release;
    } else if (s->format[0] == '+' && s->format[1] == 'l') { /* list */
        const void** b = calloc(2, sizeof(void*));
        b[1] = calloc(1, sizeof(int32_t));                   /* offsets {0} */
        a->n_buffers = 2; a->buffers = b;
        a->n_children = 1;
        a->children = calloc(1, sizeof(struct ArrowArray*));
        a->children[0] = empty_array(s->children[0]);
        a->release = root_array_release;
    } else if (s->format[0] == 'u') {                        /* utf8 */
        const void** b = calloc(3, sizeof(void*));
        b[1] = calloc(1, sizeof(int32_t));                   /* offsets {0} */
        a->n_buffers = 3; a->buffers = b;
    } else {                                                 /* int32 etc. */
        const void** b = calloc(2, sizeof(void*));
        a->n_buffers = 2; a->buffers = b;
    }
    return a;
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name, struct ArrowArrayStream* out,
                                        struct AdbcError* error)
{
    (void)depth; (void)catalog; (void)db_schema; (void)table_name;
    (void)table_types; (void)column_name;
    if (!connection || !connection->private_data || !out) { set_error(error, "invalid args"); return ADBC_STATUS_INVALID_ARGUMENT; }
    adbc_conn_t* c = connection->private_data;

    /* Collect distinct catalog names from SQLTables. */
    int cap = 16, n = 0; char** cats = malloc((size_t)cap * sizeof(char*));
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, c->dbc, &stmt) == SQL_SUCCESS &&
        SQL_SUCCEEDED(SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0))) {
        char cat[256]; SQLLEN ind;
        while (SQLFetch(stmt) == SQL_SUCCESS) {
            SQLGetData(stmt, 1, SQL_C_CHAR, cat, sizeof(cat), &ind);
            if (ind == SQL_NULL_DATA || !cat[0]) continue;
            int seen = 0;
            for (int i = 0; i < n; i++) if (strcmp(cats[i], cat) == 0) { seen = 1; break; }
            if (seen) continue;
            if (n == cap) { cap *= 2; cats = realloc(cats, (size_t)cap * sizeof(char*)); }
            cats[n++] = strdup(cat);
        }
    }
    if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    oneshot_t* o = calloc(1, sizeof(*o));
    build_objects_schema(&o->schema);

    /* Root struct array: catalog_name (populated) + catalog_db_schemas (empty
     * list per catalog -- deeper levels are a documented follow-up). */
    o->array.length = n;
    o->array.n_children = 2;
    o->array.children = calloc(2, sizeof(struct ArrowArray*));
    const void** rb = calloc(1, sizeof(void*)); o->array.n_buffers = 1; o->array.buffers = rb;

    /* child 0: catalog_name utf8 */
    adbc_col_t tc; memset(&tc, 0, sizeof(tc)); tc.kind = 2;
    for (int i = 0; i < n; i++) {
        col_reserve(&tc, i + 1);
        if (i == 0) tc.offsets[0] = 0;
        size_t l = strlen(cats[i]);
        if (tc.chars_len + l + 1 > tc.chars_cap) {
            tc.chars_cap = tc.chars_cap ? tc.chars_cap : 64;
            while (tc.chars_len + l + 1 > tc.chars_cap) tc.chars_cap *= 2;
            tc.chars = realloc(tc.chars, tc.chars_cap);
        }
        memcpy(tc.chars + tc.chars_len, cats[i], l); tc.chars_len += l;
        tc.offsets[i + 1] = (int32_t)tc.chars_len; tc.valid[i] = 1;
    }
    struct ArrowArray* cat_arr = calloc(1, sizeof(struct ArrowArray));
    build_child(&tc, n, cat_arr);
    free_col_buffers(&tc);
    o->array.children[0] = cat_arr;

    /* child 1: catalog_db_schemas list<struct> -- empty for each catalog */
    struct ArrowArray* lst = calloc(1, sizeof(struct ArrowArray));
    lst->length = n;
    const void** lb = calloc(2, sizeof(void*));
    int32_t* offs = calloc((size_t)(n + 1), sizeof(int32_t));   /* all 0 */
    lb[1] = offs; lst->n_buffers = 2; lst->buffers = lb;
    lst->n_children = 1; lst->children = calloc(1, sizeof(struct ArrowArray*));
    lst->children[0] = empty_array(o->schema.children[1]->children[0]);  /* element schema */
    lst->release = root_array_release;
    o->array.children[1] = lst;
    o->array.release = root_array_release;

    for (int i = 0; i < n; i++) free(cats[i]);
    free(cats);

    memset(out, 0, sizeof(*out));
    out->get_schema = oneshot_get_schema;
    out->get_next = oneshot_get_next;
    out->get_last_error = oneshot_get_last_error;
    out->release = oneshot_release;
    out->private_data = o;
    return ADBC_STATUS_OK;
}

/* ── Driver-manager entry point ──────────────────────────────── */

AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error)
{
    if (version != ADBC_VERSION_1_0_0) { set_error(error, "unsupported ADBC version"); return ADBC_STATUS_NOT_IMPLEMENTED; }
    if (!raw_driver) { set_error(error, "null driver"); return ADBC_STATUS_INVALID_ARGUMENT; }
    struct AdbcDriver* d = raw_driver;
    memset(d, 0, sizeof(*d));

    d->DatabaseNew              = AdbcDatabaseNew;
    d->DatabaseSetOption        = AdbcDatabaseSetOption;
    d->DatabaseInit             = AdbcDatabaseInit;
    d->DatabaseRelease          = AdbcDatabaseRelease;

    d->ConnectionNew            = AdbcConnectionNew;
    d->ConnectionInit           = AdbcConnectionInit;
    d->ConnectionRelease        = AdbcConnectionRelease;
    d->ConnectionGetTableSchema = AdbcConnectionGetTableSchema;
    d->ConnectionGetTableTypes  = AdbcConnectionGetTableTypes;
    d->ConnectionGetObjects     = AdbcConnectionGetObjects;

    d->StatementNew             = AdbcStatementNew;
    d->StatementSetSqlQuery     = AdbcStatementSetSqlQuery;
    d->StatementPrepare         = AdbcStatementPrepare;
    d->StatementBind            = AdbcStatementBind;
    d->StatementExecuteQuery    = AdbcStatementExecuteQuery;
    d->StatementRelease         = AdbcStatementRelease;
    return ADBC_STATUS_OK;
}
