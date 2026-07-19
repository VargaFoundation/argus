/*
 * Argus ODBC micro-benchmark.
 *
 * Connects with a DSN-less connection string and times a query's fetch loop,
 * reporting rows, wall-clock and rows/second over N iterations. Useful for
 * comparing backends and tracking fetch-path performance (Phase 2 / Arrow).
 *
 * Usage:
 *   bench "CONNECTION_STRING" "SQL" [iterations]
 *
 * Examples:
 *   bench "BACKEND=trino;HOST=localhost;PORT=8080;UID=test;Database=tpch" \
 *         "SELECT * FROM tiny.lineitem" 5
 *   bench "BACKEND=mysql;HOST=127.0.0.1;PORT=3306;UID=root;PWD=x;Database=d" \
 *         "SELECT * FROM big_table" 10
 */
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1.0e6;
}

static void diag(SQLSMALLINT type, SQLHANDLE h)
{
    SQLCHAR state[6], msg[512];
    SQLINTEGER native;
    SQLSMALLINT len;
    if (SQLGetDiagRec(type, h, 1, state, &native, msg, sizeof(msg), &len)
        == SQL_SUCCESS)
        fprintf(stderr, "  %s: %s\n", state, msg);
}

/* One iteration: execute + drain every row/column, return rows fetched. */
static long run_once(SQLHDBC dbc, const char *sql, int *ncols_out)
{
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt) != SQL_SUCCESS) return -1;

    if (!SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS))) {
        fprintf(stderr, "exec failed:\n");
        diag(SQL_HANDLE_STMT, stmt);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return -1;
    }

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);
    if (ncols_out) *ncols_out = ncols;

    long rows = 0;
    char buf[256];
    SQLLEN ind;
    /* ARGUS_BENCH_NODECODE=1 fetches rows without materializing any cell, to
     * isolate backend fetch+parse cost from the ODBC decode (SQLGetData). */
    int decode = getenv("ARGUS_BENCH_NODECODE") == NULL;
    /* ARGUS_BENCH_CKSUM=1 accumulates a content checksum so two runs (e.g. fast
     * JSON vs DOM) can be proven byte-identical. */
    int cksum_on = getenv("ARGUS_BENCH_CKSUM") != NULL;
    unsigned long long cksum = 1469598103934665603ULL;   /* FNV-1a offset */
    while (SQLFetch(stmt) == SQL_SUCCESS) {
        if (decode)
            for (SQLSMALLINT c = 1; c <= ncols; c++) {
                SQLRETURN gr = SQLGetData(stmt, c, SQL_C_CHAR, buf, sizeof(buf), &ind);
                if (cksum_on && SQL_SUCCEEDED(gr)) {
                    long n = (ind == SQL_NULL_DATA) ? -1 : ind;
                    cksum = (cksum ^ (unsigned long long)(n & 0xff)) * 1099511628211ULL;
                    if (n > 0) {
                        long m = n < (long)sizeof(buf) ? n : (long)sizeof(buf);
                        for (long k = 0; k < m; k++)
                            cksum = (cksum ^ (unsigned char)buf[k]) * 1099511628211ULL;
                    }
                }
            }
        rows++;
    }
    if (cksum_on)
        fprintf(stderr, "  cksum=%016llx rows=%ld\n", cksum, rows);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return rows;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s \"CONN_STRING\" \"SQL\" [iterations]\n",
                argv[0]);
        return 2;
    }
    const char *conn_str = argv[1];
    const char *sql = argv[2];
    int iters = argc > 3 ? atoi(argv[3]) : 5;
    if (iters < 1) iters = 1;

    SQLHENV env;
    SQLHDBC dbc;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    if (!SQL_SUCCEEDED(SQLDriverConnect(dbc, NULL, (SQLCHAR *)conn_str, SQL_NTS,
                                        NULL, 0, NULL, SQL_DRIVER_NOPROMPT))) {
        fprintf(stderr, "connect failed:\n");
        diag(SQL_HANDLE_DBC, dbc);
        return 1;
    }

    /* Warm-up (not timed) so connection/plan caches are primed. */
    int ncols = 0;
    long warm = run_once(dbc, sql, &ncols);
    if (warm < 0) return 1;

    double total = 0.0, best = 1e30, worst = 0.0;
    long rows = 0;
    for (int i = 0; i < iters; i++) {
        double t0 = now_ms();
        rows = run_once(dbc, sql, &ncols);
        double dt = now_ms() - t0;
        if (rows < 0) return 1;
        total += dt;
        if (dt < best) best = dt;
        if (dt > worst) worst = dt;
    }

    double avg = total / iters;
    double rps = avg > 0.0 ? (double)rows / (avg / 1000.0) : 0.0;
    printf("rows=%ld cols=%d iters=%d | avg=%.1f ms  min=%.1f  max=%.1f | "
           "%.0f rows/s  %.0f cells/s\n",
           rows, ncols, iters, avg, best, worst, rps, rps * ncols);

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return 0;
}
