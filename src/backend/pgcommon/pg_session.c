#include "pg_common.h"
#include "argus/compat.h"
#include "argus/error.h"
#include "argus/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Connection lifecycle for the PostgreSQL family.
 *
 * Everything goes through PQconnectdbParams with a keyword/value array rather
 * than a concatenated conninfo string: values come from an ODBC connection
 * string the driver does not control, and a password containing a space or a
 * single quote must not be able to change the meaning of the conninfo.
 *
 * libpq covers, natively, several things the other Argus backends had to build
 * by hand: TLS with full certificate verification, SCRAM-SHA-256, GSSAPI /
 * Kerberos (including SSPI on Windows), and a comma-separated host list with
 * automatic failover. So the mapping below is mostly a translation table, and
 * the driver adds no transport code of its own.
 */

#define PG_DEFAULT_PORT 5432

/*
 * The ODBC layer substitutes the literal string "default" when no DATABASE was
 * given (src/odbc/connect.c:157) — a sensible name for Hive, and a database
 * that does not exist on any PostgreSQL server, so passing it straight through
 * would make every connection without an explicit DATABASE fail with
 * `FATAL: database "default" does not exist`. It is treated as "unset" here
 * and the maintenance database is used instead. Changing connect.c would
 * change the meaning of DATABASE for the other ten backends, so the fix
 * belongs on this side.
 */
static const char *effective_dbname(const char *database)
{
    if (!database || !*database) return "postgres";
    if (strcmp(database, "default") == 0) return "postgres";
    return database;
}

/* Small helper to build the keyword/value arrays without counting by hand. */
typedef struct {
    const char *keys[32];
    const char *vals[32];
    char       *owned[32];   /* values this builder allocated */
    int         n;
    int         n_owned;
} pg_kv_t;

static void kv_add(pg_kv_t *kv, const char *key, const char *val)
{
    if (!val || !*val) return;
    if (kv->n >= (int)(sizeof(kv->keys) / sizeof(kv->keys[0])) - 1) return;
    kv->keys[kv->n] = key;
    kv->vals[kv->n] = val;
    kv->n++;
}

static void kv_add_int(pg_kv_t *kv, const char *key, long value)
{
    if (kv->n_owned >= (int)(sizeof(kv->owned) / sizeof(kv->owned[0]))) return;
    char *s = g_strdup_printf("%ld", value);
    kv->owned[kv->n_owned++] = s;
    kv_add(kv, key, s);
}

static void kv_free(pg_kv_t *kv)
{
    for (int i = 0; i < kv->n_owned; i++) g_free(kv->owned[i]);
}

/*
 * sslmode from the driver's two SSL knobs.
 *
 * SSL=0 means "do not use TLS" — `disable`, not libpq's default `prefer`,
 * because a user who turned it off is entitled to know the session really is
 * plaintext. SSL=1 with SSLVerify=1 (the driver default) is `verify-full`:
 * anything weaker encrypts without authenticating, which is the failure mode
 * TLS exists to prevent. SSLVerify=0 downgrades to `require`.
 */
static const char *ssl_mode_for(const argus_dbc_t *dbc)
{
    /* SSLMODE, when given, is passed to libpq verbatim. It is the only way to
     * express `prefer`, `allow` or `verify-ca`, which the two-knob SSL/SSLVerify
     * pair cannot distinguish, and it is the spelling every PostgreSQL user and
     * every Tableau PostgreSQL connector already knows. */
    if (dbc && dbc->pg_sslmode && *dbc->pg_sslmode) return dbc->pg_sslmode;

    if (!dbc || !dbc->ssl_enabled) return "disable";
    if (!dbc->ssl_verify)          return "require";
    /* verify-full needs a CA to verify against; without one libpq falls back
     * to the system store, which is what a user supplying no CA expects. */
    return "verify-full";
}

/*
 * A per-connection switch: the connection string or DSN wins, and the
 * environment variable is the machine-wide fallback for an operator who wants
 * to flip a behaviour without editing every DSN.
 */
static bool pg_switch(int dbc_value, const char *env_name)
{
    if (dbc_value >= 0) return dbc_value != 0;
    const char *env = g_getenv(env_name);
    return env && (*env == '1' || *env == 't' || *env == 'T' ||
                   *env == 'y' || *env == 'Y');
}

int pg_connect(const pg_profile_t *profile, argus_dbc_t *dbc,
               const char *host, int port,
               const char *username, const char *password,
               const char *database, const char *auth_mechanism,
               argus_backend_conn_t *out_conn)
{
    if (!out_conn || !profile) return -1;

    pg_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) return -1;

    conn->profile  = profile;
    conn->declared = profile->engine;
    conn->detected = profile->engine;
    conn->fetch_batch = (dbc && dbc->fetch_buffer_size > 0)
                        ? dbc->fetch_buffer_size : ARGUS_DEFAULT_BATCH_SIZE;
    /* PostgreSQL's own default, and ODBC's: SQL_AUTOCOMMIT_ON. */
    conn->autocommit = true;

    conn->show_partitions =
        pg_switch(dbc ? dbc->pg_show_partitions : -1,
                  "ARGUS_PG_SHOW_PARTITIONS");
    conn->show_all_databases =
        pg_switch(dbc ? dbc->pg_show_all_databases : -1,
                  "ARGUS_PG_SHOW_ALL_DATABASES");
    conn->row_versioning =
        pg_switch(dbc ? dbc->pg_row_versioning : -1,
                  "ARGUS_PG_ROW_VERSIONING");

    pg_kv_t kv = {0};

    /* HOST may be a comma-separated failover list; libpq consumes that shape
     * natively, so it is passed straight through. */
    kv_add(&kv, "host", host && *host ? host : "localhost");
    kv_add_int(&kv, "port", port > 0 ? port : PG_DEFAULT_PORT);
    kv_add(&kv, "user", username);
    kv_add(&kv, "password", password);
    kv_add(&kv, "dbname", effective_dbname(database));

    kv_add(&kv, "sslmode", ssl_mode_for(dbc));
    if (dbc && dbc->ssl_enabled) {
        kv_add(&kv, "sslrootcert", dbc->ssl_ca_file);
        kv_add(&kv, "sslcert",     dbc->ssl_cert_file);
        kv_add(&kv, "sslkey",      dbc->ssl_key_file);
    }

    if (dbc && dbc->connect_timeout_sec > 0)
        kv_add_int(&kv, "connect_timeout", dbc->connect_timeout_sec);

    /* A dead peer must not hang a BI extract forever. tcp_user_timeout is the
     * precise control (Linux/Windows); keepalives cover the rest. */
    if (dbc && dbc->socket_timeout_sec > 0) {
        kv_add_int(&kv, "tcp_user_timeout", (long)dbc->socket_timeout_sec * 1000);
        kv_add(&kv, "keepalives", "1");
        kv_add_int(&kv, "keepalives_idle", dbc->socket_timeout_sec);
    }

    kv_add(&kv, "application_name",
           (dbc && dbc->app_name && *dbc->app_name) ? dbc->app_name : "Argus ODBC");

    /* Kerberos. libpq does GSSAPI on Linux/macOS and SSPI on Windows itself,
     * so unlike Hive/Impala there is no SASL code here — only the SPN service
     * name, which defaults to "postgres" and is overridable for load
     * balancers that present a different principal. */
    if (auth_mechanism && strcasecmp(auth_mechanism, "KERBEROS") == 0) {
        kv_add(&kv, "krbsrvname",
               (dbc && dbc->krb_service_name && *dbc->krb_service_name)
               ? dbc->krb_service_name : "postgres");
        /* Ask for GSSAPI encryption when TLS is not already doing the job. */
        if (!dbc || !dbc->ssl_enabled) kv_add(&kv, "gssencmode", "prefer");
    }

    kv.keys[kv.n] = NULL;
    kv.vals[kv.n] = NULL;

    conn->pg = PQconnectdbParams(kv.keys, kv.vals, 0);
    kv_free(&kv);

    if (!conn->pg || PQstatus(conn->pg) != CONNECTION_OK) {
        if (conn->pg) {
            pg_fail(conn, NULL, "08001");
            pg_push_diag(dbc, conn, "08001");
            PQfinish(conn->pg);
        } else if (dbc) {
            argus_set_error(&dbc->diag, "08001",
                            "[Argus][PostgreSQL] out of memory allocating connection", 0);
        }
        free(conn);
        return -1;
    }

    /* Full Unicode over the wire, whatever the server's own encoding is. */
    PQsetClientEncoding(conn->pg, "UTF8");

    pg_probe_identity(conn);
    pg_oidmap_prime(conn);

    /*
     * A declared/detected mismatch is a warning, not a failure. The connection
     * works; what will not work is the engine-specific half of the catalog —
     * BACKEND=greenplum against plain PostgreSQL finds no gp_distribution_policy
     * and silently returns less than the user expects. Saying so once at
     * connect time is much cheaper than debugging an empty Tableau navigator.
     */
    if (dbc && conn->detected != conn->declared) {
        char msg[ARGUS_MAX_MESSAGE_LEN];
        snprintf(msg, sizeof(msg),
                 "[Argus][PostgreSQL] BACKEND=%s was requested but the server "
                 "identifies as %s; engine-specific catalog metadata will be "
                 "unavailable. Use BACKEND=%s instead.",
                 pg_engine_backend_name(conn->declared),
                 pg_engine_name(conn->detected),
                 pg_engine_backend_name(conn->detected));
        argus_diag_push(&dbc->diag, "01000", msg, 0);
        ARGUS_LOG_WARN("%s", msg);
    }

    /* An explicit search_path, so unqualified names resolve the way the DSN
     * says rather than the way the role's default happens to. Sent through
     * set_config() with a bound value: a schema list is user input, and the
     * SET statement has no parameter form. */
    if (dbc && dbc->pg_search_path && *dbc->pg_search_path) {
        const char *vals[1] = { dbc->pg_search_path };
        PGresult *r = PQexecParams(conn->pg,
                                   "SELECT set_config('search_path', $1, false)",
                                   1, NULL, vals, NULL, NULL, 0);
        if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
            if (r) {
                pg_fail(conn, r, "HY000");
                PQclear(r);
            }
            ARGUS_LOG_WARN("PostgreSQL: could not set search_path to '%s'",
                           dbc->pg_search_path);
            conn->last_error[0] = '\0';
            conn->last_sqlstate[0] = '\0';
        } else {
            PQclear(r);
        }
    }

    /* QueryTimeout is a server-side setting in PostgreSQL, which is strictly
     * better than a client-side clock: the server stops doing the work. */
    if (dbc && dbc->query_timeout_sec > 0) {
        char sql[96];
        snprintf(sql, sizeof(sql), "SET statement_timeout = %d",
                 dbc->query_timeout_sec * 1000);
        PGresult *r = PQexec(conn->pg, sql);
        if (r) PQclear(r);
    }

    conn->database = strdup(effective_dbname(database));

    *out_conn = conn;
    return 0;
}

void pg_disconnect(argus_backend_conn_t raw_conn)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn) return;
    if (conn->pg) PQfinish(conn->pg);
    free(conn->database);
    pg_oidmap_free(conn);
    g_free(conn->mpp_remarks_expr);
    g_free(conn->mpp_tables_filter);
    free(conn);
}

bool pg_is_alive(argus_backend_conn_t raw_conn)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || !conn->pg) return false;

    if (PQstatus(conn->pg) != CONNECTION_OK) return false;

    /*
     * PQstatus alone is not enough: it reports the last known state, so a
     * connection the server closed while it sat in the pool still reads
     * CONNECTION_OK. A cheap round trip is what the pool actually needs.
     */
    PGresult *res = PQexec(conn->pg, "");
    ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (res) PQclear(res);
    /* Drain anything else the empty query produced. */
    PGresult *extra;
    while ((extra = PQgetResult(conn->pg)) != NULL) PQclear(extra);

    return st == PGRES_EMPTY_QUERY && PQstatus(conn->pg) == CONNECTION_OK;
}
