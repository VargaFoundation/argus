/* SPDX-License-Identifier: Apache-2.0 */
#include "pg_common.h"
#include "argus/compat.h"
#include "argus/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Engine identity.
 *
 * Greenplum and Cloudberry are PostgreSQL forks: they answer the same wire
 * protocol, they report a PostgreSQL version in server_version, and libpq
 * cannot tell them apart. But almost everything the driver does differently
 * for them — hiding child partitions from SQLTables, reading distribution
 * keys, choosing between GP6's pg_partitions and GP7's declarative
 * partitioning — depends on knowing which one answered, and which major
 * version it is.
 *
 * So one round trip at connect time settles it, and the answer is cached on
 * the connection. version() carries the fork's own name in parentheses:
 *
 *   PostgreSQL 16.2 (Ubuntu ...)                        → plain PostgreSQL
 *   PostgreSQL 12.12 (Greenplum Database 7.1.0 build 1) → Greenplum 7
 *   PostgreSQL 9.4.26 (Greenplum Database 6.25.3 ...)   → Greenplum 6
 *   PostgreSQL 14.4 (Apache Cloudberry 2.0.0 ...)       → Cloudberry 2
 *
 * "Ubuntu" and friends in the parentheses are packaging strings, so the match
 * is on the fork names specifically, not on "there are parentheses".
 */

const char *pg_engine_name(pg_engine_t engine)
{
    switch (engine) {
    case PG_ENGINE_GREENPLUM:  return "Greenplum Database";
    case PG_ENGINE_CLOUDBERRY: return "Apache Cloudberry";
    case PG_ENGINE_POSTGRES:
    default:                   return "PostgreSQL";
    }
}

const char *pg_engine_backend_name(pg_engine_t engine)
{
    switch (engine) {
    case PG_ENGINE_GREENPLUM:  return "greenplum";
    case PG_ENGINE_CLOUDBERRY: return "cloudberry";
    case PG_ENGINE_POSTGRES:
    default:                   return "postgres";
    }
}

/* Case-insensitive substring search — strcasestr is not portable. */
static const char *find_ci(const char *haystack, const char *needle)
{
    if (!haystack || !needle || !*needle) return NULL;
    size_t nlen = strlen(needle);
    for (const char *p = haystack; *p; p++)
        if (strncasecmp(p, needle, nlen) == 0) return p;
    return NULL;
}

/* Read the first integer of a dotted version starting at `p` (which points at
 * the marker); returns 0 when no version follows. */
static int version_after(const char *p, const char *marker,
                         char *out, size_t outlen)
{
    p += strlen(marker);
    while (*p == ' ') p++;
    if (*p < '0' || *p > '9') return 0;

    /* Copy the whole dotted version, e.g. "7.1.0". */
    size_t n = 0;
    const char *start = p;
    while ((*p >= '0' && *p <= '9') || *p == '.') p++;
    n = (size_t)(p - start);
    if (out && outlen > 0) {
        if (n >= outlen) n = outlen - 1;
        memcpy(out, start, n);
        out[n] = '\0';
    }
    return atoi(start);
}

int pg_probe_identity(pg_conn_t *conn)
{
    if (!conn || !conn->pg) return -1;

    /* Defaults, so a probe failure still leaves a usable connection. */
    conn->detected     = PG_ENGINE_POSTGRES;
    conn->pg_major     = 0;
    conn->engine_major = 0;
    conn->version_str[0] = '\0';

    /*
     * One round trip settles both questions: which engine this is, and which
     * of the fork-specific catalog objects it actually has. to_regclass
     * returns NULL rather than raising for a missing relation (9.4+, so
     * Greenplum 6 is covered), and pg_attribute answers the column questions,
     * so nothing here can fail on an engine that lacks any of it.
     */
    PGresult *res = PQexec(conn->pg,
        "SELECT version(), current_setting('server_version_num'), "
        "to_regclass('pg_catalog.gp_distribution_policy') IS NOT NULL, "
        "to_regclass('pg_catalog.pg_appendonly') IS NOT NULL, "
        /* pg_exttable is only useful if it has the column the REMARKS
         * fragment reads: Greenplum 5 called it location, 6 urilocation. */
        "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute "
                "WHERE attrelid = to_regclass('pg_catalog.pg_exttable') "
                "AND attname = 'urilocation' AND NOT attisdropped), "
        "to_regclass('pg_catalog.pg_partition_rule') IS NOT NULL, "
        "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute "
                "WHERE attrelid = 'pg_catalog.pg_class'::regclass "
                "AND attname = 'relispartition' AND NOT attisdropped), "
        "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute "
                "WHERE attrelid = 'pg_catalog.pg_class'::regclass "
                "AND attname = 'relam' AND NOT attisdropped), "
        /* Greenplum 5 called it attrnums, Greenplum 6 onward distkey. */
        "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute "
                "WHERE attrelid = to_regclass('pg_catalog.gp_distribution_policy') "
                "AND attname = 'distkey' AND NOT attisdropped)");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) PQclear(res);
        ARGUS_LOG_WARN("PostgreSQL: identity probe failed; "
                       "assuming plain PostgreSQL");
        return -1;
    }

    const char *banner = PQgetvalue(res, 0, 0);
    const char *vernum = PQgetvalue(res, 0, 1);

    conn->has_gp_policy          = (PQgetvalue(res, 0, 2)[0] == 't');
    conn->has_pg_appendonly      = (PQgetvalue(res, 0, 3)[0] == 't');
    conn->has_pg_exttable        = (PQgetvalue(res, 0, 4)[0] == 't');
    conn->has_partition_rule     = (PQgetvalue(res, 0, 5)[0] == 't');
    conn->has_relispartition     = (PQgetvalue(res, 0, 6)[0] == 't');
    conn->has_relam              = (PQgetvalue(res, 0, 7)[0] == 't');
    conn->has_gp_policy_distkey  = (PQgetvalue(res, 0, 8)[0] == 't');

    /* server_version_num is MMmmpp on 9.x (90426) and MMpppp from 10 on
     * (160015); dividing by 10000 yields the major in both encodings. */
    if (vernum && *vernum)
        conn->pg_major = atoi(vernum) / 10000;

    char engine_ver[64] = {0};
    const char *m;

    /* "Cloudberry Database 1.6.0" before the Apache incubation,
     * "Apache Cloudberry 2.0.0" after it. Try the longer marker first so the
     * version is read from the right place. */
    if ((m = find_ci(banner, "Cloudberry Database")) != NULL) {
        conn->detected = PG_ENGINE_CLOUDBERRY;
        conn->engine_major =
            version_after(m, "Cloudberry Database", engine_ver, sizeof(engine_ver));
    } else if ((m = find_ci(banner, "Cloudberry")) != NULL) {
        conn->detected = PG_ENGINE_CLOUDBERRY;
        conn->engine_major =
            version_after(m, "Cloudberry", engine_ver, sizeof(engine_ver));
    } else if ((m = find_ci(banner, "Greenplum Database")) != NULL) {
        conn->detected = PG_ENGINE_GREENPLUM;
        conn->engine_major =
            version_after(m, "Greenplum Database", engine_ver, sizeof(engine_ver));
    }

    if (conn->detected == PG_ENGINE_POSTGRES) {
        conn->engine_major = conn->pg_major;
        /* The bare PostgreSQL version, without the packaging suffix. */
        const char *p = banner;
        if (strncasecmp(p, "PostgreSQL ", 11) == 0) p += 11;
        size_t n = 0;
        while (p[n] && ((p[n] >= '0' && p[n] <= '9') || p[n] == '.')) n++;
        if (n >= sizeof(conn->version_str)) n = sizeof(conn->version_str) - 1;
        memcpy(conn->version_str, p, n);
        conn->version_str[n] = '\0';
    } else {
        snprintf(conn->version_str, sizeof(conn->version_str), "%s",
                 engine_ver[0] ? engine_ver : "0");
    }

    PQclear(res);

    ARGUS_LOG_INFO("PostgreSQL family: detected %s %s (PostgreSQL %d core)",
                   pg_engine_name(conn->detected),
                   conn->version_str, conn->pg_major);
    return 0;
}

/*
 * SQL_DBMS_VER. The engine's own version, not the PostgreSQL core version it
 * embeds: a BI tool gating on "Greenplum >= 7" must not be told "12.12".
 */
bool pg_get_server_version(argus_backend_conn_t raw_conn, char *buf, size_t buflen)
{
    pg_conn_t *conn = (pg_conn_t *)raw_conn;
    if (!conn || buflen == 0 || !conn->version_str[0]) return false;
    snprintf(buf, buflen, "%s", conn->version_str);
    return true;
}
