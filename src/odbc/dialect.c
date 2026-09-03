/*
 * Per-backend SQL dialect tables.
 *
 * These drive two things that must agree: what SQLGetInfo advertises
 * (SQL_STRING_FUNCTIONS & friends, SQL_IDENTIFIER_QUOTE_CHAR) and what
 * escape.c can actually translate. The bitmaps are derived from fn_map by
 * argus_dialect_fn_bitmap(), so a backend physically cannot advertise a
 * function it has no rendering for.
 *
 * Rule for adding entries: only add a function when the engine is *known* to
 * have it with ODBC's semantics. Under-claiming costs a little pushdown;
 * over-claiming makes the server reject a query the driver promised to handle.
 * ODBC's DAYOFWEEK is 1=Sunday..7=Saturday and LOCATE takes (needle, haystack)
 * — engines that differ need a fix-up in the template, or the entry is omitted.
 *
 * How far each table has been verified — this matters, because reading the
 * engine's documentation is demonstrably not enough:
 *
 *   trino     every entry executed against a live server and its value checked
 *             (tests/integration/test_bi_escapes.c). This is what caught
 *             repeat() returning an array rather than a string.
 *   pinot     every entry executed against a live Pinot QuickStart. Caught
 *             concat()'s separator third argument, strpos() being 0-based, and
 *             round() not being decimal rounding.
 *   mysql     every advertised entry executed against a live MariaDB 11, and
 *             the four intentionally-omitted functions confirmed refused by the
 *             driver rather than mis-sent (see the MySQL section below).
 *   bigquery  every entry executed against the goccy/bigquery-emulator from
 *             tests/integration/docker-compose.yml. One gap: the emulator's
 *             trunc() takes a single argument, so the 2-arg TRUNCATE form is
 *             unverified — real BigQuery documents TRUNC(X[, N]), so the entry
 *             stands rather than being narrowed to fit an emulator subset.
 *   hive      every entry executed against a live HiveServer2 (apache/hive
 *             3.1.3, NOSASL). Caught now(): Hive has no such function, it is
 *             current_timestamp().
 *   impala    every entry executed against a live Impala quickstart (4.5.0).
 *             Caught the TIMESTAMP literal: Impala rejects ANSI TIMESTAMP '…'
 *             and needs CAST('…' AS TIMESTAMP), so its literal style is
 *             ARGUS_LIT_CAST, not ANSI like Hive. Its integration tests use the
 *             SASL/Kerberos path, so they need a driver built with libgssapi;
 *             the dialect itself was verified through the NOSASL path.
 *   postgres  every entry executed against a live PostgreSQL 16 and its value
 *             checked (tests/integration/test_postgres_escapes.c, which covers
 *             the whole table rather than the shared subset test_bi_escapes
 *             probes). Caught round()/trunc(): PostgreSQL has the two-argument
 *             form only for numeric, so {fn ROUND(float_col, 1)} fails with
 *             42883 unless the template casts. Also confirmed log() is base 10
 *             while ln() is natural, that extract(second) returns a fractional
 *             numeric, and that to_char() blank-pads day and month names to
 *             nine characters without the FM prefix.
 *   greenplum, cloudberry
 *             NOT live-verified. Both are PostgreSQL forks (Greenplum 6 is
 *             PostgreSQL 9.4, Greenplum 7 is 12, Cloudberry is 14) and every
 *             entry in the shared table exists in 9.4, so the table is carried
 *             over from postgres — but neither engine has a maintained public
 *             container image that could be brought up here, so this is
 *             inference from lineage, which the Impala entry above shows is
 *             not sufficient on its own. Verify before relying on it:
 *             BI_BACKEND=greenplum BI_HOST=… ./test_bi_escapes, and
 *             PG_BACKEND=greenplum PG_HOST=… ./test_postgres_escapes.
 *   others    ansi_fns, see below.
 *
 * To verify one: bring up the backend from tests/integration/docker-compose.yml
 * and point test_bi_escapes at it with BI_BACKEND / BI_HOST / BI_PORT.
 */

#include <string.h>
#include <stddef.h>
#include "argus/dialect.h"
#include "argus/handle.h"

/* ── Trino ───────────────────────────────────────────────────────
 * Trino has no LEFT/RIGHT; substr() covers both (a negative start counts from
 * the end). day_of_week() is 1=Monday, so ODBC's 1=Sunday needs the rotation. */
static const argus_fn_entry_t trino_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    1, ARGUS_FN_VARIADIC, "concat($*)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substr($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "REPLACE",   ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPLACE,   3, 3, "replace($1, $2, $3)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "substr($1, 1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "substr($1, -($2))" },
    { "LOCATE",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LOCATE,    2, 2, "strpos($2, $1)" },
    /* Trino's repeat() builds an array, not a string, so string repetition has
     * to go back through array_join. */
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "array_join(repeat($1, $2), '')" },
    { "SPACE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_SPACE,     1, 1, "array_join(repeat(' ', $1), '')" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceiling($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "mod($1, $2)" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    1, 2, "round($*)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "ln($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log10($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "TRUNCATE",  ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TRUNCATE, 1, 2, "truncate($*)" },
    { "RAND",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_RAND,     0, 0, "random()" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "coalesce($1, $2)" },
    { "USERNAME",  ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_USERNAME,  0, 0, "current_user" },
    { "DBNAME",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_DBNAME,    0, 0, "current_catalog" },

    { "NOW",               ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_NOW,               0, 0, "current_timestamp" },
    { "CURDATE",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURDATE,           0, 0, "current_date" },
    { "CURRENT_DATE",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_DATE,      0, 0, "current_date" },
    { "CURRENT_TIME",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIME,      0, 0, "current_time" },
    { "CURRENT_TIMESTAMP", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIMESTAMP, 0, 0, "current_timestamp" },
    { "YEAR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_YEAR,       1, 1, "year($1)" },
    { "QUARTER",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_QUARTER,    1, 1, "quarter($1)" },
    { "MONTH",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTH,      1, 1, "month($1)" },
    { "WEEK",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_WEEK,       1, 1, "week($1)" },
    { "DAYOFMONTH", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFMONTH, 1, 1, "day($1)" },
    { "DAYOFYEAR",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFYEAR,  1, 1, "day_of_year($1)" },
    { "DAYOFWEEK",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFWEEK,  1, 1, "((day_of_week($1) % 7) + 1)" },
    { "HOUR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_HOUR,       1, 1, "hour($1)" },
    { "MINUTE",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MINUTE,     1, 1, "minute($1)" },
    { "SECOND",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_SECOND,     1, 1, "second($1)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── Hive ────────────────────────────────────────────────────────
 * HiveQL. No LEFT/RIGHT and no TRUNCATE before 2.2, so substr() stands in and
 * TRUNCATE is omitted. MOD uses the % operator, which both Hive and Impala have
 * (Hive's named function is pmod, whose negative-operand semantics differ from
 * ODBC's). dayofweek() is 1=Sunday, matching ODBC. */
static const argus_fn_entry_t hive_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    1, ARGUS_FN_VARIADIC, "concat($*)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substr($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "substr($1, 1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "substr($1, -($2))" },
    { "LOCATE",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LOCATE,    2, 3, "locate($*)" },
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "repeat($1, $2)" },
    { "SPACE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_SPACE,     1, 1, "space($1)" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceil($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "(($1) % ($2))" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    1, 2, "round($*)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "ln($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log10($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "RAND",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_RAND,     0, 0, "rand()" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "coalesce($1, $2)" },
    { "USERNAME",  ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_USERNAME,  0, 0, "current_user()" },
    { "DBNAME",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_DBNAME,    0, 0, "current_database()" },

    /* Hive has no now(): it is current_timestamp() (verified live — now()
     * raises a SemanticException). CURDATE/CURRENT_DATE take no parens. */
    { "NOW",               ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_NOW,               0, 0, "current_timestamp()" },
    { "CURDATE",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURDATE,           0, 0, "current_date" },
    { "CURRENT_DATE",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_DATE,      0, 0, "current_date" },
    { "CURRENT_TIMESTAMP", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIMESTAMP, 0, 0, "current_timestamp" },
    { "YEAR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_YEAR,       1, 1, "year($1)" },
    { "QUARTER",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_QUARTER,    1, 1, "quarter($1)" },
    { "MONTH",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTH,      1, 1, "month($1)" },
    { "WEEK",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_WEEK,       1, 1, "weekofyear($1)" },
    { "DAYOFMONTH", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFMONTH, 1, 1, "day($1)" },
    { "DAYOFWEEK",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFWEEK,  1, 1, "dayofweek($1)" },
    { "HOUR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_HOUR,       1, 1, "hour($1)" },
    { "MINUTE",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MINUTE,     1, 1, "minute($1)" },
    { "SECOND",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_SECOND,     1, 1, "second($1)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── Impala ──────────────────────────────────────────────────────
 * HiveQL-compatible, but Impala does have left()/right()/truncate()/dayofyear()
 * and spells the current user user(). */
static const argus_fn_entry_t impala_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    1, ARGUS_FN_VARIADIC, "concat($*)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substr($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "REPLACE",   ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPLACE,   3, 3, "replace($1, $2, $3)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "left($1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "right($1, $2)" },
    { "LOCATE",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LOCATE,    2, 3, "locate($*)" },
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "repeat($1, $2)" },
    { "SPACE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_SPACE,     1, 1, "space($1)" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceil($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "(($1) % ($2))" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    1, 2, "round($*)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "ln($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log10($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "TRUNCATE",  ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TRUNCATE, 1, 2, "truncate($*)" },
    { "RAND",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_RAND,     0, 0, "rand()" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "ifnull($1, $2)" },
    { "USERNAME",  ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_USERNAME,  0, 0, "user()" },
    { "DBNAME",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_DBNAME,    0, 0, "current_database()" },

    { "NOW",               ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_NOW,               0, 0, "now()" },
    { "CURDATE",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURDATE,           0, 0, "current_date()" },
    { "CURRENT_DATE",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_DATE,      0, 0, "current_date()" },
    { "CURRENT_TIMESTAMP", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIMESTAMP, 0, 0, "current_timestamp()" },
    { "YEAR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_YEAR,       1, 1, "year($1)" },
    { "QUARTER",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_QUARTER,    1, 1, "quarter($1)" },
    { "MONTH",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTH,      1, 1, "month($1)" },
    { "WEEK",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_WEEK,       1, 1, "weekofyear($1)" },
    { "DAYOFMONTH", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFMONTH, 1, 1, "dayofmonth($1)" },
    { "DAYOFYEAR",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFYEAR,  1, 1, "dayofyear($1)" },
    { "DAYOFWEEK",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFWEEK,  1, 1, "dayofweek($1)" },
    { "HOUR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_HOUR,       1, 1, "hour($1)" },
    { "MINUTE",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MINUTE,     1, 1, "minute($1)" },
    { "SECOND",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_SECOND,     1, 1, "second($1)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── MySQL wire ──────────────────────────────────────────────────
 * One dialect for four engines: MariaDB, StarRocks, Doris and ClickHouse. The
 * first three are MySQL-syntax-compatible; ClickHouse only aliases part of the
 * MySQL surface, so anything it spells differently or with different semantics
 * is left out rather than guessed:
 *   LOCATE     — ClickHouse's takes (haystack, needle), the reverse of ODBC's
 *   RAND       — ClickHouse's returns a UInt32, not a fraction in [0,1)
 *   DAYOFWEEK  — ClickHouse's is 1=Monday, ODBC wants 1=Sunday
 *   SPACE      — not aliased by ClickHouse
 *   DBNAME/USERNAME — ClickHouse spells these currentDatabase()/currentUser()
 * Splitting this into per-engine dialects is the real fix, but it needs the
 * backend to report which engine it reached.
 *
 * Verified against a live MariaDB 11: all 35 advertised entries execute and
 * return the right value, and the four omitted functions above are refused by
 * the driver (42000) rather than sent to the server. The omission does cost
 * MariaDB/StarRocks/Doris the LOCATE/SPACE/DAYOFWEEK pushdown they would in
 * fact accept — that is the price of one shared table, paid so ClickHouse never
 * receives a query that means something different there. */
static const argus_fn_entry_t mywire_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    1, ARGUS_FN_VARIADIC, "concat($*)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substring($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "REPLACE",   ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPLACE,   3, 3, "replace($1, $2, $3)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "left($1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "right($1, $2)" },
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "repeat($1, $2)" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceiling($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "mod($1, $2)" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    1, 2, "round($*)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "log($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log10($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "TRUNCATE",  ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TRUNCATE, 2, 2, "truncate($1, $2)" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "ifnull($1, $2)" },

    { "NOW",               ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_NOW,               0, 0, "now()" },
    { "CURDATE",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURDATE,           0, 0, "curdate()" },
    { "CURRENT_DATE",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_DATE,      0, 0, "current_date" },
    { "CURRENT_TIMESTAMP", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIMESTAMP, 0, 0, "current_timestamp" },
    { "YEAR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_YEAR,       1, 1, "year($1)" },
    { "QUARTER",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_QUARTER,    1, 1, "quarter($1)" },
    { "MONTH",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTH,      1, 1, "month($1)" },
    { "DAYOFMONTH", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFMONTH, 1, 1, "dayofmonth($1)" },
    { "DAYOFYEAR",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFYEAR,  1, 1, "dayofyear($1)" },
    { "HOUR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_HOUR,       1, 1, "hour($1)" },
    { "MINUTE",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MINUTE,     1, 1, "minute($1)" },
    { "SECOND",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_SECOND,     1, 1, "second($1)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── BigQuery ────────────────────────────────────────────────────
 * GoogleSQL. Date parts go through EXTRACT; BigQuery's DAYOFWEEK is 1=Sunday,
 * which is already ODBC's convention. */
static const argus_fn_entry_t bigquery_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    1, ARGUS_FN_VARIADIC, "concat($*)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substr($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "REPLACE",   ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPLACE,   3, 3, "replace($1, $2, $3)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "left($1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "right($1, $2)" },
    { "LOCATE",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LOCATE,    2, 2, "strpos($2, $1)" },
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "repeat($1, $2)" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceil($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "mod($1, $2)" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    1, 2, "round($*)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "ln($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log10($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "TRUNCATE",  ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TRUNCATE, 1, 2, "trunc($*)" },
    { "RAND",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_RAND,     0, 0, "rand()" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "ifnull($1, $2)" },
    { "USERNAME",  ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_USERNAME,  0, 0, "session_user()" },

    { "NOW",               ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_NOW,               0, 0, "current_timestamp()" },
    { "CURDATE",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURDATE,           0, 0, "current_date()" },
    { "CURRENT_DATE",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_DATE,      0, 0, "current_date()" },
    { "CURRENT_TIME",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIME,      0, 0, "current_time()" },
    { "CURRENT_TIMESTAMP", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIMESTAMP, 0, 0, "current_timestamp()" },
    { "YEAR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_YEAR,       1, 1, "extract(year from $1)" },
    { "QUARTER",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_QUARTER,    1, 1, "extract(quarter from $1)" },
    { "MONTH",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTH,      1, 1, "extract(month from $1)" },
    { "WEEK",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_WEEK,       1, 1, "extract(week from $1)" },
    { "DAYOFMONTH", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFMONTH, 1, 1, "extract(day from $1)" },
    { "DAYOFYEAR",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFYEAR,  1, 1, "extract(dayofyear from $1)" },
    { "DAYOFWEEK",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFWEEK,  1, 1, "extract(dayofweek from $1)" },
    { "HOUR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_HOUR,       1, 1, "extract(hour from $1)" },
    { "MINUTE",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MINUTE,     1, 1, "extract(minute from $1)" },
    { "SECOND",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_SECOND,     1, 1, "extract(second from $1)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── Apache Pinot ────────────────────────────────────────────────
 * Verified function by function against a live Pinot QuickStart. Pinot is
 * Calcite-based, and that made three things look obvious and be wrong:
 *
 *   concat()  its third argument is a SEPARATOR, not a third string:
 *             concat('a','b','-') is "a-b". ODBC's variadic CONCAT therefore
 *             maps only to the 2-argument form — a variadic mapping would have
 *             produced wrong values with no error at all.
 *   strpos()  0-based, and -1 when absent. ODBC's LOCATE is 1-based and 0 when
 *             absent, which the +1 turns out to satisfy exactly, including the
 *             not-found case.
 *   round()   NOT decimal rounding: round(3.456, 1) is 3 (nearest multiple).
 *             roundDecimal() is the one that means what ODBC's ROUND means.
 *
 * substr() is 0-based with an exclusive end and is deliberately unused;
 * substring() is 1-based with a length, exactly ODBC's SUBSTRING.
 *
 * No date/time entries: Pinot's year()/month()/hour() take epoch millis rather
 * than a temporal value, CURRENT_DATE and CURRENT_TIMESTAMP do not resolve at
 * all, and now() returns millis rather than a timestamp. None of them match
 * ODBC's semantics, so none are advertised. RAND is absent (no random()). */
static const argus_fn_entry_t pinot_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    2, 2, "concat($1, $2)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substring($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "REPLACE",   ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPLACE,   3, 3, "replace($1, $2, $3)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "left($1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "right($1, $2)" },
    { "LOCATE",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LOCATE,    2, 2, "(strpos($2, $1) + 1)" },
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "repeat($1, $2)" },
    { "SPACE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_SPACE,     1, 1, "repeat(' ', $1)" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceil($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "mod($1, $2)" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    2, 2, "roundDecimal($1, $2)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "ln($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log10($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "coalesce($1, $2)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── ANSI fallback ───────────────────────────────────────────────
 * Used by phoenix, druid, flightsql and kudu, and by any backend we don't
 * know. Deliberately tiny. These backends previously advertised all 48 scalar
 * functions and could translate none of them; a short honest list is strictly
 * better than a long false one.
 *
 * It used to also claim CURRENT_DATE and CURRENT_TIMESTAMP on the grounds that
 * they are SQL-92 and every engine here is Calcite-based. Probing Pinot proved
 * that reasoning wrong — it resolves neither — so the fallback now claims only
 * what is genuinely universal. That is the whole lesson of this file: an
 * engine's lineage is not evidence about its function set.
 *
 * Extend per backend with its own table once its functions have been verified
 * against a live server (see pinot_fns above); the derived bitmaps follow.
 *
 * Phoenix is the next candidate: a viable Query Server image exists
 * (boostport/hbase-phoenix-all-in-one, see docker-compose.yml), but its PQS
 * defaults to Avatica PROTOBUF while this driver speaks Avatica JSON, so live
 * probing needs a JSON-configured server first. Until then Phoenix stays here. */
static const argus_fn_entry_t ansi_fns[] = {
    { "UCASE",  ARGUS_FN_GROUP_STRING,  SQL_FN_STR_UCASE,  1, 1, "UPPER($1)" },
    { "LCASE",  ARGUS_FN_GROUP_STRING,  SQL_FN_STR_LCASE,  1, 1, "LOWER($1)" },
    { "ABS",    ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,    1, 1, "ABS($1)" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── PostgreSQL ──────────────────────────────────────────────────
 * Shared by the postgres, greenplum and cloudberry backends: all three are the
 * same SQL, and every entry below exists in PostgreSQL 9.4, which is what
 * Greenplum 6 is built on (GP7 is PG 12, Cloudberry is PG 14). The three
 * dialects are separate registry entries all the same, so a per-engine
 * divergence can be expressed the day one appears without moving anything.
 *
 * The traps this table has to get right, none of which is guessable:
 *   ROUND/TRUNCATE  PostgreSQL only has the two-argument form for `numeric`;
 *                   round(double precision, int) does not exist and raises
 *                   "function round(double precision, integer) does not
 *                   exist". Casting the first argument is what makes {fn
 *                   ROUND(col, 2)} work on a float column. ODBC's own
 *                   signature is two mandatory arguments, so nothing is lost.
 *   MOD             mod() exists for integer and numeric but not for double
 *                   precision; ODBC's MOD is specified over integers.
 *   LOG / LOG10     PostgreSQL's one-argument log() is base 10 and ln() is
 *                   natural — the reverse of the naming in most engines.
 *   LOCATE          ODBC is LOCATE(needle, haystack); strpos() is
 *                   strpos(haystack, needle). Both are 1-based and both return
 *                   0 when absent, so the swap is the whole fix — no offset.
 *   DAYOFWEEK       extract(dow) is 0=Sunday..6=Saturday, ODBC is
 *                   1=Sunday..7=Saturday.
 *   DAYNAME/MONTHNAME  to_char() blank-pads to 9 characters without the FM
 *                   prefix, which shows up as trailing spaces in a report.
 *   WEEK            deliberately absent: extract(week) is ISO-8601, so
 *                   2021-01-01 is week 53 (of 2020), while ODBC's WEEK puts
 *                   January 1st in week 1. Advertising it would silently
 *                   produce wrong week numbers, which is worse than the
 *                   function being refused.
 */
static const argus_fn_entry_t postgres_fns[] = {
    { "CONCAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_CONCAT,    1, ARGUS_FN_VARIADIC, "concat($*)" },
    { "LENGTH",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LENGTH,    1, 1, "length($1)" },
    { "SUBSTRING", ARGUS_FN_GROUP_STRING, SQL_FN_STR_SUBSTRING, 2, 3, "substring($*)" },
    { "LTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LTRIM,     1, 1, "ltrim($1)" },
    { "RTRIM",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RTRIM,     1, 1, "rtrim($1)" },
    { "LCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_LCASE,     1, 1, "lower($1)" },
    { "UCASE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_UCASE,     1, 1, "upper($1)" },
    { "REPLACE",   ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPLACE,   3, 3, "replace($1, $2, $3)" },
    { "LEFT",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_LEFT,      2, 2, "left($1, $2)" },
    { "RIGHT",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_RIGHT,     2, 2, "right($1, $2)" },
    { "LOCATE",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_LOCATE,    2, 2, "strpos($2, $1)" },
    { "REPEAT",    ARGUS_FN_GROUP_STRING, SQL_FN_STR_REPEAT,    2, 2, "repeat($1, $2)" },
    { "SPACE",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_SPACE,     1, 1, "repeat(' ', $1)" },
    { "ASCII",     ARGUS_FN_GROUP_STRING, SQL_FN_STR_ASCII,     1, 1, "ascii($1)" },
    { "CHAR",      ARGUS_FN_GROUP_STRING, SQL_FN_STR_CHAR,      1, 1, "chr($1)" },

    { "ABS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ABS,      1, 1, "abs($1)" },
    { "CEILING",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_CEILING,  1, 1, "ceiling($1)" },
    { "FLOOR",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_FLOOR,    1, 1, "floor($1)" },
    { "MOD",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_MOD,      2, 2, "mod($1, $2)" },
    { "ROUND",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ROUND,    2, 2, "round(($1)::numeric, $2)" },
    { "TRUNCATE",  ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TRUNCATE, 2, 2, "trunc(($1)::numeric, $2)" },
    { "SQRT",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SQRT,     1, 1, "sqrt($1)" },
    { "POWER",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_POWER,    2, 2, "power($1, $2)" },
    { "LOG",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG,      1, 1, "ln($1)" },
    { "LOG10",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_LOG10,    1, 1, "log($1)" },
    { "EXP",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_EXP,      1, 1, "exp($1)" },
    { "SIGN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIGN,     1, 1, "sign($1)" },
    { "PI",        ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_PI,       0, 0, "pi()" },
    { "RAND",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_RAND,     0, 0, "random()" },
    { "ACOS",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ACOS,     1, 1, "acos($1)" },
    { "ASIN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ASIN,     1, 1, "asin($1)" },
    { "ATAN",      ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN,     1, 1, "atan($1)" },
    { "ATAN2",     ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_ATAN2,    2, 2, "atan2($1, $2)" },
    { "COS",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COS,      1, 1, "cos($1)" },
    { "COT",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_COT,      1, 1, "cot($1)" },
    { "SIN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_SIN,      1, 1, "sin($1)" },
    { "TAN",       ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_TAN,      1, 1, "tan($1)" },
    { "DEGREES",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_DEGREES,  1, 1, "degrees($1)" },
    { "RADIANS",   ARGUS_FN_GROUP_NUMERIC, SQL_FN_NUM_RADIANS,  1, 1, "radians($1)" },

    { "IFNULL",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_IFNULL,    2, 2, "coalesce($1, $2)" },
    { "USERNAME",  ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_USERNAME,  0, 0, "current_user" },
    { "DBNAME",    ARGUS_FN_GROUP_SYSTEM, SQL_FN_SYS_DBNAME,    0, 0, "current_database()" },

    { "NOW",               ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_NOW,               0, 0, "current_timestamp" },
    { "CURDATE",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURDATE,           0, 0, "current_date" },
    { "CURRENT_DATE",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_DATE,      0, 0, "current_date" },
    { "CURTIME",           ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURTIME,           0, 0, "current_time" },
    { "CURRENT_TIME",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIME,      0, 0, "current_time" },
    { "CURRENT_TIMESTAMP", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_CURRENT_TIMESTAMP, 0, 0, "current_timestamp" },
    { "YEAR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_YEAR,       1, 1, "extract(year from ($1))" },
    { "QUARTER",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_QUARTER,    1, 1, "extract(quarter from ($1))" },
    { "MONTH",      ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTH,      1, 1, "extract(month from ($1))" },
    { "DAYOFMONTH", ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFMONTH, 1, 1, "extract(day from ($1))" },
    { "DAYOFYEAR",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFYEAR,  1, 1, "extract(doy from ($1))" },
    { "DAYOFWEEK",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYOFWEEK,  1, 1, "(extract(dow from ($1)) + 1)" },
    { "HOUR",       ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_HOUR,       1, 1, "extract(hour from ($1))" },
    { "MINUTE",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MINUTE,     1, 1, "extract(minute from ($1))" },
    { "SECOND",     ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_SECOND,     1, 1, "floor(extract(second from ($1)))" },
    { "DAYNAME",    ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_DAYNAME,    1, 1, "to_char(($1), 'FMDay')" },
    { "MONTHNAME",  ARGUS_FN_GROUP_TIMEDATE, SQL_FN_TD_MONTHNAME,  1, 1, "to_char(($1), 'FMMonth')" },

    { NULL, 0, 0, 0, 0, NULL }
};

/* ── Dialect registry ────────────────────────────────────────────
 * The quote characters keep the values (and the reasoning) that info.c used:
 * HiveQL, the MySQL wire dialect and BigQuery quote identifiers with a
 * backtick; Trino, Phoenix, Pinot, Druid, Flight SQL and Kudu follow the ANSI
 * double quote. A wrong value makes every generated query fail on the server.
 *
 * Every backend renders {d}/{ts} as SQL-92 literals (DATE '...'), and
 * SQL_DATETIME_LITERALS is reported from this field so the two cannot
 * disagree. Verified for trino, bigquery and pinot; assumed for the rest.
 * "Assumed" is a real caveat here rather than a formality — Pinot accepts
 * DATE '...' yet rejects CURRENT_DATE, so an engine's SQL-92 coverage is not
 * all-or-nothing and cannot be inferred from its lineage. */
static const argus_dialect_t argus_dialects[] = {
    { "trino",    "\"", ARGUS_LIT_ANSI, true,  false, false, trino_fns, NULL },
    { "hive",     "`",  ARGUS_LIT_ANSI, true,  true,  false, hive_fns, NULL },
    /* Impala rejects the ANSI TIMESTAMP '…' literal (ParseException) but accepts
     * CAST('…' AS TIMESTAMP), and CAST works for DATE too — verified live. */
    { "impala",   "`",  ARGUS_LIT_CAST, true,  true,  false, impala_fns, NULL },
    { "mysql",    "`",  ARGUS_LIT_ANSI, true,  true,  false, mywire_fns, NULL },
    { "bigquery", "`",  ARGUS_LIT_ANSI, true,  true,  false, bigquery_fns, NULL },
    { "phoenix",  "\"", ARGUS_LIT_ANSI, false, false, false, ansi_fns, NULL },
    { "pinot",    "\"", ARGUS_LIT_ANSI, false, false, false, pinot_fns, NULL },
    { "druid",    "\"", ARGUS_LIT_ANSI, false, false, false, ansi_fns, NULL },
    { "flightsql","\"", ARGUS_LIT_ANSI, false, false, false, ansi_fns, NULL },
    { "kudu",     "\"", ARGUS_LIT_ANSI, false, false, false, ansi_fns, NULL },
    /* Greenplum and Cloudberry share PostgreSQL's table today; they are listed
     * separately so a divergence can be expressed without a structural change,
     * and so each carries its own verification provenance (see the header).
     *
     * backslash_escapes is false: standard_conforming_strings has defaulted to
     * on since PostgreSQL 9.1, so '\' inside a literal is an ordinary
     * character and doubling it would turn 'C:\path' into 'C:\\path'.
     * Verified on PostgreSQL 16 — SHOW standard_conforming_strings is on and
     * length('C:\path') is 7. Greenplum 6 derives from PostgreSQL 9.4 and
     * Cloudberry from 14, so both are on the same side of that default.
     * pg_strings is what marks the two forms that do escape or quote on this
     * family regardless: E'...' strings and $tag$...$tag$ dollar quoting.
     *
     * The call template renders {call f(a,b)} as SELECT * FROM f(a,b), which is
     * what psqlODBC does and what an ODBC application means by it: the thing
     * that returns a result set in PostgreSQL is a *function*. PostgreSQL 11's
     * CALL statement is for procedures, which return nothing, and no BI tool
     * asks for one through {call}. This is also what makes SQL_PROCEDURES
     * answer "Y" for these backends — the info type promises the invocation
     * syntax works, and now it does. */
    { "postgres",  "\"", ARGUS_LIT_ANSI, true, false, true, postgres_fns, "SELECT * FROM $1" },
    { "greenplum", "\"", ARGUS_LIT_ANSI, true, false, true, postgres_fns, "SELECT * FROM $1" },
    { "cloudberry","\"", ARGUS_LIT_ANSI, true, false, true, postgres_fns, "SELECT * FROM $1" },
};

static const argus_dialect_t argus_ansi_dialect = {
    "ansi", "\"", ARGUS_LIT_ANSI, false, false, false, ansi_fns, NULL
};

#define ARGUS_DIALECT_COUNT (sizeof(argus_dialects) / sizeof(argus_dialects[0]))

const argus_dialect_t *argus_dialect_by_name(const char *backend_name)
{
    if (backend_name) {
        for (size_t i = 0; i < ARGUS_DIALECT_COUNT; i++) {
            if (strcmp(argus_dialects[i].name, backend_name) == 0)
                return &argus_dialects[i];
        }
    }
    return &argus_ansi_dialect;
}

const argus_dialect_t *argus_dialect_for(const argus_dbc_t *dbc)
{
    if (dbc && dbc->backend && dbc->backend->name)
        return argus_dialect_by_name(dbc->backend->name);
    return &argus_ansi_dialect;
}

/* ── Lexical helpers ──────────────────────────────────────────── */

/* A byte that can be part of an identifier: '$' after one is not a dollar
 * quote, and an E before a quote is a keyword only when it stands alone. */
static bool ident_char(char c)
{
    unsigned char u = (unsigned char)c;
    return (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') ||
           (u >= '0' && u <= '9') || u == '_' || u >= 0x80;
}

const char *argus_sql_skip_quoted(const char *text, const char *p,
                                  const argus_dialect_t *dialect)
{
    char quote = *p;
    char ident = dialect->quote_char && dialect->quote_char[0]
                 ? dialect->quote_char[0] : '"';
    bool backslash;

    if (quote == '\'') {
        /* A string literal everywhere. E'...' is PostgreSQL's escape string;
         * the E is a keyword, not the tail of an identifier (tablee'x'). */
        backslash = dialect->backslash_escapes ||
                    (dialect->pg_strings && p > text &&
                     (p[-1] == 'E' || p[-1] == 'e') &&
                     (p - 1 == text || !ident_char(p[-2])));
    } else if (quote == '"') {
        /* An identifier where the dialect quotes identifiers with it, a
         * string literal (Hive, MySQL wire, BigQuery) where it does not. */
        backslash = ident != '"' && dialect->backslash_escapes;
    } else {
        backslash = false;   /* a backtick identifier: doubling only */
    }

    for (p++; *p; p++) {
        if (backslash && *p == '\\') {
            if (!p[1]) return NULL;   /* the escape has nothing to escape */
            p++;                       /* the escaped byte, whatever it is */
            continue;
        }
        if (*p == quote) {
            if (p[1] == quote) { p++; continue; }   /* doubled: part of it */
            return p + 1;
        }
    }
    return NULL;
}

const char *argus_sql_skip_dollar_quoted(const char *text, const char *p,
                                         const argus_dialect_t *dialect)
{
    if (!dialect->pg_strings || *p != '$') return p;
    /* Part of an identifier (a$b), not a delimiter. */
    if (p > text && ident_char(p[-1])) return p;

    /* $$ or $tag$, tag being an identifier that does not start with a
     * digit: $1 is a parameter reference. */
    const char *t = p + 1;
    if (*t != '$') {
        if (!ident_char(*t) || (*t >= '0' && *t <= '9')) return p;
        while (ident_char(*t)) t++;
        if (*t != '$') return p;
    }
    size_t delim_len = (size_t)(t + 1 - p);

    for (const char *q = t + 1; *q; q++) {
        if (*q == '$' && strncmp(q, p, delim_len) == 0)
            return q + delim_len;
    }
    return NULL;
}

const char *argus_sql_skip_comment(const char *p)
{
    if (p[0] == '-' && p[1] == '-') {
        p += 2;
        while (*p && *p != '\n') p++;
        return p;
    }
    if (p[0] == '/' && p[1] == '*') {
        const char *end = strstr(p + 2, "*/");
        return end ? end + 2 : p + strlen(p);
    }
    return NULL;
}

static int ascii_casecmp(const char *a, const char *b)
{
    while (*a && *b) {
        char ca = *a, cb = *b;
        if (ca >= 'a' && ca <= 'z') ca = (char)(ca - 'a' + 'A');
        if (cb >= 'a' && cb <= 'z') cb = (char)(cb - 'a' + 'A');
        if (ca != cb) return (unsigned char)ca - (unsigned char)cb;
        a++; b++;
    }
    return (unsigned char)*a - (unsigned char)*b;
}

const argus_fn_entry_t *argus_dialect_find_fn(const argus_dialect_t *dialect,
                                              const char *odbc_name)
{
    if (!dialect || !dialect->fn_map || !odbc_name) return NULL;

    for (const argus_fn_entry_t *e = dialect->fn_map; e->odbc_name; e++) {
        if (ascii_casecmp(e->odbc_name, odbc_name) == 0)
            return e;
    }
    return NULL;
}

SQLUINTEGER argus_dialect_fn_bitmap(const argus_dialect_t *dialect,
                                    argus_fn_group_t group)
{
    SQLUINTEGER mask = 0;

    if (!dialect || !dialect->fn_map) return 0;

    for (const argus_fn_entry_t *e = dialect->fn_map; e->odbc_name; e++) {
        if (e->group == group) mask |= e->bit;
    }
    return mask;
}

size_t argus_dialect_count(void)
{
    return ARGUS_DIALECT_COUNT;
}

const argus_dialect_t *argus_dialect_at(size_t index)
{
    if (index >= ARGUS_DIALECT_COUNT) return NULL;
    return &argus_dialects[index];
}
