#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <stdio.h>
#include "argus/handle.h"
#include "argus/caps.h"
#include "argus/dialect.h"

/*
 * The non-regression net for per-backend SQLGetInfo capabilities.
 *
 * SQLGetInfo used to answer several types from hardcoded constants and one
 * strcmp chain over backend names. Making them per-backend is the only way the
 * PostgreSQL family can report real transactions, a "schema" that is a schema,
 * and PostgreSQL's 63-character identifier limit — but ten existing backends
 * must keep answering exactly what they answered before, and those answers are
 * what BI tools have been tested against.
 *
 * So this walks *every* registered backend and asserts that any backend
 * without a capability descriptor still gets the legacy value, field by field.
 * It is deliberately mechanical: the point is that adding a caps struct to one
 * backend cannot silently move another.
 */

extern void argus_backends_init(void);
extern const argus_backend_t *argus_backend_find(const char *name);
extern size_t argus_backend_count(void);
extern const argus_backend_t *argus_backend_at(size_t index);

static argus_dbc_t *dbc_for(const argus_backend_t *backend)
{
    argus_env_t *env = NULL;
    argus_alloc_env(&env);
    env->odbc_version = SQL_OV_ODBC3;

    argus_dbc_t *dbc = NULL;
    argus_alloc_dbc(env, &dbc);
    dbc->host = strdup("testhost");
    dbc->database = strdup("testdb");
    dbc->backend = backend;
    return dbc;
}

static void free_dbc(argus_dbc_t *dbc)
{
    argus_env_t *env = dbc->env;
    argus_free_dbc(dbc);
    argus_free_env(env);
}

static void get_str(argus_dbc_t *dbc, SQLUSMALLINT type, char *out, size_t n)
{
    SQLSMALLINT len = 0;
    out[0] = '\0';
    assert_int_equal(SQLGetInfo((SQLHDBC)dbc, type, out, (SQLSMALLINT)n, &len),
                     SQL_SUCCESS);
}

static SQLUSMALLINT get_u16(argus_dbc_t *dbc, SQLUSMALLINT type)
{
    SQLUSMALLINT v = 0xFFFF;
    assert_int_equal(SQLGetInfo((SQLHDBC)dbc, type, &v, sizeof(v), NULL),
                     SQL_SUCCESS);
    return v;
}

static SQLUINTEGER get_u32(argus_dbc_t *dbc, SQLUSMALLINT type)
{
    SQLUINTEGER v = 0xFFFFFFFFu;
    assert_int_equal(SQLGetInfo((SQLHDBC)dbc, type, &v, sizeof(v), NULL),
                     SQL_SUCCESS);
    return v;
}

/*
 * Every backend that declares no capabilities must still see the exact values
 * SQLGetInfo returned before capabilities existed.
 */
static void test_backends_without_caps_are_unchanged(void **state)
{
    (void)state;
    argus_backends_init();

    size_t n = argus_backend_count();
    assert_true(n > 0);

    for (size_t i = 0; i < n; i++) {
        const argus_backend_t *b = argus_backend_at(i);
        assert_non_null(b);
        if (b->caps) continue;          /* opted in; checked separately */

        argus_dbc_t *dbc = dbc_for(b);
        char buf[128];

        get_str(dbc, SQL_CATALOG_TERM, buf, sizeof(buf));
        if (strcmp(buf, "catalog") != 0)
            fail_msg("%s: SQL_CATALOG_TERM changed to '%s'", b->name, buf);

        get_str(dbc, SQL_SCHEMA_TERM, buf, sizeof(buf));
        if (strcmp(buf, "database") != 0)
            fail_msg("%s: SQL_SCHEMA_TERM changed to '%s'", b->name, buf);

        get_str(dbc, SQL_PROCEDURE_TERM, buf, sizeof(buf));
        if (strcmp(buf, "procedure") != 0)
            fail_msg("%s: SQL_PROCEDURE_TERM changed to '%s'", b->name, buf);

        /* SQL_PROCEDURES is derived from the dialect's {call} template, not
         * from the capability struct: it promises the invocation syntax works,
         * and only a dialect that can render it can keep that promise. */
        get_str(dbc, SQL_PROCEDURES, buf, sizeof(buf));
        if (strcmp(buf, "N") != 0)
            fail_msg("%s: SQL_PROCEDURES changed to '%s'", b->name, buf);

        get_str(dbc, SQL_DESCRIBE_PARAMETER, buf, sizeof(buf));
        if (strcmp(buf, "N") != 0)
            fail_msg("%s: SQL_DESCRIBE_PARAMETER changed to '%s'", b->name, buf);

        if (get_u16(dbc, SQL_MAX_IDENTIFIER_LEN) != 128)
            fail_msg("%s: SQL_MAX_IDENTIFIER_LEN changed", b->name);

        if (get_u16(dbc, SQL_TXN_CAPABLE) != SQL_TC_NONE)
            fail_msg("%s: SQL_TXN_CAPABLE changed", b->name);

        if (get_u32(dbc, SQL_TXN_ISOLATION_OPTION) != 0)
            fail_msg("%s: SQL_TXN_ISOLATION_OPTION changed", b->name);

        if (get_u32(dbc, SQL_DEFAULT_TXN_ISOLATION) != 0)
            fail_msg("%s: SQL_DEFAULT_TXN_ISOLATION changed", b->name);

        if (get_u16(dbc, SQL_ODBC_SQL_CONFORMANCE) != SQL_OSC_MINIMUM)
            fail_msg("%s: SQL_ODBC_SQL_CONFORMANCE changed", b->name);

        free_dbc(dbc);
    }
}

/*
 * SQL_DBMS_NAME was a strcmp chain: hive, impala and trino had display names
 * and everything else fell back to the raw backend name. Both halves have to
 * survive the move into capability descriptors.
 */
static void test_dbms_name_is_preserved(void **state)
{
    (void)state;
    argus_backends_init();

    struct { const char *backend; const char *expect; } known[] = {
        { "hive",      "Apache Hive" },
        { "impala",    "Apache Impala" },
        { "trino",     "Trino" },
        { "phoenix",   "Apache Phoenix" },
        { "pinot",     "Apache Pinot" },
        { "druid",     "Apache Druid" },
        { "kudu",      "Apache Kudu" },
        { "mysql",     "MySQL" },
        { "bigquery",  "Google BigQuery" },
        { "flightsql", "Arrow Flight SQL" },
        { "postgres",  "PostgreSQL" },
    };

    for (size_t i = 0; i < sizeof(known) / sizeof(known[0]); i++) {
        const argus_backend_t *b = argus_backend_find(known[i].backend);
        if (!b) continue;               /* not compiled in */
        argus_dbc_t *dbc = dbc_for(b);
        char buf[128];
        get_str(dbc, SQL_DBMS_NAME, buf, sizeof(buf));
        assert_string_equal(buf, known[i].expect);
        free_dbc(dbc);
    }

    /*
     * A backend with no descriptor at all still reports its own name. Every
     * registered backend has one now, so the fallback is exercised with a
     * descriptor of the shape a new backend starts from.
     */
    argus_backend_t bare;
    memset(&bare, 0, sizeof(bare));
    bare.name = "brand_new_engine";
    argus_dbc_t *dbc = dbc_for(&bare);
    char buf[128];
    get_str(dbc, SQL_DBMS_NAME, buf, sizeof(buf));
    assert_string_equal(buf, "brand_new_engine");
    free_dbc(dbc);
}

/*
 * SQL_IDENTIFIER_CASE was SQL_IC_LOWER for every backend, which is only
 * true for some of them: Phoenix folds to upper, and BigQuery, Druid,
 * Pinot and Kudu store an identifier exactly as written.
 */
static void test_identifier_case_follows_the_engine(void **state)
{
    (void)state;
    argus_backends_init();

    struct { const char *backend; SQLUSMALLINT expect; } cases[] = {
        { "hive",     SQL_IC_LOWER },
        { "impala",   SQL_IC_LOWER },
        { "trino",    SQL_IC_LOWER },
        { "postgres", SQL_IC_LOWER },
        { "phoenix",  SQL_IC_UPPER },
        { "pinot",    SQL_IC_SENSITIVE },
        { "druid",    SQL_IC_SENSITIVE },
        { "kudu",     SQL_IC_SENSITIVE },
        { "bigquery", SQL_IC_SENSITIVE },
        { "mysql",    SQL_IC_MIXED },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const argus_backend_t *b = argus_backend_find(cases[i].backend);
        if (!b) continue;               /* not compiled in */
        argus_dbc_t *dbc = dbc_for(b);
        if (get_u16(dbc, SQL_IDENTIFIER_CASE) != cases[i].expect)
            fail_msg("%s: SQL_IDENTIFIER_CASE is %u, expected %u",
                     b->name, get_u16(dbc, SQL_IDENTIFIER_CASE),
                     cases[i].expect);
        free_dbc(dbc);
    }
}

/*
 * SQL_KEYWORDS handed every backend Hive's list. An application quotes what
 * this names, so a MySQL connection was told AUTO_INCREMENT was safe bare
 * and TRANSFORM was not.
 */
static void test_keywords_follow_the_engine(void **state)
{
    (void)state;
    argus_backends_init();

    const argus_backend_t *b = argus_backend_find("mysql");
    if (b) {
        argus_dbc_t *dbc = dbc_for(b);
        char buf[512];
        get_str(dbc, SQL_KEYWORDS, buf, sizeof(buf));
        assert_non_null(strstr(buf, "AUTO_INCREMENT"));
        assert_null(strstr(buf, "TRANSFORM"));
        free_dbc(dbc);
    }

    b = argus_backend_find("hive");
    if (b) {
        argus_dbc_t *dbc = dbc_for(b);
        char buf[512];
        get_str(dbc, SQL_KEYWORDS, buf, sizeof(buf));
        assert_non_null(strstr(buf, "TRANSFORM"));
        free_dbc(dbc);
    }
}

/* No backend at all: the defaults must still hold, since SQLGetInfo is legal
 * before SQLConnect for several of these types. */
static void test_unconnected_dbc_uses_defaults(void **state)
{
    (void)state;
    argus_dbc_t *dbc = dbc_for(NULL);

    char buf[128];
    get_str(dbc, SQL_SCHEMA_TERM, buf, sizeof(buf));
    assert_string_equal(buf, "database");
    get_str(dbc, SQL_PROCEDURES, buf, sizeof(buf));
    assert_string_equal(buf, "N");
    assert_int_equal(get_u16(dbc, SQL_TXN_CAPABLE), SQL_TC_NONE);
    assert_int_equal(get_u16(dbc, SQL_MAX_IDENTIFIER_LEN), 128);

    get_str(dbc, SQL_DBMS_NAME, buf, sizeof(buf));
    assert_string_equal(buf, "Unknown");

    free_dbc(dbc);
}

/*
 * The defaulting helpers themselves. The whole design rests on a zero-valued
 * field meaning "the legacy answer", and on SQL_TC_NONE and SQL_OSC_MINIMUM
 * both being 0 so the two most important defaults come for free — which is
 * true, but is exactly the kind of thing that should be asserted rather than
 * assumed.
 */
static void test_defaulting_helpers(void **state)
{
    (void)state;

    assert_int_equal(SQL_TC_NONE, 0);
    assert_int_equal(SQL_OSC_MINIMUM, 0);

    const argus_backend_caps_t *d = argus_caps_for(NULL);
    assert_non_null(d);
    assert_null(d->dbms_name);
    assert_int_equal(d->txn_capable, SQL_TC_NONE);
    assert_false(d->describe_parameter);

    assert_string_equal(argus_caps_str(NULL, "fallback"), "fallback");
    assert_string_equal(argus_caps_str("set", "fallback"), "set");
    assert_int_equal(argus_caps_u16(0, 128), 128);
    assert_int_equal(argus_caps_u16(63, 128), 63);
}

/* The PostgreSQL family, which is the reason capabilities exist. Skipped when
 * the backends were not compiled in. */
static void test_postgres_family_caps(void **state)
{
    (void)state;
    argus_backends_init();

    struct { const char *backend; const char *dbms; } pg[] = {
        { "postgres",   "PostgreSQL" },
        { "greenplum",  "Greenplum Database" },
        { "cloudberry", "Apache Cloudberry" },
    };

    for (size_t i = 0; i < sizeof(pg) / sizeof(pg[0]); i++) {
        const argus_backend_t *b = argus_backend_find(pg[i].backend);
        if (!b) continue;
        argus_dbc_t *dbc = dbc_for(b);
        char buf[128];

        get_str(dbc, SQL_DBMS_NAME, buf, sizeof(buf));
        assert_string_equal(buf, pg[i].dbms);

        /* A schema is a schema here, and a catalog is a database. Reporting
         * "database" for SQL_SCHEMA_TERM is what makes a BI tool label the
         * wrong level of the hierarchy. */
        get_str(dbc, SQL_SCHEMA_TERM, buf, sizeof(buf));
        assert_string_equal(buf, "schema");
        get_str(dbc, SQL_CATALOG_TERM, buf, sizeof(buf));
        assert_string_equal(buf, "database");

        /* PostgreSQL truncates identifiers at 63 bytes, not 128. */
        assert_int_equal(get_u16(dbc, SQL_MAX_IDENTIFIER_LEN), 63);

        /* Real transactions, and the isolation levels PostgreSQL actually
         * distinguishes. READ UNCOMMITTED is deliberately absent: PostgreSQL
         * accepts the syntax and silently gives READ COMMITTED, so claiming it
         * would be the over-declaring this codebase argues against. */
        assert_int_equal(get_u16(dbc, SQL_TXN_CAPABLE), SQL_TC_ALL);
        SQLUINTEGER iso = get_u32(dbc, SQL_TXN_ISOLATION_OPTION);
        assert_true(iso & SQL_TXN_READ_COMMITTED);
        assert_true(iso & SQL_TXN_REPEATABLE_READ);
        assert_true(iso & SQL_TXN_SERIALIZABLE);
        assert_false(iso & SQL_TXN_READ_UNCOMMITTED);
        assert_int_equal(get_u32(dbc, SQL_DEFAULT_TXN_ISOLATION),
                         SQL_TXN_READ_COMMITTED);

        get_str(dbc, SQL_DESCRIBE_PARAMETER, buf, sizeof(buf));
        assert_string_equal(buf, "Y");

        /* "Y" only because escape.c can now render {call ...}; the dialect's
         * template is the single source of both. */
        get_str(dbc, SQL_PROCEDURES, buf, sizeof(buf));
        assert_string_equal(buf, "Y");
        assert_non_null(argus_dialect_by_name(pg[i].backend)->call_tmpl);

        free_dbc(dbc);
    }
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_backends_without_caps_are_unchanged),
        cmocka_unit_test(test_dbms_name_is_preserved),
        cmocka_unit_test(test_identifier_case_follows_the_engine),
        cmocka_unit_test(test_keywords_follow_the_engine),
        cmocka_unit_test(test_unconnected_dbc_uses_defaults),
        cmocka_unit_test(test_defaulting_helpers),
        cmocka_unit_test(test_postgres_family_caps),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
