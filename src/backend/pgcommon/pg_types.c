#include "pg_common.h"
#include <stdio.h>
#include <string.h>

/*
 * PostgreSQL type OID → ODBC type.
 *
 * Two inputs matter, and the second one is the part generic ODBC drivers get
 * wrong: the OID says *what* the type is, and atttypmod says how it was
 * declared. Without atttypmod every varchar(20) is reported as an unbounded
 * string and every numeric(10,2) as an unconstrained decimal, which is exactly
 * how a BI tool ends up allocating 64 KB per cell for a postcode column.
 *
 * atttypmod encoding (from PostgreSQL's own catalog code):
 *   varchar(n)/char(n)   n + VARHDRSZ, so n = atttypmod - 4
 *   numeric(p,s)         ((p << 16) | s) + VARHDRSZ
 *   time/timestamp(p)    p directly
 *   -1                   not declared with a modifier
 */

#define VARHDRSZ 4

static int typmod_len(int atttypmod)
{
    return (atttypmod > VARHDRSZ) ? atttypmod - VARHDRSZ : 0;
}

static int numeric_precision(int atttypmod)
{
    if (atttypmod < VARHDRSZ) return 0;
    return ((atttypmod - VARHDRSZ) >> 16) & 0xFFFF;
}

static int numeric_scale(int atttypmod)
{
    if (atttypmod < VARHDRSZ) return 0;
    return (atttypmod - VARHDRSZ) & 0xFFFF;
}

SQLSMALLINT pg_oid_to_sql_type(Oid oid, int atttypmod)
{
    switch (oid) {
    case PG_OID_BOOL:        return SQL_BIT;
    case PG_OID_INT2:        return SQL_SMALLINT;
    case PG_OID_INT4:        return SQL_INTEGER;
    case PG_OID_OID:         return SQL_INTEGER;
    case PG_OID_INT8:        return SQL_BIGINT;
    case PG_OID_FLOAT4:      return SQL_REAL;
    case PG_OID_FLOAT8:      return SQL_DOUBLE;
    case PG_OID_NUMERIC:     return SQL_NUMERIC;

    case PG_OID_BYTEA:       return SQL_LONGVARBINARY;

    case PG_OID_CHAR:        return SQL_CHAR;      /* internal one-byte "char" */
    case PG_OID_NAME:        return SQL_VARCHAR;
    case PG_OID_BPCHAR:      return SQL_CHAR;
    case PG_OID_VARCHAR:
        /* varchar with no declared length has no meaningful column size, so it
         * is a long type — the same call psqlODBC makes. */
        return (atttypmod > VARHDRSZ) ? SQL_VARCHAR : SQL_LONGVARCHAR;

    case PG_OID_TEXT:
    case PG_OID_XML:
    case PG_OID_JSON:
    case PG_OID_JSONB:       return SQL_LONGVARCHAR;

    case PG_OID_DATE:        return SQL_TYPE_DATE;
    case PG_OID_TIME:        return SQL_TYPE_TIME;
    /* timetz carries a UTC offset that ODBC's TIME struct has nowhere to put;
     * the offset is dropped on conversion to SQL_C_TYPE_TIME, as it is in
     * psqlODBC. Applications that need it should select it as text. */
    case PG_OID_TIMETZ:      return SQL_TYPE_TIME;
    case PG_OID_TIMESTAMP:
    case PG_OID_TIMESTAMPTZ: return SQL_TYPE_TIMESTAMP;

    /* No ODBC interval type matches PostgreSQL's month/day/microsecond triple
     * (SQL_INTERVAL_* are all single-unit), so the text form is the honest
     * answer rather than a lossy pick. */
    case PG_OID_INTERVAL:    return SQL_VARCHAR;

    /* money renders with the locale's currency symbol and separators; it is
     * not a number a client can parse portably. */
    case PG_OID_MONEY:       return SQL_VARCHAR;

    case PG_OID_BIT:
    case PG_OID_VARBIT:      return SQL_VARCHAR;   /* a string of 0/1 */

    case PG_OID_UUID:        return SQL_GUID;

    /* Arrays, ranges, composites, enums, domains over anything exotic, and
     * every extension type: PostgreSQL always has a text representation, and
     * handing that over is better than guessing a structure. */
    default:                 return SQL_LONGVARCHAR;
    }
}

SQLULEN pg_column_size(Oid oid, int atttypmod)
{
    switch (oid) {
    case PG_OID_BOOL:        return 1;
    case PG_OID_INT2:        return 5;
    case PG_OID_INT4:
    case PG_OID_OID:         return 10;
    case PG_OID_INT8:        return 19;
    case PG_OID_FLOAT4:      return 7;
    case PG_OID_FLOAT8:      return 15;

    case PG_OID_NUMERIC: {
        int p = numeric_precision(atttypmod);
        /* An undeclared numeric can hold up to 1000 digits, which no ODBC
         * application allocates for. 38 is the practical ceiling every other
         * driver reports and what SQL_DECIMAL consumers expect. */
        return (SQLULEN)(p > 0 ? p : 38);
    }

    case PG_OID_CHAR:        return 1;
    case PG_OID_NAME:        return 63;
    case PG_OID_BPCHAR:
    case PG_OID_VARCHAR: {
        int n = typmod_len(atttypmod);
        return (SQLULEN)(n > 0 ? n : 0);   /* 0 = unbounded */
    }

    case PG_OID_UUID:        return 36;
    case PG_OID_DATE:        return 10;    /* YYYY-MM-DD */

    case PG_OID_TIME:
    case PG_OID_TIMETZ: {
        /* HH:MM:SS plus '.' and the fractional digits when declared. */
        int p = (atttypmod >= 0) ? atttypmod : 6;
        return (SQLULEN)(p > 0 ? 8 + 1 + p : 8);
    }

    case PG_OID_TIMESTAMP:
    case PG_OID_TIMESTAMPTZ: {
        int p = (atttypmod >= 0) ? atttypmod : 6;
        return (SQLULEN)(p > 0 ? 19 + 1 + p : 19);
    }

    default:
        return 0;   /* unbounded / driver-defined */
    }
}

SQLSMALLINT pg_decimal_digits(Oid oid, int atttypmod)
{
    switch (oid) {
    case PG_OID_NUMERIC: {
        int p = numeric_precision(atttypmod);
        /* Only meaningful alongside a declared precision. */
        return (SQLSMALLINT)(p > 0 ? numeric_scale(atttypmod) : 0);
    }
    case PG_OID_TIME:
    case PG_OID_TIMETZ:
    case PG_OID_TIMESTAMP:
    case PG_OID_TIMESTAMPTZ:
        /* PostgreSQL stores microseconds and defaults to 6 fractional digits
         * when no precision was declared. */
        return (SQLSMALLINT)((atttypmod >= 0) ? atttypmod : 6);
    default:
        return 0;
    }
}

uint8_t pg_native_kind(Oid oid)
{
    switch (oid) {
    case PG_OID_BOOL:
    case PG_OID_INT2:
    case PG_OID_INT4:
    case PG_OID_INT8:
    case PG_OID_OID:
        return ARGUS_NATIVE_I64;
    case PG_OID_FLOAT4:
    case PG_OID_FLOAT8:
        return ARGUS_NATIVE_F64;
    default:
        return ARGUS_NATIVE_NONE;
    }
}

/*
 * SQLGetTypeInfo.
 *
 * Synthesised as a UNION ALL of literal rows, the same shape every other Argus
 * backend uses, with the 16 ODBC column aliases carried on the first branch.
 * Ordered by DATA_TYPE as the specification requires.
 */
int pg_get_type_info(argus_backend_conn_t conn, SQLSMALLINT sql_type,
                     argus_backend_op_t *out_op)
{
    (void)sql_type;

    const char *query =
        "SELECT * FROM (VALUES "
        /* TYPE_NAME, DATA_TYPE, COLUMN_SIZE, LITERAL_PREFIX, LITERAL_SUFFIX,
         * CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE,
         * UNSIGNED_ATTRIBUTE, FIXED_PREC_SCALE, AUTO_UNIQUE_VALUE,
         * LOCAL_TYPE_NAME, MINIMUM_SCALE, MAXIMUM_SCALE, NUM_PREC_RADIX */
        "('bytea',       -4::int2, 2147483647::int, NULL, NULL, NULL, 1::int2, 0::int2, 0::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int), "
        "('bigint',      -5::int2,         19::int, NULL, NULL, NULL, 1::int2, 0::int2, 2::int2, 0::int2,    0::int2, 0::int2, NULL, 0::int2,    0::int2,    10::int), "
        "('text',        -1::int2, 2147483647::int, '''',  '''', NULL, 1::int2, 1::int2, 3::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int), "
        "('char',         1::int2,   10485760::int, '''',  '''', 'length', 1::int2, 1::int2, 3::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int), "
        "('numeric',      2::int2,       1000::int, NULL, NULL, 'precision,scale', 1::int2, 0::int2, 2::int2, 0::int2, 0::int2, 0::int2, NULL, 0::int2, 1000::int2, 10::int), "
        "('integer',      4::int2,         10::int, NULL, NULL, NULL, 1::int2, 0::int2, 2::int2, 0::int2,    0::int2, 0::int2, NULL, 0::int2,    0::int2,    10::int), "
        "('smallint',     5::int2,          5::int, NULL, NULL, NULL, 1::int2, 0::int2, 2::int2, 0::int2,    0::int2, 0::int2, NULL, 0::int2,    0::int2,    10::int), "
        "('real',         7::int2,          7::int, NULL, NULL, NULL, 1::int2, 0::int2, 2::int2, 0::int2,    0::int2, 0::int2, NULL, NULL::int2, NULL::int2, 10::int), "
        "('double precision', 8::int2,     15::int, NULL, NULL, NULL, 1::int2, 0::int2, 2::int2, 0::int2,    0::int2, 0::int2, NULL, NULL::int2, NULL::int2, 10::int), "
        "('character varying', 12::int2, 10485760::int, '''', '''', 'length', 1::int2, 1::int2, 3::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int), "
        "('boolean',     -7::int2,          1::int, NULL, NULL, NULL, 1::int2, 0::int2, 2::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int), "
        "('date',        91::int2,         10::int, '''',  '''', NULL, 1::int2, 0::int2, 2::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int), "
        "('time',        92::int2,         15::int, '''',  '''', 'precision', 1::int2, 0::int2, 2::int2, NULL::int2, 0::int2, 0::int2, NULL, 0::int2, 6::int2, NULL::int), "
        "('timestamp',   93::int2,         26::int, '''',  '''', 'precision', 1::int2, 0::int2, 2::int2, NULL::int2, 0::int2, 0::int2, NULL, 0::int2, 6::int2, NULL::int), "
        "('uuid',       -11::int2,         36::int, '''',  '''', NULL, 1::int2, 0::int2, 2::int2, NULL::int2, 0::int2, 0::int2, NULL, NULL::int2, NULL::int2, NULL::int) "
        ") AS t(\"TYPE_NAME\", \"DATA_TYPE\", \"COLUMN_SIZE\", \"LITERAL_PREFIX\", "
        "\"LITERAL_SUFFIX\", \"CREATE_PARAMS\", \"NULLABLE\", \"CASE_SENSITIVE\", "
        "\"SEARCHABLE\", \"UNSIGNED_ATTRIBUTE\", \"FIXED_PREC_SCALE\", "
        "\"AUTO_UNIQUE_VALUE\", \"LOCAL_TYPE_NAME\", \"MINIMUM_SCALE\", "
        "\"MAXIMUM_SCALE\", \"NUM_PREC_RADIX\") "
        "ORDER BY \"DATA_TYPE\"";

    return pg_execute_buffered(conn, query, out_op);
}
