-- Seed data for the PostgreSQL-family integration tests.
--
-- Mounted at /docker-entrypoint-initdb.d in the `postgres` compose service, and
-- equally runnable by hand against any PostgreSQL:
--     psql -h HOST -U argus -d argusdb -f 01-seed.sql
--
-- It has to cover three things the tests actually assert:
--   * one table per interesting type, so the OID → ODBC mapping is exercised
--     against a real RowDescription rather than a hand-built one;
--   * a partitioned table with many children, so SQLTables can be shown to
--     return the parent and not the children — the whole point of the
--     partition filter;
--   * a foreign-key pair, a comment and an index, for the catalog functions.

CREATE SCHEMA IF NOT EXISTS argus_test;
SET search_path TO argus_test, public;

-- ── Types ────────────────────────────────────────────────────────
DROP TABLE IF EXISTS all_types CASCADE;
CREATE TABLE all_types (
    c_bool          boolean,
    c_int2          smallint,
    c_int4          integer,
    c_int8          bigint,
    c_float4        real,
    c_float8        double precision,
    c_numeric       numeric(12,3),
    c_numeric_free  numeric,
    c_char          char(5),
    c_varchar       varchar(20),
    c_text          text,
    c_bytea         bytea,
    c_date          date,
    c_time          time(3),
    c_timestamp     timestamp(6),
    c_timestamptz   timestamptz,
    c_interval      interval,
    c_uuid          uuid,
    c_json          json,
    c_jsonb         jsonb,
    c_int_array     integer[]
);
COMMENT ON TABLE all_types IS 'every type the driver maps';

INSERT INTO all_types VALUES (
    true, 32767, 2147483647, 9223372036854775807,
    1.5, 2.25, 12345.678, 3.14159,
    'abc', 'hello', 'a longer piece of text',
    '\x00ff10'::bytea,
    DATE '2024-02-29', TIME '23:59:59.123', TIMESTAMP '2024-02-29 23:59:59.123456',
    TIMESTAMPTZ '2024-02-29 23:59:59.123456+00',
    INTERVAL '1 day 2 hours', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid,
    '{"a":1}'::json, '{"b":2}'::jsonb, ARRAY[1,2,3]
);
INSERT INTO all_types (c_bool) VALUES (NULL);   -- an all-NULL row

-- ── Referential integrity, comments, indexes ─────────────────────
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id      integer PRIMARY KEY,
    name    varchar(50) NOT NULL,
    region  text
);
COMMENT ON TABLE  customers IS 'customer dimension';
COMMENT ON COLUMN customers.region IS 'sales region';
CREATE INDEX customers_region_idx ON customers (region);

CREATE TABLE orders (
    order_id    bigint,
    customer_id integer NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    amount      numeric(10,2),
    placed_on   date,
    PRIMARY KEY (order_id, customer_id)
);

INSERT INTO customers VALUES (1, 'Alice', 'north'), (2, 'Bob', 'south');
INSERT INTO orders
SELECT g, 1 + (g % 2), (g * 1.25)::numeric(10,2), DATE '2024-01-01' + g
FROM generate_series(1, 500) g;

-- ── A partitioned table with many children ───────────────────────
-- This is what a BI tool must NOT be shown 24 extra tables for.
DROP TABLE IF EXISTS events CASCADE;
CREATE TABLE events (
    id       bigint,
    happened date NOT NULL,
    payload  text
) PARTITION BY RANGE (happened);

DO $$
DECLARE
    m date;
BEGIN
    FOR i IN 0..23 LOOP
        m := DATE '2023-01-01' + (i || ' months')::interval;
        EXECUTE format(
            'CREATE TABLE events_%s PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            to_char(m, 'YYYY_MM'), m, m + INTERVAL '1 month');
    END LOOP;
END $$;

INSERT INTO events
SELECT g, DATE '2023-01-01' + (g * 10), 'p' || g
FROM generate_series(0, 60) g;

-- ── A view and a function, for SQLTables and SQLProcedures ───────
CREATE OR REPLACE VIEW big_orders AS
    SELECT * FROM orders WHERE amount > 100;

-- Schema-qualified inside the body on purpose: a SQL function resolves
-- unqualified names with the *caller's* search_path, and the escape test calls
-- it from a connection that has none set.
CREATE OR REPLACE FUNCTION order_count(cust integer)
RETURNS bigint LANGUAGE sql STABLE AS
    $$ SELECT count(*) FROM argus_test.orders WHERE customer_id = cust $$;

ANALYZE customers;
ANALYZE orders;
ANALYZE events;

-- ── A nation/region pair for tests/integration/test_bi_escapes.c ─
-- The escape probe defaults to Trino's tpch.tiny.nation/region; these are the
-- PostgreSQL-family equivalents, so the same test runs unchanged with
-- BI_TABLE=argus_test.nation BI_TABLE2=argus_test.region.
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS region CASCADE;

CREATE TABLE region (
    regionkey integer PRIMARY KEY,
    name      varchar(25) NOT NULL
);
CREATE TABLE nation (
    nationkey integer PRIMARY KEY,
    name      varchar(25) NOT NULL,
    regionkey integer NOT NULL REFERENCES region(regionkey)
);

INSERT INTO region VALUES (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST');
INSERT INTO nation VALUES
    (17,'PERU',1),(24,'UNITED STATES',1),(8,'INDIA',2),(6,'FRANCE',3),(23,'UNITED KINGDOM',3);

ANALYZE nation;
ANALYZE region;

-- ── Domains and an enum ──────────────────────────────────────────
-- A column declared over a domain reports the *domain's* OID in the wire
-- protocol, so without resolving it the driver would report an unbounded
-- string for postcode instead of varchar(10).
DROP TABLE IF EXISTS domains CASCADE;
DROP DOMAIN IF EXISTS postcode CASCADE;
DROP DOMAIN IF EXISTS positive_int CASCADE;
DROP TYPE IF EXISTS mood CASCADE;

CREATE DOMAIN postcode     AS varchar(10) CHECK (VALUE <> '');
CREATE DOMAIN positive_int AS integer     CHECK (VALUE > 0);
CREATE TYPE   mood         AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE domains (
    pc     postcode,
    n      positive_int,
    m      mood,
    plain  integer
);
INSERT INTO domains VALUES ('75001', 42, 'happy', 7);
