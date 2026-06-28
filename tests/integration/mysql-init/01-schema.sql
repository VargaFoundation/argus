-- Seed data for the MySQL-wire backend integration tests.
-- Loaded automatically by the mariadb container (docker-entrypoint-initdb.d),
-- into the MARIADB_DATABASE (testdb).

CREATE TABLE IF NOT EXISTS products (
  id      INT PRIMARY KEY,
  name    VARCHAR(50) NOT NULL,
  price   DECIMAL(10,2),
  qty     BIGINT,
  rating  DOUBLE,
  created DATE,
  notes   VARCHAR(100)
);

INSERT INTO products VALUES
  (1, 'Widget', 19.99, 100,  4.5, '2024-01-15', 'popular'),
  (2, 'Gadget', 49.50, 25,   3.8, '2024-02-20', NULL),
  (3, 'Gizmo',   5.00, 1000, 4.9, '2024-03-10', 'cheap');
