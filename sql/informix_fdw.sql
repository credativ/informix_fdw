--
-- Set server parameters
--
-- NOTE: we turn off ECHO temporarily, since
--       sourcing the external regression test settings
--       might cause installcheck errors because of
--       varying settings (e.g. database name).
--
\set ECHO off
\set ON_ERROR_STOP 1
\i ./sql/regression_variables
\unset ON_ERROR_STOP
\set ECHO all

--
-- Suppress WARNINGs during regression test. This will
-- cause errors in case someone uses a database with logging.
--
SET client_min_messages TO ERROR;

--
-- Load extension
--

CREATE EXTENSION informix_fdw;

--
-- Create foreign server
--

CREATE SERVER test_server
FOREIGN DATA WRAPPER informix_fdw
OPTIONS (informixserver :'INFORMIXSERVER',
         informixdir :'INFORMIXDIR');

CREATE USER MAPPING FOR CURRENT_USER
SERVER test_server
OPTIONS (username :'INFORMIXUSER', password :'INFORMIXPASSWORD');

--
-- predicate pushdown test
--
CREATE FOREIGN TABLE inttest(f1 bigint not null, f2 integer, f3 smallint)
SERVER test_server
OPTIONS (table 'inttest',
         client_locale :'CLIENT_LOCALE',
         db_locale :'DB_LOCALE',
         database :'INFORMIXDB');

SELECT * FROM inttest WHERE f1 = 101 ORDER BY f1;
SELECT * FROM inttest WHERE f1 = 101 OR f2 IS NULL ORDER BY f1;

-- should succeed, uses 8 byte int value (MAX_INT) and NULL
SELECT f2, f3 FROM inttest WHERE f1 >= 4294967296;

--
-- BLOB tests
--
CREATE FOREIGN TABLE byte_test(id integer, data bytea)
SERVER test_server
OPTIONS (table 'byte_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB');

CREATE FOREIGN TABLE text_test(id integer, data_binary bytea, data_text text)
SERVER test_server
OPTIONS (query 'SELECT * FROM text_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB',
        enable_blobs '1');

-- should fail
SELECT id, data FROM byte_test ORDER BY id;

-- should succeed
ALTER FOREIGN TABLE byte_test OPTIONS(ADD enable_blobs '1');

-- should succeed now
SELECT id, data FROM byte_test ORDER BY id;

-- should fail, since column name/list vary
SELECT id, data_binary, data_text FROM text_test WHERE data_text = 'bar' ORDER BY id;

DROP FOREIGN TABLE text_test;

CREATE FOREIGN TABLE text_test(id integer, data_binary bytea, data_text text)
SERVER test_server
OPTIONS (query 'SELECT id, data AS data_binary, data AS data_text FROM text_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB',
        enable_blobs '1');

ALTER FOREIGN TABLE text_test OPTIONS (ADD disable_predicate_pushdown '1');

-- should fail
SELECT id, data_text AS data FROM text_test WHERE data_text = 'bar' ORDER BY id;

-- should succeed
SELECT id, data_binary, data_text FROM text_test ;

-- should succeed, but no result
SELECT id, data_text FROM text_test WHERE data_text = 'blabla' ORDER BY id;

-- should succeed, with result
SELECT id, data_text FROM text_test WHERE data_text LIKE '%Heise.de%' ORDER BY id;

--
-- DATE/DATETIME tests
--
CREATE FOREIGN TABLE datetime_test (
    f1 timestamp with time zone,
    f2 character varying,
    f3 date,
    f4 bigint
)
SERVER test_server
OPTIONS (client_locale :'CLIENT_LOCALE',
         database :'INFORMIXDB',
         db_locale :'DB_LOCALE',
         gl_date '%iY-%m-%d',
         query 'SELECT f1 AS f1, "comment" AS f2, dt AS f3, id AS f4 FROM datetime_test'
);

SELECT * FROM datetime_test ORDER BY f4;

--
-- DECIMAL test (converts to PostgreSQL NUMERIC)
--
CREATE FOREIGN TABLE dec_test (val1 numeric(9,2), val2 numeric(8,0))
SERVER test_server
OPTIONS(database :'INFORMIXDB',
        client_locale :'CLIENT_LOCALE',
        query 'SELECT value AS val1, value AS val2 FROM dec_test');

SELECT * FROM dec_test ORDER BY val1;

--
-- NVARCHAR
--
CREATE FOREIGN TABLE nvarchar_test
(
        val1 varchar(200),
        val2 text,
        val3 bpchar(200)
)
SERVER test_server
OPTIONS(database :'INFORMIXDB',
        client_locale :'CLIENT_LOCALE',
        query 'SELECT val AS val1, val AS val2, val AS val3 FROM nvarchar_test');

-- Should succeed
SELECT * FROM nvarchar_test ORDER BY val1;

DROP FOREIGN TABLE nvarchar_test;

CREATE FOREIGN TABLE nvarchar_test
(
        val1 varchar(200),
        val2 text,
        val3 bpchar(2) -- too short
)
SERVER test_server
OPTIONS(database :'INFORMIXDB',
        client_locale :'CLIENT_LOCALE',
        query 'SELECT val AS val1, val AS val2, val AS val3 FROM nvarchar_test');

-- Should fail
SELECT * FROM nvarchar_test ORDER BY val1;

--
-- Test SERIAL/BOOLEAN datatype
--
CREATE FOREIGN TABLE serial_test
(
        f1 bigint NOT NULL,
        f2 varchar,
        flag boolean
)
SERVER test_server
OPTIONS(database :'INFORMIXDB',
        client_locale :'CLIENT_LOCALE',
        table 'serial_test');

-- should succeed
SELECT f1, f2, flag FROM serial_test ORDER BY f1;

-- should succeed
SELECT f1, f2, flag FROM serial_test WHERE flag IS NULL;

-- should succeed
SELECT f2 FROM serial_test WHERE f1 = 100 OR flag IS NULL;

--
-- Check NOT NULL constraints
--
ALTER FOREIGN TABLE inttest ALTER f2 SET NOT NULL;

-- should fail
SELECT f1, f2, f3 FROM inttest WHERE f2 IS NULL;

ALTER FOREIGN TABLE inttest ALTER f2 DROP NOT NULL;

--should succeed
SELECT f1, f2, f3 FROM inttest WHERE f2 IS NULL;

--------------------------------------------------------------------------------
-- The following tests checks the behavior of the Informix FDW
-- when used in self joins or joins to different foreign Informix tables.
-- This merely is a regression for statements associated to different
-- ESQL cursors internally.
--------------------------------------------------------------------------------

CREATE FOREIGN TABLE bar_serial(
       id integer NOT NULL,
       name varchar(100)
)
SERVER test_server
OPTIONS(database :'INFORMIXDB',
        client_locale :'CLIENT_LOCALE',
        table 'bar_serial');

-- one row, self join
SELECT * FROM bar_serial s1 JOIN bar_serial s2 ON (s1.id = s2.id);

-- join other Informix table, more complex
SELECT i1.f1, i2.id, substring(data_text, 1, 10)
FROM inttest i1
JOIN text_test i2 ON (i1.f1 = i2.id)
WHERE i1.f1 = 1 AND i2.id = 1;

--
-- Prepared statements
--
PREPARE p_serial_test(bigint) AS SELECT * FROM serial_test WHERE f1 = $1;
EXECUTE p_serial_test(100);
EXECUTE p_serial_test(-1);
DEALLOCATE p_serial_test;

--
-- Cursor
--
DECLARE cur SCROLL CURSOR WITH HOLD FOR SELECT * FROM inttest ORDER BY f1;
FETCH 5 FROM cur;
MOVE FIRST FROM cur;
FETCH 0 FROM cur;
FETCH 1 FROM cur;
CLOSE cur;

-- Cursor with transaction
BEGIN;

DECLARE cur SCROLL CURSOR FOR SELECT * FROM inttest ORDER BY f1;
FETCH 5 FROM cur;
MOVE FIRST FROM cur;
FETCH 0 FROM cur;
FETCH 1 FROM cur;
CLOSE cur;

COMMIT;

--
-- Test ANALYZE
--

-- should succeed, but with a no-op, since text_test
-- is based on a query.
SET client_min_messages TO WARNING;
ANALYZE VERBOSE text_test;
SET client_min_messages TO ERROR;

-- should succeed
ANALYZE inttest;

--
-- ALTER FOREIGN TABLE ... DROP COLUMN
--

--
-- Drop first column
--
ALTER FOREIGN TABLE inttest DROP COLUMN f1;

--
-- Should fail, since table definition doesn't match anymore
-- (BIGINT -> INT4 conversion attempt rejected)
--
SELECT * FROM inttest WHERE f2 = 120;

-- ANALYZE as well...
ANALYZE inttest;

--
-- Adjust foreign table definition to match local columns
--
ALTER FOREIGN TABLE inttest OPTIONS(ADD query 'SELECT f2, f3 FROM inttest', DROP table);

SELECT * FROM inttest ORDER BY f2, f3 LIMIT 5;

--
-- Add f1 column again
--
ALTER FOREIGN TABLE inttest ADD COLUMN f1 bigint;

--
-- Should still fail, can't match column list
--
SELECT * FROM inttest WHERE f1 = 101;

--
-- Adjust foreign table again to match column list.
--
ALTER FOREIGN TABLE inttest OPTIONS(SET query 'SELECT f2, f3, f1 FROM inttest');

SELECT f1, f2, f3 FROM inttest WHERE f1 = 102;

--
-- Clean up
--
DROP FOREIGN TABLE inttest;
DROP FOREIGN TABLE byte_test;
DROP FOREIGN TABLE text_test;
DROP FOREIGN TABLE datetime_test;
DROP FOREIGN TABLE dec_test;
DROP FOREIGN TABLE nvarchar_test;
DROP FOREIGN TABLE serial_test;
DROP FOREIGN TABLE bar_serial;
DROP USER MAPPING FOR CURRENT_USER SERVER test_server;

DROP SERVER test_server;
