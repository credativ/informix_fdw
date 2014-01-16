--
-- Regression tests for logged Informix databases
-- in conjunction with Informix FDW for PostgreSQL.
--
-- NOTE:
--
-- This tests makes only sense in case you are using a logged
-- Informix database, otherwise no remote transactions are used
-- and the tests *will* fail.
--

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

CREATE FOREIGN TABLE inttest(f1 bigint not null, f2 integer, f3 smallint)
SERVER test_server
OPTIONS (table 'inttest',
         client_locale :'CLIENT_LOCALE',
         db_locale :'DB_LOCALE',
         database :'INFORMIXDB');

--
-- v1 => varchar(200, 3)
-- v2 => lvarchar(200)
-- v3 => nvarchar(200)
--
CREATE FOREIGN TABLE varchar_test(id bigserial not null, v1 varchar(200), v2 text, v3 varchar(200))
SERVER test_server
OPTIONS(table 'varchar_test',
              client_locale :'CLIENT_LOCALE',
              db_locale :'DB_LOCALE',
              database :'INFORMIXDB');

--
-- Foreign table to test very long string values.
--
-- NOTE: 32739 ist the maximum length of lvarchar in Informix.
--
CREATE FOREIGN TABLE longvarchar_test(id bigserial NOT NULL, v1 varchar(32739) NOT NULL)
SERVER test_server
OPTIONS(table 'longvarchar_test',
              client_locale :'CLIENT_LOCALE',
              db_locale :'DB_LOCALE',
              database :'INFORMIXDB');

--
-- Foreign table to test Simple LO handling in the informix foreign data wrapper.
--
CREATE FOREIGN TABLE text_byte_test(id bigserial not null, v1 text, v2 bytea)
SERVER test_server
OPTIONS(table 'text_byte_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB',
        enable_blobs '1');

CREATE FOREIGN TABLE datetime_test(id bigserial not null, v1 timestamp with time zone,
                                   v2 date, v3 time)
SERVER test_server
OPTIONS(table 'datetime_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB');

--
-- Foreign table to test SERIAL values
--
CREATE FOREIGN TABLE serial_test(id serial)
SERVER test_server
OPTIONS("table" 'serial_test',
                client_locale :'CLIENT_LOCALE',
                db_locale :'DB_LOCALE',
                database :'INFORMIXDB');

--
-- Foreign table to test SERIAL8/BIGSERIAL values
--
CREATE FOREIGN TABLE serial8_test(id serial8)
SERVER test_server
OPTIONS("table" 'serial8_test',
                client_locale :'CLIENT_LOCALE',
                db_locale :'DB_LOCALE',
                database :'INFORMIXDB');

--
-- Foreign table to test INTERVAL values
--
CREATE FOREIGN TABLE interval_test (
    f1 interval,
    f2 interval,
    f3 interval
)
SERVER test_server
OPTIONS ("table" 'interval_test',
                 client_locale :'CLIENT_LOCALE',
                 db_locale :'DB_LOCALE',
                 database :'INFORMIXDB'
);

--
-- Foreign table to test DECIMAL values
--
CREATE FOREIGN TABLE decimal_test (
       f1 numeric(10,0) NOT NULL,
       f2 numeric(2,2),
       f3 numeric(10,9)
) SERVER test_server
OPTIONS ("table" 'decimal_test',
         client_locale :'CLIENT_LOCALE',
         db_locale :'DB_LOCALE',
         database :'INFORMIXDB'
);

--
-- Start a transaction.
--

BEGIN;

--
-- Simple select
--
SELECT * FROM inttest WHERE f1 = 101;

-- This should have started a transaction on the foreign server
-- as well.
SELECT tx_in_progress
FROM ifx_fdw_get_connections() ORDER BY 1;

--
-- Self join, using multiple cursors within transaction.
--
SELECT t1.f1, t2.f1
FROM inttest t1
     JOIN inttest t2 ON (t1.f1 = t2.f1)
ORDER BY 1;

--
-- Should call ifx_fdw_xact_callback()
--
ROLLBACK;

-- Transaction should be rolled back
SELECT tx_in_progress
FROM ifx_fdw_get_connections() ORDER BY 1;

--
-- Informix should have rollbacked, too
--
SELECT * FROM inttest WHERE f1 = 101;

BEGIN;

--
-- Simple select
--
SELECT * FROM inttest WHERE f1 = 101;

-- This should have started a transaction on the foreign server
-- as well.
SELECT tx_in_progress
FROM ifx_fdw_get_connections() ORDER BY 1;

--
-- Self join, using multiple cursors within transaction.
--
SELECT t1.f1, t2.f1
FROM inttest t1
     JOIN inttest t2 ON (t1.f1 = t2.f1)
ORDER BY 1;

COMMIT;

--
-- No transaction left, also show statistics
--
SELECT tx_in_progress, tx_num_commit, tx_num_rollback
FROM ifx_fdw_get_connections() ORDER BY 1;

--
-- Test DML
--

BEGIN;

--------------------------------------------------------------------------------
-- DML for Int8/Int4/Int2
--------------------------------------------------------------------------------

-- Integer values, single INSERT, succeeds
INSERT INTO inttest VALUES(-1, -2, -3);

-- Integer values, multi-INSERT, succeeds
INSERT INTO inttest VALUES(-1, -2, -3), (100, 200, 300), (400, 500, 600),
       (-100, -200, -300), (1001, 2002, 3003), (4004, 5005, 6006),
       (7007, 8008, 9009);

-- Show results
SELECT f1, f2, f3 FROM inttest ORDER BY f1;

-- DELETE values
DELETE FROM inttest WHERE f1 IN(100, -100);

SELECT f1, f2, f3 FROM inttest ORDER BY f1;

-- UPDATE values, single value
UPDATE inttest SET f1 = 101 WHERE f2 = 2002;

SELECT f1, f2, f3 FROM inttest ORDER BY f1;

-- UPDATE values, column order
UPDATE inttest SET f1 = -400, f2 = 1, f3 = 2 WHERE f1 = -1;

SELECT f1, f2, f3 FROM inttest ORDER BY f1;

-- UPDATE values, mixed column order
UPDATE inttest SET f2 = -2, f1 = -1, f3 = -3 WHERE f1 IN (4004, 7007, -400);

SELECT f1, f2, f3 FROM inttest ORDER BY f1;

-- DELETE everything
DELETE FROM inttest;

SELECT f1, f2, f3 FROM inttest ORDER BY f1;

COMMIT;

--
-- No transaction left, also show statistics
--
SELECT tx_in_progress, tx_num_commit, tx_num_rollback
FROM ifx_fdw_get_connections() ORDER BY 1;

--------------------------------------------------------------------------------
-- DML for VARCHAR/NVARCHAR/LVARCHAR
--------------------------------------------------------------------------------
BEGIN;

--
-- This file is LATIN encoded
--
SET LOCAL client_encoding TO 'ISO8859-1';

--
-- INSERT with simple characters
--
INSERT INTO varchar_test VALUES(DEFAULT, 'abc', 'def', 'ghi');

SELECT id, v1, v2, v3 FROM varchar_test ORDER BY id;

--
-- INSERT of special character (german umlaut)
--
INSERT INTO varchar_test VALUES(DEFAULT, 'ßßß', 'ÄÖÜ', 'äöü');

SELECT id, v1, v2, v3 FROM varchar_test ORDER BY id;

DELETE FROM varchar_test;

RESET client_encoding;

COMMIT;

--------------------------------------------------------------------------------
-- DML for TEXT/BYTE (Simple LO)
--------------------------------------------------------------------------------

BEGIN;

--
-- BYTEA => IFX BYTE
-- TEXT  => IFX TEXT
--
-- NOTE: TEXT is a Simple LO in Informix as well!
--

--
-- Simple string values...
--
INSERT INTO text_byte_test(v1, v2) VALUES('This is a text value',
                                          '...and this value gets converted into binary');

SELECT * FROM text_byte_test ORDER BY id ASC;;

--
-- Some special hex values for bytea...
--
INSERT INTO text_byte_test(v1, v2) VALUES('This is another text value',
                                          '\x00');
INSERT INTO text_byte_test(v1, v2) VALUES('This is another text value',
                                          '\x00AC00EF');

SELECT * FROM text_byte_test ORDER BY id ASC;

-- some deletion
DELETE FROM text_byte_test WHERE id IN (1, 2);

-- ...and finally clean everything
DELETE FROM text_byte_test;

COMMIT;

--------------------------------------------------------------------------------
-- DML for DATE/TIMESTAMP values
--------------------------------------------------------------------------------

BEGIN;

SET LOCAL DateStyle TO 'ISO, DMY';

INSERT INTO datetime_test(v1, v2, v3) VALUES('2013-08-19 15:30:00',
                                             '2013-08-19',
                                             '15:30:00');

SELECT v1, v2, v3 FROM datetime_test ORDER BY id ASC;

-- DELETE specific time value
DELETE FROM datetime_test WHERE v3 = '15:30:00';

SELECT v1, v2, v3 FROM datetime_test ORDER BY id ASC;

-- DELETE all
DELETE FROM datetime_test;

-- empty set expected
SELECT v1, v2, v3 FROM datetime_test ORDER BY id ASC;

COMMIT;

--------------------------------------------------------------------------------
-- DML for SERIAL values
--------------------------------------------------------------------------------

BEGIN;

INSERT INTO serial_test VALUES(DEFAULT);
INSERT INTO serial_test VALUES(1);
-- INSERT INT_MAX value
INSERT INTO serial_test VALUES(2147483647);

SELECT * FROM serial_test ORDER BY id ASC;

-- DELETE INT_MAX value
DELETE FROM serial_test WHERE id = 2147483647;

SELECT * FROM serial_test ORDER BY id ASC;

-- DELETE all
DELETE FROM serial_test;

-- empty set expected
SELECT * FROM serial_test ORDER BY id ASC;

COMMIT;

--------------------------------------------------------------------------------
-- DML for SERIAL8 values
--------------------------------------------------------------------------------

BEGIN;

INSERT INTO serial8_test VALUES(DEFAULT);
INSERT INTO serial8_test VALUES(1);
INSERT into serial8_test values(9223372036854775807);

SELECT * FROM serial8_test ORDER BY id ASC;

-- DELETE INT8_MAX value
DELETE FROM serial8_test WHERE id = 9223372036854775807;

SELECT * FROM serial8_test ORDER BY id ASC;

-- DELETE all
DELETE FROM serial8_test;

-- empty set expected
SELECT * FROM serial8_test ORDER BY id ASC;

COMMIT;

--------------------------------------------------------------------------------
-- DML for INTERVAL values
--------------------------------------------------------------------------------

BEGIN;

-- should work
INSERT INTO decimal_test VALUES((2^32)::numeric, 0.24, 4.91);

SAVEPOINT A;
-- should fail, exceeds precision
INSERT INTO decimal_test VALUES((2^64)::numeric, 0.1, 9.91);
ROLLBACK TO A;

-- inserts NULL
INSERT INTO decimal_test VALUES(45.491111, NULL, NULL);

SELECT * FROM decimal_test ORDER BY f1;

SELECT * FROM decimal_test WHERE f1 = 2^32;

COMMIT;

--------------------------------------------------------------------------------
-- DML for INTERVAL values
--------------------------------------------------------------------------------

BEGIN;

--
-- Informix supports interval types with a range of YYYY-MM and
-- dd hh24:mi:ss.fffff, where the fractions can have up to 5 digits.
--
-- Fractions aren't supported by the conversion from PostgreSQL to
-- Informix atm and are omitted. Interval values from PostgreSQL
-- which are overlapping the supported interval ranges in Informix
-- (e.g. 5 years 5 minutes) are truncated.
--

-- should succeed
INSERT INTO interval_test VALUES('5 years 1 month', '5 days 1 hours 1 minute 59 seconds', '3 hours 15 minutes');
INSERT INTO interval_test VALUES('5 years 15 month', '5 days 1 hours 1 minute 59 seconds', '3 hours 15 minutes');
INSERT INTO interval_test VALUES('1 years 0 month', '5 days 1 hours', '3 hours');
INSERT INTO interval_test VALUES('-100 years 0 month', '99 days 23 hours 59 minutes 59 seconds', '-24 hours 59 minutes');
INSERT INTO interval_test VALUES(NULL, NULL, NULL);

SELECT * FROM interval_test ORDER BY f1;

SELECT * FROM interval_test WHERE f1 IS NULL;

DELETE FROM interval_test WHERE f1 IS NULL;

SELECT * FROM interval_test WHERE f1 IS NULL;

DELETE FROM interval_test;

COMMIT;

BEGIN;

-- should fail, spans more than 100 days, syntax error in last interval value
INSERT INTO interval_test VALUES('-100 years 0 month', '99 days 24 hours', '-24 hours -59 minutes');

COMMIT;

DROP FOREIGN TABLE inttest;
DROP FOREIGN TABLE longvarchar_test;
DROP FOREIGN TABLE varchar_test;
DROP FOREIGN TABLE text_byte_test;
DROP FOREIGN TABLE serial_test;
DROP FOREIGN TABLE serial8_test;
DROP FOREIGN TABLE datetime_test;
DROP FOREIGN TABLE interval_test;
DROP FOREIGN TABLE decimal_test;

DROP USER MAPPING FOR CURRENT_USER SERVER test_server;
DROP SERVER test_server;
DROP EXTENSION informix_fdw;

--
-- Done.
--
