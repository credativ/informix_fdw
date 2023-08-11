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
\set ECHO none
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
-- With PostgreSQL 12, we have extra_float_digits set to 1. Make
-- this explicit for the regression tests, so we can get the same
-- output on older releases.
--
SET extra_float_digits TO 3;

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
CREATE FOREIGN TABLE text_byte_test(id bigserial not null, v1 bytea, v2 text)
SERVER test_server
OPTIONS(table 'text_byte_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB',
        enable_blobs '1');

--
-- Foreign tables to test DATETIME and DATE values.
--
CREATE FOREIGN TABLE datetime_test(id bigserial not null, v1 timestamp with time zone,
                                   v2 date, v3 time)
SERVER test_server
OPTIONS(table 'datetime_test',
        client_locale :'CLIENT_LOCALE',
        db_locale :'DB_LOCALE',
        database :'INFORMIXDB');

CREATE FOREIGN TABLE date_test(val date)
SERVER test_server
OPTIONS(table 'date_test',
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
-- Foreign table to test SERIAL8 values
--
CREATE FOREIGN TABLE serial8_test(id serial8)
SERVER test_server
OPTIONS("table" 'serial8_test',
                client_locale :'CLIENT_LOCALE',
                db_locale :'DB_LOCALE',
                database :'INFORMIXDB');

--
-- Foreign table to test BIGSERIAL values
--
CREATE FOREIGN TABLE bigserial_test(id bigserial not null, v1 varchar(20) not null)
SERVER test_server
OPTIONS("table" 'bigserial_test',
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
-- Foreign table to test FLOAT/DOUBLE PRECISION and
-- REAL values
--
CREATE FOREIGN TABLE float_test (
       val1 double precision,
       val2 real
) SERVER test_server
OPTIONS ("table" 'float_test',
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
-- ... should also use IN() pushdown ;)
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

--
-- Test IN() expression
--
SELECT * FROM inttest WHERE f1 IN (-2, -1, -3) ORDER BY f1;

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

-- IN() pushdown and quoting...
SELECT id, v1, v2, v3 FROM varchar_test WHERE v1 IN ('abc', 'def', 'ghi') ORDER BY id;

--
-- INSERT of special character (german umlaut)
--
INSERT INTO varchar_test VALUES(DEFAULT, 'ßßß', 'ÄÖÜ', 'äöü');

SELECT id, v1, v2, v3 FROM varchar_test ORDER BY id;

SELECT id, v1, v2, v3 FROM varchar_test WHERE v1 IN('ÄÖÜ', 'ßßß');

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
INSERT INTO text_byte_test(v1, v2) VALUES('This value gets converted into binary',
                                          'This is a text value');

SELECT * FROM text_byte_test ORDER BY id ASC;;

--
-- Some special hex values for bytea...
--
INSERT INTO text_byte_test(v1, v2) VALUES('\x00', 'This is another text value');
INSERT INTO text_byte_test(v1, v2) VALUES('\x00AC00EF',
                                          'This is another text value');

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

--
-- IN() expression...
-- NOTE: this must not generate a sql expression pushed down to the
--       Informix server, since TIMESTAMP(TZ)OID is not supported for pushdown.
--
DELETE FROM datetime_test WHERE v1 IN ('2013-08-19 15:4:00', '2013-08-19 15:50:00', '2013-08-19 15:20:00');

-- DELETE specific time value
DELETE FROM datetime_test WHERE v3 = '15:30:00';

SELECT v1, v2, v3 FROM datetime_test ORDER BY id ASC;

-- DELETE all
DELETE FROM datetime_test;

-- empty set expected
SELECT v1, v2, v3 FROM datetime_test ORDER BY id ASC;

-- empty set expected
SELECT val FROM date_test ORDER BY val;

-- INSERT, should succeed
INSERT INTO date_test(val) VALUES('1941-08-19');
INSERT INTO date_test(val) VALUES('0001-01-01');
SELECT val FROM date_test ORDER BY val;

SAVEPOINT _BEFORE_FAIL;

-- INSERT, should fail
INSERT INTO date_test(val) VALUES('0000-12-31');

ROLLBACK TO _BEFORE_FAIL;

SELECT val FROM date_test ORDER BY val;

-- leap year
INSERT INTO date_test(val) VALUES('2016-02-29');
SELECT val FROM date_test ORDER BY val;

-- DELETE, should succeed
DELETE FROM date_test WHERE val = '2016-02-29';
SELECT val FROM date_test ORDER BY val;

-- UPDATE, should succeed
UPDATE date_test SET val = '2016-02-29' WHERE val = '0001-01-01';
SELECT val FROM date_test ORDER BY val;

DELETE FROM date_test;

SELECT val FROM date_test ORDER BY val;

COMMIT;

--
-- We don't support PostgreSQL timestamp/timestamptz to Informix DATE conversions
--
BEGIN;

ALTER FOREIGN TABLE date_test ALTER val TYPE timestamptz;

SAVEPOINT _BEFORE_FAIL;

-- should fail
INSERT INTO date_test(val) VALUES('2016-10-10 15:25');

ROLLBACK TO _BEFORE_FAIL;

ALTER FOREIGN TABLE date_test ALTER val TYPE timestamp;

-- should fail
INSERT INTO date_test(val) VALUES('2016-10-10 15:25');

ROLLBACK TO _BEFORE_FAIL;

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
-- DML for BIGSERIAL values
--------------------------------------------------------------------------------

BEGIN;

-- NOTE: the 0 value will advance the informix sequence.

INSERT INTO bigserial_test(id, v1) VALUES(0, 'abc');
INSERT INTO bigserial_test(id, v1) VALUES(0, 'def');
INSERT into bigserial_test values(9223372036854775807, 'ghi');

SELECT * FROM bigserial_test ORDER BY id ASC;

-- DELETE INT8_MAX value
DELETE FROM bigserial_test WHERE id = 9223372036854775807;

SELECT * FROM bigserial_test ORDER BY id ASC;

-- DELETE all
DELETE FROM bigserial_test;

-- empty set expected
SELECT * FROM bigserial_test ORDER BY id ASC;

COMMIT;

--------------------------------------------------------------------------------
-- DML for DECIMAL values
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

-- UPDATE
UPDATE decimal_test SET f1 = -(2^32), f2 = -0.33, f3 = 9.999999999 WHERE f1 = 2^32;

SELECT * FROM decimal_test ORDER BY f1;

DELETE FROM decimal_test;

SELECT * FROM decimal_test ORDER BY f1;

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
INSERT INTO interval_test VALUES('-100 years 0 month', '99 days 23 hours 59 minutes 59 seconds', '24 hours 59 minutes');
INSERT INTO interval_test VALUES(NULL, NULL, NULL);

SELECT * FROM interval_test ORDER BY f1;

SELECT * FROM interval_test WHERE f1 IS NULL;

--
-- IN() expression pushdown...
-- NOTE: Must not trigger a pushed down SQL expression, since INTERVALOID is
--       not supported for pushdown.
--
SET intervalstyle TO sql_standard;
SELECT * FROM interval_test WHERE f1 IN ('5 years 1 month', '5 years 15 month');
RESET intervalstyle;

DELETE FROM interval_test WHERE f1 IS NULL;

SELECT * FROM interval_test WHERE f1 IS NULL;

DELETE FROM interval_test;

COMMIT;

BEGIN;

-- should fail, spans more than 100 days, syntax error in last interval value
INSERT INTO interval_test VALUES('-100 years 0 month', '99 days 24 hours', '-24 hours -59 minutes');

COMMIT;

--------------------------------------------------------------------------------
-- DML for REAL, FLOAT/DOUBLE PRECISION data type
--------------------------------------------------------------------------------

BEGIN;

-- DBL_MAX and FLT_MAX
INSERT INTO float_test(val1, val2)
       VALUES(1797693134862315708145274237317043567980705675258449965989174768031572607800285387605895586327668781715404589535143824642343213268894641827684675467035375169860499105765512820762454900903893289440758685084551339423045832369032229481658085593321233482747978262041,
       340282346638528859811704183484516925440.000000);

SELECT * FROM float_test ORDER BY val1 ASC;

-- -1 * DBL_MIN and -1 * FLT_MIN
INSERT INTO float_test(val1, val2)
       VALUES(-1 * 1797693134862315708145274237317043567980705675258449965989174768031572607800285387605895586327668781715404589535143824642343213268894641827684675467035375169860499105765512820762454900903893289440758685084551339423045832369032229481658085593321233482747978262041,
       -1 * 340282346638528859811704183484516925440.000000);

SELECT * FROM float_test ORDER BY val1 ASC;

-- NULL values
INSERT INTO float_test(val1, val2) VALUES(NULL, NULL);

SELECT * FROM float_test ORDER BY val1 ASC NULLS FIRST;

DELETE FROM float_test WHERE val2 = 3.40282e+38;

SELECT * FROM float_test ORDER BY val1 ASC NULLS FIRST;

-- delete NULLs
DELETE FROM float_test WHERE val1 IS NULL;

SELECT * FROM float_test ORDER BY val1 ASC NULLS FIRST;

DELETE FROM float_test;

-- empty
SELECT * FROM float_test;

INSERT INTO float_test(val1, val2)
VALUES
(1, 0.2),
(1.25, 0.250),
(1.5, 0.300),
(1.75, 0.350),
(2, 0.400),
(2.25, 0.450),
(2.5, 0.500),
(2.75, 0.550),
(3, 0.600),
(3.25, 0.650),
(3.5, 0.700),
(3.75, 0.750),
(4, 0.800),
(4.25, 0.850),
(4.5, 0.900),
(4.75, 0.950),
(5, 1.000),
(5.25, 1.050),
(5.5, 1.100),
(5.75, 1.150),
(6, 1.200),
(6.25, 1.250),
(6.5, 1.300),
(6.75, 1.350),
(7, 1.400),
(7.25, 1.450),
(7.5, 1.500),
(7.75, 1.550),
(8, 1.600),
(8.25, 1.650),
(8.5, 1.700),
(8.75, 1.750),
(9, 1.800),
(9.25, 1.850),
(9.5, 1.900),
(9.75, 1.950),
(10, 2.000);

SELECT COUNT(*) FROM float_test;

-- delete specific range
DELETE FROM float_test WHERE val1 BETWEEN 2.25 AND 3.75;

SELECT COUNT(*) FROM float_test;

-- delete specific range
SELECT val1, val2 FROM float_test WHERE val1 BETWEEN 7.00 AND 8.333333 ORDER BY val1 ASC;
DELETE FROM float_test WHERE val1 BETWEEN 7.00 AND 8.333333;
SELECT val1, val2 FROM float_test WHERE val1 BETWEEN 7.00 AND 8.333333 ORDER BY val1 ASC;

UPDATE float_test SET val2 = 0.333333333333 WHERE val1 BETWEEN 1.0 and 1.333333;

SELECT val1, val2 FROM float_test WHERE val2 = 0.333333 ORDER BY val1 ASC;

-- check IN() pushdown
SELECT val1, val2 FROM float_test WHERE val1 IN (5, 8.75, 9.75, 1.0, 1.25);

-- empty
DELETE FROM float_test;

COMMIT;

--------------------------------------------------------------------------------
-- Transaction/Savepoint tests
--------------------------------------------------------------------------------

DROP FOREIGN TABLE inttest;

CREATE FOREIGN TABLE inttest(f1 bigint not null, f2 integer, f3 smallint)
SERVER test_server
OPTIONS (table 'inttest',
         client_locale :'CLIENT_LOCALE',
         db_locale :'DB_LOCALE',
         database :'INFORMIXDB');

BEGIN;

-- insert some values
INSERT INTO inttest VALUES(-1, -2, -3), (1, 2, 3), (4, 5, 6);
SELECT * FROM inttest ORDER BY f1;

SAVEPOINT A;

INSERT INTO inttest VALUES(7, 8, 9);
SELECT * FROM inttest ORDER BY f1;

SELECT tx_in_progress FROM ifx_fdw_get_connections();

SAVEPOINT B;

DELETE FROM inttest WHERE f1 = -1;
SELECT * FROM inttest ORDER BY f1;

SELECT tx_in_progress FROM ifx_fdw_get_connections();

-- commit SAVEPOINT B;
ROLLBACK TO SAVEPOINT B;

SELECT tx_in_progress FROM ifx_fdw_get_connections();

SELECT * FROM inttest ORDER BY f1;

RELEASE SAVEPOINT B;

SELECT tx_in_progress FROM ifx_fdw_get_connections();

SELECT * FROM inttest ORDER BY f1;

-- rollback to SAVEPOINT A;
ROLLBACK TO SAVEPOINT A;

SELECT tx_in_progress FROM ifx_fdw_get_connections();

SELECT * FROM inttest ORDER BY f1;

COMMIT;

SELECT tx_in_progress FROM ifx_fdw_get_connections();

--------------------------------------------------------------------------------
-- Test CURSOR based DML
-- Starting with commit 43c8afc we disallow UPDATE and DELETE in
-- case we have an Informix foreign table with disable_rowid=1.
-- This never really worked as intended for certain UPDATE and DELETE plans
-- anyways.
--------------------------------------------------------------------------------

DROP FOREIGN TABLE inttest;

CREATE FOREIGN TABLE inttest(f1 bigint not null, f2 integer, f3 smallint)
SERVER test_server
OPTIONS (table 'inttest',
         disable_rowid '1',
         client_locale :'CLIENT_LOCALE',
         db_locale :'DB_LOCALE',
         database :'INFORMIXDB');

BEGIN;

INSERT INTO inttest(f1, f2, f3) VALUES(1001, 2002, 3003), (4004, 5005, 6006), (7007, 8008, 9009);

SELECT * FROM inttest ORDER BY f1 ASC;

-- should fail
UPDATE inttest SET f1 = -1 * 4004 WHERE f1 = 4004;

ROLLBACK;
BEGIN;

SELECT * FROM inttest ORDER BY f1 ASC;

-- should fail
DELETE FROM inttest WHERE f1 = 4004;

ROLLBACK;
BEGIN;

SELECT * FROM inttest ORDER BY f1 ASC;

-- should fail
DELETE FROM inttest;

ROLLBACK;

--------------------------------------------------------------------------------
-- Some more complicated DML statements, default behavior
--------------------------------------------------------------------------------

DROP FOREIGN TABLE inttest;

CREATE FOREIGN TABLE inttest(f1 bigint not null, f2 integer, f3 smallint)
SERVER test_server
OPTIONS (table 'inttest',
         client_locale :'CLIENT_LOCALE',
         db_locale :'DB_LOCALE',
         database :'INFORMIXDB');


BEGIN;

CREATE TEMP TABLE local_inttest(id integer) ON COMMIT DROP;
INSERT INTO local_inttest SELECT t.id FROM generate_Series(1, 1000) AS t(id);
INSERT into inttest VALUES(1, 2, 3), (4, 5, 6), (7, 8, 9);

SELECT f1, f2, f3 FROM inttest ORDER BY f1 ASC;

--
-- This UPDATE should lead to a hash join, where the remote table
-- is hashed first. With the default rowID behavior, this is expected to
-- work properly. We make other join conditions more expensive to nearly
-- force such a plan...
--
SET LOCAL enable_nestloop = off;
SET LOCAL enable_mergejoin = off;

UPDATE inttest SET f1 = t.id FROM local_inttest t WHERE t.id = f1 AND t.id BETWEEN 1 AND 2000;

SELECT f1, f2, f3 FROM inttest ORDER BY f1 ASC;

--
-- ...and DELETE.
--
DELETE FROM inttest USING local_inttest t WHERE t.id = inttest.f1;

SELECT f1, f2, f3 FROM inttest ORDER BY f1 ASC;

DELETE FROM inttest;

COMMIT;

--
-- Change ROWID-based modify action to cursor-based modify action.
--
ALTER FOREIGN TABLE inttest OPTIONS(ADD disable_rowid '1');

BEGIN;

CREATE TEMP TABLE local_inttest(id integer) ON COMMIT DROP;
INSERT INTO local_inttest SELECT t.id FROM generate_Series(1, 1000) AS t(id);
INSERT into inttest VALUES(1, 2, 3), (4, 5, 6), (7, 8, 9);

SELECT f1, f2, f3 FROM inttest ORDER BY f1 ASC;

--
-- This UPDATE should lead to a hash join, where the remote table
-- is hashed first. With the default rowID behavior, this is expected to
-- work properly. We make other join conditions more expensive to nearly
-- force such a plan...
--
SET LOCAL enable_nestloop = off;
SET LOCAL enable_mergejoin = off;

SAVEPOINT A;

--
-- This should fail, since we don't support such updates (the cursor
-- will be positioned on the wrong tuple, thus we encounter an invalid
-- state. Thus this UPDATE is rejected...
--
UPDATE inttest SET f1 = t.id FROM local_inttest t WHERE t.id = f1 AND t.id BETWEEN 1 AND 2000;

ROLLBACK TO A;
RELEASE SAVEPOINT A;

SELECT f1, f2, f3 FROM inttest ORDER BY f1 ASC;

SAVEPOINT B;

--
-- ...and DELETE.
--
-- This is expected to fail, too, since it has the same problem as the UPDATE
-- above.
--
DELETE FROM inttest USING local_inttest t WHERE t.id = inttest.f1;

ROLLBACK TO B;

SELECT f1, f2, f3 FROM inttest ORDER BY f1 ASC;

DELETE FROM inttest;

COMMIT;

--
-- Change back to default behavior
--
ALTER FOREIGN TABLE inttest OPTIONS(DROP disable_rowid);

--------------------------------------------------------------------------------
-- Tests for PREPARE
--
-- See discussion in github issue
-- https://github.com/credativ/informix_fdw/issues/31
--------------------------------------------------------------------------------

--
-- INSERT
--

BEGIN;


PREPARE ins_inttest(bigint) AS INSERT INTO inttest VALUES($1);

EXECUTE ins_inttest (1);

EXECUTE ins_inttest (2);

EXECUTE ins_inttest (3);

EXECUTE ins_inttest (4);

EXECUTE ins_inttest (5);

EXECUTE ins_inttest (6);

EXECUTE ins_inttest (7);

COMMIT;

DEALLOCATE ins_inttest;

--
-- UPDATE
--

BEGIN;

PREPARE upd_inttest(bigint) AS UPDATE inttest SET f1 = f1 WHERE f1 = $1;

EXECUTE upd_inttest (1);

EXECUTE upd_inttest (2);

EXECUTE upd_inttest (3);

EXECUTE upd_inttest (4);

EXECUTE upd_inttest (5);

EXECUTE upd_inttest (6);

EXECUTE upd_inttest (7);

COMMIT;

DEALLOCATE upd_inttest;

--
-- DELETE
--

BEGIN;

PREPARE del_inttest(bigint) AS DELETE FROM inttest WHERE f1 = $1;

EXECUTE del_inttest (1);

EXECUTE del_inttest (2);

EXECUTE del_inttest (3);

EXECUTE del_inttest (4);

EXECUTE del_inttest (5);

EXECUTE del_inttest (6);

EXECUTE del_inttest (7);

COMMIT;

DEALLOCATE del_inttest;

--------------------------------------------------------------------------------
-- Trigger Tests
--------------------------------------------------------------------------------
BEGIN;

CREATE TABLE IF NOT EXISTS delete_fdw_trigger_test(id bigint primary key);

--
-- A before trigger testing before actions on INSERT/UPDATE/DELETE
-- on a foreign table
--
CREATE OR REPLACE FUNCTION f_tg_test()
RETURNS trigger
LANGUAGE plpgsql
AS
$$
BEGIN
	IF TG_OP = 'DELETE' THEN
	   DELETE FROM inttest WHERE f1 = OLD.id;
	   RETURN OLD;
	ELSIF TG_OP = 'UPDATE' THEN
	   UPDATE inttest SET f1 = NEW.id WHERE f1 = OLD.id;
	   RETURN NEW;
	ELSIF TG_OP = 'INSERT' THEN
	   INSERT INTO inttest VALUES(NEW.id);
	   RETURN NEW;
	ELSE
           RAISE EXCEPTION 'unhandled trigger action %', TG_OP;
	END IF;
END;
$$;

--
-- A broken trigger function referencing the wrong tuple identifiers
-- according to the trigger action (NEW vs. OLD)
--
-- Basically the same as above.
--
CREATE OR REPLACE FUNCTION f_tg_test_broken()
RETURNS trigger
LANGUAGE plpgsql
AS
$$
BEGIN
	IF TG_OP = 'DELETE' THEN
	   DELETE FROM inttest WHERE f1 = NEW.id;
	   RETURN OLD;
	ELSIF TG_OP = 'UPDATE' THEN
	   UPDATE inttest SET f1 = NEW.id WHERE f1 = OLD.id;
	   RETURN NEW;
	ELSIF TG_OP = 'INSERT' THEN
	   INSERT INTO inttest VALUES(OLD.id);
	   RETURN NEW;
	ELSE
           RAISE EXCEPTION 'unhandled trigger action %', TG_OP;
	END IF;
END;
$$;

CREATE TRIGGER tg_inttest
BEFORE DELETE OR UPDATE OR INSERT ON delete_fdw_trigger_test
FOR EACH ROW EXECUTE PROCEDURE f_tg_test();

TRUNCATE delete_fdw_trigger_test;
INSERT INTO delete_fdw_trigger_test VALUES(1), (2), (3);

SELECT * FROM inttest;

DELETE FROM delete_fdw_trigger_test WHERE id = 2;

SELECT * FROM inttest;

UPDATE delete_fdw_trigger_test SET id = 4 WHERE id = 3;

SELECT * FROM inttest;

INSERT INTO delete_fdw_trigger_test VALUES(5);

SELECT * FROM inttest;

DELETE FROM delete_fdw_trigger_test;

SELECT * FROM inttest;

DROP TRIGGER tg_inttest ON delete_fdw_trigger_test;

CREATE TRIGGER tg_inttest
BEFORE DELETE OR UPDATE OR INSERT ON delete_fdw_trigger_test
FOR EACH ROW EXECUTE PROCEDURE f_tg_test_broken();

-- should fail
SAVEPOINT broken;
INSERT INTO delete_fdw_trigger_test VALUES(1), (2), (3);
ROLLBACK TO broken;

SELECT * FROM inttest;

-- should delete nothing
DELETE FROM delete_fdw_trigger_test WHERE id = 2;

SELECT * FROM inttest;

-- should update nothing
UPDATE delete_fdw_trigger_test SET id = 4 WHERE id = 3;

SELECT * FROM inttest;

DELETE FROM delete_fdw_trigger_test;

SELECT * FROM inttest;

DROP TRIGGER tg_inttest ON delete_fdw_trigger_test;

COMMIT;

--------------------------------------------------------------------------------
-- Regression Tests End, Cleanup
--------------------------------------------------------------------------------

DROP FOREIGN TABLE float_test;
DROP FOREIGN TABLE inttest;
DROP FOREIGN TABLE longvarchar_test;
DROP FOREIGN TABLE varchar_test;
DROP FOREIGN TABLE text_byte_test;
DROP FOREIGN TABLE serial_test;
DROP FOREIGN TABLE serial8_test;
DROP FOREIGN TABLE datetime_test;
DROP FOREIGN TABLE date_test;
DROP FOREIGN TABLE interval_test;
DROP FOREIGN TABLE decimal_test;
DROP FOREIGN TABLE bigserial_test;

DROP USER MAPPING FOR CURRENT_USER SERVER test_server;
DROP SERVER test_server;
DROP EXTENSION informix_fdw;

--
-- Done.
--
