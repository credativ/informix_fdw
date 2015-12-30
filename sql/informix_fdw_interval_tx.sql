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
-- These tests basically are the same like informix_fdw_tx, however,
-- they employ the IMPORT SCHEMA machinery to import the
-- test tables from the remote server.

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
-- Load extension
--
CREATE EXTENSION informix_fdw;

--
-- Use a dedicated schema
--
CREATE SCHEMA test;
SET search_path TO test;

--
-- Create required SERVER and USER MAPPING
--
CREATE SERVER test_server
FOREIGN DATA WRAPPER informix_fdw
OPTIONS (informixserver :'INFORMIXSERVER',
         informixdir :'INFORMIXDIR');

CREATE USER MAPPING FOR CURRENT_USER
SERVER test_server
OPTIONS (username :'INFORMIXUSER', password :'INFORMIXPASSWORD');

--
-- Get the foreign table via IMPORT FOREIGN TABLE
--
IMPORT FOREIGN SCHEMA informix 
LIMIT TO (weird_table) 
FROM SERVER test_server 
INTO test
OPTIONS(informixdir :'INFORMIXDIR', client_locale :'CLIENT_LOCALE', db_locale :'DB_LOCALE', database :'INFORMIXDB');

-- NOTE:
-- The Informix relation weird_table is expected to have
-- the following layout in the remote side:
--
-- CREATE TABLE IF NOT EXISTS weird_table (
--              id int PRIMARY KEY,
--              time DATETIME YEAR TO FRACTION(5),
--              ival1 INTERVAL YEAR TO YEAR,
--              ival2 INTERVAL MONTH TO MONTH,
--              ival3 INTERVAL DAY TO DAY,
--              ival4 INTERVAL HOUR TO HOUR,
--              ival5 INTERVAL MINUTE TO MINUTE,
--              ival6 INTERVAL SECOND TO SECOND,
--              ival7 INTERVAL YEAR TO MONTH,
--              ival8 INTERVAL DAY TO HOUR,
--              ival9 INTERVAL DAY TO MINUTE,
--              ival10 INTERVAL DAY TO SECOND,
--              ival11 INTERVAL HOUR TO MINUTE,
--              ival12 INTERVAL HOUR TO SECOND,
--              ival13 INTERVAL MINUTE TO SECOND,
--              text varchar(128)
-- );

--
-- Layout of local table
--
\d weird_table

--
-- INSERT
--
INSERT INTO weird_table (id, ival1) VALUES ('1', Interval '3 years');
INSERT INTO weird_table (id, ival2) VALUES ('2', Interval '3 months');
INSERT INTO weird_table (id, ival3) VALUES ('3', Interval '3 days');
INSERT INTO weird_table (id, ival4) VALUES ('4', Interval '3 hours');
INSERT INTO weird_table (id, ival5) VALUES ('5', Interval '3 minutes');
INSERT INTO weird_table (id, ival6) VALUES ('6', Interval '3 seconds');
INSERT INTO weird_table (id, ival7) VALUES ('7', Interval '3 years 3 months');
INSERT INTO weird_table (id, ival8) VALUES ('8', Interval '3 days 3 hours');
INSERT INTO weird_table (id, ival9) VALUES ('9', Interval '3 days 3 minutes');
INSERT INTO weird_table (id, ival10) VALUES ('10', Interval '3 days 3 seconds');
INSERT INTO weird_table (id, ival11) VALUES ('11', Interval '3 hours 3 minutes');
INSERT INTO weird_table (id, ival12) VALUES ('12', Interval '3 hours 3 seconds');
INSERT INTO weird_table (id, ival13) VALUES ('13', Interval '3 minutes 3 seconds');

--
-- Verify output
--
SELECT * FROM weird_table ORDER BY id ASC;

--
-- DELETE certain intervals
--
DELETE FROM weird_table WHERE ival1 = interval '3 years';

DELETE FROM weird_table WHERE ival13 = interval '3 minutes 3 seconds';

-- range...
DELETE FROM weird_table WHERE ival10 BETWEEN interval '3 days' AND interval '3 days 15 seconds';

--
-- Verify output
--
SELECT * FROM weird_table ORDER BY id ASC;

--------------------------------------------------------------------------------
-- Regression Tests End, Cleanup
--------------------------------------------------------------------------------

DELETE FROM weird_table;

RESET search_path;
DROP SCHEMA test CASCADE;
DROP USER MAPPING FOR CURRENT_USER SERVER test_server;
DROP SERVER test_server;
DROP EXTENSION informix_fdw;

--
-- Done.
--
