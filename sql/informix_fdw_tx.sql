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

-- DROP FOREIGN TABLE inttest;
-- DROP USER MAPPING FOR CURRENT_USER SERVER test_server;
-- DROP SERVER test_server;
-- DROP EXTENSION informix_fdw;

--
-- Done.
--
