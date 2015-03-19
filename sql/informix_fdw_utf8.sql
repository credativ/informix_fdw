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
-- Create foreign server
--

CREATE SERVER test_server
FOREIGN DATA WRAPPER informix_fdw
OPTIONS (informixserver :'INFORMIXSERVER',
         informixdir :'INFORMIXDIR');

CREATE USER MAPPING FOR CURRENT_USER
SERVER test_server
OPTIONS (username :'INFORMIXUSER', password :'INFORMIXPASSWORD');

CREATE FOREIGN TABLE test_utf8
(
        test text
)
SERVER test_server
OPTIONS(database :'INFORMIXDB',
        table 'test_utf8',
        client_locale :'CLIENT_LOCALE');

SELECT * FROM test_utf8;

--
-- Clean up
--
DROP FOREIGN TABLE test_utf8;
DROP USER MAPPING FOR CURRENT_USER SERVER test_server;
DROP SERVER test_server;
