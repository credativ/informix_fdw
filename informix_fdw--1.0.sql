CREATE FUNCTION ifx_fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION ifx_fdw_handler()
IS 'Informix foreign data wrapper handler';

CREATE FUNCTION ifx_fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION ifx_fdw_validator(text[], oid)
IS 'Informix foreign data wrapper options validator';

CREATE FOREIGN DATA WRAPPER informix_fdw
  HANDLER ifx_fdw_handler
  VALIDATOR ifx_fdw_validator;

COMMENT ON FOREIGN DATA WRAPPER informix_fdw
IS 'Informix foreign data wrapper';

CREATE OR REPLACE FUNCTION ifx_fdw_get_connections(OUT connection_name text,
                                                   OUT established_by_relid oid,
                                                   OUT servername text,
                                                   OUT informixdir text,
                                                   OUT database text,
                                                   OUT username text,
                                                   OUT usage integer,
                                                   OUT db_locale text,
                                                   OUT client_locale text,
                                                   OUT uses_tx boolean,
                                                   OUT db_ansi boolean)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'ifxGetConnections'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION ifx_fdw_close_connection(IN connection_name text)
RETURNS void
AS 'MODUL_PATHNAME', 'ifxCloseConnection'
LANGUAGE C VOLATILE STRICT;
