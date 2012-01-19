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
