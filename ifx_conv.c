/*-------------------------------------------------------------------------
 *
 * ifx_conv.c
 *		  Datatype conversion routines and helper functions.
 *
 * Be cautious about memory allocations outside
 * our memory context. Informix ESQL/C APIs allocate memory
 * under the hood of our PostgreSQL memory contexts. You *must*
 * call ifxRewindCallstack before re-throwing any PostgreSQL
 * elog's, otherwise you likely leak memory.
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_conv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif

#include "catalog/pg_cast.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

#if PG_VERSION_NUM >= 90500
#include "utils/ruleutils.h"
#endif

#include "utils/syscache.h"

#include "ifx_fdw.h"
#include "ifx_node_utils.h"

/*******************************************************************************
 * Helper functions
 */
static regproc getTypeCastFunction(IfxFdwExecutionState *state,
								   Oid sourceOid, Oid targetOid);
static regproc getTypeInputFunction(IfxFdwExecutionState *state,
									Oid inputOid);
static void
deparse_predicate_node(IfxPushdownOprContext *context,
					   IfxPushdownOprInfo *info);

static char *getIfxOperatorIdent(IfxPushdownOprInfo *pushdownInfo);
static char * getConstValue(Const *constNode);
static void rewriteInExprContext(Const *arrayConst,
								 IfxPushdownInOprContext *cxt);
void deparse_node_list_for_InExpr(IfxPushdownOprContext *context,
								  IfxPushdownInOprContext *in_cxt,
								  IfxPushdownOprInfo *info);
static regproc getTypeOutputFunction(Oid inputOid);

#if PG_VERSION_NUM >= 90300

static inline char *interval_to_cstring(IfxFdwExecutionState *state,
										TupleTableSlot       *slot,
										int                   attnum,
										IfxFormatMode         mode);
#endif

/*******************************************************************************
 * Implementation starts here
 */

/*
 * convertIfxDateString()
 *
 * Converts an informix formatted date string into a PostgreSQL
 * DATE datum. Conversion is supported to
 *
 * DATE
 * TEXT
 * VARCHAR
 * BPCHAR
 */
Datum convertIfxDateString(IfxFdwExecutionState *state, int attnum)
{
	Datum result;
	Oid   inputOid;
	char *val;
	regproc typeinputfunc;

	/*
	 * Init...
	 */
	result   = PointerGetDatum(NULL);

	switch(PG_ATTRTYPE_P(state, attnum))
	{
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case DATEOID:
			inputOid = PG_ATTRTYPE_P(state, attnum);
			break;
		default:
		{
			/* oops, unexpected datum conversion */
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return result;
		}
	}

	val = (char *)palloc0(IFX_DATE_BUFFER_LEN);
	if (ifxGetDateAsString(&(state->stmt_info),
						   PG_MAPPED_IFX_ATTNUM(state, attnum), val) == NULL)
	{
		/*
		 * Got a SQL null value or conversion error. Leave it up to
		 * the caller to look what's wrong (at least, we can't error
		 * out at this place, since the caller need's the chance to
		 * clean up itself).
		 */
		return result;
	}


	/*
	 * Catch errors from subsequent function calls...
	 */
	PG_TRY();
	{
		/*
		 * Try the conversion.
		 */
		typeinputfunc = getTypeInputFunction(state, inputOid);
		result = OidFunctionCall3(typeinputfunc,
								  CStringGetDatum(val),
								  ObjectIdGetDatum(InvalidOid),
								  Int32GetDatum(PG_ATTRTYPEMOD_P(state, attnum)));
	}
	PG_CATCH();
	{
		ifxRewindCallstack(&(state->stmt_info));
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
 }

/*
 * convertIfxInterval()
 *
 * Converts an Informix Interval data value into a PostgreSQL datum.
 *
 * Conversion is supported to
 * TEXT
 * BPCHAR
 * VARCHAR
 * INTERVAL
 */
Datum convertIfxInterval(IfxFdwExecutionState *state, int attnum)
{
	Datum   result;
	char   *val;
	Oid     inputOid;
	regproc typeinputfunc;

	result = PointerGetDatum(NULL);

	switch (PG_ATTRTYPE_P(state, attnum))
	{
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case INTERVALOID:
			inputOid = PG_ATTRTYPE_P(state, attnum);
			break;
		default:
		{
			/* oops, unexpected datum conversion */
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return result;
		}
	}

	/*
	 * We get the Informix INTERVAL value as a formatted character string.
	 * Prepare a buffer for it and call the appropiate conversion function
	 * from our Informix API...
	 */
	val = (char *) palloc0(IFX_DATETIME_BUFFER_LEN);

	if (ifxGetIntervalAsString(&(state->stmt_info),
							   PG_MAPPED_IFX_ATTNUM(state, attnum), val) == NULL)
	{
		/*
		 * Got a SQL null value or conversion error. Leave it up to the
		 * caller to look what's wrong. We can't error out at this stage,
		 * since we need to give the caller a chance to clean up itself.
		 */
		return result;
	}

	PG_TRY();
	{
		/*
		 * Call the target type input function, but keep an eye on
		 * possible failing conversions. The caller needs to be informed
		 * somehting went wrong...
		 */
		typeinputfunc = getTypeInputFunction(state, inputOid);
		result        = OidFunctionCall3(typeinputfunc,
										 CStringGetDatum(val),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(PG_ATTRTYPEMOD_P(state, attnum)));
	}
	PG_CATCH();
	{
		ifxRewindCallstack(&(state->stmt_info));
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* ...and we're done */
	return result;
}


/*
 * convertIfxTimestamp()
 *
 * Converts a given Informix DATETIME value into
 * a PostgreSQL timestamp.
 */
Datum convertIfxTimestampString(IfxFdwExecutionState *state, int attnum)
{
	Datum result;
	Oid   inputOid;
	char  *val;
	regproc typeinputfunc;

	/*
	 * Init ...
	 */
	result = PointerGetDatum(NULL);

	switch (PG_ATTRTYPE_P(state, attnum))
	{
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case TIMEOID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			inputOid = PG_ATTRTYPE_P(state, attnum);
			break;
		default:
		{
			/* oops, unexpected datum conversion */
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return result;
		}
	}

	/*
	 * We get the Informix DTIME value as a ANSI SQL
	 * formatted character string. Prepare a buffer for it
	 * and call the appropiate conversion function from our
	 * Informix API...
	 */
	val = (char *) palloc0(IFX_DATETIME_BUFFER_LEN);

	if (ifxGetTimestampAsString(&(state->stmt_info),
								PG_MAPPED_IFX_ATTNUM(state, attnum), val) == NULL)
	{
		/*
		 * Got a SQL null value or conversion error. Leave it up to
		 * the caller to look what's wrong (at least, we can't error
		 * out at this place, since the caller need's the chance to
		 * clean up itself).
		 */
		return result;
	}

	PG_TRY();
	{
		/*
		 * Get the input function and try the conversion. We just pass
		 * the character string into the specific type input function.
		 */
		typeinputfunc = getTypeInputFunction(state, inputOid);
		result = OidFunctionCall3(typeinputfunc,
								  CStringGetDatum(val),
								  ObjectIdGetDatum(InvalidOid),
								  Int32GetDatum(PG_ATTRTYPEMOD_P(state, attnum)));
	}
	PG_CATCH();
	{
		ifxRewindCallstack(&(state->stmt_info));
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
 }

/*
 * convertIfxFloat()
 *
 * Converts a float value into a PostgreSQL numeric or float value.
 */
Datum convertIfxFloat(IfxFdwExecutionState *state, int attnum)
{
	Datum result;
	regproc typinputfunc;

	result = PointerGetDatum(NULL);

	switch(PG_ATTRTYPE_P(state, attnum))
	{
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		{
			char *val;

			val = (char *) palloc0(IFX_MAX_FLOAT_DIGITS + 1);

			/*
			 * Call must handle NULL column value or invalid data conversion.
			 */
			if (ifxGetFloatAsString(&state->stmt_info,
									PG_MAPPED_IFX_ATTNUM(state, attnum),
									val) == NULL)
			{
				/* caller should handle indicator */
				break;
			}

			PG_TRY();
			{
				typinputfunc = getTypeInputFunction(state,
													PG_ATTRTYPE_P(state, attnum));

				/*
				 * Convert float text representation into target type.
				 *
				 * If the source type is a NUMERICOID datum, we have to
				 * take care to apply its typmods...
				 */
				if (PG_ATTRTYPE_P(state, attnum) != NUMERICOID)
				{
					result = OidFunctionCall2(typinputfunc,
											  CStringGetDatum(val),
											  ObjectIdGetDatum(InvalidOid));
				}
				else
				{
					result = OidFunctionCall3(typinputfunc,
											  CStringGetDatum(val),
											  ObjectIdGetDatum(InvalidOid),
											  PG_ATTRTYPEMOD_P(state, attnum));
				}
			}
			PG_CATCH();
			{
				ifxRewindCallstack(&(state->stmt_info));
				PG_RE_THROW();
			}
			PG_END_TRY();

			break;
		}
		default:
		{
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			break;
		}
	}

	return result;
}

/*
 * convertIfxDecimal()
 *
 * Converts a decimal value into a PostgreSQL numeric datum.
 * Note that convertIfxDecimal() works by converting the
 * character representation of a dec_t value formerly retrieved
 * from an informix column. This we must be aware of any locale
 * settings here.
 *
 * Currently, we support conversion to numeric types only.
 */
Datum convertIfxDecimal(IfxFdwExecutionState *state, int attnum)
{
	Datum result;
	Oid inputOid;
	char *val;
	regproc typinputfunc;

	/*
	 * Init...
	 */
	result = PointerGetDatum(NULL);

	switch(PG_ATTRTYPE_P(state, attnum))
	{
		case TEXTOID:
		case VARCHAROID:
		case NUMERICOID:
		case CASHOID:
		{
			inputOid = PG_ATTRTYPE_P(state, attnum);
			break;
		}
		default:
		{
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return result;
		}
	}

	val = (char *) palloc0(IFX_DECIMAL_BUF_LEN + 1);

	/*
	 * Get the value from the informix column and check wether
	 * the character string is valid. Don't go further, if not...
	 */
	if (ifxGetDecimal(&state->stmt_info,
					  PG_MAPPED_IFX_ATTNUM(state, attnum), val) == NULL)
	{
		/* caller should handle indicator */
		return result;
	}

	/*
	 * Type input function known and target column looks compatible,
	 * let's try the conversion...
	 */
	PG_TRY();
	{
		/*
		 * Get the type input function.
		 */
		typinputfunc = getTypeInputFunction(state, inputOid);

		/*
		 * Watch out for typemods
		 */
		result = OidFunctionCall3(typinputfunc,
								  CStringGetDatum(val),
								  ObjectIdGetDatum(InvalidOid),
								  Int32GetDatum(PG_ATTRTYPEMOD_P(state, attnum)));
	}
	PG_CATCH();
	{
		ifxRewindCallstack(&(state->stmt_info));
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/*
 * convertIfxInt()
 *
 * Converts either an 2-, 4-, or 8-byte informix integer value
 * into a corresponding PostgreSQL datum. The target type
 * range is checked and conversion refused if it doesn't
 * match. We also support conversion into either TEXT, VARCHAR
 * and bpchar.
 */
Datum convertIfxInt(IfxFdwExecutionState *state, int attnum)
{
	Datum result = PointerGetDatum(NULL);
	PgAttrDef pg_def;

	/*
	 * Setup stuff...
	 */
	pg_def = state->pgAttrDefs[attnum];

	/*
	 * Do the conversion...
	 */
	switch(pg_def.atttypid)
	{
		case INT2OID:
		{
			int16 val;

			/* accepts int2 only */
			if (IFX_ATTRTYPE_P(state, attnum) != IFX_SMALLINT)
			{
				IFX_ATTR_SETNOTVALID_P(state, attnum);
				return PointerGetDatum(NULL);
			}

			val = ifxGetInt2(&(state->stmt_info),
							 PG_MAPPED_IFX_ATTNUM(state, attnum));
			result = Int16GetDatum(val);

			break;
		}
		case INT4OID:
		{
			int val;

			/* accepts int2 and int4/serial */
			if ((IFX_ATTRTYPE_P(state, attnum) != IFX_SMALLINT)
				&& (IFX_ATTRTYPE_P(state, attnum) != IFX_INTEGER)
				&& (IFX_ATTRTYPE_P(state, attnum) != IFX_SERIAL))
			{
				IFX_ATTR_SETNOTVALID_P(state, attnum);
				return PointerGetDatum(NULL);
			}

			val = ifxGetInt4(&(state->stmt_info),
							 PG_MAPPED_IFX_ATTNUM(state, attnum));
			result = Int32GetDatum(val);

			break;
		}
		case INT8OID:
			/*
			 * Note that the informix int8 value retrieved by
			 * ifxGetInt8() is converted into its *character*
			 * representation. We leave it up to the typinput
			 * routine to convert it back to a PostgreSQL BIGINT.
			 * So fall through and do the work below.
			 */
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		{
			/*
			 * Try the conversion...
			 */
			PG_TRY();
			{
				if ((IFX_ATTRTYPE_P(state, attnum) == IFX_INT8)
					|| (IFX_ATTRTYPE_P(state, attnum) == IFX_SERIAL8)
					|| (IFX_ATTRTYPE_P(state, attnum) == IFX_BIGSERIAL)
					|| (IFX_ATTRTYPE_P(state, attnum) == IFX_INFX_INT8))
				{
					char *buf;
					regproc typinputfunc;

					buf = (char *) palloc0(IFX_INT8_CHAR_LEN + 1);

					/*
					 * Extract the value from the sqlvar tuple.
					 *
					 * We are forced to special case IFX_INFX_INT8 here, too,
					 * BIGINT and INT8 are incompatible.
					 */
					switch (IFX_ATTRTYPE_P(state, attnum))
					{
						case IFX_INT8:
						case IFX_SERIAL8:
						case IFX_BIGSERIAL:
							/* INT8 */
							buf = ifxGetInt8(&(state->stmt_info),
											 PG_MAPPED_IFX_ATTNUM(state, attnum), buf);
							break;
						case IFX_INFX_INT8:
							/* BIGINT */
							buf = ifxGetBigInt(&(state->stmt_info),
											   PG_MAPPED_IFX_ATTNUM(state, attnum), buf);
							break;
						default:
							pfree(buf);
							buf = NULL;
					}

					/*
					 * Check wether we have a valid buffer returned from the
					 * conversion routine. We might get a NULL pointer in case Informix
					 * failed to convert the INT8 into a proper int8 value. We might then
					 * get a NOT NULL indicator, but still stumple across the conversion error.
					 */
					if (state->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM(state, attnum)].indicator
						== INDICATOR_NOT_NULL)
					{

						/*
						 * Check for null pointer in buf. This is not expected
						 * and means an error occured.
						 */
						if (buf == NULL)
						{
							ifxRewindCallstack(&(state->stmt_info));
							elog(ERROR,
								 "could not convert informix int8 value");
						}

						/*
						 * Finally call the type input function and we're
						 * done.
						 */
						typinputfunc = getTypeInputFunction(state, PG_ATTRTYPE_P(state, attnum));
						result = OidFunctionCall2(typinputfunc,
												  CStringGetDatum(pstrdup(buf)),
												  ObjectIdGetDatum(InvalidOid));
					}
				}
				else
				{
					/*
					 * We have a compatible integer type here
					 * and a character target type. In this case
					 * we simply call the cast function of the designated
					 * target type and let it do the legwork...
					 */
					regproc typcastfunc;
					Oid     sourceOid;

					/*
					 * need the source type OID
					 *
					 * XXX: we might better do it earlier when
					 *      retrieving the IFX types ???
					 */
					if ((IFX_ATTRTYPE_P(state, attnum) == IFX_INTEGER)
						|| (IFX_ATTRTYPE_P(state, attnum) == IFX_SERIAL))
						sourceOid = INT4OID;
					else
						/* only INT2 left... */
						sourceOid = INT2OID;

					/*
					 * Execute the cast function and we're done...
					 */
					typcastfunc = getTypeCastFunction(state, sourceOid,
													  PG_ATTRTYPE_P(state, attnum));

					if (sourceOid == INT4OID)
						result = OidFunctionCall1(typcastfunc,
												  Int32GetDatum(ifxGetInt4(&(state->stmt_info),
																		   PG_MAPPED_IFX_ATTNUM(state, attnum))));
					else
						/* only INT2 left... */
						result = OidFunctionCall1(typcastfunc,
												  Int16GetDatum(ifxGetInt2(&(state->stmt_info),
																		   PG_MAPPED_IFX_ATTNUM(state, attnum))));
				}
			}
			PG_CATCH();
			{
				ifxRewindCallstack(&(state->stmt_info));
				PG_RE_THROW();
			}
			PG_END_TRY();

			break;
		}
		default:
		{
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return PointerGetDatum(NULL);
		}
	}

	return result;
}

#if PG_VERSION_NUM >= 90300

/*
 * Converts an interval column attribute into its string
 * representation. interval_to_cstring() throws an error in case no
 * valid format string for the given interval value can be generated.
 * This could happen for example when creating a FMT_IFX format
 * string with an interval which is outside the allowed range
 * Informix accepts.
 *
 * This function always returns the interval in ANSI format without
 * any fractions (YYYY-MM or DD HH24:MI:SS), depending on the format
 * mode.
 *
 * NOTE: The function possibly throws an ereport(ERROR, ...) inside,
 *       so the caller should be prepared to deal with conversion
 *       errors.
 */
static inline char *interval_to_cstring(IfxFdwExecutionState *state,
										TupleTableSlot       *slot,
										int                   attnum,
										IfxFormatMode         mode)
{
	char *format = NULL;
	Datum datval;
	IfxTemporalRange range;
	bool isnull = false;

	datval = slot_getattr(slot, attnum + 1, &isnull);

	/* NOTE: The minimal field identifier we support is TU_SECONDS! */
	range  = ifxGetTemporalQualifier(&(state->stmt_info),
									 IFX_ATTR_PARAM_ID(state, attnum));
	range.precision = IFX_TU_SECOND;
	format = ifxGetIntervalFormatString(range, mode);

	/* In case we cannot retrieve a valid format string, abort. */
	if (format == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("cannot get a format string for interval attribute %d",
							   attnum)));
	}

	datval = DirectFunctionCall2(interval_to_char,
								 datval,
								 PointerGetDatum(cstring_to_text(format)));
	return text_to_cstring(DatumGetTextP(datval));
}

/*
 * Store the given string value into the specified
 * SQLDA handle, depending on wich target type we have.
 */
void setIfxText(IfxFdwExecutionState *state,
				TupleTableSlot *slot,
				int attnum)
{
	/*
	 * Sanity check, SQLDA available ?
	 */
	Assert(state->stmt_info.sqlda != NULL);

	/*
	 * In case we have a null value, set the indicator value accordingly.
	 */
	IFX_SET_INDICATOR_P(state, attnum,
						((slot->tts_isnull[attnum]) ? INDICATOR_NULL : INDICATOR_NOT_NULL));

	switch(IFX_ATTRTYPE_P(state, attnum))
	{
		case IFX_TEXT:
		case IFX_BYTES:
			break;
		default:
			break;
	}
}

/*
 * setIfxFloat()
 *
 * Converts a float datum into a character string
 * suitable to be converted into a compatible
 * Informix datatype.
 *
 * All float data conversion to informix are required
 * to happen as a valid float value in its character representation,
 * ifxSetupDataBufferAligned() strictly employs CSTRINGTYPE for
 * this type conversion.
 */
void setIfxFloat(IfxFdwExecutionState *state,
				 TupleTableSlot       *slot,
				 int attnum)
{
	regproc  typout;
	char    *strval;
	IfxAttrDef ifxval;
	Datum    datum;
	bool     isnull = false;

	/* Sanity check, SQLDA available? */
	Assert(state->stmt_info.sqlda != NULL);

	strval = NULL;

	datum = slot_getattr(slot, attnum + 1, &isnull);

	/*
	 * Take care for NULL values.
	 */
	if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
		&& ! isnull
		&& IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{
		/*
		 * We must carefully check for any conversion errors to do
		 * the cleanup right away.
		 *
		 * We just call the type output function to get the
		 * character representation for the current source value.
		 */
		PG_TRY();
		{
			typout = getTypeOutputFunction(PG_ATTRTYPE_P(state, attnum));
			strval = DatumGetCString(OidFunctionCall1(typout,
													  datum));

			/*
			 * If the typ output function succeed, we have a
			 * valid buffer, pass it down to try to store them
			 * as a FLOAT value.
			 */
			if (strval == NULL)
			{
				ifxRewindCallstack(&(state->stmt_info));
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("informix_fdw: unexpected NULL value for attribute \"%d\" in float string representation",
								attnum)));
			}

		}
		PG_CATCH();
		{
			ifxRewindCallstack(&(state->stmt_info));
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	/*
	 * Assign the value to the Informix SQLDA structure.
	 * ifxSetFloat() takes enough care for NULL values, but be sure
	 * to trap any conversion errors.
	 */
	ifxSetFloat(&(state->stmt_info),
				IFX_ATTR_PARAM_ID(state, attnum),
				strval);

	/* Check... */

	ifxval = state->stmt_info.ifxAttrDefs[IFX_ATTR_PARAM_ID(state, attnum)];

	if ( (ifxval.indicator == INDICATOR_NOT_VALID)
		 && (ifxval.converrcode != 0) )
	{
		switch (ifxval.converrcode)
		{
			case IFX_CONVERSION_OVERFLOW:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH),
						 errmsg("informix_fdw: overflow during float datatype conversion (attnum \"%d\")",
								attnum)));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("informix_fdw: conversion error \"%d\"(attnum \"%d\")",
								ifxval.converrcode, attnum)));
				break;
		}
	}

	return;
}

/*
 * setIfxDecimal()
 *
 * Converts a decimal value from PostgreSQL into
 * an Informix value and stores it into the specified attribute.
 *
 * Given a PostgreSQL numeric value, we don't check
 * the scale and precision of the target type wether it does fit
 * the given value. We rely on the Informix routines to return
 * an appropiate error in this case.
 */
void setIfxDecimal(IfxFdwExecutionState *state,
				   TupleTableSlot       *slot,
				   int attnum)
{
	regproc  typout;
	char    *strval;
	Datum    datum;
	bool     isnull;

	/* Sanity check, SQLDA available? */
	Assert(state->stmt_info.sqlda != NULL);
	strval = NULL;

	datum = slot_getattr(slot, attnum + 1, &isnull);

	/*
	 * Take care of NULL datums.
	 */
	if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
		&& ! isnull
		&& IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{

		/*
		 * Take care of a possible failure during conversion.
		 */
		PG_TRY();
		{
			/*
			 * We need to take care wether we deal with a CASHOID datum
			 * or any other NUMERICOID datum. If the former occurs, we
			 * first cast to a decimal value, since Informix can't deal
			 * with a locale aware representation of a MONEY value (that's why
			 * we can't use the type output function for a CASHOID datum
			 * directly).
			 *
			 * In any case we expect the source datum to be converted
			 * into a character string. Call the datum output function to
			 * get it's text representation.
			 */
			if (PG_ATTRTYPE_P(state, attnum) == CASHOID)
			{
				regproc castfunc = getTypeCastFunction(state, CASHOID, NUMERICOID);
				Datum   castval  = OidFunctionCall1(castfunc, datum);

				typout = getTypeOutputFunction(NUMERICOID);

				strval = DatumGetCString(OidFunctionCall1(typout,
														  castval));
			}
			else
			{
				typout = getTypeOutputFunction(PG_ATTRTYPE_P(state, attnum));

				strval = DatumGetCString(OidFunctionCall1(typout,
														  datum));
			}
		}
		PG_CATCH();
		{
			ifxRewindCallstack(&(state->stmt_info));
			PG_RE_THROW();
		}
		PG_END_TRY();


		/*
		 * strval must be a valid character string at this point.
		 */
		if (strval == NULL)
		{
			ifxRewindCallstack(&(state->stmt_info));
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("informix_fdw: unexpected NULL value for attribute \"%d\" in numeric string representation",
							attnum)));
		}
	}

	/*
	 * Assign the value into the Informix SQLDA structure. ifxSetDecimal()
	 * takes enough care for NULL values, so no additional checks required.
	 */
	switch(IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{
		case IFX_MONEY:
		case IFX_DECIMAL:
		{
			ifxSetDecimal(&(state->stmt_info),
						  IFX_ATTR_PARAM_ID(state, attnum),
						  strval);
			break;
		}
		default:
		{
			/* unsupported conversion */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("informix_fdw: unsupported conversion from type OID \"%u\" to informix type \"%d\"",
							PG_ATTRTYPE_P(state, attnum),
							IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)))));
		}
	}
}

/*
 * Transforms an PostgreSQL interval type
 * into an compatible Informix Interval value.
 *
 * Internally, setIfxInterval() tries to convert the
 * given PostgreSQL attribute value into a valid formatted
 * character string representing an interval suitable to be
 * passed to Informix. E.g:
 *
 * YEAR TO MONTH: YYYY-MM
 * DAY TO FRACTION: DD HH24:MI:SS
 *
 * Currently, setIfxInterval doesn't convert fractions from a given
 * interval value and ignores them.
 */
void setIfxInterval(IfxFdwExecutionState *state,
					TupleTableSlot       *slot,
					int                   attnum)
{
	/* Sanity check, SQLDA available? */
	Assert(state->stmt_info.sqlda != NULL);

	switch(IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{
		case IFX_INTERVAL:
		{
			/*
			 * Interval must be in a string format suitable for
			 * Informix.
			 */
			char *strval = NULL;
			IfxTemporalRange range;
			Datum datum;
			bool isnull = false;

			datum = slot_getattr(slot, attnum + 1, &isnull);

			/*
			 * If a datum is NULL, there's no reason to try to convert
			 * it into a character string. Mark it accordingly and we're done.
			 */
			if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
				&& ! isnull
				&& IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
			{
				/*
				 * Get the value and convert it into a character representation
				 * suitable to be passed down to Informix.
				 */
				PG_TRY();
				{
					switch(PG_ATTRTYPE_P(state, attnum))
					{
						case INTERVALOID:
						{
							strval = interval_to_cstring(state, slot, attnum, FMT_PG);
							break;
						}
						case VARCHAROID:
						case TEXTOID:
						{
							/*
							 * If the source value is a text type we expect it to be
							 * in a format already suitable to be converted by our
							 * Informix API to an Informix interval type. Be prepared
							 * to deal with conversion errors, though...
							 */
							strval = text_to_cstring(DatumGetTextP(datum));

							break;
						}
						default:
						{
							/* unsupported conversion */
							ereport(ERROR,
									(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
									 errmsg("informix_fdw unsupported type OID \"%u\" for conversion",
											PG_ATTRTYPE_P(state, attnum))));
						}
					}
				}
				PG_CATCH();
				{
					ifxRewindCallstack(&(state->stmt_info));
					PG_RE_THROW();
				}
				PG_END_TRY();
			}

			elog(DEBUG4, "informix_fdw: attnum %d, converted interval \"%s\"",
				 attnum, ((strval != NULL) ? strval : "NULL"));

			/*
			 * Copy the value into the Informix SQLDA structure. If successful, we're done.
			 */
			range = ifxGetTemporalQualifier(&(state->stmt_info),
											IFX_ATTR_PARAM_ID(state, attnum));
			range.precision = IFX_TU_SECOND;
			ifxSetIntervalFromString(&(state->stmt_info),
									 IFX_ATTR_PARAM_ID(state, attnum),
									 ifxGetIntervalFormatString(range, FMT_IFX),
									 strval);

			break;
		}
		default:
			break;
	}
}

/*
 * Assigns the specified Datum as an Informix DATE value
 * to the current SQLDA structure provided by the given
 * execution state handle.
 *
 * Conversion is supported from a PostgreSQL DATE source type
 * to an Informix DATE type only.
 */
void setIfxDate(IfxFdwExecutionState *state,
				TupleTableSlot       *slot,
				int                   attnum)
{
	char  *strval = NULL;
	Datum  datval = PointerGetDatum(NULL);
	Datum  datum;
	bool   isnull = false;

	/* Sanity check, SQLDA available? */
	Assert(state->stmt_info.sqlda != NULL);
	Assert(IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)) == IFX_DATE);

	/*
	 * Only IFX_DATE is supported here as a target type.
	 */
	if (IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)) == IFX_DATE)
	{
		/*
		 * Target type must be an Informix DATE type. However, we convert
		 * the PostgreSQL datum into it's ANSI string representation first, which
		 * makes it easier to pass them over to Informix. A date is always formatted
		 * as yyyy-mm-dd, ifxSetDateFromString() will transfer it back into ESQL/C binary
		 * representation.
		 */

		datum = slot_getattr(slot, attnum + 1, &isnull);

		/*
		 * Don't try to convert a NULL datum.
		 */
		if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
			&& ! isnull
			&& IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
		{
			PG_TRY();
			{
				text *format;
				Datum castval;
				regproc castfunc;

				/*
				 * Cast the DATE datum to a timestamp first and convert it
				 * into a ISO8601 compatible string format. Inefficient, but we do
				 * it this way to be independent from any locale and give a
				 * static input format to our Informix conversion routines.
				 */
				castfunc = getTypeCastFunction(state, DATEOID, TIMESTAMPOID);
				castval  = OidFunctionCall1(castfunc, datum);

				format = cstring_to_text(
					ifxGetIntervalFormatString(
						ifxGetTemporalQualifier(&(state->stmt_info),
												IFX_ATTR_PARAM_ID(state, attnum)),
						FMT_PG));
				datval = DirectFunctionCall2(timestamp_to_char,
											 castval,
											 PointerGetDatum(format));
				strval = text_to_cstring(DatumGetTextP(datval));

				/*
				 * Sanity check, C string has maximum allowed buffer length?
				 */
				Assert((strval != NULL)
					   && (strlen(strval) <= IFX_DATETIME_BUFFER_LEN));

				pfree(format);
			}
			PG_CATCH();
			{
				ifxRewindCallstack(&state->stmt_info);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}
	}
	else
	{
		/* unknown target type */
		elog(ERROR, "informix_fdw could not convert local type %u to target type %d",
			 PG_ATTRTYPE_P(state, attnum),
			 IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)));
	}


	elog(DEBUG4, "informix_fdw: attnum %d, converted temporal value \"%s\"",
		 attnum, ((strval != NULL) ? strval : "NULL"));

	/*
	 * Put the converted DATE string into SQLDA
	 *
	 * NOTE: We already should have set the proper indicator for
	 *       potential NULL values above...
	 */
	ifxSetDateFromString(&state->stmt_info,
						 IFX_ATTR_PARAM_ID(state, attnum),
						 strval);

	/* cleanup */
	if (strval != NULL)
		pfree(strval);
}

/*
 * Encapsulates assignment from a PostgreSQL
 * timestamp or date value into an informix DATETIME
 * or DATE value.
 */
void setIfxDateTimestamp(IfxFdwExecutionState *state,
						 TupleTableSlot       *slot,
						 int                   attnum)
{
	/*
	 * Sanity check, SQLDA available ?
	 */
	Assert(state->stmt_info.sqlda != NULL);

	if (IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)) == IFX_DTIME)
	{
		Datum   datval;
		Datum   datum;
		bool    isnull = false;
		char   *strval = NULL;

		/*
		 * Target type must be an Informix timestamp or date value.
		 *
		 * NOTE: Informix DATE and DATETIME value doesn't understand time zones, nor
		 *       do they have the same precision. Because of this we always
		 *       convert a TIMESTAMP or DATE value into its ANSI string format
		 *       (that is yyyy-mm-dd hh24:mi:ss) first.
		 */

		datum = slot_getattr(slot, attnum + 1, &isnull);

		/*
		 * Don't try to convert a NULL timestamp value into a character string,
		 * but step into the conversion routine to set all required
		 * SQLDA info accordingly.
		 */
		if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
		    && ! isnull
		    && IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
		{
			text *format;

			PG_TRY();
			{
				/*
				 * Make a string from the given DATE or TIMESTAMP(TZ)
				 * datum in ANSI format.
				 */
				switch(PG_ATTRTYPE_P(state, attnum))
				{
					case TIMESTAMPTZOID:
					{
						format = cstring_to_text(ifxGetIntervalFormatString(
													 ifxGetTemporalQualifier(&(state->stmt_info),
																			 IFX_ATTR_PARAM_ID(state, attnum)),
													 FMT_PG));
						datval = DirectFunctionCall2(timestamptz_to_char,
													 datum,
													 PointerGetDatum(format));
						break;
					}
					case DATEOID:
					{
						/*
						 * Cast any DATEOID datum to timestamp first, as we'd like to call
						 * to_char directly which is only present for timestamp datums.
						 *
						 * This is what effectively also happens if you call to_char() on the SQL
						 * level, too.
						 */
						Datum castval;
						regproc castfunc;

						castfunc = getTypeCastFunction(state, DATEOID, TIMESTAMPOID);
						castval  = OidFunctionCall1(castfunc, datum);

						format = cstring_to_text(ifxGetIntervalFormatString(
													 ifxGetTemporalQualifier(&(state->stmt_info),
																			 IFX_ATTR_PARAM_ID(state, attnum)),
													 FMT_PG));

						datval = DirectFunctionCall2(timestamp_to_char,
													 castval,
													 PointerGetDatum(format));

						break;

					}
					case TIMESTAMPOID:
					{
						format = cstring_to_text(ifxGetIntervalFormatString(
													 ifxGetTemporalQualifier(&(state->stmt_info),
																			 IFX_ATTR_PARAM_ID(state, attnum)),
													 FMT_PG));
						datval = DirectFunctionCall2(timestamp_to_char,
													 datum,
													 PointerGetDatum(format));
						break;
					}
					case TIMEOID:
					{
						regproc castproc;
						Datum   castval;

						/*
						 * Cast the time value into an interval type,
						 * which can then be converted into a valid text
						 * string, suitable to be passed down to Informix.
						 */
						castproc = getTypeCastFunction(state, TIMEOID, INTERVALOID);
						castval  = OidFunctionCall1(castproc, datum);

						/*
						 * Now try to convert the value
						 */
						format = cstring_to_text("hh24:mi:ss");
						datval = DirectFunctionCall2(interval_to_char,
													 castval,
													 PointerGetDatum(format));
						break;
					}
					default:
						/* we shouldn't reach this, so error out hard */
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("informix_fdw unsupported type OID \"%u\" for conversion",
										PG_ATTRTYPE_P(state, attnum))));
						break;
				}

				strval = text_to_cstring(DatumGetTextP(datval));
			}
			PG_CATCH();
			{
				ifxRewindCallstack(&state->stmt_info);
				PG_RE_THROW();
			}
			PG_END_TRY();

			/*
			 * Sanity check, C string valid?
			 */
			Assert(strval != NULL);
			Assert(strlen(strval) <= IFX_DATETIME_BUFFER_LEN);

			/* cleanup */
			pfree(format);
		}

		elog(DEBUG4, "informix_fdw: attnum %d, converted temporal value \"%s\"",
			 attnum, ((strval != NULL) ? strval : "NULL"));

		if (PG_ATTRTYPE_P(state, attnum) == TIMEOID)
		{
			ifxSetTimeFromString(&state->stmt_info,
								 IFX_ATTR_PARAM_ID(state, attnum),
								 strval);
		}
		else
		{
			ifxSetTimestampFromString(&state->stmt_info,
									  IFX_ATTR_PARAM_ID(state, attnum),
									  strval);
		}

		/* ...and we're done */
		if (strval != NULL)
			pfree(strval);

	}
	else
	{
		/* unknown target type */
		elog(ERROR, "informix_fdw could not convert local type %u to target type %d",
			 PG_ATTRTYPE_P(state, attnum),
			 IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)));
	}
}

/*
 * Encapsulate assignment from character types to
 * informix values. This functions expects the
 * value as a null terminated C string, suitable
 * to be converted into the appropiate Informix
 * target type.
 *
 * We also accept a NULL pointer in val, since we need
 * to set all necessary sqlvar members according to
 * reflect a correct NULL value.
 */
void setIfxCharString(IfxFdwExecutionState *state,
					  int                   attnum,
					  char                 *val,
					  int                   len)
{
	/*
	 * Sanity check, SQLDA available ?
	 */
	Assert(state->stmt_info.sqlda != NULL);

	switch(IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{
		case IFX_BYTES:
		case IFX_TEXT:
		{
			ifxSetSimpleLO(&state->stmt_info,
						   IFX_ATTR_PARAM_ID(state, attnum),
						   val, len);
			break;
		}
		case IFX_LVARCHAR:
		case IFX_CHARACTER:
		case IFX_VCHAR:
		case IFX_NCHAR:
		case IFX_NVCHAR:
		{
			/* Handle string types */
			ifxSetText(&state->stmt_info,
					   IFX_ATTR_PARAM_ID(state, attnum),
					   val);
			break;
		}
		default:
			/* unknown target type */
			elog(ERROR, "informix_fdw could not convert local type %u to target type %d",
				 PG_ATTRTYPE_P(state, attnum), IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)));
	}
}

/*
 * Encapsulates assignment from integer values to Informix
 * bind parameters.
 */
void setIfxInteger(IfxFdwExecutionState *state,
				   TupleTableSlot       *slot,
				   int                   attnum)
{
	Datum datum;
	bool isnull = false;

	datum = slot_getattr(slot, attnum + 1, &isnull);

	/*
	 * Sanity check, SQLDA available ?
	 */
	Assert(state->stmt_info.sqlda != NULL);

	switch(IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{
		case IFX_SMALLINT:
		{
			short val;

			val = DatumGetInt16(datum);

			/*
			 * Copy the value into the SQLDA structure.
			 */
			ifxSetInt2(&state->stmt_info,
					   IFX_ATTR_PARAM_ID(state, attnum),
					   val);

			break;
		}
		case IFX_INTEGER:
			/* fall through to IFX_SERIAL */
		case IFX_SERIAL:
		{
			int val;

			/*
			 * Get the integer value.
			 */
			val = DatumGetInt32(datum);

			/*
			 * Copy the value into the SQLDA structure.
			 */
			ifxSetInteger(&state->stmt_info,
						  IFX_ATTR_PARAM_ID(state, attnum),
						  val);
			break;
		}
		case IFX_INT8:
			/*
			 * Fall through
			 *
			 * Differences are handled below in a common
			 * code block.
			 *
			 * Specifically, the various *SERIAL* and INT* datatypes
			 * have different binary representation, so that we convert them first
			 * to their string representation. Since the target type is
			 * handled by different API calls in ESQL/C, we employ specific
			 * conversion routines below.
			 */
		case IFX_INFX_INT8:
		case IFX_SERIAL8:
		case IFX_BIGSERIAL:
		{
			char *strval;
			regproc typout;

			/*
			 * Convert the int8 value to its character representation.
			 */
			typout = getTypeOutputFunction(PG_ATTRTYPE_P(state, attnum));
			strval = DatumGetCString(OidFunctionCall1(typout,
													  DatumGetInt64(datum)));
			if ((IFX_ATTRTYPE_P(state, attnum) == IFX_INT8)
				|| (IFX_ATTRTYPE_P(state, attnum) == IFX_SERIAL8))
			{
				/* INT8 (Informix ifx_int8_t) */
				ifxSetInt8(&state->stmt_info,
						   IFX_ATTR_PARAM_ID(state, attnum),
						   strval);
			}
			else
			{
				/* BIGINT, BIGSERIAL and IFX_INFX_INT8 */
				ifxSetBigint(&state->stmt_info,
							 IFX_ATTR_PARAM_ID(state, attnum),
							 strval);
			}

			/*
			 * Check wether conversion was successful
			 */
			break;
		}
		default:
			elog(ERROR, "informix_fdw could not convert local type %u to target type %d",
				 PG_ATTRTYPE_P(state, attnum), IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)));
			break;
	}
}

#endif

static regproc getTypeOutputFunction(Oid inputOid)
{
	regproc result;
	HeapTuple type_tuple;

	/*
	 * Which output function to lookup?
	 */
	type_tuple = SearchSysCache1(TYPEOID, inputOid);

	if (!HeapTupleIsValid(type_tuple))
	{
		/*
		 * Oops, this is not expected...
		 */
		elog(ERROR,
			 "cache lookup failed for output function for type %u",
			 inputOid);
	}

	result = ((Form_pg_type) GETSTRUCT(type_tuple))->typoutput;
	ReleaseSysCache(type_tuple);
	return result;
}


/*
 * Returns the type input function for the
 * specified type OID. Throws an error in case
 * no valid input function could be found.
 */
static regproc getTypeInputFunction(IfxFdwExecutionState *state,
								   Oid inputOid)
{
	regproc result;
	HeapTuple type_tuple;

	/*
	 * Get the type input function.
	 */
	type_tuple = SearchSysCache1(TYPEOID,
								 inputOid);
	if (!HeapTupleIsValid(type_tuple))
	{
		/*
		 * Oops, this is not expected...
		 *
		 * Don't throw an ERROR here immediately, but inform the caller
		 * that something went wrong. We need to give the caller time
		 * to cleanup itself...
		 */
		ifxRewindCallstack(&(state->stmt_info));
		elog(ERROR,
			 "cache lookup failed for input function for type %u",
			 inputOid);
	}

	result = ((Form_pg_type) GETSTRUCT(type_tuple))->typinput;
	ReleaseSysCache(type_tuple);

	return result;
}

/*
 * Returns the type case function for the specified
 * source and target OIDs. Throws an error in case
 * no cast function could be found.
 */
static regproc getTypeCastFunction(IfxFdwExecutionState *state,
								   Oid sourceOid, Oid targetOid)
{
	regproc result;
	HeapTuple cast_tuple;

	cast_tuple = SearchSysCache2(CASTSOURCETARGET,
								 sourceOid,
								 targetOid);

	if (!HeapTupleIsValid(cast_tuple))
	{
		ifxRewindCallstack(&(state->stmt_info));
		elog(ERROR,
			 "cache lookup failed for cast from %u to %u",
			 sourceOid, targetOid);
	}

	result = ((Form_pg_cast) GETSTRUCT(cast_tuple))->castfunc;
	ReleaseSysCache(cast_tuple);

	return result;
}


/*
 * convertIfxBoolean
 *
 * Converts the specified informix attribute
 * into a PostgreSQL boolean datum. If the target type
 * is a boolean, the function tries to convert the value
 * directly, otherwise the value is casted to the
 * requested target type, if possible.
 *
 * Supported target types are
 *
 * TEXTOID
 * VARCHAROID
 * CHAROID
 * BPCHAROID
 * BOOLOID
 *
 */
Datum convertIfxBoolean(IfxFdwExecutionState *state, int attnum)
{
	Datum result;
	Oid sourceOid;
	Oid targetOid;
	char val;

	/*
	 * Init variables...
	 */
	result = PointerGetDatum(NULL);
	val = ifxGetBool(&(state->stmt_info), attnum);
	sourceOid = InvalidOid;
	targetOid = InvalidOid;

	/*
	 * If the target type is not supposed to be compatible,
	 * reject any conversion attempts.
	 */
	switch(PG_ATTRTYPE_P(state, attnum))
	{
		case BOOLOID:
		case CHAROID:
		{
			result = BoolGetDatum(val);
			break;
		}
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		{
			regproc typecastfunc;

			sourceOid = BOOLOID;
			targetOid = PG_ATTRTYPE_P(state, attnum);

			typecastfunc = getTypeCastFunction(state, sourceOid, targetOid);

			/*
			 * Execute the cast function.
			 */
			result = OidFunctionCall1(typecastfunc,
									  CharGetDatum(val));
			break;
		}
		default:
			/* not supported */
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			break;
	}

	return result;
}

/*
 * convertIfxSimpleLO
 *
 * Converts a simple large object into a corresponding
 * PostgreSQL datum. Currently supported are the following
 * conversions:
 *
 *  INFORMIX | POSTGRESQL
 * -----------------------
 *  TEXT     | TEXT
 *  TEXT     | VARCHAR
 *  TEXT     | BPCHAR
 *  TEXT     | BYTEA
 */
Datum convertIfxSimpleLO(IfxFdwExecutionState *state, int attnum)
{
	Datum  result;
	char  *val;
	Oid    inputOid;
	regproc typeinputfunc;
	long    buf_size;

	result = PointerGetDatum(NULL);

	/*
	 * Target type OID supported?
	 */
	switch (PG_ATTRTYPE_P(state, attnum))
	{
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		case BYTEAOID:
			inputOid = PG_ATTRTYPE_P(state, attnum);
			break;
		default:
		{
			/* oops, unsupported datum conversion */
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return result;
		}
	}

	/*
	 * ifxGetTextFromLocator returns a pointer into
	 * the locator structure, just reuse it during the
	 * FETCH but don't try to deallocate it. This is done
	 * later by the ESQL/C API after the FETCH finishes...
	 */
	val = ifxGetTextFromLocator(&(state->stmt_info),
								PG_MAPPED_IFX_ATTNUM(state, attnum),
								&buf_size);

	/*
	 * Check indicator value. In case we got NULL,
	 * nothing more to do.
	 */
	if (IFX_ATTR_ISNULL_P(state, attnum)
		|| (! IFX_ATTR_IS_VALID_P(state, attnum)))
		return result;

	elog(DEBUG3, "blob size fetched: %ld", buf_size);

	/*
	 * It looks like Informix returns a locator with an
	 * empty string always as a NULL pointer to its data buffer...
	 *
	 * Check wether the buffer size is zero. We formerly checked
	 * wether the datum is valid above, so we can proceed safely without
	 * any checks. In this case we can be sure that the datum must
	 * be an empty string (but only if val is set to NULL!).
	 */
	if ((buf_size <= 0)
		&& (val == NULL))
	{
		val = "\0";
	}

	PG_TRY();
	{
		/*
		 * If the target type is a varlena, go on. Take care for
		 * typemods however...
		 */
		typeinputfunc = getTypeInputFunction(state, inputOid);

		switch (inputOid)
		{
			case TEXTOID:
			case VARCHAROID:
			case BPCHAROID:
			{
				/*
				 * Take care for typmods...
				 */
				if (PG_ATTRTYPEMOD_P(state, attnum) != -1)
				{
					result = OidFunctionCall3(typeinputfunc,
											  CStringGetDatum(val),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(PG_ATTRTYPEMOD_P(state, attnum)));
				}
				else
				{
					result = OidFunctionCall2(typeinputfunc,
											  CStringGetDatum(val),
											  ObjectIdGetDatum(InvalidOid));
				}
				break;
			}
			case BYTEAOID:
			{
				bytea *binary_data;
				int    len;

				/*
				 * Allocate a bytea datum. Don't use strlen() for
				 * val, in case the source column is of type BYTE. Instead,
				 * rely on the loc_buffer size, Informix has returned to us.
				 */
				len = buf_size;
				binary_data = (bytea *) palloc0(VARHDRSZ + len);

				SET_VARSIZE(binary_data, len + VARHDRSZ);
				memcpy(VARDATA(binary_data), val, len);
				IFX_SETVAL_P(state, attnum, PointerGetDatum(binary_data));
				result = IFX_GETVAL_P(state, attnum);
			}

			break;
		}

	}
	PG_CATCH();
	{
		ifxRewindCallstack(&(state->stmt_info));
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/*
 * convertIfxCharacterString
 *
 * Converts a given character string formerly retrieved
 * from Informix into the given PostgreSQL destination type.
 *
 * Supported informix character types are:
 *
 * CHAR
 * VARCHAR
 * LVARCHAR
 * NVARCHAR
 *
 * The caller must have prepared the column definitions
 * before.
 *
 * Handled target types are
 *
 * BPCHAROID
 * VARCHAROID
 * TEXTOID
 *
 * The converted value is assigned to the execution state
 * context. Additionally, the converted value is returned to
 * caller directly. In case an error occured, a NULL datum
 * is returned.
 */

Datum convertIfxCharacterString(IfxFdwExecutionState *state, int attnum)
{
	Datum      result;
	PgAttrDef  pg_def;
	char      *val;

	/*
	 * Initialize stuff...
	 */
	pg_def = state->pgAttrDefs[attnum];

	/*
	 * Sanity check, fail in case we are called
	 * on incompatible data type mapping.
	 */
	switch (pg_def.atttypid)
	{
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case BYTEAOID:
			break;
		default:
		{
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return PointerGetDatum(NULL);
		}
	}

	/*
	 * Retrieve the character string from the informix result
	 * set. Caller must have checked for INDICATOR_NULL before...
	 */
	val = ifxGetText(&(state->stmt_info),
					 PG_MAPPED_IFX_ATTNUM(state, attnum));

	/*
	 * Check the state of the value. In case of a NULL value,
	 * nothing more to do.
	 */
	if (IFX_ATTR_ISNULL_P(state, attnum))
		return PointerGetDatum(NULL);

	/*
	 * If the target type is compatible with the source type,
	 * convert it directly. We do this with TEXT and VARCHAR only,
	 * since for the other types it might be necessary to apply typmods...
	 */
	if ((pg_def.atttypid == TEXTOID)
		|| (pg_def.atttypid == BYTEAOID)
		|| ((pg_def.atttypid == VARCHAROID) && (pg_def.atttypmod == -1)))
	{
		if (PG_ATTRTYPE_P(state, attnum) == TEXTOID)
		{
			text *text_val;

			/*
			 * Otherwise convert the character string into a text
			 * value. We must be cautious here, since the column length
			 * stored in the column definition struct only reports the *overall*
			 * length of the data buffer, *not* the value length itself.
			 *
			 */

			/*
			 * XXX: What about encoding conversion??
			 */

			text_val = cstring_to_text_with_len(val, strlen(val));
			IFX_SETVAL_P(state, attnum, PointerGetDatum(text_val));
			result = IFX_GETVAL_P(state, attnum);
		}
		else
		{
			/* binary BYTEA value */
			bytea *binary_data;
			int    len;

			/*
			 * Allocate a bytea datum. We can use strlen here, because
			 * we know that our source value must be a valid
			 * character string.
			 */
			len = strlen(val);
			binary_data = (bytea *) palloc0(VARHDRSZ + len);

			SET_VARSIZE(binary_data, len + VARHDRSZ);
			memcpy(VARDATA(binary_data), val, len);
			IFX_SETVAL_P(state, attnum, PointerGetDatum(binary_data));
			result = IFX_GETVAL_P(state, attnum);
		}
	}
	else
	{
		regproc typeinputfunc;
		HeapTuple  conv_tuple;

		conv_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_def.atttypid));

		if (!HeapTupleIsValid(conv_tuple))
		{
			/*
			 * Oops, this is not expected...
			 *
			 * Don't throw an ERROR here immediately, but inform the caller
			 * that something went wrong. We need to give the caller time
			 * to cleanup itself...
			 */
			IFX_ATTR_SETNOTVALID_P(state, attnum);
			return PointerGetDatum(NULL);
		}

		/*
		 * Catch any errors from the following function calls, or
		 * we likely leak memory allocated by the ESQL/C API...
		 */
		PG_TRY();
		{
			/*
			 * Try the conversion...
			 */
			typeinputfunc = ((Form_pg_type) GETSTRUCT(conv_tuple))->typinput;

			result = OidFunctionCall3(typeinputfunc,
									  CStringGetDatum(val),
									  ObjectIdGetDatum(InvalidOid),
									  Int32GetDatum(pg_def.atttypmod));
		}
		PG_CATCH();
		{
			ifxRewindCallstack(&(state->stmt_info));
			PG_RE_THROW();
		}
		PG_END_TRY();

		ReleaseSysCache(conv_tuple);
	}

	return result;
}

/*
 * Returns a character string with the operator
 * identifier used for Informix.
 *
 * If the specified operator is of type IFX_OPR_NOT_SUPPORTED
 * getIfxOperatorIdent will return a NULL string.
 *
 * NOTE: If you are going to update mapPushdownOperator()
 *       don't forget to keep getIfxOperatorIdent in sync!
 */
static char *getIfxOperatorIdent(IfxPushdownOprInfo *pushdownInfo)
{
	char *result = NULL;

	switch(pushdownInfo->type)
	{
		case IFX_OPR_NOT_SUPPORTED:
			result = NULL;
			break;
		case IFX_OPR_GE:
			result = ">=";
			break;
		case IFX_OPR_LE:
			result = "<=";
			break;
		case IFX_OPR_LT:
			result = "<";
			break;
		case IFX_OPR_GT:
			result = ">";
			break;
		case IFX_OPR_EQUAL:
			result = "=";
			break;
		case IFX_OPR_NEQUAL:
			result = "<>";
			break;
		case IFX_OPR_LIKE:
			result = "LIKE";
			break;
		default:
			/* should not happen */
			elog(ERROR, "could not deparse operator type %d",
				 pushdownInfo->type);
			break;
	}

	return result;

}

IfxOprType mapPushdownOperator(Oid oprid, IfxPushdownOprInfo *pushdownInfo)
{
	char *oprname;
	HeapTuple oprtuple;
	Form_pg_operator oprForm;

	Assert(oprid != InvalidOid);
	Assert(pushdownInfo != NULL);

	oprtuple = SearchSysCache1(OPEROID, oprid);

	if (!HeapTupleIsValid(oprtuple))
		elog(ERROR, "cache lookup failed for operator %u", oprid);

	oprForm  = (Form_pg_operator)GETSTRUCT(oprtuple);
	oprname  = pstrdup(NameStr(oprForm->oprname));

	ReleaseSysCache(oprtuple);

	/*
	 * Currently we support Postgresql internal
	 * operators only. Ignore all operators living
	 * in other schemas than pg_catalog.
	 *
	 * We might relax this some time, since we push
	 * down the operator names based on string
	 * comparisons.
	 */
	if (oprForm->oprnamespace != PG_CATALOG_NAMESPACE)
		return IFX_OPR_NOT_SUPPORTED;

	if (strcmp(oprname, ">=") == 0)
	{
		pushdownInfo->type = IFX_OPR_GE;
		return IFX_OPR_GE;
	}
	else if (strcmp(oprname, "<=") == 0)
	{
		pushdownInfo->type = IFX_OPR_LE;
		return IFX_OPR_LE;
	}
	else if (strcmp(oprname, "<") == 0)
	{
		pushdownInfo->type = IFX_OPR_LT;
		return IFX_OPR_LT;
	}
	else if (strcmp(oprname, ">") == 0)
	{
		pushdownInfo->type = IFX_OPR_GT;
		return IFX_OPR_GT;
	}
	else if (strcmp(oprname, "=") == 0)
	{
		pushdownInfo->type = IFX_OPR_EQUAL;
		return IFX_OPR_EQUAL;
	}
	else if (strcmp(oprname, "<>") == 0)
	{
		pushdownInfo->type = IFX_OPR_NEQUAL;
		return IFX_OPR_NEQUAL;
	}
	else if (strcmp(oprname, "~~") == 0)
	{
		pushdownInfo->type = IFX_OPR_LIKE;
		return IFX_OPR_LIKE;
	}
	else
	{
		pushdownInfo->type = IFX_OPR_NOT_SUPPORTED;
		return IFX_OPR_NOT_SUPPORTED;
	}

	/* currently never reached */
	return IFX_OPR_UNKNOWN;
}

/*
 * Create a list with a RTE for the given foreign table.
 */
static List *make_deparse_context(Oid foreignRelid)
{
	/*
	 * Get the relation name for the given foreign table.
	 *
	 * XXX: This currently works only, because we rely on the fact that
	 *      a foreign table is equally named like on the foreign server.
	 *      Bad style (tm), but i'm not sure how to make this transparently
	 *      without using additional FDW options.
	 */;
	return deparse_context_for(get_rel_name(foreignRelid), foreignRelid);
}

#define IFX_MARK_PREDICATE_NOT_SUPPORTED(a) \
	(a)->type = IFX_OPR_NOT_SUPPORTED;

#define IFX_MARK_PREDICATE_ELEM(a, b) \
	(b)->predicates = lappend((b)->predicates, (a)); \
	(b)->count++;

#define IFX_MARK_PREDICATE_BOOL(a, b) \
	(b)->predicates = lappend((b)->predicates, (a)); \
	(b)->count++;

static inline bool isCompatibleForPushdown(Oid typeOid)
{
	switch(typeOid)
	{
		case INTERVALOID:
		case TIMESTAMPOID:
		case TIMEOID:
		case TIMESTAMPTZOID:
		case TIMETZOID:
		case DATEOID:
			return false;
	}

	/* all other values are safe */
	return true;
}

static void ifxMakeCookedOpExpr(IfxPushdownOprInfo *info,
								Node               *old,
								Node               *append)
{
	OpExpr *oldop = (OpExpr *)old;
	OpExpr *newop;

	/*
	 * NOTE: We make a copy of this node to make it
	 *       safe to fiddle with it later.
	 */
	newop = makeNode(OpExpr);

	newop->opno         = oldop->opno;
	newop->opfuncid     = oldop->opfuncid;
	newop->opresulttype = oldop->opresulttype;
	newop->opretset     = oldop->opretset;
	newop->opcollid     = oldop->opcollid;
	newop->inputcollid  = oldop->inputcollid;
	newop->location     = oldop->location;

	/*
	 * If we already have analyzed some of the operands
	 * copy them over.
	 */
	if (info->arg_idx < info->num_args)
	{
		int i = 0;
		for (i = 0; i < info->arg_idx; i++)
		{
			newop->args = lappend(newop->args,
								  copyObject(list_nth(oldop->args, i)));
		}
	}

	/*
	 * Append the current operand.
	 */
	newop->args = lappend(newop->args, append);
	info->arg_idx++;

	/*
	 * Mark this new operator as finished or currently in progress
	 * according if there are any remaining operands.
	 */
	info->deparsetype
		= (info->num_args > info->arg_idx) ? IFX_MAKE_COOKED_EXPR : IFX_COOKED_EXPR;
	info->expr          = (Expr *)newop;
}

static void ifxMakeCookedExpr(IfxPushdownOprInfo *info,
							  Node               *old,
							  Node               *append)
{
	/*
	 * Dispatch into the appropiate cooking recipe...
	 */
	switch(old->type)
	{
		case T_OpExpr:
			ifxMakeCookedOpExpr(info, old, append);
			break;
		default:
			/* exit silently */
			break;
	}
}

static void ifxAppendCookedOpExpr(IfxPushdownOprInfo *info,
								  Node               *node)
{
	Assert(info->num_args >= info->arg_idx);
	Assert(info->expr != NULL);

	/*
	 * Check wether we are currently creating a new
	 * cooked operator expression or just need to deparse
	 * this one. If true, we need to make sure to append
	 * this expression properly to the current operator.
	 */
	if (info->deparsetype == IFX_MAKE_COOKED_EXPR)
	{
		Node *ncopy = copyObject(node);

		/*
		 * If this Const has a type which requires
		 * special handling, act on it respectively.
		 */
		((OpExpr *)info->expr)->args
			= lappend(((OpExpr *)info->expr)->args,
					  ncopy);

		/*
		 * Increase the index of the currently cooked
		 * expression argument. This is required so that we
		 * can mark this operator as finalized.
		 */
		info->arg_idx++;

		info->deparsetype = (info->num_args == info->arg_idx) ? IFX_COOKED_EXPR
			: info->deparsetype;
	}
	else
		info->arg_idx++;
}

/*
 * Handle a cooked expression.
 */
static void ifxAppendCookedExpr(IfxPushdownOprInfo *info,
								Node               *node)
{
	Assert(node != NULL);
	Assert(info != NULL);

	/*
	 * Dispatch the appropiate cooking recipe...
	 */
	switch(info->expr->type)
	{
		case T_OpExpr:
			ifxAppendCookedOpExpr(info, node);
			break;
		default:
			/* exit silently */
			break;
	}
}

/*
 * Analysis the specified node wether there's
 * required work before being deparsed properly.
 */
static Const *ifxConvertNodeConst(Const *oldNode, bool *converted,
								  bool *supported)
{
	Const *result = oldNode;

	/* Initialize */
	*converted = false;

	/*
	 * We act on certain datatypes which cannot
	 * pushed down to informix without conversion.
	 */
	switch(oldNode->consttype)
	{
		case BPCHAROID:
		{
			Const *newNode;
			Datum datVal;

			/*
			 * Rewrite the given BPCHAR datum into a varchar without
			 * a typmod attached (-1 in this case).
			 */
			if (oldNode->constisnull)
				datVal= PointerGetDatum(NULL);
		else
				datVal = DirectFunctionCall3(varcharin,
											 CStringGetDatum(TextDatumGetCString(oldNode->constvalue)),
											 InvalidOid,
											 oldNode->consttypmod);

			newNode = makeConst(VARCHAROID,
								-1,
								oldNode->constcollid,
								oldNode->constlen,
								datVal,
								oldNode->constisnull,
								false);

			result = newNode;
			*converted = true;

			break;
		}
		case TEXTOID:
		{
			/*
			 * Rewrite a text datum into a varchar. This way
			 * the ruleutils machinery will do the right thing(tm)
			 * to spill out a deparsed expression suitable for
			 * Informix.
			 */
			Const *newNode;
			Datum  datVal;
			int    newtypmod;

			if (oldNode->constisnull)
				datVal = PointerGetDatum(NULL);
			else
				datVal = DirectFunctionCall3(varcharin,
											 CStringGetDatum(text_to_cstring(DatumGetTextP(oldNode->constvalue))),
											 InvalidOid,
											 oldNode->consttypmod);

			if (oldNode->consttypmod == -1)
			{
				if (VARSIZE(DatumGetVarCharP(datVal)) <= IFX_MAX_VARCHAR_LEN)
					newtypmod = IFX_MAX_VARCHAR_LEN + VARHDRSZ;
				else
				{
					*supported = false;
					break;
				}
			}
			else
				newtypmod = oldNode->consttypmod;

			newNode = makeConst(VARCHAROID,
								newtypmod,
								oldNode->constcollid,
								oldNode->constlen,
								datVal,
								oldNode->constisnull,
								false);
			result = newNode;
			*converted = true;
			break;
		}
		default:
			/* nothing to do */
			result = oldNode;
			break;
	}

	return result;
}

/*
 * Handle a cooked or deparse expression according
 * to the current state of the specified IfxPushdownOprInfo
 * structure.
 */
static void ifxCookExpr(IfxPushdownOprInfo *info,
						Node               *operexpr,
						Node               *operandexpr)
{
	switch(info->deparsetype)
	{
		case IFX_DEPARSED_EXPR:
			ifxMakeCookedExpr(info, operexpr, operandexpr);
			break;
		case IFX_MAKE_COOKED_EXPR:
			ifxAppendCookedExpr(info, operandexpr);
			break;
		case IFX_COOKED_EXPR:
			break;
		default:
			break;
	}
}

/*
 * Makes a List of Const values with all
 * extracted Datum from the given array type.
 * arrayConst must be a Const * node referencing
 * an ArrayType.
 */
static void rewriteInExprContext(Const *arrayConst,
								 IfxPushdownInOprContext *cxt)
{
	Datum    *array_elem;
	bool     *array_nulls;

	int       element_num;
	int16     element_len;

	ArrayType *in_array;
	int        i;

	bool       element_byval;
	char       element_align;

	Const     *new_const;
	Var       *ref_var;

	Assert((arrayConst != NULL)
		   && (arrayConst->consttype != InvalidOid)
		   && (cxt != NULL)
		   && (cxt->colref != NULL));

	in_array = DatumGetArrayTypeP(arrayConst->constvalue);

	/* deconstruct the array datum */
	get_typlenbyvalalign(ARR_ELEMTYPE(in_array),
						 &element_len,
						 &element_byval,
						 &element_align);
	deconstruct_array(in_array,
					  ARR_ELEMTYPE(in_array),
					  element_len,
					  element_byval,
					  element_align,
					  &array_elem,
					  &array_nulls,
					  &element_num);

	/*
	 * Build a new Var reference reflecting the
	 * <VAR> = <CONST> operator expression...
	 *
	 */
	if (cxt->target_type != InvalidOid)
	{
		ref_var = makeVar(cxt->colref->varno,
						  cxt->colref->varattno,
						  cxt->target_type,
						  cxt->target_typmod,
						  cxt->target_collid,
						  cxt->colref->varlevelsup);

		/*
		 * This will replace the original Var expression
		 * node retrieved from the foreign table, since we want
		 * to have a compatible expression here for all rewritten
		 * elements of the IN() expression.
		 */
		cxt->colref = ref_var;
	}
	else
	{
		ref_var = copyObject(cxt->colref);
	}

	/*
	 *
	 */

	for (i = 0; i < element_num; i++)
	{
		if (array_nulls[i])
			continue;

		elog(DEBUG5, "deconstructed array element, type %u",
			 ARR_ELEMTYPE(in_array));

		/*
		 * We might get a type coercion action by a RelabelType here
		 * in this case the current rewrite context should have
		 * a target_type OID set to a valid value.
		 */
		if (cxt->target_type != InvalidOid)
		{
			new_const = makeConst(cxt->target_type,
								  cxt->target_typmod,
								  cxt->target_collid,
								  element_len,
								  array_elem[i],
								  array_nulls[i],
								  element_byval);
		}
		else
		{
			/* A defined target type OID means that we are forced to
			 * make a Const node based on the extracted Var node before.
			 */
			Var *exVar = cxt->colref;

			new_const = makeConst(exVar->vartype,
								  exVar->vartypmod,
								  exVar->varcollid,
								  element_len,
								  array_elem[i],
								  array_nulls[i],
								  element_byval);
		}

		/*
		 * The array datum is cooked into a Const expression
		 * node now.
		 */
		cxt->elements = lappend(cxt->elements, new_const);
	}

}

/*
 * Gets a list of nodes and returns them
 * as a formatted SQL IN() expression.
 *
 * We refuse to work on any IfxDeparseType != IFX_COOKED_EXPR. That's
 * because we rely on OpExpr nodes to be cooked during
 * deparse analysis in ifx_predicate_walker() earlier.
 *
 * The info structure passed to deparse_node_list_for_InExpr() gets rewritten
 * during the parse analysis to create a compatible IfxPushdownOprInfo
 * struct for ifxFilterQuals() in ifx_fdw.c. Please note that this
 * works for a placeholder info struct marked with IFX_OPR_IN only
 * at the moment.
 */
void deparse_node_list_for_InExpr(IfxPushdownOprContext *context,
								  IfxPushdownInOprContext *in_cxt,
								  IfxPushdownOprInfo *info)
{
	int visited = 0;
	List           *dpc;
	ListCell       *cell;
	StringInfoData *buf;

	Assert((context != NULL)
		   && (info != NULL)
		   && (in_cxt != NULL));

	/*
	 * Deparse analysis should call us only in case
	 * we have a IFX_COOKED_EXPR. If not, just mark this as
	 * unsupported...
	 */
	if ((info->deparsetype != IFX_COOKED_EXPR)
		|| (info->type != IFX_OPR_IN))
	{
		IFX_MARK_PREDICATE_NOT_SUPPORTED(info);
		return;
	}

	/*
	 * Anything to do ?
	 */
	if (list_length(in_cxt->elements) <= 0)
	{
		/* Assume unsupported operator ... */
		IFX_MARK_PREDICATE_NOT_SUPPORTED(info);
		return;
	}

	/*
	 * Adjust varno. The RTE currently present aren't adjusted according
	 * to the FDW entries we get for the ForeignScan node. We call
	 * ChangeVarNodes() to adjust them to make them usable by deparsing
	 * later.
	 */
	ChangeVarNodes((Node *)in_cxt->colref, context->foreign_rtid, 1, 0);
	dpc = make_deparse_context(context->foreign_relid);

	/*
	 * Begin of IN() expression ...
	 */
	buf = makeStringInfo();
	info->type = IFX_OPR_IN;
	initStringInfo(buf);

	/*
	 * Deparse column reference
	 */
	appendStringInfo(buf, "%s IN(",
					 deparse_expression((Node *)in_cxt->colref,
										dpc,
										false, false));

	foreach(cell, in_cxt->elements)
	{
		Node *node = (Node *)lfirst(cell);

		/*
		 * We expect certain kinds of Expr nodes here,
		 * currently we support Const nodes only, but this might
		 * change sometime in the future.
		 */
		switch(node->type)
		{
			case T_Const:
			{
				Const *constval = (Const *) node;

				/*
				 * Check wether this Const node has
				 * a datatype which can be pushed down safely.
				 */
				if (!isCompatibleForPushdown(constval->consttype))
				{
					IFX_MARK_PREDICATE_NOT_SUPPORTED(info);
					break;
				}
				visited++;
				appendStringInfo(buf, "%s", getConstValue((Const *) node));

				if (visited < list_length(in_cxt->elements))
					appendStringInfoString(buf, ", ");
				break;
			}
			default:
				IFX_MARK_PREDICATE_NOT_SUPPORTED(info);
				break;
		}
	} /* foreach */

	appendStringInfoString(buf, ")");
	info->expr_string = cstring_to_text(buf->data);

}

/*
 * ifx_predicate_tree_walker()
 *
 * Examine the expression node. We expect a OPEXPR
 * here always in the form
 *
 * FDW col = CONST
 * FDW col != CONST
 * FDW col >(=) CONST
 * FDW col <(=) CONST
 *
 */
bool ifx_predicate_tree_walker(Node *node, struct IfxPushdownOprContext *context)
{
	IfxPushdownOprInfo *info;

	if (node == NULL)
		return false;

	/* Check for overly complicated expressions */
	check_stack_depth();

	/*
	 * Handle BoolExpr. Recurse into its arguments
	 * and try to decode its OpExpr accordingly.
	 */
	if (IsA(node, BoolExpr))
	{
		BoolExpr *boolexpr;
		ListCell *cell;

		boolexpr = (BoolExpr *) node;

		/*
		 * Decode the arguments to the BoolExpr
		 * directly, don't leave it to expression_tree_walker()
		 *
		 * By going down this route, we are able to push the
		 * IfxPushdownOprInfo into our context list in the right
		 * order.
		 */
		foreach(cell, boolexpr->args)
		{
			Node *bool_arg = (Node *) lfirst(cell);

			ifx_predicate_tree_walker(bool_arg, context);

			/*
			 * End of arguments?
			 * If true, don't add additional pushdown info
			 */
			if (PG_LIST_NEXT_ITEM(boolexpr->args, cell) != NULL)
			{
				/*
				 * Save boolean expression type.
				 */
				info = palloc(sizeof(IfxPushdownOprInfo));
				info->expr        = NULL;
				info->num_args    = 0;
				info->arg_idx     = 0;
				info->deparsetype = IFX_DEPARSED_EXPR;

				switch (boolexpr->boolop)
				{
					case AND_EXPR:
						info->type        = IFX_OPR_AND;
						info->expr_string = cstring_to_text("AND");
						break;
					case OR_EXPR:
						info->type        = IFX_OPR_OR;
						info->expr_string = cstring_to_text("OR");
						context->has_or_expr = true;
						break;
					case NOT_EXPR:
						info->type        = IFX_OPR_NOT;
						info->expr_string = cstring_to_text("NOT");
						break;
					default:
						elog(ERROR, "unsupported boolean expression type");
				}

				/* Push to the predicates list, but don't mark it
				 * as a pushdown expression */
				IFX_MARK_PREDICATE_BOOL(info, context);
			}
		}

		/* done */
		return true;
	}

	/*
	 * Check for <var> IN (...)
	 */
	else if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *scalarOpr = (ScalarArrayOpExpr *) node;
		IfxPushdownInOprContext in_cxt;
		ListCell *cell;

		info = palloc(sizeof(IfxPushdownOprInfo));
		info->expr = (Expr *)node;
		info->expr_string = NULL;
		info->deparsetype = IFX_DEPARSED_EXPR;
		info->num_args    = 1;
		info->arg_idx     = 0;

		/*
		 * Prepare the context to build a IN()
		 * expression suitable for us...
		 */
		in_cxt.colref        = NULL;
		in_cxt.target_type   = InvalidOid;
		in_cxt.target_typmod = -1;
		in_cxt.target_collid = InvalidOid;
		in_cxt.elements      = NIL;

		/* Safe operator OID and function OID */
		in_cxt.opno          = scalarOpr->opno;
		in_cxt.opfunc        = scalarOpr->opfuncid;

		/*
		 * Analyze operands. This is expected to be an array datum. The way
		 * we deparse this is as follows:
		 *
		 * - Extract the Var expression from the argument (or RelabelType, if any)
		 *   and assign their properties to the current IfxPushdownInOprContext
		 *   structure. This structure will collect all necessary deparse
		 *   information to make it easier to deparse the whole array expression
		 *   back to SQL.
		 * - Move the Datums of all array members into the element list of the
		 *   new IfxPushdownInOprContext structure. See rewriteInExprContext()
		 *   for more details.
		 */
		foreach(cell, scalarOpr->args)
		{
			Node *scalar_operand = (Node *) lfirst(cell);

			/*
			 * Take care for a RelabelType here.
			 * NOTE: Iff we don't find a Var type, we abort immediately
			 */
			if (IsA(scalar_operand, RelabelType))
			{
				RelabelType *relabelOpr = (RelabelType *) scalar_operand;

				/*
				 * Remember type coercion...
				 */
				in_cxt.target_type = relabelOpr->resulttype;
				in_cxt.target_typmod = relabelOpr->resulttypmod;
				in_cxt.target_collid = relabelOpr->resultcollid;

				if (!IsA(relabelOpr->arg, Var))
				{
					/*
					 * If no supported expression is found, return.
					 */
					context->count_removed++;
					return true;
				}
				else
				{
					/*
					 * Is a Var, we need this expression to cook
					 * our own expression.
					 */
					elog(DEBUG5, "extracting column reference");
					in_cxt.colref = (Var *)copyObject(relabelOpr->arg);
				}
			}
			else if (IsA(scalar_operand, Var))
			{
				/*
				 * Is a Var, we need this expression to cook
				 * our own expression.
				 */
				in_cxt.colref = (Var *)copyObject(scalar_operand);
			}
			else if (IsA(scalar_operand, Const))
			{
				elog(DEBUG5, "extracting array elements");

				/*
				 * Rewrite the current IfxPushdownInOprContext
				 * so that it can be passed to dpearse_node_list_for_InExpr().
				 */
				rewriteInExprContext((Const *) scalar_operand,
									 &in_cxt);
			}
			else
			{
				/* Not a supported operand so far ... */
				context->count_removed++;
				return true;
			}
		} /* for i in scalarOpr->args */

		/*
		 * Prepare a placeholder pushdown info struct for
		 * deparsing the cooked operator expressions. This info
		 * struct gets rewritten during deparse_node_list_for_InExpr() and
		 * and will carry a deparsed SQL string afterwards.
		 */
		info->deparsetype  = IFX_COOKED_EXPR;
		info->type         = IFX_OPR_IN;
		info->num_args     = 0; /* doesn't hold anything in expr */
		info->arg_idx      = 0;
		info->expr         = NULL;

		/*
		 * Mark this predicate for pushdown.
		 */
		IFX_MARK_PREDICATE_ELEM(info, context);

		/*
		 * Deparse the expression node...
		 *
		 * Since deparse_predicate_node() isn't designed to
		 * deparse whole BoolExpr trees or complicater things, we
		 * instrument deparse_node_list_InExpr(), which will return
		 *
		 */
		deparse_node_list_for_InExpr(context,
									 &in_cxt,
									 info);
	}

	/*
	 * Check for <var> IS NULL or <var> IS NOT NULL
	 */
	else if (IsA(node, NullTest))
	{
		NullTest *ntest;
		bool      operand_supported;

		ntest = (NullTest *)node;
		info = palloc(sizeof(IfxPushdownOprInfo));
		info->expr = (Expr *)node;
		info->expr_string = NULL;
		info->deparsetype = IFX_DEPARSED_EXPR;
		info->num_args    = 1;
		info->arg_idx     = 0;

		/*
		 * NullTest on composite types can be thrown away immediately.
		 */
		if (ntest->argisrow)
			return true;

		/*
		 * [NOT] NULL ...
		 */
		switch (ntest->nulltesttype)
		{
			case IS_NULL:
				info->type = IFX_IS_NULL;
				break;
			case IS_NOT_NULL:
				info->type = IFX_IS_NOT_NULL;
				break;
		}

		/*
		 * Assume this expression to be able to be pushed
		 * down...
		 */
		operand_supported = true;

		/*
		 * ...else check wether the expression is passed suitable
		 * for pushdown.
		 */
		switch (((Expr *)(ntest->arg))->type)
		{
			case T_Var:
			{
				Var *var = (Var *) ntest->arg;

				elog(DEBUG2, "varno %d, bogus_varno %d, varlevelsup %d",
					 var->varno, context->foreign_rtid, var->varlevelsup);

				if (var->varno != context->foreign_rtid ||
					var->varlevelsup != 0)
					operand_supported = false;
				break;
			}
			default:
				operand_supported = false;
		}

		/*
		 * If no supported expression is found, return.
		 */
		if (!operand_supported)
		{
			context->count_removed++;
			return true;
		}

		/*
		 * Mark this expression for pushdown.
		 */
		IFX_MARK_PREDICATE_ELEM(info, context);

		/*
		 * Deparse the expression node...
		 */
		deparse_predicate_node(context, info);
	}

	/*
	 * Check wether this is an OpExpr. If true,
	 * recurse into it...
	 */
	else if (IsA(node, OpExpr))
	{
		OpExpr *opr;

		opr               = (OpExpr *)node;
		info              = palloc(sizeof(IfxPushdownOprInfo));
		info->expr        = (Expr *)node;
		info->deparsetype = IFX_DEPARSED_EXPR;
		info->num_args    = list_length(opr->args);
		info->arg_idx     = 0;
		info->expr_string = NULL;

		if (mapPushdownOperator(opr->opno, info) != IFX_OPR_NOT_SUPPORTED)
		{
			ListCell *cell;
			bool      operand_supported;

			/*
			 * Examine the operands of this operator expression. Please
			 * note that we don't get further here, we stop at the first
			 * layer even when there are more nested expressions.
			 *
			 * NOTE: during analysis the IfxPushdownOprInfo might get rewritten
			 *       into an IFX_COOKED_EXPR expression.
			 */
			operand_supported = true;
			foreach(cell, opr->args)
			{
				Node *oprarg = lfirst(cell);

				switch(oprarg->type)
				{
					case T_Var:
					{
						Var *var = (Var *) oprarg;

						elog(DEBUG2, "varno %d, bogus_varno %d, varlevelsup %d",
							 var->varno, context->foreign_rtid, var->varlevelsup);

						if (var->varno != context->foreign_rtid ||
							var->varlevelsup != 0)
							operand_supported = false;

						ifxCookExpr(info, node, oprarg);
						break;
					}
					case T_Const:
					{
						Const *const_val = (Const *) oprarg;
						bool   converted = false;
						Const *converted_const;

						/*
						 * Check wether this constant value has a datatype
						 * which cannot be safely pushed down.
						 */
						if (!isCompatibleForPushdown(const_val->consttype))
						{
							operand_supported = false;
							break;
						}

						/*
						 * We might need to rewrite the Datum if it's not already
						 * in a format we need so we can it safely push down
						 * to the Informix server.
						 *
						 * NOTE: We cook a new operator expression iff any conversion
						 *       on the given Const value was done. converted_const might
						 *       the same pointer to const_val, in case no conversion
						 *       is done!
						 *
						 *       ifxConvertNodeConst() might also set operand_supported
						 *       to false in case it discovers that the specified
						 *       constant cannot be parsed into an Informix compatible
						 *       expression.
						 */
						converted_const = ifxConvertNodeConst(const_val, &converted,
															  &operand_supported);

						if (!operand_supported)
							break;

						ifxCookExpr(info, node, (Node *)converted_const);

						break;
					}
					case T_RelabelType:
					{
						RelabelType *r = (RelabelType *) oprarg;

						/*
						 * Type coercion between binary compatible datatypes.
						 * Try to extract the interesting part for us.
						 *
						 * XXX: This might not be really safe collation-wise, but
						 *      make it possible right now...
						 */
						elog(DEBUG2, "relabel type detected");
						elog(DEBUG2, "relabel type is %s", nodeToString(r));

						if (IsA(r->arg, Var))
						{
							/*
							 * We need to try harder here, since we only want to extract
							 * a Var and Const expression out of a relabel operation.
							 *
							 * The idea here is to construct a PostgreSQL/Informix
							 * compatible operator expression, suitable to be deparsed
							 * below. We do this in the hope to reuse all the parsing
							 * infrastructure PostgreSQL gives us, and, since Informix
							 * has its syntax very close to Postgres, also may pushdown
							 * some "special" expressions.
							 *
							 * So copy over and rewrite this operator into an expression
							 * suitable for our needs.
							 */
							ifxCookExpr(info, node, (Node *)r->arg);
						}
						else
							operand_supported = false;

						break;
					}
					default:
						operand_supported = false;
						break;
				}

				/* exit immediately */
				if(!operand_supported)
				{
					context->count_removed++;
					break;
				}
			}

			/*
			 * If any operand not supported is found, don't
			 * bother adding this operator expression to the pushdown
			 * predicate list.
			 */
			if (!operand_supported)
			{
				context->count_removed++;
				return true; /* done */
			}

			/*
			 * Mark this predicate for pushdown.
			 */
			IFX_MARK_PREDICATE_ELEM(info, context);

			/*
			 * Deparse the expression node...
			 */
			deparse_predicate_node(context, info);
		}
		else
		{
			/* Operator not supported */
			context->count_removed++;
			return true;
		}
	}

	/* reached in case no further nodes to be examined */
	return false;
}

/*
 * Deparse the given operator expression into a string assigned
 * to the specified IfxPushdownOprInfo pointer.
 *
 * Caller should defend against IFX_OPR_NOT_SUPPORTED!
 */
static void
deparse_predicate_node(IfxPushdownOprContext *context,
					   IfxPushdownOprInfo *info)
{
	List     *dpc; /* deparse context */
	Node     *copy_obj;
	StringInfoData predstr; /* buffer for deparsing */

	/*
	 * Sanity check against non supported expressions.
	 */
	Assert(info->type != IFX_OPR_NOT_SUPPORTED);

	if (info->deparsetype == IFX_COOKED_EXPR)
		elog(DEBUG5, "deparsing a cooked epxression");

	/*
	 * Copy the expression node. We don't want to allow
	 * ChangeVarNodes() to fiddle directly on the baserestrictinfo
	 * nodes.
	 *
	 * We don't need this to do in case this is a cooked
	 * expression created for pushdown only.
	 */
	if (info->deparsetype != IFX_COOKED_EXPR)
		copy_obj = copyObject((Node *)info->expr);
	else
		copy_obj = (Node *)info->expr;

	/*
	 * Adjust varno. The RTE currently present aren't adjusted according
	 * to the FDW entries we get for the ForeignScan node. We call
	 * ChangeVarNodes() to adjust them to make them usable by deparsing
	 * later.
	 */
	ChangeVarNodes(copy_obj, context->foreign_rtid, 1, 0);
	dpc = make_deparse_context(context->foreign_relid);

	/* Buffer to hold deparsed predicate string. */
	initStringInfo(&predstr);

	/*
	 * Given a unary or binary operator, deparse
	 * the arguments accordingly.
	 *
	 * The only unary operator supported so far is the
	 * NullTest expression, see ifx_predicate_walker() for
	 * details;
	 */
	if (info->num_args == 1)
	{
		appendStringInfoString(&predstr, deparse_expression(copy_obj, dpc, false, false));
	}
	else if (info->num_args > 1)
	{
		Node *oprarg_left;
		Node *oprarg_right;
		char *oprstr; /* deparsed operator identifier */
		char *left;
		char *right;

		/*
		 * At this point we *know* that we can receive operator
		 * expressions via IfxPushdownOprInfo only (otherwise something
		 * else went utterly wrong before).
		 */
		oprstr       = getIfxOperatorIdent(info);
		Assert(oprstr != NULL); /* should not happen */

		oprarg_left  = (Node *)linitial(((OpExpr *)info->expr)->args);
		oprarg_right = (Node *)lsecond(((OpExpr *)info->expr)->args);

		if (IsA(oprarg_left, Const))
			left = getConstValue((Const *)oprarg_left);
		else
			left = deparse_expression(oprarg_left, dpc, false, false);

		if (IsA(oprarg_right, Const))
			right = getConstValue((Const *) oprarg_right);
		else
			right = deparse_expression(oprarg_right, dpc, false, false);

		appendStringInfo(&predstr, "%s %s %s",
						 left,
						 oprstr,
						 right);

	}

	info->expr_string = cstring_to_text(predstr.data);
	elog(DEBUG1, "deparsed pushdown predicate %d, %s",
		 context->count - 1,
		 text_to_cstring(info->expr_string));
}

static char * getConstValue(Const *constNode)
{
	regproc typout;
	char   *result;
	Assert(constNode != NULL);

	if (constNode->constisnull)
		return "NULL";

	/*
	 * Get the type datum text representation via its output function.
	 */
	typout = getTypeOutputFunction(constNode->consttype);

	/*
	 * Take care for datatype quoting.
	 *
	 * XXX: This is really ugly and i hate it, but currently we don't
	 *      use the SQLDA structure for prepared _SELECT_ statements,
	 *      e.g. via ifxExecuteStmtSqlda(). This certainly needs
	 *      more love in the future...
	 */
	switch (constNode->consttype)
	{
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		case BYTEAOID:
		case TIMEOID:
		case TIMETZOID:
		case TIMESTAMPOID:
		case DATEOID:
		case TIMESTAMPTZOID:
		case INTERVALOID:
			result = quote_literal_cstr(DatumGetCString(
											OidFunctionCall1(typout,
															 constNode->constvalue)));
			break;
		default:
			result = DatumGetCString(
				OidFunctionCall1(typout,
								 constNode->constvalue));
			break;
	}

	return result;
}
