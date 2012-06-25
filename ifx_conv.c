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
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_conv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#include "ifx_fdw.h"

/*******************************************************************************
 * Helper functions
 */
static regproc getTypeCastFunction(IfxFdwExecutionState *state,
								   Oid sourceOid, Oid targetOid);
static regproc getTypeInputFunction(IfxFdwExecutionState *state,
									Oid inputOid);

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
	if (ifxGetDateAsString(&(state->stmt_info), attnum, val) == NULL)
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
	 * Try the conversion.
	 */
	typeinputfunc = getTypeInputFunction(state, inputOid);

	PG_TRY();
	{
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

	 if (ifxGetTimestampAsString(&(state->stmt_info), attnum, val) == NULL)
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
	  * Get the input function and try the conversion. We just pass
	  * the character string into the specific type input function.
	  */
	 typeinputfunc = getTypeInputFunction(state, inputOid);

	 PG_TRY();
	 {
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
  * convertIfxInt()
  *
  * Converts either an 2-, 4-, or 8-byte informix integer value
  * into a corresponding PostgreSQL datum. The target type
  * range is checked and conversion refused if it doesn't
  * match. We also support conversion into either TEXT, VARCHAR
  * and bpchar.
  *
  * XXX: What about NUMERIC???
  */
 Datum convertIfxInt(IfxFdwExecutionState *state, int attnum)
 {
	 Datum result;
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
			 int2 val;

			 /* accepts int2 only */
			 if (IFX_ATTRTYPE_P(state, attnum) != IFX_SMALLINT)
			 {
				 IFX_ATTR_SETNOTVALID_P(state, attnum);
				 return PointerGetDatum(NULL);
			 }

			 val = ifxGetInt2(&(state->stmt_info), attnum);
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

			 val = ifxGetInt4(&(state->stmt_info), attnum);
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
					 || (IFX_ATTRTYPE_P(state, attnum) == IFX_INFX_INT8))
				 {
					char *buf;
					regproc typinputfunc;

					buf = (char *) palloc0(IFX_INT8_CHAR_LEN + 1);

					/* extract the value from the sqlvar tuple */
					buf = ifxGetInt8(&(state->stmt_info), attnum, buf);

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
					if (IFX_ATTRTYPE_P(state, attnum) == IFX_INTEGER)
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
																		   attnum)));
					else
						/* only INT2 left... */
						result = OidFunctionCall1(typcastfunc,
												  Int16GetDatum(ifxGetInt2(&(state->stmt_info),
																		   attnum)));
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

	ReleaseSysCache(type_tuple);

	result = ((Form_pg_type) GETSTRUCT(type_tuple))->typinput;
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
					 attnum);

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
		 * Try the conversion...
		 */
		typeinputfunc = ((Form_pg_type) GETSTRUCT(conv_tuple))->typinput;

		PG_TRY();
		{
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
	/* pushdownInfo->pg_opr_nsp   = oprForm->oprnamespace; */
	/* pushdownInfo->pg_opr_left  = oprForm->oprleft; */
	/* pushdownInfo->pg_opr_right = oprForm->oprright; */
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
 * Only CONST and VAR expressions are currently supported.
 */
bool ifx_predicate_tree_walker(Node *node, struct IfxPushdownOprContext *context)
{
	IfxPushdownOprInfo *info;

	if (node == NULL)
		return false;

	/*
	 * Check wether this is an OpExpr. If true,
	 * recurse into it...
	 */
	if (IsA(node, OpExpr))
	{
		IfxPushdownOprInfo *info;
		OpExpr *opr;

		info = palloc(sizeof(IfxPushdownOprInfo));
		info->expr = opr  = (OpExpr *)node;
		info->expr_string = NULL;

		if (mapPushdownOperator(opr->opno, info) != IFX_OPR_NOT_SUPPORTED)
		{
			text *node_string;

			context->predicates = lappend(context->predicates, info);
			context->count++;

			/*
			 * Try to deparse the OpExpr and save it
			 * into the IfxPushdownOprInfo structure...
			 */
			node_string = cstring_to_text(nodeToString(info->expr));
			info->expr_string = DatumGetTextP(OidFunctionCall3((regproc)2509,
															   PointerGetDatum(node_string),
															   ObjectIdGetDatum(context->foreign_relid),
															   BoolGetDatum(true)));

			elog(DEBUG1, "deparsed pushdown predicate %d, %s",
				 context->count - 1,
				 text_to_cstring(info->expr_string));
			return expression_tree_walker(node, ifx_predicate_tree_walker,
										  (void *) context);
		}
	}

	info = list_nth(context->predicates, context->count - 1);

	if (! IsA(node, Var) && ! IsA(node, Const))
	{
		elog(DEBUG3, "removed opr %d with type %d from pushdown list",
			 context->count - 1,
			 info->type);

		info->type = IFX_OPR_NOT_SUPPORTED;
		/* done */
		return true;
	}

	return false;

}
