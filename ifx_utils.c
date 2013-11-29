/*-------------------------------------------------------------------------
 *
 * ifx_utils.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "ifx_fdw.h"
#include "ifx_node_utils.h"

static void ifxFdwExecutionStateToList(Const *const_vals[],
									   IfxFdwExecutionState *state);
static Datum
ifxFdwPlanDataAsBytea(IfxConnectionInfo *coninfo);

typedef struct ifxTemporalFormatIdent
{
	char *_IFX;
	char *_PG;
} ifxTemporalFormatIdent;

/*
 * Defines various format strings to convert
 * Informix temporal types.
 *
 * NOTE: NULL indicates that this position is currently
 *       unused, but to ease the access through ranges
 *       we still use the array indexes at this point.
 *       Any code iterating through this ident array must
 *       be aware of NULL dereferencing!
 */
static ifxTemporalFormatIdent ifxTemporalFormat[] =
{
	{ "%Y", "YYYY" },
	{ NULL, NULL },
	{ "%m", "MM" },
	{ NULL, NULL },
	{ "%d", "DD" },
	{ NULL, NULL},
	{ "%H", "HH24" },
	{ NULL, NULL },
	{ "%M", "MI" },
	{ NULL, NULL },
	{ "%S", "SS" },
	{ "%F", "MS" },
	{ "%2F", "MS" },
	{ "%3F", "MS" },
	{ "%4F", "US" },
	{ "%5F", "US" }
};

#define IFX_PG_INTRVL_FORMAT(ident, mode) \
	(((mode) == FMT_PG) ? ifxTemporalFormat[(ident)]._PG \
	 : ifxTemporalFormat[(ident)]._IFX)

/*
 * Deserialize data from fdw_private, passed
 * from the planner via PlanForeignScan().
 *
 * This will initialize certain fields from
 * data previously retrieved in ifxPlanForeignScan().
 */
void ifxDeserializeFdwData(IfxFdwExecutionState *state,
						   void *fdw_private)
{
	List *params;
	Assert(state != NULL);

	params = (List *) fdw_private;

	Assert(params != NIL);

	state->stmt_info.query = ifxGetSerializedStringField(params,
														 SERIALIZED_QUERY);
	state->stmt_info.cursor_name = ifxGetSerializedStringField(params,
															   SERIALIZED_CURSOR_NAME);
	state->stmt_info.stmt_name = ifxGetSerializedStringField(params,
															 SERIALIZED_STMT_NAME);
	state->stmt_info.call_stack = ifxGetSerializedInt16Field(params,
															 SERIALIZED_CALLSTACK);
	state->stmt_info.predicate = ifxGetSerializedStringField(params,
															 SERIALIZED_QUALS);
	state->stmt_info.cursorUsage = ifxGetSerializedInt32Field(params,
															  SERIALIZED_CURSOR_TYPE);
	state->stmt_info.special_cols = ifxGetSerializedInt16Field(params,
															   SERIALIZED_SPECIAL_COLS);
	state->stmt_info.refid        = ifxGetSerializedInt32Field(params,
															   SERIALIZED_REFID);
	state->affectedAttrNums       = list_nth(params, AFFECTED_ATTR_NUMS_IDX);
}

/*
 * Copies the plan data hold by the specified
 * IfxConnectionInfo pointer into a bytea datum,
 * suitable to be passed over by ifxSerializePlanData().
 */
static Datum
ifxFdwPlanDataAsBytea(IfxConnectionInfo *coninfo)
{
	bytea *result;

	result = (bytea *) palloc(sizeof(IfxPlanData) + VARHDRSZ);
	SET_VARSIZE(result, sizeof(IfxPlanData));
	memcpy(VARDATA(result), &(coninfo->planData), sizeof(IfxPlanData));
	return PointerGetDatum(result);
}

/*
 * Deserializes a IfxPlanData pointer from the
 * given list of Const values. Suitable to be used to
 * retrieve a IfxPlanData struct formerly serialized
 * by ifxSerializePlanData().
 */
void
ifxDeserializePlanData(IfxPlanData *planData,
					   void *fdw_private)
{
	Const *const_expr;
	bytea *bvalue;
	List  *vals;

	vals = (List *) fdw_private;
	const_expr = (Const *) list_nth(vals, SERIALIZED_PLAN_DATA);

	Assert((const_expr != NULL)
		   && (planData != NULL)
		   && (const_expr->consttype == BYTEAOID));

	bvalue = DatumGetByteaP(const_expr->constvalue);

	elog(DEBUG1, "deserialized planData %s\n, varsize %d",
		 nodeToString(const_expr),
		 VARSIZE(bvalue));

	memcpy(planData, VARDATA(bvalue), sizeof(IfxPlanData));
}

/*
 * Serialize the execution state into a list of
 * Const nodes.
 *
 * We are going to ignore the sqlstate at this point, because
 * (hopefully) we are done with all SQL stuff and checked
 * for errors before calling this function.
 */
static void ifxFdwExecutionStateToList(Const *const_vals[],
									   IfxFdwExecutionState *state)
{
	Assert(state != NULL);

	const_vals[SERIALIZED_QUERY]
		= makeFdwStringConst(state->stmt_info.query);

	const_vals[SERIALIZED_STMT_NAME]
		= makeFdwStringConst(state->stmt_info.stmt_name);

	const_vals[SERIALIZED_CURSOR_NAME]
		= makeFdwStringConst(state->stmt_info.cursor_name);

	const_vals[SERIALIZED_CALLSTACK]
		= makeFdwInt16Const(state->stmt_info.call_stack);

	if (state->stmt_info.predicate != NULL)
		const_vals[SERIALIZED_QUALS]
			= makeFdwStringConst(state->stmt_info.predicate);
	else
		const_vals[SERIALIZED_QUALS]
			= makeFdwStringConst("");

	const_vals[SERIALIZED_CURSOR_TYPE]
		= makeFdwInt32Const(state->stmt_info.cursorUsage);

	const_vals[SERIALIZED_SPECIAL_COLS]
		= makeFdwInt16Const(state->stmt_info.special_cols);

	const_vals[SERIALIZED_REFID]
		= makeFdwInt32Const(state->stmt_info.refid);
}

/*
 * Saves all necessary parameters from the specified structures
 * into a list, suitable to pass it over from the planner
 * to executor (thus it converts all values into expressions
 * usable with copyObject() later on). This is required
 * to save values across ifxPlanForeignScan() and
 * ifxBeginForeignScan()...
 *
 * The current layout of the returned list is as follows:
 *
 * 1. Const with a bytea value, holding the binary representation
 *    of IfxPlanData struct
 * 2. - 4. String fields of IfxFdwExecutionState, that are:
 *         query, stmt_name, cursor_name
 * 5. short int value saving the state of call_stack
 *
 */
List * ifxSerializePlanData(IfxConnectionInfo *coninfo,
							IfxFdwExecutionState *state,
							PlannerInfo *plan)
{
	int    i;
	List  *result;
	MemoryContext old_cxt;
	SERIALIZED_DATA(vals);

	old_cxt = MemoryContextSwitchTo(plan->planner_cxt);

	result      = NIL;

	/*
	 * Save the IfxPlanData struct, then
	 * serialize all fields from IfxFdwExecutionState.
	 */

	vals[SERIALIZED_PLAN_DATA] = makeConst(BYTEAOID, -1, InvalidOid, -1,
										   ifxFdwPlanDataAsBytea(coninfo),
										   false, false);

	/*
	 * Save data from execution state into array.
	 */
	ifxFdwExecutionStateToList(vals, state);

	/*
	 * Serialize values from Const array.
	 */
	for (i = 0; i < N_SERIALIZED_FIELDS; i++)
	{
		result = lappend(result, vals[i]);
	}

	/*
	 * ifxFdwExecutionStateToList() doesn't fold
	 * the affectedAttrNums list into the Const array, we
	 * need to address it separately here.
	 *
	 * NOTE:
	 *
	 * This should always be the last list member, since
	 * this makes it possible to address it via
	 * AFFECTED_ATTR_NUMS_IDX macro directly.
	 */
	result = lappend(result, state->affectedAttrNums);

	MemoryContextSwitchTo(old_cxt);

	return result;
}

char * ifxGetSerializedStringField(List *list, int ident)
{
	Const *const_expr;
	char  *result;

	const_expr = (Const *) list_nth(list, ident);

	Assert(const_expr->consttype == TEXTOID);

	result     = text_to_cstring(DatumGetTextP(const_expr->constvalue));
	return result;
}

int ifxGetSerializedInt32Field(List *list, int ident)
{
	Const *const_expr;
	int    result;

	const_expr = (Const *) list_nth(list, ident);

	Assert(const_expr->consttype == INT4OID);

	result     = DatumGetInt32(const_expr->constvalue);
	return result;
}

int16 ifxGetSerializedInt16Field(List *list, int ident)
{
	Const *const_expr;
	int16  result;

	const_expr = (Const *) list_nth(list, ident);

	Assert(const_expr->consttype == INT2OID);

	result     = DatumGetInt16(const_expr->constvalue);
	return result;
}

Datum ifxSetSerializedInt32Field(List *list, int ident, int value)
{
	Const *const_expr;

	const_expr = (Const *) list_nth(list, ident);

	Assert(const_expr->consttype == INT4OID);

	const_expr->constvalue = Int32GetDatum(value);
	return const_expr->constvalue;
}

Datum ifxSetSerializedInt16Field(List *list, int ident, int16 value)
{
	Const *const_expr;

	const_expr = (Const *) list_nth(list, ident);

	Assert(const_expr->consttype = INT2OID);
	const_expr->constvalue = Int16GetDatum(value);
	return const_expr->constvalue;
}

/*
 * Returns a format string for a given Interval
 * qualifier range. This format string is suitable to be
 * passed to ESQL/C format routines directly to convert
 * a string formatted interval value back into its binary
 * representation.
 *
 * In case the range qualifier of the given range value is
 * out of the valid ranges an Informix interval value allows,
 * NULL is returned.
 *
 * To summarize, the following interval ranges are supported
 * currently:
 *
 * - TU_YEAR - TU_MONTH (gives YYYY-MM)
 * - TU_DAY - TU_F1-5 (gives DD HH24:MIN:SS.FFFFF)
 *
 * ifxGetIntervalFromString() recognizes the precision of the
 * given range as the lowest digit to be returned within the format
 * string.
 */
char *ifxGetIntervalFormatString(IfxTemporalRange range, IfxFormatMode mode)
{
	StringInfoData strbuf;
	int i;

	initStringInfo(&strbuf);

	i = range.start;

	while ((i <= range.end) && (i <= range.precision))
	{
		if (IFX_PG_INTRVL_FORMAT(i, mode) == NULL)
		{
			i++;
			continue;
		}

		appendStringInfoString(&strbuf, IFX_PG_INTRVL_FORMAT(i, mode));

		/* next one ... */
		i++;

		/*
		 * Append a correct filler character...if necessary ;)
		 */
		if ((i < range.end)
			&& (i > range.start))
		{
			switch(i - 1)
			{
				case IFX_TU_MONTH:
				case IFX_TU_YEAR:
					appendStringInfoString(&strbuf, "-");
					break;
				case IFX_TU_DAY:
					appendStringInfoString(&strbuf, " ");
					break;
				case IFX_TU_HOUR:
				case IFX_TU_MINUTE:
					appendStringInfoString(&strbuf, ":");
					break;
				case IFX_TU_SECOND:
					/* must be fraction here */
					//appendStringInfoString(&strbuf, ".");
				case IFX_TU_F1:
				case IFX_TU_F2:
				case IFX_TU_F3:
				case IFX_TU_F4:
				case IFX_TU_F5:
					/* abort since this is the lowest precision */
					i = range.end;
					break;
				default:
					break;
			}
		}
	}

	return strbuf.data;
}

#if PG_VERSION_NUM >= 90300

/*
 * Generates a SQL statement for DELETE operation on a
 * remote Informix table. Assumes the caller already
 * had initialized the specified IfxFdwExecutionState
 * and IfxConnectionInfo handles correctly.
 *
 * The generated query string will be stored into the
 * specified execution state structure.
 */
void ifxGenerateDeleteSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo)
{
	StringInfoData sql;

	/* Sanity check */
	Assert((state != NULL) && (coninfo != NULL)
		   && (state->stmt_info.cursor_name != NULL)
		   && (coninfo->tablename != NULL));

	/*
	 * Generate the DELETE statement. Again, we use the underlying
	 * cursor from the remote scan to delete it's current tuple
	 * by using the CURRENT OF <cursor> syntax.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql, "DELETE FROM %s WHERE CURRENT OF %s",
					 coninfo->tablename,
					 state->stmt_info.cursor_name);
	state->stmt_info.query = sql.data;
}

/*
 * Generates a SQL statement for UPDATE operation on a
 * remote Informix table. Assumes the caller already
 * had initialized the specified IfxFdwExecutionState and
 * IfxConnectionInfo handles correctly.
 *
 * The generated query string will be stored into the
 * specified execution state structure.
 */
void ifxGenerateUpdateSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo,
						  PlannerInfo          *root,
						  Index                 rtindex)
{
	StringInfoData sql;
	bool           first;
	ListCell      *cell;

	/* Sanity checks */
	Assert((state != NULL)
		   && (coninfo != NULL)
		   && (coninfo->tablename != NULL));

	if (state->affectedAttrNums == NIL)
		elog(ERROR, "empty column list for foreign table");

	initStringInfo(&sql);
	appendStringInfo(&sql, "UPDATE %s SET ", coninfo->tablename);

	/*
	 * Dispatch list of attributes numbers to their
	 * corresponding identifiers.
	 *
	 * It is important to keep this list consistent
	 * to the same order we receive all affected rows from
	 * the local modify command. Otherwise we get into trouble.
	 */
	first = true;
	foreach(cell, state->affectedAttrNums)
	{
		int attnum = lfirst_int(cell);

		if (!first)
			appendStringInfoString(&sql, ", ");
		first = false;

		appendStringInfoString(&sql,
							   dispatchColumnIdentifier(rtindex, attnum, root));
		appendStringInfoString(&sql, " = ? ");
	}

	/*
	 * Finally the WHERE condition needs to be added. Since
	 * we rely on an updatable cursor for now, it's enough
	 * to append WHERE CURRENT OF <cursor>...
	 */
	appendStringInfo(&sql, "WHERE CURRENT OF %s", state->stmt_info.cursor_name);

	/*
	 * And we're done.
	 */
	state->stmt_info.query = sql.data;
}

/*
 * Generates a SQL statement for INSERT action on a
 * remote Informix table. Assumes the caller already
 * had initialized the specified IfxFdwExecutionState and
 * IfxConnectionInfo handles correctly.
 *
 * The generated query string will be stored into the
 * specified execution state structure.
 */
void ifxGenerateInsertSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo,
						  PlannerInfo          *root,
						  Index                 rtindex)
{
	StringInfoData  sql;
	ListCell       *cell;
	bool            first;
	int             i;

	Assert(state != NULL);
	Assert((coninfo != NULL) && (coninfo->tablename));

	if (state->affectedAttrNums == NIL)
		elog(ERROR, "empty column list for foreign table");

	initStringInfo(&sql);
	appendStringInfoString(&sql, "INSERT INTO ");

	/*
	 * Execution state already carries the table name...
	 *
	 */
	appendStringInfoString(&sql, coninfo->tablename);
	appendStringInfoString(&sql, "(");

	/*
	 * Dispatch list of attributes numbers to their
	 * corresponding identifiers.
	 */
	first = true;
	foreach(cell, state->affectedAttrNums)
	{
		int attnum = lfirst_int(cell);

		if (!first)
			appendStringInfoString(&sql, ", ");
		first = false;

		appendStringInfoString(&sql,
							   dispatchColumnIdentifier(rtindex, attnum, root));
	}

	appendStringInfoString(&sql, ") VALUES(");

	/*
	 * Create a list of question marks suitable to be passed
	 * for PREPARE...
	 */
	first = true;
	for(i = 0; i < list_length(state->affectedAttrNums); i++)
	{
		if (!first)
			appendStringInfoString(&sql, ", ");
		first = false;

		appendStringInfoString(&sql, "?");
	}

	appendStringInfoString(&sql, ")");
	state->stmt_info.query = sql.data;
}

#endif
