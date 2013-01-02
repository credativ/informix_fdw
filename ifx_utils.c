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
}

bytea *
ifxFdwPlanDataAsBytea(IfxConnectionInfo *coninfo)
{
	bytea *data;

	data = (bytea *) palloc(sizeof(IfxPlanData) + VARHDRSZ);
	memcpy(VARDATA(data), &(coninfo->planData), sizeof(IfxPlanData));
	return data;
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

	const_vals[SERIALIZED_QUALS]
		= makeFdwStringConst(state->stmt_info.predicate);

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
										   PointerGetDatum(ifxFdwPlanDataAsBytea(coninfo)),
										   false, false);
	ifxFdwExecutionStateToList(vals, state);

	for (i = 0; i < N_SERIALIZED_FIELDS; i++)
	{
		result = lappend(result, vals[i]);
	}

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
