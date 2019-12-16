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

#if PG_VERSION_NUM >= 90500
#include <access/htup_details.h>
#endif

#include <utils/syscache.h>

static void ifxFdwExecutionStateToList(Const *const_vals[],
									   IfxFdwExecutionState *state);
static Datum
ifxFdwPlanDataAsBytea(IfxConnectionInfo *coninfo);

#if PG_VERSION_NUM >= 90500
static char *ifxPgIntervalQualifierString(IfxTemporalRange range);
#endif

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
	{ "%4F", "MS" },
	{ "%5F", "MS" }
};

/*
 * Maps DATETIME and INTERVAL qualifiers to
 * PostgreSQL modifiers. The array index have to
 * match the IFX_TU_* macros in ifx_type_compat.h.
 */
char *ifxPgTemporalQualifier[]
	= {
	"YEAR",
	NULL,
	"MONTH",
	NULL,
	"DAY",
	NULL,
	"HOUR",
	NULL,
	"MINUTE",
	NULL,
	"SECOND",
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
	state->use_rowid              = ifxGetSerializedInt16Field(params,
															   SERIALIZED_USE_ROWID);
	state->has_after_row_triggers = ifxGetSerializedInt16Field(params,
															   SERIALIZED_HAS_AFTER_TRIGGERS);

	/*
	 * This has to be the last entry, see ifxSerializedPlanData()
	 * for details!
	 */
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

	const_vals[SERIALIZED_USE_ROWID]
		= makeFdwInt16Const(state->use_rowid);

	const_vals[SERIALIZED_HAS_AFTER_TRIGGERS]
		= makeFdwInt16Const(state->has_after_row_triggers);
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
 * 2. - 10. String or int fields of IfxFdwExecutionState, that are:
 *         query, stmt_name, cursor_name, ...
 * 11. The last member is always the affectedAttrNums list from the
 *     state structure.
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
					appendStringInfoString(&strbuf, ".");
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
	appendStringInfo(&sql, "DELETE FROM %s",
					 coninfo->tablename);

	/*
	 * We need to append the WHERE expression, but we
	 * need to distinguish between using a ROWID to identify
	 * the remote target tuple or (if disable_rowid was specified)
	 * the name of the updatable cursor.
	 */
	if (coninfo->disable_rowid)
		appendStringInfo(&sql, " WHERE CURRENT OF %s",
					 state->stmt_info.cursor_name);
	else
		appendStringInfo(&sql, " WHERE rowid = ?");

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
	 * Finally the WHERE condition needs to be added.
	 *
	 * Per default we use the ROWID to identify the remote tuple for the
	 * UPDATE target, but we might also fallback to an updatable cursor
	 * if disable_rowid was passed to the table.
	 */
	if (coninfo->disable_rowid)
		appendStringInfo(&sql, "WHERE CURRENT OF %s", state->stmt_info.cursor_name);
	else
		appendStringInfo(&sql, "WHERE rowid = ?");

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

/*
 * If the specified connection handle was initialized
 * with DELIMIDENT, ifxQuoteIdent() will return a quoted
 * identifier.
 *
 * NOTE: if DELIMIDENT is *not* set, ifxQuoteIdent() will
 *       return the same unmodified pointer for ident!
 */
char *ifxQuoteIdent(IfxConnectionInfo *coninfo, char *ident)
{
	if (coninfo->delimident == 0)
		return ident;
	else
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "\"%s\"", ident);
		return buf.data;
	}
}

#if PG_VERSION_NUM >= 90500

/*
 * Given an informix INTERVAL range definition, return
 * a possible matching declaration for PostgreSQL.
 *
 * Not all declarations from Informix do have a matching
 * declaration in PostgreSQL. If an INTERVAL range in Informix
 * doesn't correspond to a compatible declaration in PostgreSQL,
 * we just return an empty string, which indicates that the given
 * temporal range doesn't have an equivalent.
 */
static char *ifxPgIntervalQualifierString(IfxTemporalRange range)
{
	int i_start = range.start;
	int i_end   = -1; /* indicates empty qualifier string! */
	StringInfoData buf;

	/*
	 * Check if the specified range is valid and supported.
	 *
	 * Currently supported *ranges* in PostgreSQL are
	 *
	 * YEAR TO MONTH
	 *
	 * DAY TO HOUR
	 * DAY TO MINUTE
	 * DAY TO SECOND
	 *
	 * HOUR TO MINUTE
	 * HOUR TO SECOND
	 *
	 * MINUTE TO SECOND
	 *
	 * So it's enough to look at YEAR, DAY, HOUR and MINUTE to
	 * determine any possible range declarations. If start and end define
	 * just a single entity, we return that instead.
	 */
	if (((range.start % 2) > 0)
		|| ((range.end %2) > 0))
		return "";

	if (range.start == range.end)
		return ifxPgTemporalQualifier[range.start];

	initStringInfo(&buf);

	switch(range.start)
	{
		case IFX_TU_YEAR:
			i_end = (range.end == IFX_TU_MONTH) ? range.end : -1;
			break;

		case IFX_TU_DAY:
		case IFX_TU_HOUR:
			if (range.end >= IFX_TU_SECOND)
				i_end = (range.end - (range.end % IFX_TU_SECOND));
			else
				i_end = range.end;

			break;

		case IFX_TU_MINUTE:
			/* only remaining range is MINUTE TO SECOND */
			i_end = ((range.end - (range.end % IFX_TU_SECOND)) == IFX_TU_SECOND)
				? range.end : -1;
			break;
		default:
			i_end = -1;
	}

	if (i_end != -1)
		appendStringInfo(&buf, "%s TO %s",
						 ifxPgTemporalQualifier[i_start],
						 ifxPgTemporalQualifier[i_end]);

	return buf.data;
}

char *ifxMakeColTypeDeclaration(IfxAttrDef *colDef)
{
	HeapTuple ht;
	Oid       targetTypeId;
	StringInfoData buf;

	/*
	 * Lookup the matching PostgreSQL TYPEOID.
	 */
	targetTypeId = ifxTypeidToPg(ifxMaskTypeId(colDef->type),
								 colDef->extended_id);

	if (targetTypeId == InvalidOid)
		elog(ERROR, "could not convert informix type \"%d\"",
			 ifxMaskTypeId(colDef->type));

	initStringInfo(&buf);

	ht = SearchSysCache1(TYPEOID, ObjectIdGetDatum(targetTypeId));
	if (HeapTupleIsValid(ht))
	{
		Form_pg_type typetup = (Form_pg_type) GETSTRUCT(ht);
		char *typname;
		short min_len; /* min_len is not used in PostgreSQL column declarations */
		short max_len;

		/*
		 * Copy the typename.
		 */
		typname = pstrdup(NameStr(typetup->typname));

		/*
		 * Lookup typmods, but don't encode them.
		 */
		ifxDecodeColumnLength(colDef->type,
							  colDef->len,
							  &min_len,
							  &max_len);

		elog(DEBUG5, "typename=%s, min=%d, max=%d",
			 typname, min_len, max_len);

		if (((min_len > 0) && (max_len > 0))
			 || ((colDef->type == IFX_DTIME)
				 || (colDef->type == IFX_INTERVAL)))
		{
			/* Probably an encoded VARCHAR type with
			 * minimum and maximum length ? */

			if (ifxCharColumnLen(colDef->type, colDef->len) > 0)
				appendStringInfo(&buf, "%s(%d)", typname, max_len);
			else
			{
				/*
				 * We must handle DATETIME and INTERVAL in case
				 * they have special qualifiers.
				 */
				switch (colDef->type)
				{
					case IFX_DTIME:
					{
						/*
						 * In case there's a FRACTION attached to the Informix
						 * DATETIME value, we try to match it to the PostgreSQL
						 * timestamp as well.
						 */
						if ((max_len - (max_len % IFX_TU_SECOND)) >= IFX_TU_SECOND)
						{
							appendStringInfo(&buf, "%s(%d)",
											 typname,
											 max_len - IFX_TU_SECOND);
						}
						else
						{
							appendStringInfo(&buf, "%s",
											 typname);
						}
						break;
					}
					case IFX_INTERVAL:
					{
						IfxTemporalRange range;
						char *intv_qual;

						range.start = min_len;
						range.end  = max_len;
						range.precision = IFX_TU_SECOND;

						intv_qual = ifxPgIntervalQualifierString(range);

						if (range.end >= IFX_TU_SECOND)
						{
							/*
							 * This INTERVAL has a fraction value assigned.
							 */
							appendStringInfo(&buf, "%s %s(%d)",
											 typname, intv_qual,
											 range.end - IFX_TU_SECOND);
						}
						else
						{
							appendStringInfo(&buf, "%s %s",
											 typname, intv_qual);
						}
						break;
					}
					default:
						appendStringInfoString(&buf, typname);
						break;
				}
			}
		}
		else if ((min_len == 0) && (max_len > 0))
		{
			/*
			 * Probably a character string column type with
			 * an upper limit?
			 */
			if (ifxCharColumnLen(colDef->type, colDef->len) > 0)
				appendStringInfo(&buf, "%s(%d)",
								 typname, max_len);
			else
				appendStringInfoString(&buf, typname);
		}
		else
		{
			appendStringInfoString(&buf, typname);
		}

		/*
		 * Define a NOT NULL constraint if required.
		 */
		if (colDef->indicator == INDICATOR_NOT_NULL)
			appendStringInfoString(&buf, " NOT NULL");

		/* ...and we're done */
		ReleaseSysCache(ht);
	}

	return buf.data;
}

/*
 * Map Informix type ids to PostgreSQL OID types.
 *
 * This merely is a suggestion what we think an Informix type
 * maps at its best to a builtin PostgreSQL type.
 */
Oid ifxTypeidToPg(IfxSourceType typid, IfxExtendedType extended_id)
{
	Oid mappedOid = InvalidOid;

	switch (typid)
	{
		case IFX_TEXT:
			mappedOid = TEXTOID;
			break;
		case IFX_CHARACTER:
			mappedOid = BPCHAROID;
			break;
		case IFX_SMALLINT:
			mappedOid = INT2OID;
			break;
		case IFX_SERIAL:
		case IFX_INTEGER:
			mappedOid = INT4OID;
			break;
		case IFX_FLOAT:
			mappedOid = FLOAT8OID;
			break;
		case IFX_SMFLOAT:
			mappedOid = FLOAT4OID;
			break;
		case IFX_MONEY:
		case IFX_DECIMAL:
			mappedOid = NUMERICOID;
			break;
		case IFX_DATE:
			mappedOid = DATEOID;
			break;
		case IFX_DTIME:
			mappedOid = TIMESTAMPOID;
			break;
		case IFX_BYTES:
			mappedOid = BYTEAOID;
			break;
		case IFX_VCHAR:
			mappedOid = VARCHAROID;
			break;
		case IFX_INTERVAL:
			mappedOid = INTERVALOID;
			break;
		case IFX_NCHAR:
		case IFX_NVCHAR:
		case IFX_LVARCHAR:
			mappedOid = TEXTOID;
			break;
		case IFX_INT8:
		case IFX_SERIAL8:
		case IFX_INFX_INT8:
		case IFX_BIGSERIAL:
			mappedOid = INT8OID;
			break;
		case IFX_BOOLEAN:
			mappedOid = BOOLOID;
			break;

		case IFX_UDTVAR:
			/*
			 * This is only a BE visible opaque type id
			 * indicating a variable length user defined (built-in)
			 * data type, like LVARCHAR. The conversion routines
			 * usually won't see this typeid, since ESQL/C would
			 * have transferred a defined FE typeid for this (like
			 * SQLLVARCHAR). Assume a TEXTOID column as the right target
			 * for this.
			 *
			 * This might not work for all cases, but for now and according
			 * to
			 *
			 * http://www-01.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.sqlr.doc/ids_sqr_026.htm
			 *
			 * LVARCHAR is the only pre-defined candidate so far (IFX_XTD_LVARCHAR).
			 * The external representation of a variable length type is a character
			 * string anyways, so always convert them to TEXT (others won't likely
			 * be suitable anyways).
			 */
			mappedOid = TEXTOID;
			break;
		case IFX_UDTFIXED:
			/*
			 * FIXED user-defined types are special, since we have multiple types
			 * fitting this category according to $INFORMIXDIR/incl/esql/sqltypes.h.
			 * These are
			 *
			 * BOOLEAN, BLOB and CLOB.
			 *
			 * Those are distinguished by the sysxtdtypes system catalog via referencing
			 * them by syscolumns.extended_id.
			 */
			switch (extended_id)
			{
				case IFX_XTD_BOOLEAN:
					mappedOid = BOOLOID;
					break;
				case IFX_XTD_BLOB:
				case IFX_XTD_CLOB:
					mappedOid = BYTEAOID;
					break;
				default:
					break;
			}
			break;

		/*
		 * The following types aren't handled right now.
		 * Return InvalidOid in this case.
		 */
		case IFX_NULL:
		case IFX_SET:
		case IFX_MULTISET:
		case IFX_LIST:
		case IFX_ROW:
		case IFX_COLLECTION:
		case IFX_ROWREF:
			/* not handled */
			break;

		default:
			/* return InvalidOid in this case */
			break;
	}

	return mappedOid;
}

/*
 * ifxGetTableDetailsSQL
 *
 * Returns an SQL string allowing to retrieve table column definitions
 * suitable to be used by ifxGetForeignTableDetails().
 */
char *ifxGetTableDetailsSQL(IfxSourceType tabid)
{
	StringInfoData buf;
	char *get_column_info = "SELECT tabname, colno, colname, coltype, collength, extended_id"
		"  FROM systables a, syscolumns b"
		" WHERE a.tabid = b.tabid AND a.tabid = %d"
		" ORDER BY colno;";

	initStringInfo(&buf);
	appendStringInfo(&buf, get_column_info, (int) tabid);

	return buf.data;
}

/*
 * ifxGetTableImportListSQL
 *
 * Returns a SQL statement suitable to be passed to Informix
 * to retrieve a list of table names, owner and tabid matching
 * the definitions specified by an IMPORT FOREIGN SCHEMA statement.
 */
char *ifxGetTableImportListSQL(IfxConnectionInfo *coninfo,
							   ImportForeignSchemaStmt *stmt)
{
	char *get_table_info = "SELECT tabid, trim(owner), tabname FROM systables WHERE tabid >= 100 AND owner = '%s'";
	StringInfoData buf;
	char *table_list;

	table_list = ifxGetTableListAsStringConn(coninfo, stmt->table_list);
	initStringInfo(&buf);
	appendStringInfo(&buf, get_table_info, stmt->remote_schema);

	switch (stmt->list_type)
	{
		case FDW_IMPORT_SCHEMA_LIMIT_TO:
		{
			appendStringInfo(&buf, "%s%s%s", " AND tabname IN (", table_list, ")");
			break;
		}
		case FDW_IMPORT_SCHEMA_EXCEPT:
		{
			appendStringInfo(&buf, "%s%s%s", " AND tabname NOT IN (", table_list, ")");
			break;
		}
		default:
			/* FDW_IMPORT_SCHEMA_ALL, nothing needs to be done */
			break;
	}

	appendStringInfoString(&buf, " ORDER BY tabname DESC");
	return buf.data;
}

/*
 * Generate a list of tables as a comma-separated list and
 * returns it as a character string.
 *
 * Will return a NULL pointer as a result if table_list is NIL or empty.
 */
char *ifxGetTableListAsStringConn(IfxConnectionInfo *coninfo, List *table_list)
{
	StringInfoData buf;
	ListCell *cell;
	bool      first_item;

	/* short cut if list is NIL or empty */
	if ((table_list != NIL) && (list_length(table_list) <= 0))
		return NULL;


	initStringInfo(&buf);

	first_item = true;
	foreach(cell, table_list)
	{
		RangeVar *rv = (RangeVar *) lfirst(cell);

		if (first_item)
			first_item = false;
		else
			appendStringInfoString(&buf, ", ");

		appendStringInfo(&buf, "'%s'", rv->relname);
	}

	return buf.data;
}

/*
 * Gets a list of pointers to IfxImportTableDef structures
 * and generates a script with CREATE FOREIGN TABLE statements
 * from it. The script finally is returned as a list of C strings,
 * each being a CREATE FOREIGN TABLE statement.
 */
List *ifxCreateImportScript(IfxConnectionInfo *coninfo,
							ImportForeignSchemaStmt *stmt,
							List *candidates,
							Oid serverOid)
{
	List *result = NIL;
	ListCell *cell;
	ForeignServer     *server;

	if ((candidates == NIL) || (list_length(candidates) <= 0))
		return result;

	/* initialization stuff */
	server = GetForeignServer(serverOid);

	foreach(cell, candidates)
	{
		StringInfoData     buf;
		ListCell          *cell_cols;
		IfxImportTableDef *tableDef;
		bool               firstCol = true;

		initStringInfo(&buf);
		tableDef = (IfxImportTableDef *) lfirst(cell);

		elog(DEBUG1, "generate SQL script for foreign table %s",
			 quote_identifier(tableDef->tablename));

		appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n",
						 quote_identifier(tableDef->tablename));

		foreach(cell_cols, tableDef->columnDef)
		{
			IfxAttrDef *colDef = (IfxAttrDef *) lfirst(cell_cols);
			char       *dtbuf;

			dtbuf = ifxMakeColTypeDeclaration(colDef);

			if (firstCol)
				appendStringInfo(&buf,
								 "%s %s",
								 quote_identifier(colDef->name),
								 dtbuf);
			else
				appendStringInfo(&buf,
								 ",\n%s %s",
								 quote_identifier(colDef->name),
								 dtbuf);

			firstCol = false;
		}


		/*
		 * Make SERVER and OPTIONS clause.
		 */
		appendStringInfo(&buf, ") SERVER %s OPTIONS(",
						 quote_identifier(server->servername));
		appendStringInfo(&buf, "%s '%s'",
						 quote_identifier("table"),
						 tableDef->tablename);
		appendStringInfo(&buf, ", %s '%s'",
						 "client_locale",
						 coninfo->client_locale);

		/*
		 * If we encounter a foreign table which references
		 * any BYTES or TEXT columns on the Informix side, we
		 * should better set the enable_blobs option ...
		 */
		if (tableDef->special_cols & IFX_HAS_BLOBS)
			appendStringInfoString(&buf, ",enable_blobs '1'");

		/*
		 * DB_LOCALE is optional but although not required
		 * we attach it to the table declaration if set.
		 */
		if (coninfo->db_locale != NULL)
			appendStringInfo(&buf, ", %s '%s'",
							 "db_locale",
							 coninfo->db_locale);

		appendStringInfo(&buf, ", %s '%s'",
						 "database",
						 coninfo->database);
		appendStringInfoString(&buf, ");");

		elog(DEBUG3, "informix_fdw script: %s", buf.data);

		/* store the script entry */
		result = lappend(result, buf.data);
	}

	return result;
}

#endif
