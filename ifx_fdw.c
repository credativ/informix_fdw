/*-------------------------------------------------------------------------
 *
 * ifx_fdw.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/indexing.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "ifx_fdw.h"
#include "ifx_conncache.h"

PG_MODULE_MAGIC;

/*
 * Object options using this wrapper module
 */
struct IfxFdwOption
{
	const char *optname;
	Oid         optcontext;
};

/*
 * Valid options for informix_fdw.
 */
static struct IfxFdwOption ifx_valid_options[] =
{
	{ "informixserver",   ForeignServerRelationId },
	{ "user",             UserMappingRelationId },
	{ "password",         UserMappingRelationId },
	{ "database",         ForeignTableRelationId },
	{ "query",            ForeignTableRelationId },
	{ "table",            ForeignTableRelationId },
	{ "estimated_rows",   ForeignTableRelationId },
	{ "connection_costs", ForeignTableRelationId },
	{ NULL,               ForeignTableRelationId }
};

/*
 * Data structure for intercall data
 * used by ifxGetConnections().
 */
struct ifx_sp_call_data
{
	HASH_SEQ_STATUS *hash_status;
	TupleDesc        tupdesc;
};

/*
 * informix_fdw handler and validator function
 */
extern Datum ifx_fdw_handler(PG_FUNCTION_ARGS);
extern Datum ifx_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(ifx_fdw_handler);
PG_FUNCTION_INFO_V1(ifx_fdw_validator);
PG_FUNCTION_INFO_V1(ifxGetConnections);

/*******************************************************************************
 * FDW callback routines.
 */

static FdwPlan *ifxPlanForeignScan(Oid foreignTableOid,
								   PlannerInfo *planInfo,
								   RelOptInfo *baserel);

static void
ifxExplainForeignScan(ForeignScanState *node, ExplainState *es);

static void
ifxBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *ifxIterateForeignScan(ForeignScanState *node);

static void ifxEndForeignScan(ForeignScanState *node);

/*******************************************************************************
 * FDW helper functions.
 */
static StringInfoData *
ifxFdwOptionsToStringBuf(Oid context);

static bool
ifxIsValidOption(const char *option, Oid context);

static void
ifxGetOptions(Oid foreigntableOid, IfxConnectionInfo *coninfo);

static StringInfoData *
ifxGetDatabaseString(IfxConnectionInfo *coninfo);

static StringInfoData *
ifxGenerateConnName(IfxConnectionInfo *coninfo);
static char *
ifxGenStatementName(IfxConnectionInfo *coninfo);
static char *
ifxGenDescrName(IfxConnectionInfo *coninfo);

static void
ifxGetOptionDups(IfxConnectionInfo *coninfo, DefElem *def);

static void ifxConnInfoSetDefaults(IfxConnectionInfo *coninfo);

static IfxConnectionInfo *ifxMakeConnectionInfo(Oid foreignTableOid);

static char *ifxGenCursorName(IfxConnectionInfo *coninfo);

static void ifxPgColumnData(Oid foreignTableOid, IfxFdwExecutionState *festate);

static IfxFdwExecutionState *makeIfxFdwExecutionState();

static IfxSqlStateClass
ifxCatchExceptions(IfxStatementInfo *state, unsigned short stackentry);

static inline void ifxPopCallstack(IfxStatementInfo *info,
								   unsigned short stackentry);
static inline void ifxPushCallstack(IfxStatementInfo *info,
									unsigned short stackentry);

/*******************************************************************************
 * Implementation starts here
 */

/*
 * ifxPushCallstack()
 *
 * Updates the call stack with the new
 * stackentry.
 */
static inline void ifxPushCallstack(IfxStatementInfo *info,
									unsigned short stackentry)
{
	if (stackentry == 0)
		return;
	info->call_stack |= stackentry;
}

/*
 * ifxPopCallstack()
 *
 * Sets the status of the call stack to the
 * given state.
 */
static inline void ifxPopCallstack(IfxStatementInfo *info,
								   unsigned short stackentry)
{
	info->call_stack &= ~stackentry;
}

/*
 * ifxRewindCallstack()
 *
 * Gets the call back and tries to free
 * all resources associated with the call stack
 * in the given state.
 */
static void ifxRewindCallstack(IfxStatementInfo *info)
{
	/*
	 * NOTE: IFX_STACK_DESCRIBE doesn't need any special handling here,
	 * so just ignore it until the end of rewinding the call stack
	 * and set it to IFX_STACK_EMPTY if everything else is undone.
	 */

	if ((info->call_stack & IFX_STACK_OPEN) == IFX_STACK_OPEN)
	{
		ifxCloseCursor(info);
		elog(DEBUG2, "informix_fdw: undo open");
		ifxPopCallstack(info, IFX_STACK_OPEN);
	}

	if ((info->call_stack & IFX_STACK_ALLOCATE) == IFX_STACK_ALLOCATE)
	{
		/* ifxDeallocateDescriptor(info->descr_name); */
		elog(DEBUG2, "informix_fdw: undo allocate");
		if (info->sqlda != NULL)
			free(info->sqlda);
		ifxPopCallstack(info, IFX_STACK_ALLOCATE);
	}

	if ((info->call_stack & IFX_STACK_DECLARE) == IFX_STACK_DECLARE)
	{
		ifxFreeResource(info);
		elog(DEBUG2, "informix_fdw: undo declare");
		ifxPopCallstack(info, IFX_STACK_DECLARE);
	}

	if ((info->call_stack & IFX_STACK_PREPARE) == IFX_STACK_PREPARE)
	{
		ifxFreeResource(info);
		elog(DEBUG2, "informix_fdw: undo prepare");
		ifxPopCallstack(info, IFX_STACK_PREPARE);
	}

	info->call_stack = IFX_STACK_EMPTY;
}

/*
 * Trap errors from the informix FDW API.
 *
 * This function checks exceptions from ESQL
 * and creates corresponding NOTICE, WARN or ERROR
 * messages.
 *
 */
static IfxSqlStateClass ifxCatchExceptions(IfxStatementInfo *state,
										   unsigned short stackentry)
{
	IfxSqlStateClass errclass;

	/*
	 * Set last error, if any
	 */
	errclass = ifxSetException(state);

	if (errclass != IFX_SUCCESS)
	{
		/*
		 * Obtain the error message. Since ifxRewindCallstack()
		 * will release any associated resources before we can
		 * print an ERROR message, we save the current from
		 * the caller within an IfxSqlStateMessage structure.
		 */
		IfxSqlStateMessage message;

		elog(DEBUG1, "informix FDW exception count: %d",
			 state->exception_count);

		ifxGetSqlStateMessage(1, &message);

		switch (errclass)
		{
			case IFX_RT_ERROR:
				/* log FATAL */
				ifxRewindCallstack(state);
				ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
								errmsg("informix FDW error: \"%s\"",
									   message.text),
								errdetail("SQLSTATE %s (SQLCODE=%d)",
										  message.sqlstate, message.sqlcode)));
				break;
			case IFX_WARNING:
				/* log WARN */
				ereport(WARNING, (errcode(ERRCODE_FDW_ERROR),
								  errmsg("informix FDW warning: \"%s\"",
										 message.text),
								  errdetail("SQLSTATE %s", message.sqlstate)));
				break;
			case IFX_ERROR:
				/* log ERROR */
				ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
								errmsg("informix FDW error: \"%s\"",
									   message.text),
								errdetail("SQLSTATE %s", message.sqlstate)));
				break;
			case IFX_NOT_FOUND:
			default:
				/* needs no log */
				break;
		}
	}

	/*
	 * IFX_SUCCESS
	 */
	ifxPushCallstack(state, stackentry);
	elog(DEBUG1, "push callstack %u", state->call_stack);

	return errclass;
}

/*
 * Returns a fully initialized pointer to
 * an IfxFdwExecutionState structure. All pointers
 * are initialized to NULL.
 */
static IfxFdwExecutionState *makeIfxFdwExecutionState()
{
	IfxFdwExecutionState *state = palloc(sizeof(IfxFdwExecutionState));

	bzero(state->stmt_info.conname, IFX_CONNAME_LEN + 1);
	state->stmt_info.cursorUsage = IFX_DEFAULT_CURSOR;

	state->stmt_info.query        = NULL;
	state->stmt_info.cursor_name  = NULL;
	state->stmt_info.stmt_name    = NULL;
	state->stmt_info.descr_name   = NULL;
	state->stmt_info.sqlda        = NULL;
	state->stmt_info.ifxAttrCount = 0;
	state->stmt_info.ifxAttrDefs  = NULL;
	state->stmt_info.call_stack   = IFX_STACK_EMPTY;
	state->stmt_info.row_size     = 0;

	bzero(state->stmt_info.sqlstate, 6);
	state->stmt_info.exception_count = 0;

	state->pgAttrCount = 0;
	state->pgAttrDefs  = NULL;
	state->values = NULL;

	return state;
}

/*
 * Retrieve the local column definition of the
 * foreign table (attribute number, type and additional
 * options).
 */
static void ifxPgColumnData(Oid foreignTableOid, IfxFdwExecutionState *festate)
{
	HeapTuple         tuple;
	Relation          attrRel;
	SysScanDesc       scan;
	ScanKeyData       key[2];
	Form_pg_attribute attrTuple;
	Relation          foreignRel;
	int               attrIndex;

	attrIndex = 0;

	/* open foreign table, should be locked already */
	foreignRel = heap_open(foreignTableOid, NoLock);
	festate->pgAttrCount = RelationGetNumberOfAttributes(foreignRel);
	heap_close(foreignRel, NoLock);

	festate->pgAttrDefs = palloc(sizeof(PgAttrDef) * festate->pgAttrCount);

	/*
	 * Get all attributes for the given foreign table.
	 */
	attrRel = heap_open(AttributeRelationId, AccessShareLock);
	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(foreignTableOid));
	ScanKeyInit(&key[1], Anum_pg_attribute_attnum,
				BTGreaterStrategyNumber, F_INT2GT,
				Int16GetDatum((int2)0));
	scan = systable_beginscan(attrRel, AttributeRelidNumIndexId, true,
							  SnapshotNow, 2, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		/* don't rely on attnum directly */
		++attrIndex;
		attrTuple = (Form_pg_attribute) GETSTRUCT(tuple);

		/* Check for dropped columns. Any match is recorded
		 * by setting the corresponding attribute number to -1.
		 */
		if (attrTuple->attisdropped)
		{
			festate->pgAttrDefs[attrIndex - 1].attnum = -1;
			festate->pgAttrDefs[attrIndex - 1].atttypid = -1;
			festate->pgAttrDefs[attrIndex - 1].atttypmod = -1;
			festate->pgAttrDefs[attrIndex - 1].attname = NULL;
			continue;
		}

		/*
		 * Protect against corrupted numbers in pg_class.relnatts
		 * and number of attributes retrieved from pg_attribute.
		 */
		if (attrIndex > festate->pgAttrCount)
		{
			systable_endscan(scan);
			heap_close(attrRel, AccessShareLock);
			elog(ERROR, "unexpected number of attributes in foreign table");
		}

		/*
		 * Save the attribute and all required properties for
		 * later usage.
		 */
		festate->pgAttrDefs[attrIndex - 1].attnum = attrTuple->attnum;
		festate->pgAttrDefs[attrIndex - 1].atttypid = attrTuple->atttypid;
		festate->pgAttrDefs[attrIndex - 1].atttypmod = attrTuple->atttypmod;
		festate->pgAttrDefs[attrIndex - 1].attname = pstrdup(NameStr(attrTuple->attname));
	}

	systable_endscan(scan);
	heap_close(attrRel, AccessShareLock);
}

/*
 * Checks for duplicate and redundant options.
 *
 * Check for redundant options. Error out in case we've found
 * any duplicates or, in case it is an empty option, assign
 * it to the connection info.
 */
static void
ifxGetOptionDups(IfxConnectionInfo *coninfo, DefElem *def)
{
	Assert(coninfo != NULL);

	if (strcmp(def->defname, "informixdir") == 0)
	{
		if (coninfo->informixdir)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: informixdir(%s)",
								   defGetString(def))));

		coninfo->informixdir = defGetString(def);
	}


	if (strcmp(def->defname, "servername") == 0)
	{
		if (coninfo->servername)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: servername(%s)",
								   defGetString(def))));

		coninfo->servername = defGetString(def);
	}

	if (strcmp(def->defname, "database") == 0)
	{
		if (coninfo->database)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: database(%s)",
								   defGetString(def))));

		coninfo->database = defGetString(def);
	}

	if (strcmp(def->defname, "username") == 0)
	{
		if (coninfo->database)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: username(%s)",
								   defGetString(def))));

		coninfo->username = defGetString(def);
	}

	if (strcmp(def->defname, "password") == 0)
	{
		if (coninfo->password)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: password(%s)",
								   defGetString(def))));

		coninfo->password = defGetString(def);
	}

	if (strcmp(def->defname, "query") == 0)
	{
		if (coninfo->tablename)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("conflicting options: query cannot be used with table")
						));

		if (coninfo->query)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("conflicting or redundant options: query (%s)", defGetString(def))
						));

		coninfo->tablename = defGetString(def);
	}

	if (strcmp(def->defname, "table") == 0)
	{
		if (coninfo->query)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("conflicting options: query cannot be used with query")));

		if (coninfo->tablename)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: table(%s)",
								   defGetString(def))));

		coninfo->tablename = defGetString(def);
	}

	/*
	 * Check wether cost parameters are already set.
	 */
	if (strcmp(def->defname, "estimated_rows") == 0)
	{
		/*
		 * Try to convert the cost value into a double value.
		 */
		char *endp;
		char *val;

		val = defGetString(def);
		coninfo->planData.estimated_rows = strtof(val, &endp);

		if (val == endp && coninfo->planData.estimated_rows < 0.0)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("\"%s\" is not a valid number for parameter \"estimated_rows\"",
								   val)));

	}

	if (strcmp(def->defname, "connection_costs") == 0)
	{
		/*
		 * Try to convert the cost value into a double value
		 */
		char *endp;
		char *val;

		val = defGetString(def);
		coninfo->planData.connection_costs = strtof(val, &endp);

		if (val == endp && coninfo->planData.connection_costs < 0)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("\"%s\" is not a valid number for parameter \"estimated_rows\"",
								   val)));
	}

}

/*
 * Returns the database connection string
 * as 'dbname@servername'
 */
static StringInfoData *
ifxGetDatabaseString(IfxConnectionInfo *coninfo)
{
	StringInfoData *buf;

	buf = makeStringInfo();
	initStringInfo(buf);
	appendStringInfo(buf, "%s@%s", coninfo->database, coninfo->servername);

	return buf;
}

/*
 * Create a unique name for the database connection.
 *
 * Currently the name is generated by concatenating the
 * database name, server name and user into a single string.
 */
static StringInfoData *
ifxGenerateConnName(IfxConnectionInfo *coninfo)
{
	StringInfoData *buf;

	buf = makeStringInfo();
	initStringInfo(buf);
	appendStringInfo(buf, "%s%s%s", coninfo->username, coninfo->database,
					 coninfo->servername);

	return buf;
}

Datum
ifx_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	fdwRoutine->PlanForeignScan    = ifxPlanForeignScan;
	fdwRoutine->ExplainForeignScan = ifxExplainForeignScan;
	fdwRoutine->BeginForeignScan   = ifxBeginForeignScan;
	fdwRoutine->IterateForeignScan = ifxIterateForeignScan;
	fdwRoutine->EndForeignScan     = ifxEndForeignScan;
	fdwRoutine->ReScanForeignScan  = NULL;

	PG_RETURN_POINTER(fdwRoutine);
}

/*
 * Validate options passed to the INFORMIX FDW (that are,
 * FOREIGN DATA WRAPPER, SERVER, USER MAPPING and FOREIGN TABLE)
 */
Datum
ifx_fdw_validator(PG_FUNCTION_ARGS)
{
	List     *ifx_options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid       catalogOid = PG_GETARG_OID(1);
	IfxConnectionInfo coninfo = {0};
	ListCell *cell;

	/*
	 * Check options passed to this FDW. Validate values and required
	 * arguments.
	 */
	foreach(cell, ifx_options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		/*
		 * Unknown option specified, print an error message
		 * and a hint message what's wrong.
		 */
		if (!ifxIsValidOption(def->defname, catalogOid))
		{
			StringInfoData *buf;

			buf = ifxFdwOptionsToStringBuf(catalogOid);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", buf->len ? buf->data : "<none>")
						));
		}

		ifxGetOptionDups(&coninfo, def);
	}

	PG_RETURN_VOID();
}

/*
 * Retrieves options for ifx_fdw foreign data wrapper.
 */
 static void
ifxGetOptions(Oid foreigntableOid, IfxConnectionInfo *coninfo)
{
	ForeignTable  *foreignTable;
	ForeignServer *foreignServer;
	UserMapping   *userMap;
	List          *options;
	ListCell      *elem;
	bool           mandatory[2] = { false, false };
	int            i;

	Assert(coninfo != NULL);

	foreignTable  = GetForeignTable(foreigntableOid);
	foreignServer = GetForeignServer(foreignTable->serverid);
	userMap       = GetUserMapping(GetUserId(), foreignTable->serverid);

	options = NIL;
	options = list_concat(options, foreignTable->options);
	options = list_concat(options, foreignServer->options);
	options = list_concat(options, userMap->options);

	/*
	 * Retrieve required arguments.
	 */
	foreach(elem, options)
	{
		DefElem *def = (DefElem *) lfirst(elem);

		elog(DEBUG1, "ifx_fdw set param %s=%s",
			 def->defname, defGetString(def));

		/*
		 * "informixserver" defines the INFORMIXSERVER to connect to
		 */
		if (strcmp(def->defname, "informixserver") == 0)
		{
			coninfo->servername = pstrdup(defGetString(def));
			mandatory[0] = true;
		}

		/*
		 * "informixdir" defines the INFORMIXDIR environment
		 * variable.
		 */
		if (strcmp(def->defname, "informixdir") == 0)
		{
			coninfo->informixdir = pstrdup(defGetString(def));
			mandatory[1] = true;
		}

		if (strcmp(def->defname, "database") == 0)
		{
			coninfo->database = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "username") == 0)
		{
			coninfo->username = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "password") == 0)
		{
			coninfo->password = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "table") == 0)
		{
			coninfo->tablename = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "query") == 0)
		{
			coninfo->query = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "estimated_rows") == 0)
		{
			char *val;

			val = defGetString(def);
			coninfo->planData.estimated_rows = strtof(val, NULL);
		}

		if (strcmp(def->defname, "connection_costs") == 0)
		{
			char *val;

			val = defGetString(def);
			coninfo->planData.connection_costs = strtof(val, NULL);
		}
	}

	/*
	 * Check for mandatory options
	 */
	for (i = 0; i < IFX_REQUIRED_CONN_KEYWORDS; i++)
	{
		if (!mandatory[i])
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
							errmsg("missing required FDW options (informixserver, informixdir)")));
	}

}

/*
 * Generate a unique statement identifier to create
 * on the target database. Informix requires us to build
 * a unique name among all concurrent connections.
 *
 * Returns a palloc'ed string containing a statement identifier
 * suitable to pass to an Informix database.
 */
static char *ifxGenStatementName(IfxConnectionInfo *coninfo)
{
	char *stmt_name;
	size_t stmt_name_len;

	stmt_name_len = strlen(coninfo->conname) + 15;
	stmt_name     = (char *) palloc(stmt_name_len + 1);
	bzero(stmt_name, stmt_name_len + 1);

	snprintf(stmt_name, stmt_name_len, "%s_stmt%d",
			 coninfo->conname, MyBackendId);

	return stmt_name;
}

static char *ifxGenDescrName(IfxConnectionInfo *coninfo)
{
	char *descr_name;
	size_t descr_name_len;

	descr_name_len = strlen(coninfo->conname) + 16;
	descr_name     = (char *)palloc(descr_name_len + 1);
	bzero(descr_name, descr_name_len + 1);

	snprintf(descr_name, descr_name_len, "%s_descr%d",
			 coninfo->conname, MyBackendId);

	return descr_name;
}


/*
 * Generate a unique cursor identifier
 */
static char *ifxGenCursorName(IfxConnectionInfo *coninfo)
{
	char *cursor_name;
	size_t len;

	len = strlen(coninfo->conname) + 10;
	cursor_name = (char *) palloc(len + 1);
	bzero(cursor_name, len + 1);

	snprintf(cursor_name, len, "%s_cur%d",
			 coninfo->conname, MyBackendId);
	return cursor_name;
}

static void
ifxBeginForeignScan(ForeignScanState *node, int eflags)
{
	IfxConnectionInfo    *coninfo;
	IfxFdwExecutionState *festate;
	IfxCachedConnection  *cached;
	Oid                   foreignTableOid;
	bool                  conn_cached;
	size_t                row_size;

	foreignTableOid = RelationGetRelid(node->ss.ss_currentRelation);
	Assert((foreignTableOid != InvalidOid));
	coninfo = ifxMakeConnectionInfo(foreignTableOid);

	/*
	 * XXX: ifxPlanForeignScan() already should have added a cached
	 * connection entry for the requested table. If we don't
	 * find any entry in the connection cache, we treat this as an error
	 * for now. Maybe I need to revert this, but for the initial
	 * coding it seems the best option.
	 */
	cached = ifxConnCache_add(foreignTableOid, coninfo->conname,
							  &conn_cached);

	Assert(conn_cached);

	festate = makeIfxFdwExecutionState();

	if (coninfo->query)
		festate->stmt_info.query = coninfo->query;
	else
	{
		size_t len = strlen(coninfo->tablename) + 31;

		festate->stmt_info.query = (char *) palloc(len);
		snprintf(festate->stmt_info.query, len,
				 "SELECT * FROM \"%s\" FOR READ ONLY",
				 coninfo->tablename);
	}

	/*
	 * Get the definition of the local foreign table attributes.
	 */
	ifxPgColumnData(foreignTableOid, festate);

	/*
	 * Save the connection identifier.
	 */
	StrNCpy(festate->stmt_info.conname, coninfo->conname, IFX_CONNAME_LEN);

	/*
	 * Generate a statement identifier. Required to uniquely
	 * identify the prepared statement within Informix.
	 */
	festate->stmt_info.stmt_name = ifxGenStatementName(coninfo);

	/*
	 * An identifier for the dynamically allocated
	 * DESCRITPOR area.
	 */
	festate->stmt_info.descr_name = ifxGenDescrName(coninfo);

	/*
	 * ...and finally the cursor name.
	 */
	festate->stmt_info.cursor_name = ifxGenCursorName(coninfo);

	/* Prepare the query. */
	elog(DEBUG1, "prepare query \"%s\"", festate->stmt_info.query);
	ifxPrepareQuery(&festate->stmt_info);
	ifxCatchExceptions(&festate->stmt_info, IFX_STACK_PREPARE);

	/*
	 * Declare the cursor for the prepared
	 * statement.
	 */
	elog(DEBUG1, "declare cursor \"%s\"", festate->stmt_info.cursor_name);
	ifxDeclareCursorForPrepared(&festate->stmt_info);
	ifxCatchExceptions(&festate->stmt_info, IFX_STACK_DECLARE);

	/*
	 * Create a descriptor handle for the prepared
	 * query, so we can obtain information about returned
	 * columns.
	 */
	/* elog(DEBUG1, "allocate descriptor area \"%s\"", festate->stmt_info.descr_name); */
	/* ifxAllocateDescriptor(festate->stmt_info.descr_name, 2); */
	/* ifxCatchExceptions(&festate->stmt_info, IFX_STACK_ALLOCATE); */

	/*
	 * Populate the DESCRIPTOR area.
	 */
	elog(DEBUG1, "populate descriptor area for statement \"%s\"",
		 festate->stmt_info.stmt_name);
	ifxDescribeAllocatorByName(&festate->stmt_info);
	ifxCatchExceptions(&festate->stmt_info, IFX_STACK_ALLOCATE | IFX_STACK_DESCRIBE);

	/*
	 * Get the number of columns.
	 */
	festate->stmt_info.ifxAttrCount = ifxDescriptorColumnCount(&festate->stmt_info);
	elog(DEBUG1, "get descriptor column count %d",
		 festate->stmt_info.ifxAttrCount);
	ifxCatchExceptions(&festate->stmt_info, 0);
	festate->stmt_info.ifxAttrDefs = palloc(festate->stmt_info.ifxAttrCount
											* sizeof(IfxAttrDef));

	/*
	 * Populate result set column info array.
	 */
	if ((row_size =ifxGetColumnAttributes(&festate->stmt_info)) <= 0)
	{
		/* whoops, no memory to allocate? Something surely went wrong,
		 * so abort */
		ifxRewindCallstack(&festate->stmt_info);
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("could not initialize informix column properties")));
	}

	/*
	 * NOTE:
	 *
	 * ifxGetColumnAttributes obtained all information about the
	 * returned column and stored them within the informix SQLDA and
	 * sqlvar structs. However, we don't want to allocate memory underneath
	 * our current memory context, thus we allocate the required memory structure
	 * on top here.
	 */
	festate->stmt_info.data = (char *) palloc(festate->stmt_info.row_size);
	festate->stmt_info.indicator = (short *) palloc(sizeof(short)
													* festate->stmt_info.ifxAttrCount);

	/*
	 * Assign sqlvar pointers to the allocated memory area.
	 */
	ifxSetupDataBufferAligned(&festate->stmt_info);

	/*
	 * And finally: open the cursor
	 */
	elog(DEBUG1, "open cursor \"%s\"",
		 festate->stmt_info.cursor_name);
	ifxOpenCursorForPrepared(&festate->stmt_info);
	ifxCatchExceptions(&festate->stmt_info, IFX_STACK_OPEN);

	node->fdw_state = (void *) festate;
}

static void ifxColumnValueByAttNum(IfxFdwExecutionState *state, int attnum)
{
	Assert(state != NULL);
	Assert(attnum >= 0);

	/*
	 * Retrieve values from Informix and try to convert
	 * into a appropiate PostgreSQL datum.
	 */

	switch (state->stmt_info.ifxAttrDefs[attnum].type)
	{
		case IFX_INTEGER:
		case IFX_SERIAL:
		{
			state->values[attnum].val = Int32GetDatum(ifxGetInt(&(state->stmt_info),
																attnum));
			state->values[attnum].def = &(state->stmt_info.ifxAttrDefs[attnum]);
			break;
		}
		case IFX_CHARACTER:
		case IFX_VCHAR:
		case IFX_NCHAR:
		case IFX_NVCHAR:
			/* TO DO */
			break;
		case IFX_TEXT:
			/* TO DO */
			break;
		case IFX_BYTES:
			/* TO DO */
			break;
		default:
		{
			ifxRewindCallstack(&state->stmt_info);
			elog(ERROR, "\"%d\" is not a known informix type id",
				state->stmt_info.ifxAttrDefs[attnum].type);
			break;
		}
	}
}

static void ifxEndForeignScan(ForeignScanState *node)
{
	IfxFdwExecutionState *state;

	state = (IfxFdwExecutionState *) node->fdw_state;

	/*
	 * Dispose SQLDA resource.
	 */

	/*
	 * Dispose any resources.
	 */
	ifxRewindCallstack(&state->stmt_info);
}

static TupleTableSlot *ifxIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot       *tupleSlot = node->ss.ss_ScanTupleSlot;
	IfxFdwExecutionState *state;
	IfxSqlStateClass      errclass;
	int i;

	state = (IfxFdwExecutionState *) node->fdw_state;

	tupleSlot->tts_mintuple   = NULL;
	tupleSlot->tts_buffer     = InvalidBuffer;
	tupleSlot->tts_tuple      = NULL;
	tupleSlot->tts_shouldFree = false;

	/*
	 * Fetch tuple from cursor
	 */
	ifxFetchRowFromCursor(&state->stmt_info);

	/*
	 * Catch any informix exception. We also need to
	 * check for IFX_NOT_FOUND, in which case no more rows
	 * must be processed.
	 */
	errclass = ifxSetException(&(state->stmt_info));

	if (errclass != IFX_SUCCESS)
	{

		if (errclass == IFX_NOT_FOUND)
		{
			/*
			 * Create an empty tuple slot and we're done.
			 */
			elog(DEBUG2, "informix fdw scan end");

			tupleSlot->tts_isempty = true;
			tupleSlot->tts_nvalid  = 0;
			/* XXX: not required here ifxRewindCallstack(&(state->stmt_info)); */
			return tupleSlot;
		}

		/*
		 * All other error/warning cases should be catched.
		 */
		ifxCatchExceptions(&(state->stmt_info), 0);
	}

	/*
	 * Allocate slots for column value data.
	 */
	state->values = palloc(sizeof(IfxValue)
						   * state->stmt_info.ifxAttrCount);

	tupleSlot->tts_isempty = false;
	tupleSlot->tts_nvalid = state->stmt_info.ifxAttrCount;
	tupleSlot->tts_values = (Datum *) palloc(sizeof(Datum)
											 * tupleSlot->tts_nvalid);
	tupleSlot->tts_isnull = (bool *) palloc(sizeof(bool)
											* tupleSlot->tts_nvalid);

	/*
	 * The cursor should now be positioned at the current row
	 * we want to retrieve. Loop through the columns and retrieve
	 * their values. Note: No conversion into a PostgreSQL specific
	 * datatype is done so far.
	 */
	for (i = 0; i <= state->stmt_info.ifxAttrCount - 1; i++)
	{
		elog(DEBUG1, "get column %d", i);

		/*
		 * Retrieve a converted datum from the current
		 * column and store it within state context.
		 */
		ifxColumnValueByAttNum(state, i);

		/*
		 * It might happen that the FDW table has dropped
		 * columns...check for them and insert a NULL value instead..
		 */
		if (state->pgAttrDefs[i].attnum < 0)
		{
			tupleSlot->tts_isnull[i] = true;
			tupleSlot->tts_values[i] = PointerGetDatum(NULL);
			continue;
		}

		/*
		 * Same for retrieved NULL values...
		 */
		if (state->stmt_info.ifxAttrDefs[i].indicator == INDICATOR_NULL)
		{
			tupleSlot->tts_isnull[i] = true;
			tupleSlot->tts_values[i] = PointerGetDatum(NULL);
			continue;
		}

		/*
		 * ifxColumnValueByAttnum() has already converted the current
		 * column value into a datum. We just need to assign it to the
		 * tupleSlot and we're done.
		 */
		tupleSlot->tts_isnull[i] = false;
		tupleSlot->tts_values[i] = state->values[i].val;
	}

	return tupleSlot;
}

/*
 * Returns a new allocated pointer
 * to IfxConnectionInfo.
 */
static IfxConnectionInfo *ifxMakeConnectionInfo(Oid foreignTableOid)
{
	IfxConnectionInfo *coninfo;
	StringInfoData    *buf;
	StringInfoData    *dsn;

	/*
	 * Initialize connection handle, set
	 * defaults.
	 */
	coninfo = (IfxConnectionInfo *) palloc(sizeof(IfxConnectionInfo));
	bzero(coninfo->conname, IFX_CONNAME_LEN + 1);
	ifxConnInfoSetDefaults(coninfo);
	ifxGetOptions(foreignTableOid, coninfo);

	buf = ifxGenerateConnName(coninfo);
	StrNCpy(coninfo->conname, buf->data, IFX_CONNAME_LEN);

	dsn = ifxGetDatabaseString(coninfo);
	coninfo->dsn = pstrdup(dsn->data);

	return coninfo;
}

static bytea *
ifxFdwPlanDataAsBytea(IfxConnectionInfo *coninfo)
{
	bytea *data;
	int    len = 0;

	data = (bytea *) palloc(len + VARHDRSZ);
	memcpy(VARDATA(data), &(coninfo->planData), sizeof(IfxPlanData));
	return data;
}

static FdwPlan *
ifxPlanForeignScan(Oid foreignTableOid, PlannerInfo *planInfo, RelOptInfo *baserel)
{

	IfxConnectionInfo *coninfo;
	StringInfoData    *buf;
	bool               conn_cached;
	FdwPlan           *plan;
	bytea             *plan_data;
	IfxSqlStateClass   err;

	/*
	 * Prepare a generic plan structure
	 */
	plan = makeNode(FdwPlan);

	/*
	 * If not already done, initialize cache data structures.
	 */
	InformixCacheInit();

	/*
	 * Initialize connection structures and retrieve FDW options
	 */

	coninfo = ifxMakeConnectionInfo(foreignTableOid);
	elog(DEBUG1, "informix connection dsn \"%s\"", coninfo->dsn);

	/*
	 * Lookup the connection name in the connection cache.
	 */
	ifxConnCache_add(foreignTableOid, coninfo->conname, &conn_cached);

	/*
	 * Establish a new INFORMIX connection with transactions,
	 * in case a new one needs to be created. Otherwise make
	 * the requested connection current.
	 */
	if (!conn_cached)
	{
		ifxCreateConnectionXact(coninfo);

		/*
		 * A new connection probably has less cache affinity on the
		 * server than a cached one. So if this is a fresh connection,
		 * reflect it in the startup cost.
		 */
		plan->startup_cost = 500;
	}
	else
	{
		/*
		 * Make the requested connection current.
		 */
		ifxSetConnection(coninfo);

		plan->startup_cost = 100;
	}

	/*
	 * Check connection status. This should happen directly
	 * after connection establishing, otherwise we might get confused by
	 * other ESQL API calls in the meantime.
	 */
	if ((err = ifxConnectionStatus()) != IFX_CONNECTION_OK)
	{
		if (err == IFX_CONNECTION_WARN)
		{
			IfxSqlStateMessage message;
			ifxGetSqlStateMessage(1, &message);

			ereport(WARNING, (errcode(ERRCODE_FDW_ERROR),
							  errmsg("opened informix connection with warnings"),
							  errdetail("informix warning: \"%s\"",
										message.text)));
		}

		if (err == IFX_CONNECTION_ERROR)
			elog(ERROR, "could not open connection to informix server: SQLCODE=%d",
				 ifxGetSqlCode());
	}

	return plan;
}

/*
 * ifxExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
ifxExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	IfxConnectionInfo coninfo;
	IfxFdwExecutionState *festate;

	festate = (IfxFdwExecutionState *) node->fdw_state;

	/*
	 * XXX: We need to get the info from the cached connection!
	 */

	/* Fetch options  */
	ifxGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				  &coninfo);

	/* Give some possibly useful info about startup costs */
	if (es->costs)
	{
		ExplainPropertyFloat("Remote server startup cost",
							 coninfo.planData.connection_costs, 4, es);
		ExplainPropertyFloat("Remote table row estimate",
							 coninfo.planData.estimated_rows, 4, es);
		ExplainPropertyText("Informix query", festate->stmt_info.query, es);
	}
}


static void ifxConnInfoSetDefaults(IfxConnectionInfo *coninfo)
{
	Assert(coninfo != NULL);

	if (coninfo == NULL)
		return;

	coninfo->planData.estimated_rows = 100.0;
	coninfo->planData.connection_costs = 100.0;
}

static StringInfoData *
ifxFdwOptionsToStringBuf(Oid context)
{
	StringInfoData      *buf;
	struct IfxFdwOption *ifxopt;

	buf = makeStringInfo();
	initStringInfo(buf);

	for (ifxopt = ifx_valid_options; ifxopt->optname; ifxopt++)
	{
		if (context == ifxopt->optcontext)
		{
			appendStringInfo(buf, "%s%s", (buf->len > 0) ? "," : "",
							 ifxopt->optname);
		}
	}

	return buf;
}

/*
 * Check if specified option is actually known
 * to the Informix FDW.
 */
static bool
ifxIsValidOption(const char *option, Oid context)
{
	struct IfxFdwOption *ifxopt;

	for (ifxopt = ifx_valid_options; ifxopt->optname; ifxopt++)
	{
		if (context == ifxopt->optcontext
			&& strcmp(ifxopt->optname, ifxopt->optname) == 0)
		{
			return true;
		}
	}
	/*
	 * Only reached in case of mismatch
	 */
	return false;
}

Datum
ifxGetConnections(PG_FUNCTION_ARGS)
{
	FuncCallContext *fcontext;
	TupleDesc        tupdesc;
	AttInMetadata   *attinmeta;
	int              conn_processed;
	int              conn_expected;
	struct ifx_sp_call_data *call_data;

	attinmeta = NULL;

	/* First call */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		fcontext = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(fcontext->multi_call_memory_ctx);

        /*
		 * Are we called in a context which accepts a record?
		 */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		/*
		 * Check wether informix connection cache is already
		 * initialized. If not, no active connections are present,
		 * thus we don't have to do anything.
		 */
		if (!IfxCacheIsInitialized)
		{
			fcontext->max_calls = 0;
			fcontext->user_fctx = NULL;
		}
		else
		{
			fcontext->max_calls = hash_get_num_entries(ifxCache.connections);
			elog(DEBUG1, "found %d entries in informix connection cache",
				 fcontext->max_calls);

			/*
			 * Retain the status of the hash search and other info.
			 */
			call_data = (struct ifx_sp_call_data *) palloc(sizeof(struct ifx_sp_call_data));
			call_data->hash_status = (HASH_SEQ_STATUS *) palloc(sizeof(HASH_SEQ_STATUS));
			call_data->tupdesc     = tupdesc;

			/*
			 * It is already guaranteed that the connection cache
			 * is alive. Prepare for sequential read of all active connections.
			 */
			hash_seq_init(call_data->hash_status, ifxCache.connections);
			fcontext->user_fctx = call_data;
		}

		/*
		 * Prepare attribute metadata.
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		fcontext->attinmeta = attinmeta;
		MemoryContextSwitchTo(oldcontext);
	}

	fcontext = SRF_PERCALL_SETUP();
	conn_processed = fcontext->call_cntr;
	conn_expected  = fcontext->max_calls;

	if (conn_processed < conn_expected)
	{
		IfxCachedConnection *conn_cached;
		Datum                values[2];
		bool                 nulls[2];
		HeapTuple            tuple;
		Datum                result;

		call_data = (struct ifx_sp_call_data *) fcontext->user_fctx;
		Assert(call_data != NULL);
		conn_cached = (IfxCachedConnection *) hash_seq_search(call_data->hash_status);

		/*
		 * Values array. This will hold the values to be returned
		 * as C strings.
		 */
		elog(DEBUG1, "connection name %s", conn_cached->ifx_connection_name);
		values[0] = PointerGetDatum(cstring_to_text(conn_cached->ifx_connection_name));
		values[1] = Int32GetDatum(conn_cached->establishedByOid);

		nulls[0] = false;
		nulls[1] = false;

		/*
		 * Build the result tuple.
		 */
		tuple = heap_form_tuple(call_data->tupdesc, values, nulls);

		/*
		 * Transform the result tuple into a valid datum.
		 */
		result = HeapTupleGetDatum(tuple);

		/*
		 * Finalize...
		 */
		SRF_RETURN_NEXT(fcontext, result);
	}
	else
	{
		call_data = (struct ifx_sp_call_data *) fcontext->user_fctx;
		/*
		 * Done processing. Terminate hash_seq_search(), since we haven't
		 * processed forward until NULL (but only if we had processed
		 * any connections).
		 */
		if (fcontext->max_calls > 0)
			hash_seq_term(call_data->hash_status);

		SRF_RETURN_DONE(fcontext);
	}
}
