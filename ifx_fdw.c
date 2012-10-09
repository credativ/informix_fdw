/*-------------------------------------------------------------------------
 *
 * ifx_fdw.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_fdw.c
 *
 *-------------------------------------------------------------------------
 */


#include "ifx_fdw.h"
#include "ifx_node_utils.h"
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
	{ "gl_datetime",      ForeignTableRelationId },
	{ "gl_date",          ForeignTableRelationId },
	{ "client_locale",    ForeignTableRelationId },
	{ "db_locale",        ForeignTableRelationId },
	{ "disable_predicate_pushdown", ForeignTableRelationId },
	{ NULL,                         ForeignTableRelationId }
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
 * FDW internal macros
 */

#if PG_VERSION_NUM < 90200
#define PG_SCANSTATE_PRIVATE_P(a) \
(List *) (FdwPlan *)(((ForeignScan *)(a)->ss.ps.plan)->fdwplan)->fdw_private
#else
#define PG_SCANSTATE_PRIVATE_P(a) \
(List *) ((ForeignScan *)(a)->ss.ps.plan)->fdw_private
#endif

/*******************************************************************************
 * FDW helper functions.
 */
static void ifxSetupFdwScan(IfxConnectionInfo    **coninfo,
							IfxFdwExecutionState **state,
							List    **plan_values,
							Oid       foreignTableOid);

static IfxFdwExecutionState *makeIfxFdwExecutionState();

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

static IfxSqlStateClass
ifxCatchExceptions(IfxStatementInfo *state, unsigned short stackentry);

static inline void ifxPopCallstack(IfxStatementInfo *info,
								   unsigned short stackentry);
static inline void ifxPushCallstack(IfxStatementInfo *info,
									unsigned short stackentry);

static void ifxColumnValueByAttNum(IfxFdwExecutionState *state, int attnum,
								   bool *isnull);


static void ifxPrepareCursorForScan(IfxStatementInfo *info,
									IfxConnectionInfo *coninfo);

static char *ifxFilterQuals(PlannerInfo *planInfo,
							RelOptInfo *baserel,
							Oid foreignTableOid);

static void ifxPrepareParamsForScan(IfxFdwExecutionState *state,
									IfxConnectionInfo *coninfo);

/*******************************************************************************
 * FDW callback routines.
 */

#if PG_VERSION_NUM >= 90200

static void ifxGetForeignRelSize(PlannerInfo *root,
								 RelOptInfo *baserel,
								 Oid foreignTableId);
static void ifxGetForeignPaths(PlannerInfo *root,
							   RelOptInfo *baserel,
							   Oid foreignTableId);
static ForeignScan *ifxGetForeignPlan(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreignTableId,
									  ForeignPath *best_path,
									  List *tlist,
									  List *scan_clauses);

#else

static FdwPlan *ifxPlanForeignScan(Oid foreignTableOid,
								   PlannerInfo *planInfo,
								   RelOptInfo *baserel);

#endif

static void
ifxExplainForeignScan(ForeignScanState *node, ExplainState *es);

static void
ifxBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *ifxIterateForeignScan(ForeignScanState *node);

static void ifxReScanForeignScan(ForeignScanState *state);

static void ifxEndForeignScan(ForeignScanState *node);

static void ifxPrepareScan(IfxConnectionInfo *coninfo,
						   IfxFdwExecutionState *state);

/*******************************************************************************
 * SQL status and helper functions.
 */

Datum
ifxGetConnections(PG_FUNCTION_ARGS);

/*******************************************************************************
 * Implementation starts here
 */

/*
 * Entry point for scan preparation. Does all the leg work
 * for preparing the query and cursor definitions before
 * entering the executor.
 */
static void ifxPrepareScan(IfxConnectionInfo *coninfo,
						   IfxFdwExecutionState *state)
{
	/*
	 * Prepare parameters of the state structure
	 * for scan later.
	 */
	ifxPrepareParamsForScan(state, coninfo);

	/* Finally do the cursor preparation */
	ifxPrepareCursorForScan(&state->stmt_info, coninfo);
}

static void ifxSetupFdwScan(IfxConnectionInfo **coninfo,
							IfxFdwExecutionState **state,
							List    **plan_values,
							Oid       foreignTableOid)
{
	bool                  conn_cached;
	IfxSqlStateClass      err;

	/*
	 * If not already done, initialize cache data structures.
	 */
	InformixCacheInit();

	/*
	 * Save parameters for later use
	 * in executor.
	 */
	*plan_values = NIL;

	/*
	 * Initialize connection structures and retrieve FDW options
	 */

	*coninfo = ifxMakeConnectionInfo(foreignTableOid);
	elog(DEBUG1, "informix connection dsn \"%s\"", (*coninfo)->dsn);

	/*
	 * Make a generic informix execution state
	 * structure.
	 */
	*state = makeIfxFdwExecutionState();

	/*
	 * Lookup the connection name in the connection cache.
	 */
	ifxConnCache_add(foreignTableOid, *coninfo, &conn_cached);

	/*
	 * Establish a new INFORMIX connection with transactions,
	 * in case a new one needs to be created. Otherwise make
	 * the requested connection current.
	 */
	if (!conn_cached)
	{
		ifxCreateConnectionXact(*coninfo);
		elog(DEBUG2, "created new cached informix connection \"%s\"",
			 (*coninfo)->conname);
	}
	else
	{
		/*
		 * Make the requested connection current.
		 */
		ifxSetConnection(*coninfo);
		elog(DEBUG2, "reusing cached informix connection \"%s\"",
			 (*coninfo)->conname);
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

			ereport(WARNING, (errcode(WARNING),
							  errmsg("opened informix connection with warnings"),
							  errdetail("informix SQLSTATE %s: \"%s\"",
										message.sqlstate, message.text)));
		}

		if (err == IFX_CONNECTION_ERROR)
		{
			/*
			 * If we are here, something went wrong with connection
			 * establishing. Remove the already cached entry and force
			 * the connection to re-established again later.
			 */
			ifxConnCache_rm((*coninfo)->conname, &conn_cached);

			/* finally, error out */
			elog(ERROR, "could not open connection to informix server: SQLCODE=%d",
				 ifxGetSqlCode());
		}
	}

	/*
	 * Check if this database was opened using
	 * transactions.
	 */
	if (ifxGetSQLCAWarn(SQLCA_WARN_TRANSACTIONS) == 'W')
	{
		elog(DEBUG1, "informix database connection using transactions");
		(*coninfo)->tx_enabled = 1;
	}
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
	state->stmt_info.predicate    = NULL;
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

#if PG_VERSION_NUM >= 90200

/*
 * Get the foreign informix relation estimates. This function
 * is also responsible to setup the informix database connection
 * and create a corresponding cached connection, if not already
 * done.
 */
static void ifxGetForeignRelSize(PlannerInfo *planInfo,
								 RelOptInfo *baserel,
								 Oid foreignTableId)
{
	IfxConnectionInfo    *coninfo;
	List                 *plan_values;
	IfxFdwExecutionState *state;
	IfxFdwPlanState      *planState;

	elog(DEBUG3, "informix_fdw: get foreign relation size");

	planState = palloc(sizeof(IfxFdwPlanState));

	/*
	 * Establish remote informix connection or get
	 * a already cached connection from the informix connection
	 * cache.
	 */
	ifxSetupFdwScan(&coninfo, &state, &plan_values, foreignTableId);

	/*
	 * Check for predicates that can be pushed down
	 * to the informix server, but skip it in case the user
	 * has set the disable_predicate_pushdown option...
	 */
	if (coninfo->predicate_pushdown)
	{
		state->stmt_info.predicate = ifxFilterQuals(planInfo, baserel,
													foreignTableId);
		elog(DEBUG2, "predicate for pushdown: %s", state->stmt_info.predicate);
	}
	else
	{
		elog(DEBUG2, "predicate pushdown disabled");
		state->stmt_info.predicate = "";
	}

	/*
	 * Establish the remote query on the informix server. To do this,
	 * we create the cursor, which will allow us to get the cost estimates
	 * informix calculates for the query execution. We _don't_ open the
	 * cursor yet, this is left to the executor later.
	 */
	ifxPrepareScan(coninfo, state);

	/*
	 * Now it should be possible to get the cost estimates
	 * from the actual cursor.
	 */
	coninfo->planData.estimated_rows = (double) ifxGetSQLCAErrd(SQLCA_NROWS_PROCESSED);
	coninfo->planData.costs          = (double) ifxGetSQLCAErrd(SQLCA_NROWS_WEIGHT);

	/* should be calculated nrows from foreign table */
	baserel->rows        = coninfo->planData.estimated_rows;
	planState->coninfo   = coninfo;
	planState->state     = state;
	baserel->fdw_private = (void *) planState;
}

/*
 * Create possible access paths for the foreign data
 * scan. Consider any pushdown predicate and create
 * an appropiate path for it.
 */
static void ifxGetForeignPaths(PlannerInfo *root,
							   RelOptInfo *baserel,
							   Oid foreignTableId)
{
	IfxFdwPlanState *planState;

	elog(DEBUG3, "informix_fdw: get foreign paths");

	planState = (IfxFdwPlanState *) baserel->fdw_private;

	/*
	 * Create a generic foreign path for now. We need to consider any
	 * restriction quals later, to get a smarter path generation here.
	 *
	 * For example, it is quite interesting to consider any index scans
	 * or sorted output on the remote side and reflect it in the
	 * choosen paths (helps nested loops et al.).
	 */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 planState->coninfo->planData.costs,
									 planState->coninfo->planData.costs,
									 NIL,
									 NULL,
									 NIL));
}

static ForeignScan *ifxGetForeignPlan(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreignTableId,
									  ForeignPath *best_path,
									  List *tlist,
									  List *scan_clauses)
{
	Index scan_relid;
	IfxFdwPlanState  *planState;
	List             *plan_values;

	elog(DEBUG3, "informix_fdw: get foreign plan");

	scan_relid = baserel->relid;
	planState = (IfxFdwPlanState *) baserel->fdw_private;

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/*
	 * Serialize current plan data into a format suitable
	 * for copyObject() later. This is required to be able to
	 * push down the collected information here down to the
	 * executor.
	 */
	plan_values = ifxSerializePlanData(planState->coninfo,
									   planState->state,
									   root);

	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,
							plan_values);
}

#else

/*
 * ifxPlanForeignScan
 *
 * Plans a foreign scan on an remote informix relation.
 */
static FdwPlan *
ifxPlanForeignScan(Oid foreignTableOid, PlannerInfo *planInfo, RelOptInfo *baserel)
{
	IfxConnectionInfo    *coninfo;
	FdwPlan              *plan;
	List                 *plan_values;
	IfxFdwExecutionState *state;

	elog(DEBUG3, "informix_fdw: plan scan");

	/*
	 * Prepare a generic plan structure
	 */
	plan = makeNode(FdwPlan);

	/*
	 * Establish remote informix connection or get
	 * a already cached connection from the informix connection
	 * cache.
	 */
	ifxSetupFdwScan(&coninfo, &state, &plan_values, foreignTableOid);

	/*
	 * Check for predicates that can be pushed down
	 * to the informix server, but skip it in case the user
	 * has set the disable_predicate_pushdown option...
	 */
	if (coninfo->predicate_pushdown)
	{
		state->stmt_info.predicate = ifxFilterQuals(planInfo, baserel,
													foreignTableOid);
		elog(DEBUG2, "predicate for pushdown: %s", state->stmt_info.predicate);
	}
	else
	{
		elog(DEBUG2, "predicate pushdown disabled");
		state->stmt_info.predicate = "";
	}

	/*
	 * Prepare parameters of the state structure
	 * and cursor definition.
	 */
	ifxPrepareScan(coninfo, state);

	/*
	 * After declaring the cursor we are able to retrieve
	 * row and cost estimates via SQLCA fields. Do that and save
	 * them into the IfxPlanData structure member of
	 * IfxConnectionInfo and, more important, assign the
	 * values to our plan node.
	 */
	coninfo->planData.estimated_rows = (double) ifxGetSQLCAErrd(SQLCA_NROWS_PROCESSED);
	coninfo->planData.costs          = (double) ifxGetSQLCAErrd(SQLCA_NROWS_WEIGHT);
	baserel->rows = coninfo->planData.estimated_rows;
	plan->startup_cost = 0.0;
	plan->total_cost = coninfo->planData.costs + plan->startup_cost;

	/*
	 * Save parameters to our plan. We need to make sure they
	 * are copyable by copyObject(), so use a list with
	 * bytea const nodes.
	 *
	 * NOTE: we *must* not allocate serialized nodes within
	 *       the current memory context, because this will crash
	 *       on prepared statements and subsequent EXECUTE calls
	 *       since they will be freed after. Instead, use the
	 *       planner context, which will remain as long as
	 *       the plan exists.
	 *
	 */
	plan_values = ifxSerializePlanData(coninfo, state, planInfo);
	plan->fdw_private = plan_values;

	return plan;
}

#endif

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
void ifxRewindCallstack(IfxStatementInfo *info)
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
		/*
		 * Deallocating allocated memory by sqlda data structure
		 * is going to be little tricky here: sqlda is allocated
		 * by the Informix ESQL/C API, so we don't have any influence
		 * via memory contexts...we aren't allowed to use pfree()!
		 *
		 * The memory area for SQL data values retrieved by any
		 * FETCH from the underlying cursor is allocated by palloc(),
		 * however. We don't free them immediately and leave this up
		 * to memory context cleanup.
		 */
		ifxDeallocateSQLDA(info);
		elog(DEBUG2, "informix_fdw: undo allocate");
		ifxPopCallstack(info, IFX_STACK_ALLOCATE);
	}

	if ((info->call_stack & IFX_STACK_DECLARE) == IFX_STACK_DECLARE)
	{
		ifxFreeResource(info, IFX_STACK_DECLARE);
		elog(DEBUG2, "informix_fdw: undo declare");
		ifxPopCallstack(info, IFX_STACK_DECLARE);
	}

	if ((info->call_stack & IFX_STACK_PREPARE) == IFX_STACK_PREPARE)
	{
		ifxFreeResource(info, IFX_STACK_PREPARE);
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
				/*
				 * log FATAL
				 *
				 * There's no ERRCODE_FDW_FATAL, so we go with a HV000 error
				 * code for now, but print out the error message as FATAL.
				 */
				ifxRewindCallstack(state);
				ereport(FATAL, (errcode(ERRCODE_FDW_ERROR),
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

	return errclass;
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

	if (strcmp(def->defname, "gl_date") == 0)
	{
		if (coninfo->gl_date)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: gl_date(%s)",
								   defGetString(def))));

		coninfo->gl_date = defGetString(def);
	}

	if (strcmp(def->defname, "db_locale") == 0)
	{
		if (coninfo->db_locale)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: db_locale(%s)",
								   defGetString(def))));

		coninfo->db_locale = defGetString(def);
	}


	if (strcmp(def->defname, "gl_datetime") == 0)
	{
		if (coninfo->gl_datetime)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: gl_datetime(%s)",
								   defGetString(def))));

		coninfo->gl_datetime = defGetString(def);
	}

	if (strcmp(def->defname, "client_locale") == 0)
	{
		if (coninfo->client_locale)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: gl_date(%s)",
								   defGetString(def))));

		coninfo->client_locale = defGetString(def);
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

	fdwRoutine->ExplainForeignScan = ifxExplainForeignScan;
	fdwRoutine->BeginForeignScan   = ifxBeginForeignScan;
	fdwRoutine->IterateForeignScan = ifxIterateForeignScan;
	fdwRoutine->EndForeignScan     = ifxEndForeignScan;
	fdwRoutine->ReScanForeignScan  = ifxReScanForeignScan;

	#if PG_VERSION_NUM < 90200

	fdwRoutine->PlanForeignScan    = ifxPlanForeignScan;

	#else

	fdwRoutine->GetForeignRelSize = ifxGetForeignRelSize;
	fdwRoutine->GetForeignPaths   = ifxGetForeignPaths;
	fdwRoutine->GetForeignPlan    = ifxGetForeignPlan;

	#endif


	PG_RETURN_POINTER(fdwRoutine);
}

/*
 * ifxReScanForeignScan
 *
 *   Restart the scan with new parameters.
 */
static void ifxReScanForeignScan(ForeignScanState *state)
{
	elog(DEBUG1, "informix_fdw: rescan");
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

		/*
		 * Duplicates present in current options list?
		 */
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
	bool           mandatory[4] = { false, false, false, false };
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

		elog(DEBUG3, "ifx_fdw set param %s=%s",
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
			mandatory[3] = true;
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

		if (strcmp(def->defname, "gl_date") == 0)
		{
			coninfo->gl_date = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "gl_datetime") == 0)
		{
			coninfo->gl_datetime = pstrdup(defGetString(def));
		}

		if (strcmp(def->defname, "client_locale") == 0)
		{
			coninfo->client_locale = pstrdup(defGetString(def));
			mandatory[2] = true;
		}

		if (strcmp(def->defname, "db_locale") == 0)
		{
			coninfo->db_locale = pstrdup(defGetString(def));
			mandatory[2] = true;
		}

		if (strcmp(def->defname, "disable_predicate_pushdown") == 0)
		{
			/* we don't bother about the value passed to
			 * this argument, treat its existence to disable
			 * predicate pushdown.
			 */
			coninfo->predicate_pushdown = 0;
		}
	}

	/*
	 * Check for mandatory options
	 */
	for (i = 0; i < IFX_REQUIRED_CONN_KEYWORDS; i++)
	{
		if (!mandatory[i])
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
							errmsg("missing required FDW options (informixserver, informixdir, client_locale, database)")));
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

/*
 * Prepare informix query object identifier
 */
static void ifxPrepareParamsForScan(IfxFdwExecutionState *state,
									IfxConnectionInfo *coninfo)
{
	StringInfoData *buf;

	buf = makeStringInfo();
	initStringInfo(buf);

	/*
	 * Record the given query and pass it over
	 * to the state structure.
	 */
	if (coninfo->query)
	{
		if ((state->stmt_info.predicate != NULL)
			&& (strlen(state->stmt_info.predicate) > 0)
			&& coninfo->predicate_pushdown)
		{
			appendStringInfo(buf, "%s WHERE %s",
							 coninfo->query,
							 state->stmt_info.predicate);
		}
		else
		{
			appendStringInfo(buf, "%s",
							 coninfo->query);
		}
	}
	else
	{
		if ((state->stmt_info.predicate != NULL)
			&& (strlen(state->stmt_info.predicate) > 0)
			&& coninfo->predicate_pushdown)
		{
			appendStringInfo(buf, "SELECT * FROM %s WHERE %s FOR READ ONLY",
							 coninfo->tablename,
							 state->stmt_info.predicate);
		}
		else
		{
			appendStringInfo(buf, "SELECT * FROM %s FOR READ ONLY",
							 coninfo->tablename);
		}
	}

	state->stmt_info.query = buf->data;

	/*
	 * Save the connection identifier.
	 */
	StrNCpy(state->stmt_info.conname, coninfo->conname, IFX_CONNAME_LEN);
}

/*
 * ifxBeginForeignScan
 *
 * Implements FDW BeginForeignScan callback function.
 */
static void
ifxBeginForeignScan(ForeignScanState *node, int eflags)
{
	IfxConnectionInfo    *coninfo;
	IfxFdwExecutionState *festate;
	IfxCachedConnection  *cached;
	Oid                   foreignTableOid;
	bool                  conn_cached;
	List                 *plan_values;

	elog(DEBUG3, "informix_fdw: begin scan");

	plan_values = PG_SCANSTATE_PRIVATE_P(node);
	foreignTableOid = RelationGetRelid(node->ss.ss_currentRelation);
	Assert((foreignTableOid != InvalidOid));
	coninfo= ifxMakeConnectionInfo(foreignTableOid);

	/*
	 * ifxPlanForeignScan() already should have added a cached
	 * connection entry for the requested table.
	 */
	cached = ifxConnCache_add(foreignTableOid, coninfo,
							  &conn_cached);

	Assert(conn_cached && cached != NULL);

	festate = makeIfxFdwExecutionState();

	/*
	 * Record our FDW state structures.
	 */
	node->fdw_state = (void *) festate;

	/*
	 * Cached plan data present?
	 */
	if (PG_SCANSTATE_PRIVATE_P(node) != NULL)
	{
		/*
		 * Retrieved cached parameters formerly prepared
		 * by ifxPlanForeignScan().
		 */
		ifxDeserializeFdwData(festate, plan_values);
	}
	else
	{
		elog(DEBUG1, "informix_fdw no cached plan data");
		ifxPrepareParamsForScan(festate, coninfo);
	}

	/*
	 * Recheck if everything is already prepared on the
	 * informix server. If not, we are either in a rescan condition
	 * or a cached query plan is used. Redo all necessary preparation
	 * previously done in the planning state. We do this to save
	 * some cycles when just doing plain SELECTs.
	 */
	if (festate->stmt_info.call_stack == IFX_STACK_EMPTY)
		ifxPrepareCursorForScan(&festate->stmt_info, coninfo);

	/*
	 * Get the definition of the local foreign table attributes.
	 */
	ifxPgColumnData(foreignTableOid, festate);

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		elog(DEBUG1, "informix_fdw: explain only");
		return;
	}

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
	if ((festate->stmt_info.row_size = ifxGetColumnAttributes(&festate->stmt_info)) == 0)
	{
		/* oops, no memory to allocate? Something surely went wrong,
		 * so abort */
		ifxRewindCallstack(&festate->stmt_info);
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("could not initialize informix column properties")));
	}

	/*
	 * NOTE:
	 *
	 * ifxGetColumnAttributes() obtained all information about the
	 * returned column and stored them within the informix SQLDA and
	 * sqlvar structs. However, we don't want to allocate memory underneath
	 * our current memory context, thus we allocate the required memory structure
	 * on top here. ifxSetupDataBufferAligned() will assign the allocated
	 * memory area to the SQLDA structure and will maintain the data offsets
	 * properly aligned.
	 */
	festate->stmt_info.data = (char *) palloc0(festate->stmt_info.row_size);
	festate->stmt_info.indicator = (short *) palloc0(sizeof(short)
													* festate->stmt_info.ifxAttrCount);

	/*
	 * Assign sqlvar pointers to the allocated memory area.
	 */
	ifxSetupDataBufferAligned(&festate->stmt_info);

	/*
	 * Open the cursor.
	 */
	elog(DEBUG1, "open cursor \"%s\"",
		 festate->stmt_info.cursor_name);
	ifxOpenCursorForPrepared(&festate->stmt_info);
	ifxCatchExceptions(&festate->stmt_info, IFX_STACK_OPEN);

}

static void ifxColumnValueByAttNum(IfxFdwExecutionState *state, int attnum,
								   bool *isnull)
{
	Assert(state != NULL && attnum >= 0);
	Assert(state->stmt_info.data != NULL);
	Assert(state->values != NULL);
	Assert(state->pgAttrDefs);

	/*
	 * Setup...
	 */
	state->values[attnum].def = &state->stmt_info.ifxAttrDefs[attnum];
	IFX_SETVAL_P(state, attnum, PointerGetDatum(NULL));
	*isnull = false;

	/*
	 * Retrieve values from Informix and try to convert
	 * into an appropiate PostgreSQL datum.
	 */

	switch (IFX_ATTRTYPE_P(state, attnum))
	{
		case IFX_SMALLINT:
			/*
			 * All int values are handled
			 * by convertIfxInt()...so fall through.
			 */
		case IFX_INTEGER:
		case IFX_SERIAL:
		case IFX_INT8:
		case IFX_SERIAL8:
		case IFX_INFX_INT8:
		{
			Datum dat;

			dat = convertIfxInt(state, attnum);
			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));

			/*
			 * Check for errors, but only if we
			 * didnt get a validated NULL attribute from
			 * informix.
			 */
			if (! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix type id %d into pg type %u",
					 IFX_ATTRTYPE_P(state, attnum),
					 PG_ATTRTYPE_P(state, attnum));
			}

			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_CHARACTER:
		case IFX_VCHAR:
		case IFX_NCHAR:
		case IFX_LVARCHAR:
		case IFX_NVCHAR:
		{
			/* SQLCHAR, SQLVCHAR, SQLNCHAR, SQLLVARCHAR, SQLNVCHAR */
			Datum dat;

			dat = convertIfxCharacterString(state, attnum);
			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));

			/*
			 * At this point we never expect a NULL datum without
			 * having retrieved NULL from informix. Check it.
			 * If it's a validated NULL value from informix,
			 * don't throw an error.
			 */
			if ((DatumGetPointer(dat) == NULL)
				&& !*isnull)
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix type id %d into pg type %u",
					 IFX_ATTRTYPE_P(state, attnum),
					 PG_ATTRTYPE_P(state, attnum));
			}

			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_BYTES:
		case IFX_TEXT:
		{
			/* TO DO */
			Datum dat;


			dat = convertIfxSimpleLO(state, attnum);

			/*
			 * Check for invalid datum conversion.
			 */
			if (! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				elog(ERROR, "could not convert informix type id %d into pg type %u",
					 IFX_ATTRTYPE_P(state, attnum),
					 PG_ATTRTYPE_P(state, attnum));
			}

			/*
			 * Valid NULL datum?
			 */
			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);

			break;
		}
		case IFX_BOOLEAN:
		{
			/* SQLBOOL value */
			Datum dat;
			dat = convertIfxBoolean(state, attnum);

			/*
			 * Unlike other types, a NULL datum is treated
			 * like a normal FALSE value in case the indicator
			 * value tells that we got a NOT NULL column.
			 */
			if (! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix type id %d into pg type %u",
					 IFX_ATTRTYPE_P(state, attnum),
					 PG_ATTRTYPE_P(state, attnum));
			}

			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);

			break;
		}
		case IFX_DATE:
		{
			/* SQLDATE value */
			Datum dat;
			dat = convertIfxDateString(state, attnum);

			/*
			 * Valid datum?
			 */
			if ((DatumGetPointer(dat) == NULL)
				&& ! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix type id %d into pg type %u",
					 IFX_ATTRTYPE_P(state, attnum),
					 PG_ATTRTYPE_P(state, attnum));
			}

			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_DTIME:
		{
			/* SQLDTIME value */
			Datum dat;
			dat = convertIfxTimestampString(state, attnum);

			/*
			 * Valid datum?
			 */
			if ((DatumGetPointer(dat) == NULL)
				&& ! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix type id %d into pg type %u",
					 IFX_ATTRTYPE_P(state, attnum),
					 PG_ATTRTYPE_P(state, attnum));
			}

			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_DECIMAL:
		{
			/* DECIMAL value */
			Datum dat;
			dat = convertIfxDecimal(state, attnum);

			/*
			 * Valid datum?
			 */
			if ((DatumGetPointer(dat) == NULL)
				&& ! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix decimal into pg type %u",
					 PG_ATTRTYPE_P(state, attnum));
			}

			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
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
	List                 *plan_values;

	elog(DEBUG3, "informix_fdw: end scan");

	state = (IfxFdwExecutionState *) node->fdw_state;
	plan_values = PG_SCANSTATE_PRIVATE_P(node);
	ifxDeserializeFdwData(state, plan_values);

	/*
	 * Dispose SQLDA resource, allocated database objects, ...
	 */
	ifxRewindCallstack(&state->stmt_info);

	/*
	 * Save the callstack into cached plan structure. This
	 * is necessary to teach ifxBeginForeignScan() to do the
	 * right thing(tm)...
	 */
	ifxSetSerializedInt16Field(plan_values,
							   SERIALIZED_CALLSTACK,
							   state->stmt_info.call_stack);
}

static TupleTableSlot *ifxIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot       *tupleSlot = node->ss.ss_ScanTupleSlot;
	IfxFdwExecutionState *state;
	IfxSqlStateClass      errclass;
	int i;

	state = (IfxFdwExecutionState *) node->fdw_state;

	elog(DEBUG3, "informix_fdw: iterate scan");

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
		bool isnull;

		elog(DEBUG3, "get column ifx attnum %d", i);

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
		 * Retrieve a converted datum from the current
		 * column and store it within state context. This also
		 * sets and checks the indicator variable to record any
		 * NULL occurences.
		 */
		ifxColumnValueByAttNum(state, i, &isnull);

		/*
		 * Same for retrieved NULL values from informix.
		 */
		if (isnull)
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

/*
 * ifxFilterQuals
 *
 * Walk through all FDW-related predicate expressions passed
 * by baserel->restrictinfo and examine them for pushdown.
 */
static char * ifxFilterQuals(PlannerInfo *planInfo,
							 RelOptInfo *baserel,
							 Oid foreignTableOid)
{
	IfxPushdownOprContext pushdownCxt;
	ListCell             *cell;
	StringInfoData       *buf;
	char                 *oprStr;
	int i;

	Assert(foreignTableOid != InvalidOid);

	pushdownCxt.foreign_relid = foreignTableOid;
	pushdownCxt.predicates    = NIL;
	pushdownCxt.count         = 0;

	buf = makeStringInfo();
	initStringInfo(buf);

	/*
	 * Loop through the operator nodes and try to
	 * extract the pushdown expressions as a text datum
	 * to the pushdown context structure.
	 */
	foreach(cell, baserel->baserestrictinfo)
	{
		RestrictInfo *info;
		IfxPushdownOprInfo *pushAndInfo;

		info = (RestrictInfo *) lfirst(cell);

		ifx_predicate_tree_walker((Node *)info->clause, &pushdownCxt);

		/*
		 * Each list element from baserestrictinfo is AND'ed together.
		 * Record a corresponding IfxPushdownOprInfo structure in
		 * the context, so that it get decoded properly below.
		 */
		if (lnext(cell) != NULL)
		{
			pushAndInfo              = palloc(sizeof(IfxPushdownOprInfo));
			pushAndInfo->type        = IFX_OPR_AND;
			pushAndInfo->expr_string = cstring_to_text("AND");

			pushdownCxt.predicates = lappend(pushdownCxt.predicates, pushAndInfo);
			pushdownCxt.count++;
		}
	}

	/*
	 * Since restriction clauses are always AND together,
	 * assume a AND_EXPR per default.
	 */
	oprStr = "AND";

	/*
	 * Filter step done, if any predicates to be able to be
	 * pushed down are found, we have a list of IfxPushDownOprInfo
	 * structure in the IfxPushdownOprContext structure. Loop
	 * through them and attach all supported filter quals into
	 * our result buffer.
	 */
	for (i = 0; i < pushdownCxt.count; i++)
	{
		IfxPushdownOprInfo *info;

		info = (IfxPushdownOprInfo *) list_nth(pushdownCxt.predicates, i);

		if ((info->type != IFX_OPR_OR)
			&& (info->type != IFX_OPR_AND)
			&& (info->type != IFX_OPR_NOT))
		{
			if (info->type != IFX_OPR_NOT_SUPPORTED)
			{
				appendStringInfo(buf, " %s %s",
								 (i > 1) ? oprStr : "",
								 text_to_cstring(info->expr_string));
			}
			else
				continue;
		}
		else
		{
			/* save current boolean opr context */
			oprStr = text_to_cstring(info->expr_string);
			elog(DEBUG2, "decoded boolean expr %s", oprStr);
		}
	}

	/* empty string in case no pushdown predicates are found */
	return buf->data;
}

/*
 * ifxPrepareCursorForScan()
 *
 * Prepares the remote informix FDW to scan the relation.
 * This basically means to allocate the SQLDA description area and
 * declaring the cursor. The reason why this is a separate function is,
 * that we are eventually required to do it twice, once in ifxPlanForeignScan()
 * and in ifxBeginForeignScan(). The reason for this is that we need the
 * query plan from the DECLARE CURSOR statement in ifxPlanForeignScan()
 * to get the query costs from the informix server easily. However, that
 * involves declaring the cursor in ifxPlanForeignScan(), which will be then
 * reused in ifxBeginForeignScan() later. To save extra cycles and declaring
 * the cursor twice, we just reuse the cursor previously declared in
 * ifxBeginForeignScan() later. However, if used for example with a prepared
 * statement, ifxPlanForeignScan() won't be called again, instead the
 * previously plan prepared by ifxPlanForeignScan() will be re-used. Since
 * ifxEndForeignScan() already has deallocated the complete structure, we
 * are required to redeclare the cursor again, to satisfy subsequent
 * EXECUTE calls to the prepared statement. This is relatively easy
 * to check, since the only thing we need to do in ifxBeginForeignScan()
 * is to recheck wether the call stack is empty or not.
 */
static void ifxPrepareCursorForScan(IfxStatementInfo *info,
									IfxConnectionInfo *coninfo)
{
	/*
	 * Generate a statement identifier. Required to uniquely
	 * identify the prepared statement within Informix.
	 */
	info->stmt_name = ifxGenStatementName(coninfo);

	/*
	 * An identifier for the dynamically allocated
	 * DESCRIPTOR area.
	 */
	info->descr_name = ifxGenDescrName(coninfo);

	/*
	 * ...and finally the cursor name.
	 */
	info->cursor_name = ifxGenCursorName(coninfo);

	/* Prepare the query. */
	elog(DEBUG1, "prepare query \"%s\"", info->query);
	ifxPrepareQuery(info->query,
					info->stmt_name);
	ifxCatchExceptions(info, IFX_STACK_PREPARE);

	/*
	 * Declare the cursor for the prepared
	 * statement.
	 */
	elog(DEBUG1, "declare cursor \"%s\"", info->cursor_name);
	ifxDeclareCursorForPrepared(info->stmt_name,
								info->cursor_name);
	ifxCatchExceptions(info, IFX_STACK_DECLARE);
}

/*
 * ifxExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
ifxExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	IfxFdwExecutionState *festate;
	List                 *plan_values;

	festate = (IfxFdwExecutionState *) node->fdw_state;

	/*
	 * XXX: We need to get the info from the cached connection!
	 */
	plan_values = PG_SCANSTATE_PRIVATE_P(node);
	ifxDeserializeFdwData(festate, plan_values);

	/* Give some possibly useful info about startup costs */
	if (es->costs)
	{
		ExplainPropertyText("Informix query", festate->stmt_info.query, es);
	}
}


static void ifxConnInfoSetDefaults(IfxConnectionInfo *coninfo)
{
	Assert(coninfo != NULL);

	if (coninfo == NULL)
		return;

	coninfo->tx_enabled = 0;

    /* enable predicate pushdown */
	coninfo->predicate_pushdown = 1;

	coninfo->gl_date       = IFX_ISO_DATE;
	coninfo->gl_datetime   = IFX_ISO_TIMESTAMP;
	coninfo->db_locale     = NULL;
	coninfo->client_locale = NULL;
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
			elog(DEBUG2, "found %d entries in informix connection cache",
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
		Datum                values[9];
		bool                 nulls[9];
		HeapTuple            tuple;
		Datum                result;

		call_data = (struct ifx_sp_call_data *) fcontext->user_fctx;
		Assert(call_data != NULL);
		conn_cached = (IfxCachedConnection *) hash_seq_search(call_data->hash_status);

		/*
		 * Values array. This will hold the values to be returned.
		 */
		elog(DEBUG2, "connection name %s", conn_cached->ifx_connection_name);
		values[0] = PointerGetDatum(cstring_to_text(conn_cached->ifx_connection_name));
		values[1] = Int32GetDatum(conn_cached->establishedByOid);
		values[2] = PointerGetDatum(cstring_to_text(conn_cached->servername));
		values[3] = PointerGetDatum(cstring_to_text(conn_cached->informixdir));
		values[4] = PointerGetDatum(cstring_to_text(conn_cached->database));
		values[5] = PointerGetDatum(cstring_to_text(conn_cached->username));
		values[6] = Int32GetDatum(conn_cached->usage);

		nulls[0] = false;
		nulls[1] = false;
		nulls[2] = false;
		nulls[3] = false;
		nulls[4] = false;
		nulls[5] = false;
		nulls[6] = false;

		/* db_locale and client_locale might be undefined */

		if (conn_cached->db_locale != NULL)
		{
			values[7] = PointerGetDatum(cstring_to_text(conn_cached->db_locale));
			nulls[7] = false;
		}
		else
		{
			nulls[7] = true;
			values[7] = PointerGetDatum(NULL);
		}

		if (conn_cached->client_locale != NULL)
		{
			values[8] = PointerGetDatum(cstring_to_text(conn_cached->client_locale));
			nulls[8] = false;
		}
		else
		{
			nulls[8] = true;
			values[8] = PointerGetDatum(NULL);
		}

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
