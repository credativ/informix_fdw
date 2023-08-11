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

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "parser/parsetree.h"
#endif

#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#include "access/relation.h"
#endif

/* For PG14 we need add_row_identity_var() */
#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif

/*
 * For REL_16_STABLE, as of commit a61b1f74823 we need optimizer/inherit.h
 * for get_rel_all_updated_cols().
 */
#if PG_VERSION_NUM >= 160000
#include "optimizer/inherit.h"
#endif

#include "access/xact.h"
#include "utils/lsyscache.h"

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
 * Global per-backend transaction counter.
 */
extern unsigned int ifxXactInProgress;

/*
 * Valid options for informix_fdw.
 */
static struct IfxFdwOption ifx_valid_options[] =
{
	{ "informixserver",   ForeignServerRelationId },
	{ "informixdir",      ForeignServerRelationId },
	{ "delimident",       ForeignServerRelationId },
	{ "username",         UserMappingRelationId },
	{ "password",         UserMappingRelationId },
	{ "database",         ForeignTableRelationId },
	{ "database",         ForeignServerRelationId },
	{ "database",         UserMappingRelationId },
	{ "query",            ForeignTableRelationId },
	{ "table",            ForeignTableRelationId },
	{ "gl_datetime",      ForeignTableRelationId },
	{ "gl_date",          ForeignTableRelationId },
	{ "client_locale",    ForeignTableRelationId },
	{ "db_locale",        ForeignTableRelationId },
	{ "db_monetary",      ForeignTableRelationId },
	{ "db_locale",        ForeignServerRelationId },
	{ "db_monetary",      ForeignServerRelationId },
	{ "gl_datetime",      ForeignServerRelationId },
	{ "gl_date",          ForeignServerRelationId },
	{ "client_locale",    ForeignServerRelationId },
	{ "disable_predicate_pushdown", ForeignTableRelationId },
	{ "disable_rowid",              ForeignTableRelationId },
	{ "enable_blobs",               ForeignTableRelationId },
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
PG_FUNCTION_INFO_V1(ifxCloseConnection);

#if PG_VERSION_NUM >= 120000
extern PGDLLIMPORT double cpu_tuple_cost;
#endif

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

/*
 * The following definitions vary between PostgreSQL releases.
 * Thus we encapsulate them within macros, so we don't need to
 # #ifdef the function itself...
 */
#if PG_VERSION_NUM < 90300
#define IFX_PGFDWAPI_SUBXACT_COMMIT SUBXACT_EVENT_COMMIT_SUB
#else
#define IFX_PGFDWAPI_SUBXACT_COMMIT SUBXACT_EVENT_PRE_COMMIT_SUB
#endif

#if PG_VERSION_NUM < 90400
#define IFX_SYSTABLE_SCAN_SNAPSHOT SnapshotNow
#else
#define IFX_SYSTABLE_SCAN_SNAPSHOT NULL
#endif

#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 160000
#define RTE_UPDATED_COLS(planInfo, resultRel, set) \
	RangeTblEntry *rte = planner_rt_fetch((resultRel), (planInfo));	\
	(set) = bms_copy(rte->updatedCols);
#define BMS_LOOKUP_COL(set, attnum) bms_first_member((set))
#elif PG_VERSION_NUM >= 160000
#define RTE_UPDATED_COLS(planInfo, resultRel, set) \
	RelOptInfo *relInfo = find_base_rel((planInfo), (resultRel));	\
	(set) = get_rel_all_updated_cols((planInfo), (relInfo));
#define BMS_LOOKUP_COL(set, attnum) bms_next_member((set), (attnum))
#else
#define RTE_UPDATED_COLS(planInfo, resultRel, set) \
	RangeTblEntry *rte = planner_rt_fetch((resultRel), (planInfo));	\
	(set) = bms_copy(rte->modifiedCols);
#define BMS_LOOKUP_COL(set, attnum) bms_first_member((set))
#endif

/*
 * get_relid_attribute_name() is dead as of REL_11_STABLE
 * (see commit 8237f27b504ff1d1e2da7ae4c81a7f72ea0e0e3e in the
 * pg repository). Use get_attname() instead. Wrap this into a
 * compatibility macro, to safe further ifdef's...
 */
#if PG_VERSION_NUM >= 110000
#define pg_attname_by_relid(relid, attnum, missing_ok) \
	get_attname((relid), (attnum), (missing_ok))
#else
#define pg_attname_by_relid(relid, attnum, missing_ok) \
	get_relid_attribute_name((relid), (attnum))
#endif

/*
 * PostgreSQL 10 introduced TupleDescrAttr() to access
 * attributes stored in a tuple descriptor. Use that instead
 * of directly accessing the tupdesc attribute array, if available.
 *
 * CAUTION: This macro was introduced in backpatches
 *          in various major releases (e.g. with commit
 *          5b286cae3cc1c43d6eedf6cf1181d41f653c6a93),
 *          so we make sure it's not defined yet.
 */
#if PG_VERSION_NUM < 100000
#ifndef TupleDescAttr
#define TupleDescAttr(desc, index) ((desc)->attrs[(index)])
#endif
#endif

#define TUPDESC_GET_ATTR(desc, index) \
	TupleDescAttr((desc), (index))

/*******************************************************************************
 * FDW helper functions.
 */
static void ifxSetupFdwScan(IfxConnectionInfo    **coninfo,
							IfxFdwExecutionState **state,
							List    **plan_values,
							Oid       foreignTableOid,
							IfxForeignScanMode mode);

static IfxCachedConnection * ifxSetupConnection(IfxConnectionInfo **coninfo,
												Oid foreignTableOid,
												IfxForeignScanMode mode,
												bool error_ok);

static IfxFdwExecutionState *makeIfxFdwExecutionState(int refid);

static StringInfoData *
ifxFdwOptionsToStringBuf(Oid context);

static bool
ifxIsValidOption(const char *option, Oid context);

static void
ifxGetOptions(Oid foreigntableOid, IfxConnectionInfo *coninfo);

static void ifxAssignOptions(IfxConnectionInfo *coninfo,
							 List *options,
							 bool mandatory[IFX_REQUIRED_CONN_KEYWORDS]);

static StringInfoData *
ifxGetDatabaseString(IfxConnectionInfo *coninfo);

static StringInfoData *
ifxGenerateConnName(IfxConnectionInfo *coninfo);
static char *
ifxGenStatementName(int stmt_id);
static char *
ifxGenDescrName(int descr_id);

static void
ifxGetOptionDups(IfxConnectionInfo *coninfo, DefElem *def);

static void ifxConnInfoSetDefaults(IfxConnectionInfo *coninfo);

static IfxConnectionInfo *ifxMakeConnectionInfo(Oid foreignTableOid);

static void ifxStatementInfoInit(IfxStatementInfo *info,
								 int refid);

static char *ifxGenCursorName(int curid);

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
							List **excl_restrictInfo,
							Oid foreignTableOid);

static void ifxPrepareParamsForScan(IfxFdwExecutionState *state,
									IfxConnectionInfo *coninfo);

static IfxSqlStateClass
ifxFetchTuple(IfxFdwExecutionState *state);

static void
ifxGetValuesFromTuple(IfxFdwExecutionState *state,
					  TupleTableSlot *tupleSlot);

static HeapTuple ifxFdwMakeTuple(IfxFdwExecutionState *state,
								 Relation              rel,
								 ItemPointer           encoded_rowid,
								 TupleTableSlot       *slot);

static ItemPointer ifxGetRowIdForTuple(IfxFdwExecutionState *state);

__attribute__((unused)) static bool ifxCheckForAfterRowTriggers(Oid foreignTableOid,
															   IfxFdwExecutionState *state,
															   CmdType cmd);

#if PG_VERSION_NUM >= 90300

static void ifxRowIdValueToSqlda(IfxFdwExecutionState *state,
								 int                   paramId,
								 TupleTableSlot       *planSlot);

static void ifxPrepareModifyQuery(IfxStatementInfo  *info,
								  IfxConnectionInfo *coninfo,
								  CmdType            operation);

static void ifxPrepareParamsForModify(IfxFdwExecutionState *state,
									  PlannerInfo          *planInfo,
									  Index                 resultRelation,
									  ModifyTable          *plan,
									  Oid                   foreignTableOid);

static void ifxColumnValuesToSqlda(IfxFdwExecutionState *state,
								   TupleTableSlot *slot,
								   int attnum);
static IfxFdwExecutionState *ifxCopyExecutionState(IfxFdwExecutionState *state);

static int
ifxIsForeignRelUpdatable(Relation rel);

static void
ifxExplainForeignModify(ModifyTableState *mstate,
						ResultRelInfo    *rinfo,
						List             *fdw_private,
						int               subplan_index,
						ExplainState     *es);

#endif

#ifdef __USE_EDB_API__

#warning building with experimental EDB API support

static void ifx_fdw_xact_callback(XactEvent event, void *arg, bool spl_context);
static void ifx_fdw_subxact_callback(SubXactEvent event,
									 SubTransactionId subId,
									 SubTransactionId parentId,
									 void *arg,
									 bool spl_context);

#else

static void ifx_fdw_xact_callback(XactEvent event, void *arg);
static void ifx_fdw_subxact_callback(SubXactEvent event,
									 SubTransactionId subId,
									 SubTransactionId parentId,
									 void *arg);

#endif

static void ifx_fdw_xact_callback_internal(IfxCachedConnection *cached,
										   XactEvent event);

static int ifxXactFinalize(IfxCachedConnection *cached,
						   IfxXactAction action,
						   bool connection_error_ok);

#if PG_VERSION_NUM >= 90500

static void ifxGetForeignTableDetails(IfxConnectionInfo *coninfo,
									  IfxImportTableDef *tableDef,
									  int    refid);

static List * ifxGetImportCandidates(ImportForeignSchemaStmt *stmt,
									 IfxConnectionInfo       *coninfo,
									 Oid                      serverOid,
	                                 int                      refid);

static void ifxPrepareImport(ImportForeignSchemaStmt *stmt,
							 IfxConnectionInfo **coninfo,
							 Oid serveroid);

static void ifxGetImportOptions(ImportForeignSchemaStmt *stmt,
								IfxConnectionInfo       *coninfo,
								Oid                      serveroid);

#endif
/*
 * Shared Library initialization.
 */
void _PG_init(void);

/*******************************************************************************
 * FDW callback routines.
 */

/*
 * IMPORT FOREIGN SCHEMA starting with PostgreSQL 9.5
 */
#if PG_VERSION_NUM >= 90500

static List * ifxImportForeignSchema(ImportForeignSchemaStmt *stmt,
									 Oid serverOid);

#endif

/*
 * Modifyable FDW API (Starting with PostgreSQL 9.3).
 */
#if PG_VERSION_NUM >= 90300

/*
 * PG14 has changed the signature of AddForeignUpdateTargets() to a
 * different argument list, so we need to do some additional
 * version magic here, too.
 */
#if PG_VERSION_NUM < 140000
static void
ifxAddForeignUpdateTargets(Query *parsetree,
						   RangeTblEntry *target_rte,
						   Relation target_relation);
#else
static void
ifxAddForeignUpdateTargets(PlannerInfo *root,
						   Index rtindex,
						   RangeTblEntry *target_rte,
						   Relation       target_relation);
#endif

static List *
ifxPlanForeignModify(PlannerInfo *root,
					 ModifyTable *plan,
					 Index resultRelation,
					 int subplan_index);
static void
ifxBeginForeignModify(ModifyTableState *mstate,
					  ResultRelInfo *rinfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags);
static TupleTableSlot *
ifxExecForeignInsert(EState *estate,
					 ResultRelInfo *rinfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot);
static TupleTableSlot *
ifxExecForeignDelete(EState *estate,
					 ResultRelInfo *rinfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot);
static TupleTableSlot *
ifxExecForeignUpdate(EState *estate,
					 ResultRelInfo *rinfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot);

#endif

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
									  List *scan_clauses
#if PG_VERSION_NUM >= 90500
									  , Plan *outer_plan
#endif
	);

static int
ifxAcquireSampleRows(Relation relation, int elevel, HeapTuple *rows,
					 int targrows, double *totalrows, double *totaldeadrows);

static bool
ifxAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
					   BlockNumber *totalpages);

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
Datum
ifxCloseConnection(PG_FUNCTION_ARGS);

/*******************************************************************************
 * Implementation starts here
 */

#if PG_VERSION_NUM >= 90500

/*
 * Prepare IMPORT FOREIGN SCHEMA command...
 */
static void ifxPrepareImport(ImportForeignSchemaStmt *stmt,
							 IfxConnectionInfo **coninfo,
							 Oid serveroid)
{
	StringInfoData *buf;

	/*
	 * Prepare the database connection. We can't use
	 * ifxMakeConnectionInfo(), since it makes all option
	 * parsing itself and requires a foreign table OID.
	 */
	*coninfo = (IfxConnectionInfo *) palloc(sizeof(IfxConnectionInfo));
	memset((*coninfo)->conname, '\0', IFX_CONNAME_LEN + 1);
	ifxConnInfoSetDefaults(*coninfo);

	/*
	 * Read all options
	 */
	ifxGetImportOptions(stmt,
						*coninfo,
						serveroid);

	/*
	 * Generate connection identifier.
	 */
	buf = ifxGenerateConnName(*coninfo);
	strncpy((*coninfo)->conname, buf->data, IFX_CONNAME_LEN);

	/*
	 * Generate connection DSN.
	 */
	buf = ifxGetDatabaseString(*coninfo);
	(*coninfo)->dsn = pstrdup(buf->data);
}

/*
 * Retrieve options for IMPORT FOREIGN SCHEMA
 */
static void ifxGetImportOptions(ImportForeignSchemaStmt *stmt,
								IfxConnectionInfo       *coninfo,
								Oid                      serveroid)
{
	ForeignServer *foreignServer;
	UserMapping   *userMap;
	List          *options;
	int            i;
	bool           mandatory[IFX_REQUIRED_CONN_KEYWORDS] = { false, false, false, false };

	Assert(serveroid != InvalidOid);

	foreignServer = GetForeignServer(serveroid);
	userMap       = GetUserMapping(GetUserId(), serveroid);
	options       = NIL;
	options       = list_concat(options, foreignServer->options);
	options       = list_concat(options, userMap->options);
	options       = list_concat(options, stmt->options);

	ifxAssignOptions(coninfo, options, mandatory);

	/*
	 * Check for all other mandatory options
	 */
	for (i = 0; i < IFX_REQUIRED_CONN_KEYWORDS; i++)
	{
		if (!mandatory[i])
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
							errmsg("missing required FDW options (informixserver, informixdir, client_locale, database)")));
	}
}

/*
 * Create an adhoc IfxStatementInfo structure with
 * the given query. Describes, plans and executes the
 * query with a cursor and returns a pointer to it.
 */
static IfxStatementInfo *ifxExecStmt(IfxConnectionInfo *coninfo,
									 int   refid,
									 char *query)
{
	IfxStatementInfo *stmtinfo = NULL;

	/*
	 * No-op if query is not defined.
	 */
	if (query == NULL)
		return stmtinfo;

	/*
	 * Initialize a new statement info structure.
	 */
	stmtinfo = (IfxStatementInfo *) palloc(sizeof(IfxStatementInfo));
	ifxStatementInfoInit(stmtinfo, refid);

	/*
	 * Set query string and object idenfifiers required
	 * for preparing, describing and executing the query with
	 * a SCROLL cursor.
	 */
	stmtinfo->query = query;
	ifxPrepareCursorForScan(stmtinfo, coninfo);

	/*
	 * Populate the DESCRIPTOR area.
	 */
	ifxDescribeAllocatorByName(stmtinfo);
	ifxCatchExceptions(stmtinfo, IFX_STACK_ALLOCATE | IFX_STACK_DESCRIBE);

	/* Number of columns in the result set */
	stmtinfo->ifxAttrCount = ifxDescriptorColumnCount(stmtinfo);
	ifxCatchExceptions(stmtinfo, 0);

	stmtinfo->ifxAttrDefs = palloc0fast(stmtinfo->ifxAttrCount
										* sizeof(IfxAttrDef));

	/* Populate result set column info array */
	if ((stmtinfo->row_size = ifxGetColumnAttributes(stmtinfo)) == 0)
	{
		/* oops, no memory to allocate? Something surely went wrong,
		 * so abort */
		ifxRewindCallstack(stmtinfo);
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("could not initialize informix column properties")));
	}

	/*
	 * Allocate memory for SQLVAR result array.
	 */
	stmtinfo->data = (char *) palloc0fast(stmtinfo->row_size);
	stmtinfo->indicator = (short *) palloc0fast(sizeof(short)
												* stmtinfo->ifxAttrCount);

	/* Allocate memory within SQLDA structure */
	ifxSetupDataBufferAligned(stmtinfo);

	/* Finally open the cursor and we're done */
	ifxOpenCursorForPrepared(stmtinfo);
	ifxCatchExceptions(stmtinfo, IFX_STACK_OPEN);

	return stmtinfo;
}

/*
 * Get details for the given foreign table from the
 * foreign server. Currently we retrieve column names,
 * column types and NOT NULL constraints.
 */
static void ifxGetForeignTableDetails(IfxConnectionInfo *coninfo,
									  IfxImportTableDef *tableDef,
									  int    refid)
{
	IfxStatementInfo   *stmtinfo;

	stmtinfo = ifxExecStmt(coninfo, refid, ifxGetTableDetailsSQL(tableDef->tabid));

	if (stmtinfo != NULL)
	{
		IfxSqlStateClass errclass;

		/*
		 * Iterate through column list
		 */
		ifxFetchRowFromCursor(stmtinfo);

		/* obtain error class to enter result set loop */
		errclass = ifxCatchExceptions(stmtinfo, 0);

		while (errclass == IFX_SUCCESS)
		{
			IfxAttrDef *colDef;

			/*
			 * Get column information...
			 */
			colDef = (IfxAttrDef *) palloc0(sizeof(IfxAttrDef));
			colDef->type = (IfxSourceType) ifxGetInt2(stmtinfo, 3);
			colDef->len  = (int) ifxGetInt2(stmtinfo, 4);
			colDef->extended_id = (IfxExtendedType) ifxGetInt4(stmtinfo, 5);

			/*
			 * We need to flag the import handler to remember
			 * any special column here. This is required to set certain
			 * options to the CREATE FOREIGN TABLE statement later, so
			 * that the table gets the correct settings (e.g. enable_blobs).
			 */
			switch (colDef->type)
			{
				case IFX_TEXT:
				case IFX_BYTES:
					tableDef->special_cols |= IFX_HAS_BLOBS;
					break;
				case IFX_LVARCHAR:
				case IFX_BOOLEAN:
					/*
					 * Not really used anywhere yet, but also remember
					 * any OPAQUE datatypes.
					 */
					tableDef->special_cols |= IFX_HAS_OPAQUE;
					break;
				default:
					break;
			}

			/*
			 * Set the indicator value, this will
			 * define wether we need to create a NOT NULL constraint.
			 */
			if (ifxIsColumnNullable(colDef->type))
				colDef->indicator = INDICATOR_NULL;
			else
				colDef->indicator = INDICATOR_NOT_NULL;

			/*
			 * We need this identifier value to be persistent, so
			 * copy it. The cursor will move forward and reuse
			 * the column slot.
			 */
			colDef->name = pstrdup((char *) ifxGetText(stmtinfo, 2));

			elog(DEBUG3, "column list for tabid \"%d\", name = \"%s\", type = \"%d\", null = \"%d\"",
				 tableDef->tabid, colDef->name,
				 ifxSQLType(colDef->type),
				 ifxIsColumnNullable(colDef->type));

			/* ...and add 'em to the column list */
			tableDef->columnDef = lappend(tableDef->columnDef, colDef);

			/* next one and/or set loop abort condition */
			ifxFetchRowFromCursor(stmtinfo);
			errclass = ifxCatchExceptions(stmtinfo, 0);
		}

		/* ...and we're done. */
		ifxRewindCallstack(stmtinfo);
	}
}

/*
 * Prepare a list of tables matching the import criteria.
 *
 * The returned List is either NIL if no import candidates
 * are found or contains a list of pointers to
 * IfxImportTableDef structures describing the table candidate.
 */
static List * ifxGetImportCandidates(ImportForeignSchemaStmt *stmt,
									 IfxConnectionInfo       *coninfo,
									 Oid                      serverOid,
	                                 int                      refid)
{
	List         *result = NIL;
	char         *get_table_info;
	IfxStatementInfo *stmtinfo;

	Assert(coninfo != NULL);

	get_table_info = ifxGetTableImportListSQL(coninfo, stmt);
	stmtinfo       = ifxExecStmt(coninfo, refid, get_table_info);

	if (stmtinfo != NULL)
	{
		IfxSqlStateClass errclass;

		/* Iterate through table list */
		ifxFetchRowFromCursor(stmtinfo);
		errclass = ifxCatchExceptions(stmtinfo, 0);

		while (errclass == IFX_SUCCESS)
		{
			/*
			 * Extract tabid, table owner and table name from result set.
			 */
			IfxImportTableDef *tableDef;

			/*
			 * Initialize an IfxImportTableDef structure.
			 */
			tableDef = (IfxImportTableDef *) palloc0(sizeof(IfxImportTableDef));
			tableDef->tabid     = ifxGetInt4(stmtinfo, 0);
			tableDef->columnDef = NIL;

			/*
			 * Initialize the table definition to explicitely *not*
			 * having any special columns. IfxGetForeignTableDetails() will
			 * set this property right away.
			 */
			tableDef->special_cols = IFX_NO_SPECIAL_COLS;

			/*
			 * Since we need those identifier persistent, we must
			 * copy them, otherwise the cursor machinery will reuse
			 * them under us when moving the cursor forward.
			 */
			tableDef->tablename = pstrdup(ifxGetText(stmtinfo, 2));
			tableDef->owner     = pstrdup(ifxGetText(stmtinfo, 1));

			elog(DEBUG3, "import candidates: tabid %d, table owner %s, table name %s",
				 tableDef->tabid,
				 ifxQuoteIdent(coninfo, tableDef->owner),
				 ifxQuoteIdent(coninfo, tableDef->tablename));

			/*
			 * Retrieve table column list...
			 */
			ifxGetForeignTableDetails(coninfo,
									  tableDef,
									  ++refid);

			/*
			 * Push the new candidate relation to the list.
			 */
			result = lappend(result, tableDef);

			/* next one */
			ifxFetchRowFromCursor(stmtinfo);
			errclass = ifxCatchExceptions(stmtinfo, 0);
		}

		/* ...we're done */
		ifxRewindCallstack(stmtinfo);
	}

	return result;
}

/*
 * Callback for IMPORT FOREIGN SCHEMA statement
 */
static List * ifxImportForeignSchema(ImportForeignSchemaStmt *stmt,
									 Oid serverOid)
{
	List                *result = NIL;
	IfxConnectionInfo   *coninfo = NULL;
	IfxCachedConnection *cached;

	/*
	 * Prepare connection for IMPORT FOREIGN SCHEMA.
	 */
	ifxPrepareImport(stmt, &coninfo, serverOid);

	if ((cached = ifxSetupConnection(&coninfo,
									 InvalidOid,
									 IFX_IMPORT_SCHEMA,
									 true)) != NULL)
	{
		List *table_candidates = NIL;
		ListCell *cell;

		/*
		 * List of IfxImportTableDef definitions.
		 */
		table_candidates = ifxGetImportCandidates(stmt,
												  coninfo,
												  serverOid,
												  cached->con.usage);

		foreach(cell, table_candidates)
		{
			IfxImportTableDef *def = (IfxImportTableDef *) lfirst(cell);
			elog(DEBUG1, "extracted candidate: tabid = %d, tabname = %s",
				 def->tabid, def->tablename);
		}

		/*
		 * Generate the SQL script from candidates list.
		 */
		result = ifxCreateImportScript(coninfo, stmt, table_candidates, serverOid);
	}
	else
	{
		/*
		 * ifxSetupConnection() returned a NULL cache handle
		 * which shouldn't happen. Guard against this case and exit
		 * immediately.
		 */
		ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						errmsg("could not establish remote connection for server OID \"%u\"",
							   serverOid)));
	}

	return result;
}

#endif


#if PG_VERSION_NUM >= 90300

/*
 * Set the given ROWID into the Informix
 * SQLDA structure.
 */
static void ifxRowIdValueToSqlda(IfxFdwExecutionState *state,
								 int                   paramId,
								 TupleTableSlot       *planSlot)
{
	int   rowid;
	ItemPointer iptr;
	bool  isnull;

	/*
	 * Fetch the current rowid from the resjunk column...
	 */
	iptr = (ItemPointer) DatumGetPointer(ExecGetJunkAttribute(planSlot,
															  state->rowid_attno,
															  &isnull));

	/* Should be valid */
	Assert(PointerIsValid(iptr));

	if (isnull)
		elog(ERROR, "informix_fdw: could not extract rowid");

	/*
	 * Convert the ItemPointer back into a 4 Byte ROWID value
	 * for Informix. We can't rely on ItemPointerGetBlockNumber()
	 * since it will fail the Assertion for a given OffsetNumber
	 * otherwise.
	 */
	rowid = (int) ((iptr->ip_blkid.bi_hi << 16) | ((uint16) iptr->ip_blkid.bi_lo));

	/*
	 * Mark the value valid, otherwise the conversion routine
	 * will give up immediately...
	 */
	IFX_SET_INDICATOR_P(state, paramId, INDICATOR_NOT_NULL);

	/*
	 * ...let the conversion do its job.
	 */
	ifxSetInteger(&(state->stmt_info), paramId, rowid);
}

/*
 * Extra information for EXPLAIN on a modify action.
 */
static void
ifxExplainForeignModify(ModifyTableState *mstate,
						ResultRelInfo    *rinfo,
						List             *fdw_private,
						int               subplan_index,
						ExplainState     *es)
{
	/*
	 * Get the current SQL and display it in the VERBOSE output
	 * of this EXPLAIN command.
	 */
	if (es->verbose)
	{
		IfxFdwExecutionState  state;

		/* Deserialize from list */
		ifxDeserializeFdwData(&state, fdw_private);

		/* Give some possibly useful info about the remote query used */
		if (es->costs)
		{
			ExplainPropertyText("Informix query", state.stmt_info.query, es);
		}
	}
}

/*
 * Determines wether a remote Informix table is updatable.
 *
 * The Informix FDW assumes that every relation is updatable, except
 * a remote table was specified with the 'query' option.
 *
 * A foreign table might also reference a view on the remote
 * Informix server, but we leave it up to the remote server
 * to give an appropiate error message, if that remote view
 * is not updatable.
 *
 * Additionally, we check wether the disable_rowid option was
 * added to the foreign table, effectively disabling the property
 * to uniquely identify a row required to do safe DML. Disallow
 * UPDATE and DELETE in this case.
 */
static int
ifxIsForeignRelUpdatable(Relation rel)
{
	ForeignTable *table;
	bool          updatable;
	ListCell     *lc;

	elog(DEBUG3, "informix_fdw: foreign rel updatable");

	table     = GetForeignTable(RelationGetRelid(rel));
	updatable = true;

	foreach(lc, table->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "query") == 0)
		{
			updatable = false;
		}

		/*
		 * Don't allow DML on foreign tables defined with disable_rowid.
		 */
		if (strcmp(def->defname, "disable_rowid") == 0)
		{
			return (1 << CMD_INSERT) | (0 << CMD_UPDATE) | (0 << CMD_DELETE);
		}
	}

	return updatable
		? (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE)
		: 0;
}

/*
 * Copies the specified IfxFdwExecutionState structure into
 * a new palloc'ed one, but without any stateful information.
 * This makes the returned pointer suitable to be used for
 * an additional scan state.

 * The refid of the origin state and its connection identifier will be
 * kept, but no statement or query information will be copied.
 */
static IfxFdwExecutionState *ifxCopyExecutionState(IfxFdwExecutionState *state)
{
	IfxFdwExecutionState *copy;

	Assert(state != NULL);

	/*
	 * Make a dummy execution state first, but keep the
	 * refid from the origin.
	 */
	copy = makeIfxFdwExecutionState(state->stmt_info.refid);

	/*
	 * Copy connection string...
	 */
	memcpy(copy->stmt_info.conname, state->stmt_info.conname, IFX_CONNAME_LEN + 1);

	/*
	 * Copy ROWID usage indicator.
	 */
	copy->use_rowid = state->use_rowid;

	/*
	 * ...and we're done.
	 */
	return copy;
}

/*
 * ifxColumnValuesToSqlda()
 *
 * Does all the legwork to store the specified attribute
 * within the current Informix SQLDA structure.
 *
 * NOTE: attnum is the index into the internal state for
 *       the requested attribute. Thus, attnum == pg_attribute.attnum - 1!
 */
static void ifxColumnValuesToSqlda(IfxFdwExecutionState *state,
								   TupleTableSlot *slot,
								   int attnum)
{
	Datum datum;
	bool isnull = false;

	Assert(state != NULL && attnum >= 0);
	Assert(state->stmt_info.data != NULL);

	datum = slot_getattr(slot, attnum + 1, &isnull);

	/*
	 * Set the local indicator value to tell the conversion
	 * routines wether they deal with a SQL NULL datum.
	 *
	 * Check for any NULL datum before trying to get a C string
	 * representation from the datum, otherwise we will crash.
	 * Setter function are careful enough to pay attention to them,
	 * if we have initialized the ifxAttrDefs array correctly.
	 */
	IFX_SET_INDICATOR_P(state, IFX_ATTR_PARAM_ID(state, attnum),
						isnull ? INDICATOR_NULL : INDICATOR_NOT_NULL);

	/*
	 * Call data conversion routine depending on the PostgreSQL
	 * builtin source type.
	 */
	switch(PG_ATTRTYPE_P(state, attnum))
	{
		case FLOAT4OID:
		case FLOAT8OID:
		{
			setIfxFloat(state, slot, attnum);
			break;
		}
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TIDOID:
		{
			setIfxInteger(state, slot, attnum);
			break;
		}
		case NUMERICOID:
		case CASHOID:
		{
			/*
			 * We also support conversion from NUMERICOID to
			 * Informix float values, so dispatch the target type
			 * properly.
			 */
			if ( (IFX_ATTRTYPE_P(state, attnum) == IFX_FLOAT)
				 || (IFX_ATTRTYPE_P(state, attnum) == IFX_SMFLOAT) )
			{
				setIfxFloat(state, slot, attnum);
			}
			else
			{
				setIfxDecimal(state, slot, attnum);
			}
			break;
		}
		case VARCHAROID:
		case TEXTOID:
		case BPCHAROID:
		{
			/*
			 * Get a C string representation from this
			 * datum, suitable to be passed down to Informix
			 *
			 * NOTE:
			 *
			 * Check for any NULL datum before trying to get a C string
			 * representation from the text datum, otherwise we will crash.
			 * setIfxSetCharString() is careful enough to do the right thing
			 * (tm).
			 */
			int len    = 0;
			char *cval = NULL;

			if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
				&& ! isnull
				&& IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
			{
				cval = TextDatumGetCString(datum);
				Assert(cval != NULL);
				len = strlen(cval);
			}

			setIfxCharString(state, attnum, cval, len);
			break;
		}
		case BYTEAOID:
		{
			/*
			 * Convert a bytea datum into a binary column
			 * if possible. Since we need to deal with probably
			 * embedded NULLs, handle the character buffer with care.
			 */
			char *buf    = NULL;
			int   buflen = 0;

			if (! IFX_ATTR_ISNULL_P(state, IFX_ATTR_PARAM_ID(state, attnum))
				&& ! isnull
				&& IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
			{
				buf    = VARDATA_ANY((bytea *)DatumGetPointer(datum));
				buflen = VARSIZE_ANY_EXHDR((bytea *)DatumGetPointer(datum));
			}

			setIfxCharString(state, attnum, buf, buflen);
			break;
		}
		case DATEOID:
			/*
			 * We support conversion from a PostgreSQL DATE type either to
			 * an Informix DATETIME or DATE data type. However, we have to take care
			 * to choose the right conversion, since DATE and DATETIME aren't binary
			 * compatible types in Informix and thus require special handling.
			 *
			 * So, depending on the target column, choose the right conversion routine.
			 * We check specificially for DATE, all other DATETIME conversion could
			 * be handled by setIfxDateTimestamp().
			 */
			if (IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)) == IFX_DATE)
			{
				setIfxDate(state, slot, attnum);
				break;
			}

			EXPLICIT_FALL_THROUGH;
			/* fall through */
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
		case TIMEOID:
			/*
			 * Be paranoid: DATE should already be handled above, but make
			 * sure we have a compatible target type. DTIME is the only allowed
			 * target type at this point.
			 */
			if (IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)) == IFX_DTIME)
			{
				setIfxDateTimestamp(state, slot, attnum);
			}
			else
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "informix_fdw: cannot convert type oid %u of attnum %d to informix type id %d",
					 PG_ATTRTYPE_P(state, attnum),
					 attnum,
					 state->stmt_info.ifxAttrDefs[attnum].type);
			}
			break;
		case INTERVALOID:
			setIfxInterval(state, slot, attnum);
			break;
		default:
		{
			ifxRewindCallstack(&state->stmt_info);
			elog(ERROR, "informix_fdw: type \"%d\" is not supported for conversion",
				 state->stmt_info.ifxAttrDefs[attnum].type);
			break;
		}
	}

	/*
	 * Check out wether the conversion was successful.
	 */
	if (! IFX_ATTR_IS_VALID_P(state, IFX_ATTR_PARAM_ID(state, attnum)))
	{
		ifxRewindCallstack(&state->stmt_info);
		elog(ERROR, "could not convert attnum %d to informix type \"%d\", errcode %d",
			 attnum,
			 IFX_ATTRTYPE_P(state, IFX_ATTR_PARAM_ID(state, attnum)),
			 state->stmt_info.ifxAttrDefs[IFX_ATTR_PARAM_ID(state, attnum)].converrcode);
	}
}

/*
 * Lookup the specified attribute number, obtain a column
 * identifier.
 *
 * Code borrowed from contrib/postgres_fdw.c
 */
char *dispatchColumnIdentifier(int varno, int varattno, PlannerInfo *root)
{
	char          *ident = NULL;
	RangeTblEntry *rte;
	List          *col_options;
	ListCell      *cell;

	/*
	 * Take take for special varnos!
	 */
	Assert(!IS_SPECIAL_VARNO(varno));

	rte = planner_rt_fetch(varno, root);

	/*
	 * Check out if this varattno has a special
	 * column_name value attached.
	 *
	 * TODO: SELECT statements currently don't honor ifx_column_name settings,
	 *       this issue will be adressed in the very near future!
	 */
	col_options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(cell, col_options)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "ifx_column_name") == 0)
		{
			ident = defGetString(def);
			break; /* we're done */
		}
	}

	/*
	 * Rely on the local column identifier if no ifx_column_name
	 * was found.
	 */
	if (ident == NULL)
		ident = pg_attname_by_relid(rte->relid, varattno, false);

	return ident;
}

/*
 * ifxAddForeignUpdateTargets
 *
 * Injects a "rowid" column into the target list for
 * the remote table.
 *
 * NOTE:
 *
 * Informix doesn't always provide a "rowid" column for all
 * table types. Fragmented tables doesn't have a "rowid" per
 * default, so any attempts to update them will fail.
 *
 * We always append a ROWID resjunk column, however, if
 * disable_rowid is specified to an Informix foreign table,
 * we switch to an updatable cursor (which all its implications).
 */
#if PG_VERSION_NUM < 140000
static void
ifxAddForeignUpdateTargets(Query *parsetree,
						   RangeTblEntry *target_rte,
						   Relation target_relation)
#else
static void
ifxAddForeignUpdateTargets(PlannerInfo *root,
						   Index rtindex,
						   RangeTblEntry *target_rte,
						   Relation       target_relation)

#endif
{
	Var *var;

	/*
	 * Starting with PG14 we shouldn't modify the parse tree directly,
	 * instead use the appropiate API.
	 */

#if PG_VERSION_NUM < 140000

	TargetEntry *tle;

	elog(DEBUG3, "informix_fdw: add foreign targets");

	/*
	 * Append the ROWID resjunk column of type INT8
	 * to the target list.
	 */
	var = makeVar(parsetree->resultRelation,
				  SelfItemPointerAttributeNumber,
				  TIDOID,
				  -1,
				  InvalidOid,
				  0);

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup("rowid"),
						  true);

	/* Finally add it to the target list */
	parsetree->targetList = lappend(parsetree->targetList, tle);

#else

	elog(DEBUG3, "informix_fdw: add foreign targets");

	/*
	 * Append the ROWID resjunk column of type INT8
	 * to the target list.
	 */
	var = makeVar(rtindex,
				  SelfItemPointerAttributeNumber,
				  TIDOID,
				  -1,
				  InvalidOid,
				  0);
	add_row_identity_var(root, var, rtindex, "rowid");

#endif

}

/*
 * ifxPlanForeignModify
 *
 * Plans a DML statement on a Informix foreign table.
 */
static List *
ifxPlanForeignModify(PlannerInfo *root,
					 ModifyTable *plan,
					 Index resultRelation,
					 int subplan_index)
{
	List          *result = NIL;
	CmdType        operation;
	RangeTblEntry *rte;
	IfxFdwExecutionState *state;
	IfxConnectionInfo    *coninfo;
	ForeignTable         *foreignTable;
	bool                  is_table;
	ListCell             *elem;
	List                 *plan_values;

	elog(DEBUG3, "informix_fdw: plan foreign modify");

	/*
	 * Preliminary checks...we don't support updating foreign tables
	 * based on a SELECT.
	 */
	rte = planner_rt_fetch(resultRelation, root);
	foreignTable = GetForeignTable(rte->relid);
	operation = plan->operation;
	is_table = false;
	state    = NULL;

	foreach(elem, foreignTable->options)
	{
		DefElem *option = (DefElem *) lfirst(elem);
		if (strcmp(option->defname, "table") == 0)
			is_table = true;
	}

	if (!is_table)
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						errmsg("cannot modify foreign table \"%s\" which is based on a query",
							   get_rel_name(rte->relid))));
	}

#if PG_VERSION_NUM >= 90500
	/*
	 * Don't support INSERT ... ON CONFLICT.
	 */

	if (plan->onConflictAction != ONCONFLICT_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						errmsg("INSERT with ON CONFLICT clause is not supported")));
	}
#endif

	/*
	 * In case we have an UPDATE or DELETE action, retrieve the foreign scan state
	 * data belonging to the ForeignScan, initiated by the earlier scan node.
	 *
	 * We get this by referencing the corresponding RelOptInfo carried by
	 * the root PlannerInfo structure. This carries the execution state of the
	 * formerly created foreign scan, allowing us to access its current state.
	 *
	 * We need the cursor name later, to generate the WHERE CURRENT OF ... query.
	 */
	if ((operation == CMD_UPDATE)
		|| (operation == CMD_DELETE))
	{
		if ((resultRelation < root->simple_rel_array_size)
			&& (root->simple_rel_array[resultRelation] != NULL))
		{
			RelOptInfo *relInfo = root->simple_rel_array[resultRelation];
			IfxCachedConnection *cached;
			IfxFdwExecutionState *scan_state;
			IfxFdwPlanState *planState;

			/*
			 * Check if there is a foreign scan state present. This would
			 * already have initialized all required objects on the foreign
			 * Informix server.
			 *
			 * In normal cases, ifxGetForeignRelSize() should already have
			 * initialized all required objects here, since we use the scan
			 * state to retrieve optimizer stats from Informix to get estimates
			 * based on the query back from the Informix server.
			 *
			 * This is not always true, so we need to check whether a foreign scan
			 * was already initialized or not (e.g. in the prepared statement
			 * case). Check if a private execution state was properly initialized,
			 * and if not, execute the required steps to initiate one ourselves.
			 */
			if (relInfo->fdw_private == NULL) {

				planState = palloc(sizeof(IfxFdwPlanState));

				/*
				 * Establish remote informix connection or get
				 * a already cached connection from the informix connection
				 * cache.
				 */
				ifxSetupFdwScan(&coninfo, &scan_state, &plan_values,
								rte->relid, IFX_PLAN_SCAN);

				/*
				 * Check for predicates that can be pushed down
				 * to the informix server, but skip it in case the user
				 * has set the disable_predicate_pushdown option...
				 */
				if (coninfo->predicate_pushdown)
				{
					/*
					 * Also save a list of excluded RestrictInfo structures not carrying any
					 * predicate found to be pushed down by ifxFilterQuals(). Those will
					 * passed later to ifxGetForeignPlan()...
					 */
					scan_state->stmt_info.predicate = ifxFilterQuals(root, relInfo,
																	 &(planState->excl_restrictInfo),
																	 rte->relid);
					elog(DEBUG2, "predicate for pushdown: %s", scan_state->stmt_info.predicate);
				}
				else
				{
					elog(DEBUG2, "predicate pushdown disabled");
					scan_state->stmt_info.predicate = "";
				}

				/*
				 * Establish the remote query on the informix server.
				 *
				 * If we have an UPDATE or DELETE query, the foreign scan needs to
				 * employ an FOR UPDATE cursor, since we are going to reuse it
				 * during modify.
				 */
				if ((root->parse->commandType == CMD_UPDATE)
					|| (root->parse->commandType == CMD_DELETE))
				{
					scan_state->stmt_info.cursorUsage = IFX_UPDATE_CURSOR;
				}

				ifxPrepareScan(coninfo, scan_state);

			} else {

				/*
				 * Extract the state of the foreign scan.
				 */
				scan_state = (IfxFdwExecutionState *)
					((IfxFdwPlanState *)relInfo->fdw_private)->state;

			}

			/*
			 * Don't reuse the connection info from the scan state,
			 * it will carry state information not usable for us.
			 */
			coninfo = ifxMakeConnectionInfo(rte->relid);

			/*
			 * Make the connection from the associated foreign scan current.
			 * Note: we use IFX_PLAN_SCAN to get a new refid used to
			 *       generate a new statement identifier.
			 */
			cached = ifxSetupConnection(&coninfo, rte->relid,
										IFX_PLAN_SCAN, true);

			/*
			 * Extract the scan state and copy it over into a new empty one,
			 * suitable to be used by this modify action.
			 */
			state = ifxCopyExecutionState(scan_state);

			/*
			 * The copied execution state kept the refid from the
			 * scan state obtained within the foreign scan. We need
			 * to prepare our own statement for the modify action, but
			 * the connection cache already will have generated one for us.
			 * Assign this to the copied execution state.
			 */
			state->stmt_info.refid = cached->con.usage;

			/*
			 * Since ifxCopyExecutionState() won't preserve stateful
			 * information, we need to do an extra step to copy
			 * the cursor name and type.
			 */
			state->stmt_info.cursor_name = pstrdup(scan_state->stmt_info.cursor_name);
			state->stmt_info.cursorUsage = scan_state->stmt_info.cursorUsage;
		}
	}
	else
	{
		/*
		 * For an INSERT action, setup the foreign datasource from scratch
		 * (since no foreign scan is involved). We call ifxSetupFdwScan(),
		 * even if this is preparing a modify action on the informix table.
		 * This does all the legwork to initialize the database connection
		 * and associated handles. Note that we also establish a special INSERT
		 * cursor here feeded with the new values during ifxExecForeignInsert().
		 */
		ifxSetupFdwScan(&coninfo, &state, &plan_values, rte->relid, IFX_PLAN_SCAN);

		/*
		 * ...don't forget the cursor name.
		 */
		state->stmt_info.cursor_name = ifxGenCursorName(state->stmt_info.refid);
	}

	/*
	 * Check wether this foreign table has AFTER EACH ROW
	 * triggers attached. Currently this information is just
	 * for completeness, since we always include all columns
	 * in a foreign scan.
	 */
	ifxCheckForAfterRowTriggers(rte->relid,
								state,
								root->parse->commandType);

	/* Sanity check, should not happen */
	Assert((state != NULL) && (coninfo != NULL));

	/*
	 * Prepare params (retrieve affacted columns et al).
	 */
	ifxPrepareParamsForModify(state, root, resultRelation, plan, rte->relid);

	/*
	 * Generate the query.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			ifxGenerateInsertSql(state, coninfo, root, resultRelation);
			break;
		case CMD_DELETE:
			ifxGenerateDeleteSql(state, coninfo);
			break;
		case CMD_UPDATE:
			ifxGenerateUpdateSql(state, coninfo, root, resultRelation);
			break;
		default:
			break;
	}

	/*
	 * Generate a statement name for execution later.
	 * This is an unique statement identifier.
	 */
	state->stmt_info.stmt_name = ifxGenStatementName(state->stmt_info.refid);

	/*
	 * Serialize all required plan data for use in executor later.
	 */
	result = ifxSerializePlanData(coninfo, state, root);

	return result;
}

/*
 * ifxPrepareModifyQuery()
 *
 * Prepares and describes the generated modify statement. Will
 * initialize the passed IfxStatementInfo structure with a valid
 * SQLDA structure.
 */
static void ifxPrepareModifyQuery(IfxStatementInfo *info,
								  IfxConnectionInfo *coninfo,
								  CmdType operation)
{
	/*
	 * Prepare the query.
	 */
	elog(DEBUG1, "prepare query \"%s\"", info->query);
	ifxPrepareQuery(info->query,
					info->stmt_name);
	ifxCatchExceptions(info, IFX_STACK_PREPARE);

	/*
	 * In case of an INSERT command, we use an INSERT cursor.
	 */
	if (operation == CMD_INSERT)
	{
		elog(DEBUG1, "declare cursor \"%s\" for statement \"%s\"",
			 info->cursor_name,
			 info->stmt_name);
		ifxDeclareCursorForPrepared(info->stmt_name, info->cursor_name,
									info->cursorUsage);
		ifxCatchExceptions(info, IFX_STACK_DECLARE);
	}
}

static void
ifxBeginForeignModify(ModifyTableState *mstate,
					  ResultRelInfo *rinfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags)
{
	IfxConnectionInfo    *coninfo;
	IfxFdwExecutionState *state;
	Oid                   foreignTableOid;

	elog(DEBUG3, "informix_fdw: begin modify");
	foreignTableOid = RelationGetRelid(rinfo->ri_RelationDesc);

	/*
	 * Activate cached connection. We don't bother
	 * for the returned cached connection handle, we don't
	 * need it.
	 */
	ifxSetupConnection(&coninfo,
					   foreignTableOid,
					   IFX_BEGIN_SCAN,
					   true);

	/*
	 * Initialize an unassociated execution state handle (with refid -1).
	 */
	state = makeIfxFdwExecutionState(-1);

	/* Record current state structure */
	rinfo->ri_FdwState = state;

	/*
	 * Mark usage of ROWID in this modify action.
	 */
	state->use_rowid = (coninfo->disable_rowid ? false : true);

	if (state->use_rowid)
	{
		elog(DEBUG1, "informix_fdw: using ROWID based modify actions");

		/*
		 * We extract the attribute number of the ROWID resjunk
		 * column to safe some cycles during the modify actions.
		 */
		if (mstate->operation == CMD_UPDATE
			|| mstate->operation == CMD_DELETE)
		{
#if PG_VERSION_NUM < 140000
			Plan *subplan = mstate->mt_plans[subplan_index]->plan;
#else
			Plan *subplan = outerPlanState(mstate)->plan;
#endif

			state->rowid_attno = ExecFindJunkAttributeInTlist(subplan->targetlist,
															  "rowid");

			if (!AttributeNumberIsValid(state->rowid_attno))
				elog(ERROR, "informix_fdw: could not find junk rowid column");
		}
	}
	else
	{
		/*
		 * No further actions here when using updatable cursors,
		 * but give a DEBUG message nevertheless...
		 */
		elog(DEBUG1, "informix_fdw: using cursor based modify actions");
	}

	/*
	 * Deserialize plan data.
	 */
	ifxDeserializeFdwData(state, fdw_private);

	/* EXPLAIN without ANALYZE... */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		elog(DEBUG1, "informix_fdw: explain only");
		return;
	}

	/*
	 * Prepare and describe the statement.
	 */
	ifxPrepareModifyQuery(&state->stmt_info, coninfo, mstate->operation);

	/*
	 * An INSERT action need to do much more preparing work
	 * than UPDATE/DELETE: Since no foreign scan is involved, the
	 * insert modify action need to prepare its own INSERT cursor and
	 * all other required stuff.
	 *
	 * UPDATE is a little smarter here. We rely on the cursor created
	 * during the foreign scan planning phase, but also need to prepare
	 * the UPDATE statement to bind column values later during execution.
	 * So there isn't any need to declare an UPDATE cursor additionally,
	 * but the SQLDA structure needs to be initialized nevertheless.
	 *
	 * DELETE doesn't need any special actions here, all we need for
	 * it is done in the planning phase (PREPARE).
	 */
	if ((mstate->operation != CMD_DELETE) ||
		((mstate->operation == CMD_DELETE) && state->use_rowid))
	{
		/*
		 * Get column list for local table definition.
		 *
		 * XXX: Modify on a foreign Informix table relies on equally
		 *      named column identifiers.
		 */
		ifxPgColumnData(foreignTableOid, state);

		/*
		 * Describe the prepared statement into a SQLDA structure.
		 *
		 * This will return a valid SQLDA handle within our current
		 * IfxStatementInfo handle.
		 */
		elog(DEBUG1, "describe statement \"%s\"", state->stmt_info.stmt_name);

		if (mstate->operation == CMD_INSERT)
		{
			ifxDescribeAllocatorByName(&state->stmt_info);
		}
		else
		{
			/* CMD_UPDATE */
			state->stmt_info.descr_name = ifxGenDescrName(state->stmt_info.refid);
			ifxDescribeStmtInput(&state->stmt_info);
		}

		ifxCatchExceptions(&state->stmt_info, IFX_STACK_ALLOCATE | IFX_STACK_DESCRIBE);

		/*
		 * Save number of prepared column attributes.
		 */
		state->stmt_info.ifxAttrCount = ifxDescriptorColumnCount(&state->stmt_info);
		elog(DEBUG1, "get descriptor column count %d",
			 state->stmt_info.ifxAttrCount);

		/*
		 * Don't forget to open the INSERT cursor we have established
		 * ealier in the planning phase. UPDATE, the only other command
		 * type possible here, relies on the cursor from it's scanning
		 * part, so no need to do the same for it.
		 */
		if (mstate->operation == CMD_INSERT)
		{
			/*
			 * Open the associated cursor...
			 */
			elog(DEBUG1, "open cursor with query \"%s\"",
				 state->stmt_info.query);
			ifxOpenCursorForPrepared(&state->stmt_info);
			ifxCatchExceptions(&state->stmt_info, IFX_STACK_OPEN);
		}

		state->stmt_info.ifxAttrDefs = palloc(state->stmt_info.ifxAttrCount
											  * sizeof(IfxAttrDef));

		/*
		 * Populate target column info array.
		 */
		if ((state->stmt_info.row_size = ifxGetColumnAttributes(&state->stmt_info)) == 0)
		{
			/* oops, no memory to allocate? Something surely went wrong,
			 * so abort */
			ifxRewindCallstack(&state->stmt_info);
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
		state->stmt_info.data = (char *) palloc0(state->stmt_info.row_size);
		state->stmt_info.indicator = (short *) palloc0(sizeof(short)
													   * state->stmt_info.ifxAttrCount);

		/*
		 * Assign sqlvar pointers to the allocated memory area.
		 */
		ifxSetupDataBufferAligned(&state->stmt_info);
	}
}

static TupleTableSlot *
ifxExecForeignInsert(EState *estate,
					 ResultRelInfo *rinfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	IfxFdwExecutionState *state;
	int                   attnum;

	/*
	 * Setup action...
	 */
	state = rinfo->ri_FdwState;
	elog(DEBUG3, "informix_fdw: exec insert with cursor \"%s\"",
		 state->stmt_info.cursor_name);

	/*
	 * Copy column values into Informix SQLDA structure.
	 *
	 * NOTE:
	 * We preserve all columns in an INSERT statement.
	 */
	for (attnum = 0; attnum < state->pgAttrCount; attnum++)
	{
		/*
		 * Register the param id, which is also the offset
		 * into the SQLDA structure and it's array of sqlvar
		 * structs holding the parameter values to be inserted.
		 *
		 * Effectively for INSERT it's the same as the attribute
		 * number...
		 */
		state->pgAttrDefs[attnum].param_id = attnum;

		/*
		 * Push all column value into the current Informix
		 * SQLDA structure, suitable to be executed later by
		 * PUT...
		 */
		if (state->pgAttrDefs[attnum].attnum > 0)
			ifxColumnValuesToSqlda(state, slot, state->pgAttrDefs[attnum].attnum - 1);
	}

	/*
	 * Execute the INSERT. Note that we have prepared
	 * an INSERT cursor the the planning phase before, re-using it
	 * here via PUT...
	 */
	ifxPutValuesInPrepared(&state->stmt_info);
	ifxCatchExceptions(&state->stmt_info, 0);

	return slot;
}

static TupleTableSlot *
ifxExecForeignDelete(EState *estate,
					 ResultRelInfo *rinfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	IfxFdwExecutionState *state = rinfo->ri_FdwState;

	/*
	 * Setup action...
	 */
	state = rinfo->ri_FdwState;
	elog(DEBUG3, "informix_fdw: exec delete with statement \"%s\"",
		 state->stmt_info.stmt_name);

	/*
	 * Execute the DELETE action on the remote table. In
	 * case we have disable_rowid set, we just need to execute
	 * the prepared query and we're done. If disable_rowid is
	 * not set (which is the default), we need to extract the ROWID
	 * from the current tuple and pass it via SQLDA structure to
	 * the Informix API...
	 */
	ExecClearTuple(slot);

	if (!state->use_rowid)
	{
		/*
		 * The cursor should have already been positioned on the
		 * right tuple, the generated SQL query attached to the
		 * current execution state will just do a WHERE CURRENT OF
		 * to delete it.
		 */
		ifxExecuteStmt(&state->stmt_info);

		/*
		 * Check for errors.
		 */
		ifxCatchExceptions(&state->stmt_info, 0);
	}
	else
	{
		/*
		 * Assign the ROWID value to the SQLDA structure.
		 */
		ifxRowIdValueToSqlda(state,
							 state->stmt_info.ifxAttrCount - 1,
							 planSlot);

		/*
		 * Execute the DELETE statement by using the finalized
		 * SQLDA descriptor area.
		 */
		ifxExecuteStmtSqlda(&state->stmt_info);

		/*
		 * Check for errors.
		 */
		ifxCatchExceptions(&state->stmt_info, 0);
	}

	return slot;
}

static TupleTableSlot *
ifxExecForeignUpdate(EState *estate,
					 ResultRelInfo *rinfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	IfxFdwExecutionState *state = rinfo->ri_FdwState;
	ListCell *cell;
	int       param_id;

	elog(DEBUG3, "informix_fdw: exec update with cursor \"%s\"",
		 state->stmt_info.cursor_name);

	/*
	 * NOTE:
	 *
	 * We transmit only the specified rows from the
	 * local UPDATE statement to the remote table.
	 *
	 * We also need to track the parameter id to
	 * reference the correct sqlvar struct array
	 * member in our SQLDA structure.
	 */
	param_id = 0;
	foreach(cell, state->affectedAttrNums)
	{
		int attnum = lfirst_int(cell);

		state->pgAttrDefs[attnum - 1].param_id = param_id;
		ifxColumnValuesToSqlda(state, slot, attnum - 1);
		param_id++;
	}

	/*
	 * For ROWIDs enabled (which is the default),
	 * we need to get it back from the resjunk
	 * column, pass it to the SQLDA structure and execute the
	 * query. If we fall back to an updatable cursor,
	 * it's enough to just execute the statement now, since
	 * all columns values are already assigned to the SQLDA
	 * structure above.
	 */
	if (state->use_rowid)
	{
		/*
		 * Assign the ROWID to the SQLDA structure.
		 * We know that the last parameter id is
		 * used by the ROWID within the WHERE clause
		 * of this UPDATE.
		 */
		ifxRowIdValueToSqlda(state,
							 state->stmt_info.ifxAttrCount - 1,
							 planSlot);
	}

	ifxExecuteStmtSqlda(&state->stmt_info);
	ifxCatchExceptions(&state->stmt_info, 0);

	return slot;
}

static void ifxEndForeignModify(EState *estate,
								ResultRelInfo *rinfo)
{
	IfxFdwExecutionState *state = rinfo->ri_FdwState;

	elog(DEBUG3, "end foreign modify");

	/*
	 * If an INSERT cursor is in use, we must flush it, but only
	 * in case we weren't just called by an EXPLAIN...to prevent
	 * this, check wether the insert cursor was indeed opened, which
	 * is only the case if an insert action was really initiated (thus,
	 * a plain EXPLAIN never would have opened the cursor).
	 */
	if ((state->stmt_info.cursorUsage == IFX_INSERT_CURSOR)
		&& (state->stmt_info.call_stack & IFX_STACK_OPEN))
	{
		ifxFlushCursor(&state->stmt_info);
	}

	/*
	 * Catch any exceptions.
	 */
	ifxCatchExceptions(&state->stmt_info, 0);

	/*
	 * Dispose any allocated resources in case no error
	 * occurred.
	 */
	ifxRewindCallstack(&state->stmt_info);
}

/*
 * Prepare parameters for modify action.
 */
static void ifxPrepareParamsForModify(IfxFdwExecutionState *state,
									  PlannerInfo          *planInfo,
									  Index                 resultRelation,
									  ModifyTable          *plan,
									  Oid                   foreignTableOid)
{
	CmdType   operation = plan->operation;
	Relation  rel;

	/*
	 * Determine affected attributes of the modify action.
	 * No lock required, since the planner should already acquired
	 * one...
	 */
	rel = PG_RELATION_OPEN(foreignTableOid, NoLock);

	switch(operation)
	{
		case CMD_INSERT:
		{
			/*
			 * Retrieve attribute numbers for all columns. We apply all
			 * columns in an INSERT action.
			 */
			TupleDesc tupdesc = RelationGetDescr(rel);
			int       attnum;

			/*
			 * We need to set the correct cursor type here, since
			 * CMD_INSERT needs to establish its own cursor during
			 * planning. We don't have a corresponding foreign scan
			 * like in CMD_DELETE here, where we just reuse the existing
			 * cursor established during the foreign scan phase here. This
			 * make things complicater, but the only thing we need to bother
			 * at this point is that the execution state gets its cursorUsage
			 * adjusted accordingly. Any other modify action will already
			 * have the correct cursorUsage set during ifxGetForeignRelSize()!
			 */
			state->stmt_info.cursorUsage = IFX_INSERT_CURSOR;

			/*
			 * We need all columns for CMD_INSERT.
			 */
			for (attnum = 1; attnum <= tupdesc->natts; attnum++)
			{
				Form_pg_attribute pgattr = TUPDESC_GET_ATTR(tupdesc, attnum - 1);

				state->affectedAttrNums = lappend_int(state->affectedAttrNums,
													  pgattr->attnum);
			}

			/* ...and we're done */
			break;
		}

		case CMD_UPDATE:
		{
			/*
			 * For update, we only try to update the affected
			 * rows mentioned in the local UPDATE statement.
			 *
			 * No need to adjust the cursorUsage type, this already
			 * happened during ifxGetForeignRelSize().
			 *
			 * Shamelessly stolen from src/contrib/postgres_fdw.
			 */
			Bitmapset  *tmpset = NULL;
			int         colnum = -1;
			AttrNumber	col;

			RTE_UPDATED_COLS(planInfo, resultRelation, tmpset);

			while ((colnum = BMS_LOOKUP_COL(tmpset, colnum)) >= 0)
			{
				col = colnum + FirstLowInvalidHeapAttributeNumber;
				if (col <= InvalidAttrNumber)		/* shouldn't happen */
					elog(ERROR, "system-column update is not supported");
				state->affectedAttrNums = lappend_int(state->affectedAttrNums,
													  col);
			}

		}
		case CMD_DELETE:
			/*
			 * No op...
			 *
			 * NOTE: No need to adjust the cursorUsage type, this already
			 *       happened during ifxGetForeignRelSize().
			 */
			break;
		default:
			break;
	}

	PG_RELATION_CLOSE(rel, NoLock);
}

#endif

/*
 * Initializes the given IfxStatementInfo structure with
 * reasonable default values. Assigns a valid connection
 * identifier and refid from the given IfxConnectionInfo
 * structure and refid.
 *
 * The cursorUsage is set to IFX_SCROLL_CURSOR by default
 * and no special columns are assigned (IFX_NO_SPECIAL_COLS).
 */
static void ifxStatementInfoInit(IfxStatementInfo *info,
								 int refid)
{
	/* Assign the specified reference id. */
	info->refid = refid;

	memset(info->conname, '\0', IFX_CONNAME_LEN + 1);
	info->cursorUsage = IFX_SCROLL_CURSOR;

	info->query        = NULL;
	info->predicate    = NULL;
	info->cursor_name  = NULL;
	info->stmt_name    = NULL;
	info->descr_name   = NULL;
	info->sqlda        = NULL;
	info->ifxAttrCount = 0;
	info->ifxAttrDefs  = NULL;
	info->call_stack   = IFX_STACK_EMPTY;
	info->row_size     = 0;
	info->special_cols = IFX_NO_SPECIAL_COLS;

	memset(info->sqlstate, '\0', 6);
	info->exception_count = 0;
}

/*
 * Check and assign requested FDW options
 * to an IfxConnectionInfo structure.
 */
static void ifxAssignOptions(IfxConnectionInfo *coninfo,
							 List *options,
							 bool mandatory[IFX_REQUIRED_CONN_KEYWORDS])
{
	ListCell *elem;

	/* shortcut, nothing to do? */
	if (list_length(options) <= 0)
		return;

	foreach(elem, options)
	{
		DefElem *def = (DefElem *) lfirst(elem);

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

		if(strcmp(def->defname, "db_monetary") == 0)
		{
			coninfo->db_monetary = pstrdup(defGetString(def));
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

		if (strcmp(def->defname, "disable_rowid") == 0)
		{
			/*
			 * We don't bother about the specified value, we just
			 * honor if the setting was given.
			 */
			coninfo->disable_rowid = 1;
		}

		if (strcmp(def->defname, "enable_blobs") == 0)
		{
			/* we don't bother about the value passed
			 * to enable_blobs atm.
			 */
			coninfo->enable_blobs = 1;
		}

		if (strcmp(def->defname, "delimident") == 0)
		{
			/* we don't bother about the value
			 * passed to delimident.
			 */
			coninfo->delimident = 1;
		}

	}
}

/*
 * Check the specified foreign table OID if it has
 * AFTER EACH ROW triggers attached. ifxCheckForAfterRowTriggers()
 * assumes, that the caller already locked foreignTableOid for
 * inspection.
 */
static bool ifxCheckForAfterRowTriggers(Oid foreignTableOid,
										IfxFdwExecutionState *state,
										CmdType cmd)
{
	Relation rel    = PG_RELATION_OPEN(foreignTableOid, NoLock);
	bool     result = false;

	/*
	 * We are interested in after triggers only.
	 */
	if (rel->trigdesc)
	{
		switch (cmd) {
			case CMD_INSERT:
				result = (rel->trigdesc->trig_insert_after_row);
				break;
			case CMD_DELETE:
				result = (rel->trigdesc->trig_delete_after_row);
				break;
			case CMD_UPDATE:
				result = (rel->trigdesc->trig_update_after_row);
				break;
			default:
				result = false;
				break;
		}
	}

	PG_RELATION_CLOSE(rel, NoLock);

	return result;
}

/*
 * Extract the Informix ROWID from the current
 * tuple. The ROWID is encoded within a PostgreSQL ItemPointer
 * and returned to the caller.
 *
 * Returns NULL in case no ROWID could be extracted.
 */
static ItemPointer ifxGetRowIdForTuple(IfxFdwExecutionState *state)
{
	ItemPointer rowid;
	bool        isnull;

	/* init */
	rowid = NULL;

	/* Requires disable_rowid == false! */
	if (!state->use_rowid)
		return rowid;

	ifxColumnValueByAttNum(state, IFX_PGATTRCOUNT(state) - 1, &isnull);

	if (isnull)
		elog(ERROR, "unexpected NULL value for Informix ROWID");

	/*
	 * ROWID is a 4 Byte Integer encoding the logical
	 * page number within its first 3 bytes and the
	 * tuple slot number within the last byte. We encode
	 * this number into a CTID to transport the ROWID
	 * down to the modify action.
	 */

	rowid = (ItemPointer) palloc0fast(sizeof(ItemPointerData));
	ItemPointerSet(rowid,
				   DatumGetInt32(state->values[IFX_PGATTRCOUNT(state) - 1].val),
				   0);
	return rowid;
}

static HeapTuple ifxFdwMakeTuple(IfxFdwExecutionState *state,
								 Relation              rel,
								 ItemPointer           encoded_rowid,
								 TupleTableSlot       *slot)
{
	HeapTuple tuple;
	TupleDesc tupDesc;

	tupDesc  = RelationGetDescr(rel);
	tuple    = heap_form_tuple(tupDesc, slot->tts_values, slot->tts_isnull);

	if (state->use_rowid)
	{
		tuple->t_self = *encoded_rowid;
	}

	return tuple;
}

/*
 * Allocates memory for the specified structures to
 * make the usable to store Informix values retrieved by
 * ifxGetValuesFromTuple().
 */
static void
ifxSetupTupleTableSlot(IfxFdwExecutionState *state,
					   TupleTableSlot *tupleSlot)
{
	Assert((tupleSlot != NULL) && (state != NULL));

	tupleSlot->tts_nvalid = state->pgAttrCount;
	tupleSlot->tts_values = (Datum *) palloc(sizeof(Datum)
											 * tupleSlot->tts_nvalid);
	tupleSlot->tts_isnull = (bool *) palloc(sizeof(bool)
											* tupleSlot->tts_nvalid);
}

/*
 * Converts the current fetched tuple from informix into
 * PostgreSQL datums and store them into the specified
 * TupleTableSlot.
 */
static void
ifxGetValuesFromTuple(IfxFdwExecutionState *state,
					  TupleTableSlot       *tupleSlot)
{
	int i;

	/*
	 * Allocate slots for column value data.
	 *
	 * Used to retrieve Informix values by ifxColumnValueByAttnum().
	 */
	state->values = palloc0fast(sizeof(IfxValue)
								* state->stmt_info.ifxAttrCount);

	for (i = 0; i <= state->pgAttrCount - 1; i++)
	{
		bool isnull;

		elog(DEBUG5, "get column pg/ifx mapped attnum %d/%d",
			 i, PG_MAPPED_IFX_ATTNUM(state, i));

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
		 * Sanity check for assert-enabled builds
		 */
		Assert((PG_MAPPED_IFX_ATTNUM(state, i) >= 0)
			   && (PG_MAPPED_IFX_ATTNUM(state, i) < state->stmt_info.ifxAttrCount));

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
			/*
			 * If we encounter a NULL value from Informix where
			 * the local definition is NOT NULL, we throw an error.
			 *
			 * The PostgreSQL optimizer makes some assumptions about
			 * columns and their NULLability, so treat 'em accordingly.
			 */
			if (state->pgAttrDefs[i].attnotnull)
			{
				/* Reset remote resources */
				ifxRewindCallstack(&(state->stmt_info));
				elog(ERROR, "NULL value for column \"%s\" violates local NOT NULL constraint",
					 state->pgAttrDefs[i].attname);
			}

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
		tupleSlot->tts_values[i] = state->values[PG_MAPPED_IFX_ATTNUM(state, i)].val;
	}
}

/*
 * Moves the cursor one row forward and fetches the tuple
 * into the internal SQLDA informix structure referenced
 * by the specified state handle.
 *
 * If the specified IfxFdwExecutionState was prepared with a
 * ReScan event, ifxFetchTuple() will set the cursor to
 * the first tuple, in case the current cursor is SCROLLable.
 * If not, the cursor is reopened for a rescan.
 */
static IfxSqlStateClass
ifxFetchTuple(IfxFdwExecutionState *state)
{

	/*
	 * Fetch tuple from cursor
	 */
	if (state->rescan)
	{
		if (state->stmt_info.cursorUsage == IFX_SCROLL_CURSOR)
			ifxFetchFirstRowFromCursor(&state->stmt_info);
		else
		{
			elog(DEBUG3, "re-opening informix cursor in rescan state");
			ifxCloseCursor(&state->stmt_info);
			ifxCatchExceptions(&state->stmt_info, 0);

			ifxOpenCursorForPrepared(&state->stmt_info);
			ifxCatchExceptions(&state->stmt_info, 0);

			ifxFetchRowFromCursor(&state->stmt_info);
		}
		state->rescan = false;
	}
	else
	{
		ifxFetchRowFromCursor(&state->stmt_info);
	}

	/*
	 * Catch any informix exception. We also need to
	 * check for IFX_NOT_FOUND, in which case no more rows
	 * must be processed.
	 */
	return ifxSetException(&(state->stmt_info));

}

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

/*
 * Guts of connection establishing.
 *
 * Creates a new cached connection handle if not already cached
 * and sets the connection current. If already cached, make the
 * cached handle current, too.
 *
 * Returns the cached connection handle (either newly created or already
 * cached).
 */
static IfxCachedConnection * ifxSetupConnection(IfxConnectionInfo **coninfo,
												Oid foreignTableOid,
												IfxForeignScanMode mode,
												bool error_ok)
{
	IfxCachedConnection *cached_handle;
	bool                 conn_cached;
	IfxSqlStateClass     err;

	/*
	 * If not already done, initialize cache data structures.
	 */
	InformixCacheInit();

	/*
	 * Initialize connection structures and retrieve FDW options
	 *
	 * NOTE: IFX_IMPORT_SCHEMA requires a special case here, since
	 *       this operation is *not* based on a foreign table and does
	 *       setup its options itself. Thus we aren't allowed to process
	 *       FDW options here, too.
	 */

	if (mode != IFX_IMPORT_SCHEMA)
		*coninfo = ifxMakeConnectionInfo(foreignTableOid);

	elog(DEBUG1, "informix connection dsn \"%s\"", (*coninfo)->dsn);

	/*
	 * Set requested scan mode.
	 */
	(*coninfo)->scan_mode = mode;

	/*
	 * Lookup the connection name in the connection cache.
	 */
	cached_handle = ifxConnCache_add(foreignTableOid, *coninfo, &conn_cached);

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
			elog(error_ok ? ERROR : WARNING, "could not open connection to informix server: SQLCODE=%d",
				 ifxGetSqlCode());

			/* in case of !error_ok */
			return NULL;
		}
	}

	/*
	 * Give a notice if the connection supports transactions.
	 * Don't forget to register this information into the cached connection
	 * handle as well, since we didn't have this information available
	 * during connection startup and cached connection initialization.
	 *
	 * Also start a transaction. We do not care about the current state
	 * of the connection, ifxStartTransaction() does all necessary.
	 */
	if ((*coninfo)->tx_enabled == 1)
	{
		elog(DEBUG1, "informix database connection using transactions");
		cached_handle->con.tx_enabled = (*coninfo)->tx_enabled;

		/*
		 * NOTE: ifxMakeConnectionInfo() already saved the current
		 *       xact nest level, which ifxStartTransaction() needs to
		 *       know to do the right(tm) action. It will start
		 *       a new transaction or (if required) all corresponding
		 *       SAVEPOINTs on the remote server.
		 */

        /* ... and start the transaction */
        if (ifxStartTransaction(&cached_handle->con, *coninfo) < 0)
		{
			IfxSqlStateMessage message;
			ifxGetSqlStateMessage(1, &message);

			/*
			 * In case we can't emit a transaction, print a WARNING,
			 * but don't throw an error for now. We might do it
			 * the other way around, if that proves to be more correct,
			 * but leave it for now...
			 */
			elog(WARNING, "informix_fdw: could not start transaction: \"%s\", SQLSTATE %s",
				 message.text, message.sqlstate);
		}
	}

	/* ...the same for ANSI mode */
	if ((*coninfo)->db_ansi == 1)
	{
		elog(DEBUG1, "informix database runs in ANSI-mode");
		cached_handle->con.db_ansi = (*coninfo)->db_ansi;
	}

	/*
	 * Give a warning if we have mismatching DBLOCALE settings.
	 */
	if ((ifxGetSQLCAWarn(SQLCA_WARN_SET) == 'W')
		&& (ifxGetSQLCAWarn(SQLCA_WARN_DB_LOCALE_MISMATCH) == 'W'))
		elog(WARNING, "mismatching DBLOCALE \"%s\"",
			 (*coninfo)->db_locale);

	/*
	 * Give a NOTICE in case this is an INFORMIX SE
	 * database instance.
	 */
	if ((*coninfo)->is_obsolete == 1)
		elog(NOTICE, "connected to a Informix SE instance");

	return cached_handle;
}

/*
 * Setup a foreign scan. This will initialize all
 * state and connection structures as well as the
 * connection cache.
 *
 * Never ever create or prepare any database visible
 * actions here!
 */
static void ifxSetupFdwScan(IfxConnectionInfo **coninfo,
							IfxFdwExecutionState **state,
							List    **plan_values,
							Oid       foreignTableOid,
							IfxForeignScanMode mode)
{
	IfxCachedConnection  *cached_handle;

	/*
	 * Activate the required Informix database connection.
	 */
	cached_handle = ifxSetupConnection(coninfo, foreignTableOid, mode, true);

	/*
	 * Save parameters for later use
	 * in executor.
	 */
	*plan_values = NIL;

	/*
	 * Make a generic informix execution state
	 * structure.
	 */
	*state = makeIfxFdwExecutionState(cached_handle->con.usage);

	if ((*coninfo)->query)
	{
		/*
		 * If we use a foreign table based on a query, disallow
		 * ROWID retrieval.
		 */
		(*state)->use_rowid = 0;
		elog(DEBUG5, "informix_fdw: disabling ROWID forced");
	}
	else
	{
		/*
		 * If set by disable_rowid parameter, deactivate
		 * ROWID
		 */
		(*state)->use_rowid = ((*coninfo)->disable_rowid) ? false : true;
		elog(DEBUG5, "informix_fdw: using rowid %d", (*state)->use_rowid);
	}
}

/*
 * Returns a fully initialized pointer to
 * an IfxFdwExecutionState structure. All pointers
 * are initialized to NULL.
 *
 * refid should be a unique number identifying the returned
 * structure throughout the backend.
 */
static IfxFdwExecutionState *makeIfxFdwExecutionState(int refid)
{
	IfxFdwExecutionState *state = palloc(sizeof(IfxFdwExecutionState));

	ifxStatementInfoInit(&(state->stmt_info), refid);

	state->pgAttrCount = 0;
	state->pgAttrDefs  = NULL;
	state->values = NULL;
	state->rescan = false;
	state->affectedAttrNums = NIL;

	/*
	 * NOTE: This is set during preparing a modify action
	 *       in ifxBeginForeignModify(). This must not be
	 *       checked as long as ifxBeginForeignModify() has
	 *       done its work!
	 */
	state->rowid_attno = InvalidAttrNumber;
	state->use_rowid   = true;

	/*
	 * NOTE:
	 *
	 * Only PostgreSQL >= 9.4 will set and use has_after_trigger
	 * in case it detects AFTER EACH ROW triggers during the
	 * planning phase. Older version simply leave this to FALSE.
	 */
	state->has_after_row_triggers = false;

	return state;
}

#if PG_VERSION_NUM >= 90200

/*
 * Callback for ANALYZE
 */
static bool
ifxAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
					   BlockNumber *totalpages)
{
	IfxConnectionInfo    *coninfo;
	IfxCachedConnection  *cached_handle;
	IfxFdwExecutionState *state;
	ForeignTable         *foreignTable;
	ListCell             *elem;
	bool                  is_table;
	IfxPlanData           planData;
	IfxSqlStateClass      errclass;

	/*
	 * Examine wether query or table is specified to form
	 * the foreign table. In case we get a query, don't allow
	 * ANALYZE to be run...
	 */
	foreignTable = GetForeignTable(RelationGetRelid(relation));
	is_table     = false;
	*totalpages  = 1;

	foreach(elem, foreignTable->options)
	{
		DefElem *def = (DefElem *) lfirst(elem);
		if (strcmp(def->defname, "table") == 0)
			is_table = true;
	}

	/*
	 * We don't support analyzing a foreign table which is based
	 * on a SELECT. Proceed only in case coninfo->table is specified.
	 *
	 * We cannot simply error out here, since in case someone wants
	 * to ANALYZE a whole database this will abort the whole run...
	 *
	 * XXX: However, it might have already cached a database connection. Leave
	 * it for now, but we might want to close it, not sure...
	 */
	if (!is_table)
	{
		/* analyze.c already prints a WARNING message, so leave it out here */
		return false;
	}

	/*
	 * Retrieve a connection from cache or open a new one. Instruct
	 * an IFX_PLAN_SCAN, since we treat ifxAnalyzeForeignTable() which
	 * does all the setup required to do ifxAcquireSampleRows() separately.
	 *
	 * XXX: should we error out in case we get an connection error?
	 *      This will abandon the whole ANALYZE run when
	 *      issued against the whole database...
	 */
	if ((cached_handle = ifxSetupConnection(&coninfo,
											RelationGetRelid(relation),
											IFX_PLAN_SCAN,
											false)) == NULL)
	{
		/*
		 * again, analyze.c will print a "skip message" in case we abort
		 * this ANALYZE round, but give the user a hint what actually happened
		 * as an additional WARNING.
		 *
		 * Safe to exit here, since no database visible changes are made so far.
		 */
		ereport(WARNING,
				(errmsg("cannot establish remote database connection"),
				 errdetail("error retrieving or creating cached connection handle")));
		return false;
	}

	/*
	 * Catch any possible errors. Create a generic execution state which
	 * will carry any possible exceptions.
	 */
	state = makeIfxFdwExecutionState(cached_handle->con.usage);

	/*
	 * Retrieve basic statistics from Informix for this table,
	 * calculate totalpages according to them.
	 */
	ifxGetSystableStats(coninfo->tablename, &planData);

	/*
	 * Suppress any ERRORs, we don't want to interrupt a database-wide
	 * ANALYZE run...
	 */
	errclass = ifxSetException(&(state->stmt_info));

	if (errclass != IFX_SUCCESS)
	{

		if (errclass == IFX_NOT_FOUND)
		{
			/* no data found, use default 1 page
			 *
			 * XXX: could that really happen??
			 * systable *should* have a matching tuple for this
			 * table...
			 */
			elog(DEBUG1, "informix fdw: no remote stats data found for table \"%s\"",
				 RelationGetRelationName(relation));
		}

		/*
		 * All other error/warning cases should be catched. We do
		 * this here to suppress any ERROR, since we don't want to
		 * abandon a database-wise ANALYZE run...
		 *
		 * XXX: Actually i don't like this coding, maybe its better
		 *      to change ifxCatchExceptions() to mark any errors to
		 *      be ignored...
		 */
		PG_TRY();
		{
			ifxCatchExceptions(&(state->stmt_info), 0);
		}
		PG_CATCH();
		{
			IfxSqlStateMessage message;

			ifxGetSqlStateMessage(1, &message);
			ereport(WARNING, (errcode(ERRCODE_FDW_ERROR),
							  errmsg("informix FDW warning: \"%s\"",
									 message.text),
							  errdetail("SQLSTATE %s", message.sqlstate)));
		}
		PG_END_TRY();
	}
	else
	{
		elog(DEBUG2, "informix_fdw \"%s\" stats(nrows, npused, rowsize, pagesize): %2f, %2f, %d, %d",
			 RelationGetRelationName(relation), planData.nrows,
			 planData.npages, planData.row_size, planData.pagesize);

		/*
		 * Calculate and convert statistics information to
		 * match expectations of PostgreSQL...
		 *
		 * Default Informix installations run with 2KB block size
		 * but this could be configured depending on the tablespace.
		 *
		 * The idea is to calculate the numbers of pages to match
		 * the blocksize PostgreSQL currently uses to get a smarter
		 * cost estimate, thus the following formula is used:
		 *
		 * (npages * pagesize) / BLCKSZ
		 *
		 * If npage * pagesize is less than BLCKSZ, but the row estimate
		 * returned show a number larger than 0, we assume one block.
		 */
		if (planData.nrows > 0)
		{
			*totalpages
				= (BlockNumber) ((((planData.npages * planData.pagesize) / BLCKSZ) < 1)
								 ? 1
								 : (planData.npages * planData.pagesize) / BLCKSZ);
		}
		else
			*totalpages = 0;

		elog(DEBUG1, "totalpages = %d", *totalpages);
	}

	*func = ifxAcquireSampleRows;
	return true;
}

/*
 * Internal function for ANALYZE callback
 *
 * This is essentially the guts for ANALYZE <foreign table>
 */
static int
ifxAcquireSampleRows(Relation relation, int elevel, HeapTuple *rows,
					 int targrows, double *totalrows, double *totaldeadrows)
{
	Oid foreignTableId;
	IfxConnectionInfo *coninfo;
	IfxFdwExecutionState *state;
	List                 *plan_values;
	double                anl_state;
	IfxSqlStateClass      errclass;
	TupleDesc             tupDesc;
	Datum                *values;
	bool                 *nulls;
	int                   rows_visited;
	int                   rows_to_skip;

	elog(DEBUG1, "informix_fdw: analyze");

	/*
	 * Initialize stuff
	 */
	*totalrows      = 0;
	*totaldeadrows = 0;
	rows_visited   = 0;
	rows_to_skip   = -1; /* not set yet */
	foreignTableId = RelationGetRelid(relation);

	/*
	 * Establish a connection to the Informix server
	 * or get a previously cached one...there should
	 * already be a cached connection for this table, if
	 * ifxAnalyzeForeignTable() found some remote
	 * statistics to be reused.
	 *
	 * NOTE:
	 *
	 * ifxAnalyzeForeignTable should have prepare all required
	 * steps to prepare the scan finally, so we don't need to
	 * get a new scan refid...thus we pass IFX_BEGIN_SCAN to
	 * tell the connection cache that everything is already
	 * in place.
	 *
	 * This also initializes all required infrastructure
	 * to scan the remote table.
	 */
	ifxSetupFdwScan(&coninfo, &state, &plan_values,
					foreignTableId, IFX_BEGIN_SCAN);

	/*
	 * XXX: Move this into a separate function, shared
	 * code with ifxBeginForeignScan()!!!
	 */

	/*
	 * Prepare the scan. This creates a cursor we can use to
	 */
	ifxPrepareScan(coninfo, state);

	/*
	 * Get column definitions for local table...
	 */
	ifxPgColumnData(foreignTableId, state);

	/*
	 * Populate the DESCRIPTOR area, required to get
	 * the column values later...
	 */
	elog(DEBUG1, "populate descriptor area for statement \"%s\"",
		 state->stmt_info.stmt_name);
	ifxDescribeAllocatorByName(&state->stmt_info);
	ifxCatchExceptions(&state->stmt_info, IFX_STACK_ALLOCATE | IFX_STACK_DESCRIBE);

	/*
	 * Get the number of columns.
	 */
	state->stmt_info.ifxAttrCount = ifxDescriptorColumnCount(&state->stmt_info);
	elog(DEBUG1, "get descriptor column count %d",
		 state->stmt_info.ifxAttrCount);
	ifxCatchExceptions(&state->stmt_info, 0);

	/*
	 * XXX: It makes no sense to have a local column list with *more*
	 * columns than the remote table. I can't think of any use case
	 * for this atm, anyone?
	 */
	if (PG_VALID_COLS_COUNT(state) > state->stmt_info.ifxAttrCount)
	{
		ifxRewindCallstack(&(state->stmt_info));
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("foreign table \"%s\" has more columns than remote source",
							   get_rel_name(foreignTableId))));
	}

	state->stmt_info.ifxAttrDefs = palloc(state->stmt_info.ifxAttrCount
										  * sizeof(IfxAttrDef));

	/*
	 * Populate result set column info array.
	 */
	if ((state->stmt_info.row_size = ifxGetColumnAttributes(&state->stmt_info)) == 0)
	{
		/* oops, no memory to allocate? Something surely went wrong,
		 * so abort */
		ifxRewindCallstack(&state->stmt_info);
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
	state->stmt_info.data = (char *) palloc0(state->stmt_info.row_size);
	state->stmt_info.indicator = (short *) palloc0(sizeof(short)
												   * state->stmt_info.ifxAttrCount);

	/*
	 * Assign sqlvar pointers to the allocated memory area.
	 */
	ifxSetupDataBufferAligned(&state->stmt_info);

	/*
	 * Open the cursor.
	 */
	elog(DEBUG1, "open cursor \"%s\"",
		 state->stmt_info.cursor_name);
	ifxOpenCursorForPrepared(&state->stmt_info);
	ifxCatchExceptions(&state->stmt_info, IFX_STACK_OPEN);

	/*
	 * Okay, we are ready to read the tuples from the remote
	 * table now.
	 */
	anl_state = anl_init_selection_state(targrows);

	/*
	 * Prepare tuple...
	 */
	tupDesc = RelationGetDescr(relation);

	/* XXX: might differ, if we have dynamic target list
	 *      some time in the future */
	values  = (Datum *) palloc(state->pgAttrCount
							   * sizeof(Datum));
	nulls   = (bool *) palloc(state->pgAttrCount
							  * sizeof(bool));

	/*
	 * Allocate the data buffer structure required to
	 * extract column values via our API...
	 */
	state->values = palloc(sizeof(IfxValue)
						   * state->stmt_info.ifxAttrCount);

	/* Start the scan... */
	ifxFetchRowFromCursor(&(state->stmt_info));

	/*
	 * Catch exception, especially IFX_NOT_FOUND...
	 */
	errclass = ifxSetException(&(state->stmt_info));

	while (errclass == IFX_SUCCESS)
	{
		int i;

		*totalrows += 1;

		/*
		 * Allow delay...
		 */
		vacuum_delay_point();

		/*
		 * Read the tuple...
		 */
		for (i = 0; i <= state->pgAttrCount - 1; i++)
		{
			bool isnull;

			elog(DEBUG5, "get column pg/ifx mapped attnum %d/%d",
				 i, PG_MAPPED_IFX_ATTNUM(state, i));

			/* ignore dropped columns */
			if (state->pgAttrDefs[i].attnum < 0)
			{
				values[i] = PointerGetDatum(NULL);
				nulls[i]  = true;
				continue;
			}

			/*
			 * Get the converted value from Informix
			 * (we get a PostgreSQL datum from the conversion
			 * routines, suitable to be assigned directly to our
			 * values array).
			 */
			ifxColumnValueByAttNum(state, i, &isnull);

			/*
			 * Take care for NULL returned by Informix.
			 */
			if (isnull)
			{
				values[i] = PointerGetDatum(NULL);
				nulls[i]  = true;
				continue;
			}

			/*
			 * If a datum is not NULL, ifxColumnValueByAttNum()
			 * had converted the column value into a proper
			 * PostgreSQL datum.
			 */
			nulls[i] = false;
			values[i] = state->values[PG_MAPPED_IFX_ATTNUM(state, i)].val;
		}

		/*
		 * Built a HeapTuple object from the current row.
		 */
		if (rows_visited < targrows)
		{
			rows[rows_visited++] = heap_form_tuple(tupDesc, values, nulls);
		}
		else
		{
			/*
			 * Follow Vitter's algorithm as defined in
			 * src/backend/command/analyze.c.
			 *
			 * See function acquire_sample_rows() for details.
			 *
			 */

			if (rows_to_skip < 0)
				rows_to_skip = anl_get_next_S(*totalrows, targrows, &anl_state);

			if (rows_to_skip <= 0)
			{
				/*
				 * Found a suitable tuple, replace
				 * a random tuple within the rows array
				 */
				int k = (int) (targrows * anl_random_fract());
				Assert(k >= 0 && k < targrows);

				/* Free the old tuple */
				heap_freetuple(rows[k]);

				/* Assign a new one... */
				rows[k] = heap_form_tuple(tupDesc, values, nulls);
			}

			rows_to_skip -= 1;
		}

		/*
		 * Next one ...
		 */
		ifxFetchRowFromCursor(&(state->stmt_info));
		errclass = ifxSetException(&(state->stmt_info));
	}

	/* Done, cleanup ... */
	ifxRewindCallstack(&state->stmt_info);

	ereport(elevel,
			(errmsg("\"%s\": remote Informix table contains %.0f rows; "
					"%d rows in sample",
					RelationGetRelationName(relation),
					*totalrows, rows_visited)));

	return rows_visited;
}

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

	elog(DEBUG3, "informix_fdw: get foreign relation size, cmd %d",
		planInfo->parse->commandType);

	planState = palloc(sizeof(IfxFdwPlanState));

	/*
	 * Establish remote informix connection or get
	 * a already cached connection from the informix connection
	 * cache.
	 */
	ifxSetupFdwScan(&coninfo, &state, &plan_values,
					foreignTableId, IFX_PLAN_SCAN);

	/*
	 * Check for predicates that can be pushed down
	 * to the informix server, but skip it in case the user
	 * has set the disable_predicate_pushdown option...
	 */
	if (coninfo->predicate_pushdown)
	{
		/*
		 * Also save a list of excluded RestrictInfo structures not carrying any
		 * predicate found to be pushed down by ifxFilterQuals(). Those will
		 * passed later to ifxGetForeignPlan()...
		 */
		state->stmt_info.predicate = ifxFilterQuals(planInfo, baserel,
													&(planState->excl_restrictInfo),
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
	 *
	 * If we have an UPDATE or DELETE query, the foreign scan needs to
	 * employ an FOR UPDATE cursor, since we are going to reuse it
	 * during modify.
	 *
	 * There's also another difficulty here: an updatable cursor cannot
	 * be scrollable.
	 *
	 * This must happen before calling ifxPrepareScan(), this this will
	 * generate the SELECT query passed to the cursor later on!
	 *
	 * Also note: It doesn't make sense to bother with CMD_INSERT
	 * here at all, since ifxGetForeignRelSize() won't ever
	 * be called by this action.
	 */
	if ((planInfo->parse->commandType == CMD_UPDATE)
		|| (planInfo->parse->commandType == CMD_DELETE))
	{
		state->stmt_info.cursorUsage = IFX_UPDATE_CURSOR;
	}

	ifxPrepareScan(coninfo, state);

	/*
	 * Now it should be possible to get the cost estimates
	 * from the actual cursor.
	 */
	coninfo->planData.estimated_rows = (double) ifxGetSQLCAErrd(SQLCA_NROWS_PROCESSED);
	coninfo->planData.costs          = (double) ifxGetSQLCAErrd(SQLCA_NROWS_WEIGHT);

	/*
	 * Estimate total_cost in conjunction with the per-tuple cpu cost
	 * for FETCHing each particular tuple later on.
	 */
	coninfo->planData.total_costs    = coninfo->planData.costs
		+ (coninfo->planData.estimated_rows * cpu_tuple_cost);

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
#if PG_VERSION_NUM >= 90600
									 NULL,
#endif
									 baserel->rows,
									 planState->coninfo->planData.costs,
									 planState->coninfo->planData.total_costs,
									 NIL,
									 NULL,
#if PG_VERSION_NUM >= 90500
									 NULL,
#endif
									 NIL));
}

static ForeignScan *ifxGetForeignPlan(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreignTableId,
									  ForeignPath *best_path,
									  List *tlist,
									  List *scan_clauses
#if PG_VERSION_NUM >= 90500
									  , Plan *outer_plan
#endif
	)
{
	Index scan_relid;
	IfxFdwPlanState  *planState;
	List             *plan_values;

	elog(DEBUG3, "informix_fdw: get foreign plan");

	scan_relid = baserel->relid;
	planState = (IfxFdwPlanState *) baserel->fdw_private;

	/*
	 * In case we are allowed to push down query predicates, ifxFilterQuals()
	 * would have filtered out all remote scan clauses and we need to
	 * examine all excluded clauses only.
	 *
	 * NOTE: ifxFilterQuals() won't be called in case predicate_pushdown is
	 *       disabled. In this case we don't filter at all so pass all
	 *       scan claususes "as-is" but with all pseudoconstants filtered.
	 */
	if (planState->coninfo->predicate_pushdown)
		scan_clauses = extract_actual_clauses(planState->excl_restrictInfo, false);
	else
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
							plan_values
#if PG_VERSION_NUM >= 90500
							,NIL
							,NIL
							,outer_plan
#endif
		);
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
	List                 *excl_restrictInfo;

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
	ifxSetupFdwScan(&coninfo, &state, &plan_values,
					foreignTableOid, IFX_PLAN_SCAN);

	/*
	 * Check for predicates that can be pushed down
	 * to the informix server, but skip it in case the user
	 * has set the disable_predicate_pushdown option...
	 */
	if (coninfo->predicate_pushdown)
	{
		state->stmt_info.predicate = ifxFilterQuals(planInfo, baserel,
													&excl_restrictInfo,
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
				 * log Informix runtime error.
				 *
				 * There's no ERRCODE_FDW_FATAL, so we go with a HV000 error
				 * code for now, but print out the error message as ERROR.
				 *
				 * A runtime error normally means a SQL error. Formerly, we did
				 * a FATAL here, but this stroke me as far to hard (it will exit
				 * the backend). Go with an ERROR instead...
				 */
				ifxRewindCallstack(state);

				EXPLICIT_FALL_THROUGH;
			case IFX_ERROR:
			case IFX_ERROR_INVALID_NAME:
				/* log ERROR */
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
			case IFX_ERROR_TABLE_NOT_FOUND:
				/* log missing FDW table */
				ereport(ERROR, (errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
								errmsg("informix FDW missing table: \"%s\"",
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
	int               pgAttrIndex;
	int               ifxAttrIndex;

	pgAttrIndex  = 0;
	ifxAttrIndex = 0;
	festate->pgDroppedAttrCount = 0;

	/* open foreign table, should be locked already */
	foreignRel = PG_RELATION_OPEN(foreignTableOid, NoLock);
	festate->pgAttrCount = RelationGetNumberOfAttributes(foreignRel);
	PG_RELATION_CLOSE(foreignRel, NoLock);

	/*
	 * Use IFX_PGATTRCOUNT to reflect extra space for retrieval of ROWID,
	 * if necessary.
	 */
	festate->pgAttrDefs = palloc0fast(sizeof(PgAttrDef) * IFX_PGATTRCOUNT(festate));

	/*
	 * Get all attributes for the given foreign table.
	 */
	attrRel = PG_RELATION_OPEN(AttributeRelationId, AccessShareLock);
	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(foreignTableOid));
	ScanKeyInit(&key[1], Anum_pg_attribute_attnum,
				BTGreaterStrategyNumber, F_INT2GT,
				Int16GetDatum((int16)0));
	scan = systable_beginscan(attrRel, AttributeRelidNumIndexId, true,
							  IFX_SYSTABLE_SCAN_SNAPSHOT, 2, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		attrTuple = (Form_pg_attribute) GETSTRUCT(tuple);

		/*
		 * Current PostgreSQL attnum.
		 */
		++pgAttrIndex;

		/*
		 * Ignore dropped columns.
		 */
		if (attrTuple->attisdropped)
		{
			festate->pgAttrDefs[pgAttrIndex - 1].attnum = -1;

			/*
			 * In case of dropped columns, we differ from the attribute
			 * numbers used for Informix. Record them accordingly.
			 */
			festate->pgAttrDefs[pgAttrIndex - 1].ifx_attnum = -1;
			festate->pgAttrDefs[pgAttrIndex - 1].atttypid = -1;
			festate->pgAttrDefs[pgAttrIndex - 1].atttypmod = -1;
			festate->pgAttrDefs[pgAttrIndex - 1].attname = NULL;
			festate->pgDroppedAttrCount++;
			continue;
		}

		/*
		 * Don't rely on pgAttrIndex directly.
		 *
		 * RelationGetNumberOfAttributes() always counts the number
		 * of attributes *including* dropped columns.
		 *
		 * Increment ifxAttrIndex only in case we don't have
		 * a dropped column. Otherwise we won't match the
		 * Informix attribute list.
		 */
		++ifxAttrIndex;

		/*
		 * Protect against corrupted numbers in pg_class.relnatts
		 * and number of attributes retrieved from pg_attribute.
		 */
		if (pgAttrIndex > festate->pgAttrCount)
		{
			systable_endscan(scan);
			PG_RELATION_CLOSE(attrRel, AccessShareLock);
			elog(ERROR, "unexpected number of attributes in foreign table");
		}

		/*
		 * Save the attribute and all required properties for
		 * later usage.
		 */
		festate->pgAttrDefs[pgAttrIndex - 1].attnum = attrTuple->attnum;
		festate->pgAttrDefs[pgAttrIndex - 1].ifx_attnum = ifxAttrIndex;
		festate->pgAttrDefs[pgAttrIndex - 1].atttypid = attrTuple->atttypid;
		festate->pgAttrDefs[pgAttrIndex - 1].atttypmod = attrTuple->atttypmod;
		festate->pgAttrDefs[pgAttrIndex - 1].attname = pstrdup(NameStr(attrTuple->attname));
		festate->pgAttrDefs[pgAttrIndex - 1].attnotnull = attrTuple->attnotnull;

		elog(DEBUG5, "mapped attnum PG/IFX %d => %d",
			 festate->pgAttrDefs[pgAttrIndex - 1].attnum,
			 PG_MAPPED_IFX_ATTNUM(festate, pgAttrIndex - 1));
	}

	/*
	 * Request information for the resjunk ROWID column.
	 */
	if (festate->use_rowid)
	{
		Assert(IFX_PGATTRCOUNT(festate) > festate->pgAttrCount);

		festate->pgAttrDefs[IFX_PGATTRCOUNT(festate) - 1].attnum = IFX_PGATTRCOUNT(festate);
		festate->pgAttrDefs[IFX_PGATTRCOUNT(festate) - 1].ifx_attnum = IFX_PGATTRCOUNT(festate);
		festate->pgAttrDefs[IFX_PGATTRCOUNT(festate) - 1].atttypid   = INT4OID;
		festate->pgAttrDefs[IFX_PGATTRCOUNT(festate) - 1].atttypmod  = -1;
		festate->pgAttrDefs[IFX_PGATTRCOUNT(festate) - 1].attname    = "rowid";
		festate->pgAttrDefs[IFX_PGATTRCOUNT(festate) - 1].attnotnull = true;
	}

	/* finish */
	systable_endscan(scan);
	PG_RELATION_CLOSE(attrRel, AccessShareLock);
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

	if (strcmp(def->defname, "db_monetary") == 0)
	{
		if (coninfo->db_monetary)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: db_monetary(%s)",
								   defGetString(def))));

		coninfo->db_monetary = defGetString(def);
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
							errmsg("conflicting or redundant options: client_locale(%s)",
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
			/* Don't leak the password into log messages */
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("conflicting or redundant options: password")));

		coninfo->password = defGetString(def);
	}

	if (strcmp(def->defname, "query") == 0)
	{
		if (coninfo->tablename)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("conflicting options: query cannot be used with table")
						));

		if (coninfo->query)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("conflicting or redundant options: query (%s)", defGetString(def))
						));

		coninfo->tablename = defGetString(def);
	}

	if (strcmp(def->defname, "table") == 0)
	{
		if (coninfo->query)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
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
	uint32 key;

	buf = makeStringInfo();
	initStringInfo(buf);

	appendStringInfo(buf, "%s%s%s", coninfo->username, coninfo->database,
					 coninfo->servername);

	/*
	 * We create a hash key out of the connection string, which
	 * the forms the connection identifier with a "con" prefix
	 * attached.
	 */
	key = string_hash(buf->data, buf->len);

	/*
	 * Reuse the existing string buffer, original content
	 * not needed anymore.
	 */
	resetStringInfo(buf);
	appendStringInfo(buf, "con_%u", key);

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
	fdwRoutine->AnalyzeForeignTable = ifxAnalyzeForeignTable;

	#endif

	/*
	 * Since PostgreSQL 9.3 we support updatable foreign tables.
	 */
	#if PG_VERSION_NUM >= 90300

	fdwRoutine->AddForeignUpdateTargets = ifxAddForeignUpdateTargets;
	fdwRoutine->PlanForeignModify       = ifxPlanForeignModify;
	fdwRoutine->BeginForeignModify      = ifxBeginForeignModify;
	fdwRoutine->ExecForeignInsert       = ifxExecForeignInsert;
	fdwRoutine->ExecForeignDelete       = ifxExecForeignDelete;
	fdwRoutine->ExecForeignUpdate       = ifxExecForeignUpdate;
	fdwRoutine->EndForeignModify        = ifxEndForeignModify;
	fdwRoutine->IsForeignRelUpdatable   = ifxIsForeignRelUpdatable;
	fdwRoutine->ExplainForeignModify    = ifxExplainForeignModify;

	#endif

    #if PG_VERSION_NUM >= 90500

	fdwRoutine->ImportForeignSchema = ifxImportForeignSchema;

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
	IfxFdwExecutionState *fdw_state;
	fdw_state = (IfxFdwExecutionState *) state->fdw_state;

	elog(DEBUG1, "informix_fdw: rescan");

	/*
	 * We're in a rescan condition on our foreign table.
	 */
	fdw_state->rescan = true;
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

	elog(DEBUG1, "validator called");

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
	bool           mandatory[IFX_REQUIRED_CONN_KEYWORDS] = { false, false, false, false };
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
	ifxAssignOptions(coninfo, options, mandatory);

	if ((coninfo->query == NULL)
		 && (coninfo->tablename == NULL))
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("invalid options for remote table \"%s\"",
							   get_rel_name(foreignTable->relid)),
						errdetail("either parameter \"query\" or \"table\" is missing")));
	}

	/*
	 * Check for all other mandatory options
	 */
	for (i = 0; i < IFX_REQUIRED_CONN_KEYWORDS; i++)
	{
		if (!mandatory[i])
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
							errmsg("missing required FDW options (informixserver, informixdir, client_locale, database)")));
	}

}

/*
 * Returns a hashed identifier generate from the contents
 * of buf with the prefix specified in p. p isn't allowed to
 * be null, and neither buf. If one of the arguments are NULL or
 * have a zero-length string specified, the function will return NULL.
 */
__attribute__((unused)) static char * ifxHashIdentifier(StringInfoData *buf, char *prefix) {

	char *result;
	size_t ident_len = 0;
	uint32 key;

	if ((buf == NULL) || (prefix == NULL))
		return NULL;

	if (strlen(prefix) <= 0)
		return NULL;

	if (buf->len <= 0)
		return NULL;

	/*
	 * Hash the identifier string.
	 */
	key = string_hash(buf->data, buf->len);

	/*
	 * Copy over the statement identifier in a
	 * dynamically palloc'ed string buffer, since it might
	 * get reused over various call sites until query/transaction
	 * ends.
	 */
	ident_len = buf->len + strlen(prefix);
	result = (char *) palloc0(ident_len + 1);
	snprintf(result, ident_len, "%s%u",
			 prefix, key);

	return result;
}

/*
 * Generate a unique statement identifier to create
 * on the target database. Informix requires us to build
 * a unique name among all concurrent connections.
 *
 * Returns a palloc'ed string containing a statement identifier
 * suitable to pass to an Informix database.
 */
static char *ifxGenStatementName(int stmt_id)
{
	char *stmt_name = NULL;
	StringInfoData *buf;

	buf = makeStringInfo();
	initStringInfo(buf);

	appendStringInfo(buf, "s%d_%d",
					 MyBackendId, stmt_id);

	stmt_name = (char *) palloc0(buf->len + 1);
	strncpy(stmt_name, buf->data, buf->len);

	/*
	 * Free temporary resources.
	 */
	resetStringInfo(buf);
	pfree(buf);

	if (stmt_name == NULL)
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER),
						errmsg("could not generate informix identifier for statement")));

	elog(DEBUG5, "generated statement name %s", stmt_name);

	return stmt_name;
}

static char *ifxGenDescrName(int descr_id)
{
	char *descr_name = NULL;
	StringInfoData *buf;

	buf = makeStringInfo();
	initStringInfo(buf);

	appendStringInfo(buf, "d%d_%d",
					 MyBackendId, descr_id);

	descr_name = (char *) palloc0(buf->len + 1);
	strncpy(descr_name, buf->data, buf->len);

	/*
	 * Free temporary resources.
	 */
	resetStringInfo(buf);
	pfree(buf);

	if (descr_name == NULL)
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER),
						errmsg("could not generate informix descriptor identifier for statement")));

	elog(DEBUG5, "generated descriptor name %s", descr_name);

	return descr_name;
}


/*
 * Generate a unique cursor identifier
 *
 * The specified curid should be a unique number
 * identifying the returned cursor name uniquely throughout
 * the backend.
 */
static char *ifxGenCursorName(int curid)
{
	char *cursor_name = NULL;
	StringInfoData *buf;

	buf = makeStringInfo();
	initStringInfo(buf);

	appendStringInfo(buf, "c%d_%d",
					 MyBackendId, curid);

	cursor_name = (char *) palloc0(buf->len + 1);
	strncpy(cursor_name, buf->data, buf->len);

	/*
	 * Free temporary resources.
	 */
	resetStringInfo(buf);
	pfree(buf);

	if (cursor_name == NULL)
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER),
						errmsg("could not generate informix cursor identifier for statement")));

	elog(DEBUG5, "generated cursor name %s", cursor_name);

	return cursor_name;
}

/*
 * Prepare informix query object identifier
 */
static void ifxPrepareParamsForScan(IfxFdwExecutionState *state,
									IfxConnectionInfo *coninfo)
{
	StringInfoData *buf;
	char           *rowid_str;

	buf = makeStringInfo();
	initStringInfo(buf);

	/*
	 * We depend on ROWID per default.
	 */
	if (state->use_rowid == 1)
		rowid_str = ", rowid";
	else
		rowid_str = "";

	/*
	 * Record the given query and pass it over
	 * to the state structure.
	 */
	if (coninfo->query)
	{
		/*
		 * IMPORTANT: When a query was specified, we don't
		 *            retrieve the ROWID. This doesn't matter here,
		 *            since we don't support modifying such
		 *            a foreign table anyways. Force disabling rowid
		 *            in this case to be consistent regardless.
		 */
		state->use_rowid = 0;

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
		/*
		 * NOTE:
		 *
		 * Don't declare the query as READ ONLY. We can't really
		 * distinguish wether the scan is related to a DELETE or UPDATE.
		 *
		 * XXX:
		 *
		 * We declare the Informix transaction with REPEATABLE READ
		 * isolation level. Consider different modes here, e.g. FOR UPDATE
		 * with READ COMMITTED...
		 *
		 * In case disable_rowid was specified, we cannot rely on the
		 * ROWID (for example, if fragmented tables are used on the Informix
		 * server).
		 */
		if ((state->stmt_info.predicate != NULL)
			&& (strlen(state->stmt_info.predicate) > 0)
			&& coninfo->predicate_pushdown)
		{
			appendStringInfo(buf, "SELECT *%s FROM %s WHERE %s",
							 rowid_str,
							 ifxQuoteIdent(coninfo, coninfo->tablename),
							 state->stmt_info.predicate);
		}
		else
		{
			appendStringInfo(buf, "SELECT *%s FROM %s",
							 rowid_str,
							 ifxQuoteIdent(coninfo, coninfo->tablename));
		}
	}

	/*
	 * In case we got a foreign scan initiated by
	 * an UPDATE/DELETE DML command, we need to do a
	 * FOR UPDATE, otherwise the cursor won't be updatable
	 * later in the modify actions.
	 */
	if (state->stmt_info.cursorUsage == IFX_UPDATE_CURSOR)
	{
		appendStringInfoString(buf, " FOR UPDATE");
	}

	state->stmt_info.query = buf->data;

	/*
	 * Save the connection identifier.
	 */
	strncpy(state->stmt_info.conname, coninfo->conname, IFX_CONNAME_LEN);
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
	__attribute__((unused)) IfxCachedConnection  *cached;
	Oid                   foreignTableOid;
	bool                  conn_cached;
	List                 *plan_values;

	elog(DEBUG3, "informix_fdw: begin scan");

	plan_values = PG_SCANSTATE_PRIVATE_P(node);
	foreignTableOid = RelationGetRelid(node->ss.ss_currentRelation);
	Assert((foreignTableOid != InvalidOid));
	coninfo = ifxMakeConnectionInfo(foreignTableOid);

	/*
	 * Tell the connection cache that we are about to start to scan
	 * the remote table.
	 */
	coninfo->scan_mode = IFX_BEGIN_SCAN;

	/*
	 * We should have a cached connection entry for the requested table.
	 */
	cached = ifxConnCache_add(foreignTableOid, coninfo,
							  &conn_cached);

	/* should not happen here */
	Assert(conn_cached && cached != NULL);

	/* Initialize generic execution state structure */
	festate = makeIfxFdwExecutionState(-1);

	/*
	 * Make the connection current (otherwise we might
	 * get confused).
	 */
	if (conn_cached)
	{
		ifxSetConnection(coninfo);
	}

	/*
	 * Check connection status.
	 */
	if ((ifxConnectionStatus() != IFX_CONNECTION_OK)
		&& (ifxConnectionStatus() != IFX_CONNECTION_WARN))
	{
		elog(ERROR, "could not set requested informix connection");
	}

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

	/* EXPLAIN without ANALYZE... */
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

	/*
	 * XXX: It makes no sense to have a local column list with *more*
	 * columns than the remote table. I can't think of any use case
	 * for this atm, anyone?
	 */
	if (PG_VALID_COLS_COUNT(festate) > festate->stmt_info.ifxAttrCount)
	{
		ifxRewindCallstack(&(festate->stmt_info));
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("foreign table \"%s\" has more columns than remote source",
							   get_rel_name(foreignTableOid))));
	}

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
	 * Throw an error in case we select from a relation with
	 * BLOB types and enable_blobs FDW option is unset. We must not
	 * use a SCROLL cursor in this case. Switching the cursor options
	 * at this point is too late, since we already DESCRIBEd and PREPAREd
	 * the cursor. Alternatively, we could re-PREPARE the cursor as a
	 * NO SCROLL cursor again, but this strikes me as too dangerous (consider
	 * changing table definitions in the meantime).
	 *
	 * NOTE: A non-scrollable cursor requires a serialized transaction to
	 *       be safe. However, we don't enforce this isolation atm, since
	 *       Informix databases with no logging would not be queryable at all.
	 *       But someone have to keep in mind, that a ReScan of the foreign
	 *       table could lead to inconsistent data due to changed results
	 *       sets.
	 */
	if ((festate->stmt_info.special_cols & IFX_HAS_BLOBS)
		&& (festate->stmt_info.cursorUsage == IFX_SCROLL_CURSOR))
	{
		ifxRewindCallstack(&festate->stmt_info);
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("could not use a SCROLL cursor to query an "
							   "informix table with blobs"),
						errhint("set enable_blobs=1 to your foreign table "
								"to use a NO SCROLL cursor")));
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

/*
 * Extract the corresponding Informix value for the given PostgreSQL attnum
 * from the SQLDA structure. The specified attnum should be the target column
 * of the local table definition and is translated internally to the matching
 * source column on the remote table.
 */
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
	state->values[PG_MAPPED_IFX_ATTNUM(state, attnum)].def
		= &state->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM(state, attnum)];
	IFX_SETVAL_P(state, attnum, PointerGetDatum(NULL));
	*isnull = false;

	/*
	 * Retrieve values from Informix and try to convert
	 * into an appropiate PostgreSQL datum.
	 */

	switch (IFX_ATTRTYPE_P(state, attnum))
	{
		case IFX_FLOAT:
		case IFX_SMFLOAT:
		{
			Datum dat;

			dat = convertIfxFloat(state, attnum);
			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));

			/*
			 * Check for errors. convertIfxFloatAsString() might
			 * return NULL in case of an error, so make sure we
			 * catch conversion errors.
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
		case IFX_SMALLINT:
			/*
			 * All int values are handled
			 * by convertIfxInt()...so fall through.
			 */
		case IFX_INTEGER:
		case IFX_SERIAL:
		case IFX_INT8:
		case IFX_BIGSERIAL:
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
			 * Guard against invalid conversion
			 * attempts.
			 */
			if (! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("conversion in column \"%s\" with unsupported character type mapping",
									   state->pgAttrDefs[attnum].attname)));
			}

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
				elog(ERROR, "could not convert informix character type into pg type %u",
					 PG_ATTRTYPE_P(state, attnum));
			}

			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_BYTES:
		case IFX_TEXT:
		{
			Datum dat;

			dat = convertIfxSimpleLO(state, attnum);

			/*
			 * Check for invalid datum conversion.
			 */
			if (! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				elog(ERROR, "could not convert informix LO type into pg type %u",
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
				elog(ERROR, "could not convert informix boolean into pg type %u",
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
				elog(ERROR, "could not convert informix date into pg type %u",
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
				elog(ERROR, "could not convert informix datetime into pg type %u",
					 PG_ATTRTYPE_P(state, attnum));
			}

			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_INTERVAL:
		{
			/* SQLINTERVAL value */
			Datum dat;
			dat = convertIfxInterval(state, attnum);

			/* Valid datum ? */
			if ((DatumGetPointer(dat) == NULL)
				&& ! IFX_ATTR_IS_VALID_P(state, attnum))
			{
				ifxRewindCallstack(&state->stmt_info);
				elog(ERROR, "could not convert informix interval into pg type %u",
					 PG_ATTRTYPE_P(state, attnum));
			}

			*isnull = (IFX_ATTR_ISNULL_P(state, attnum));
			IFX_SETVAL_P(state, attnum, dat);
			break;
		}
		case IFX_MONEY:
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
	Relation              rel       = node->ss.ss_currentRelation;
	IfxConnectionInfo    *coninfo;
	IfxFdwExecutionState *state;
	IfxSqlStateClass      errclass;
	Oid                   foreignTableOid;
	bool                  conn_cached;

	state = (IfxFdwExecutionState *) node->fdw_state;

	elog(DEBUG3, "informix_fdw: iterate scan");

	/*
	 * Make the informix connection belonging to this
	 * iteration current.
	 */
	foreignTableOid = RelationGetRelid(node->ss.ss_currentRelation);
	coninfo= ifxMakeConnectionInfo(foreignTableOid);

	/*
	 * Set appropiate scan mode.
	 */
	coninfo->scan_mode = IFX_ITERATE_SCAN;

	/*
	 * ...and get the handle.
	 */
	ifxConnCache_add(foreignTableOid, coninfo, &conn_cached);

	/*
	 * Make the connection current (otherwise we might
	 * get confused).
	 */
	if (conn_cached)
	{
		ifxSetConnection(coninfo);
	}

	/*
	 * Check connection status.
	 */
	if ((ifxConnectionStatus() != IFX_CONNECTION_OK)
		&& (ifxConnectionStatus() != IFX_CONNECTION_WARN))
	{
		elog(ERROR, "could not set requested informix connection");
	}

	/*
	 * Prepare a virtual tuple.
	 */
	ExecClearTuple(tupleSlot);

	/*
	 * Catch any informix exception. We also need to
	 * check for IFX_NOT_FOUND, in which case no more rows
	 * must be processed.
	 */
	errclass = ifxFetchTuple(state);

	if (errclass != IFX_SUCCESS)
	{

		if (errclass == IFX_NOT_FOUND)
		{
			/*
			 * Create an empty tuple slot and we're done.
			 */
			elog(DEBUG2, "informix fdw scan end");

			/* XXX: not required here ifxRewindCallstack(&(state->stmt_info)); */
			return tupleSlot;
		}

		/*
		 * All other error/warning cases should be catched.
		 */
		ifxCatchExceptions(&(state->stmt_info), 0);
	}

	ifxSetupTupleTableSlot(state, tupleSlot);

	/*
	 * The cursor should now be positioned at the current row
	 * we want to retrieve. Loop through the columns and retrieve
	 * their values.
	 */
	ifxGetValuesFromTuple(state, tupleSlot);

	/*
	 * Get the ROWID for the current value, if required.
	 */
	if (state->use_rowid)
	{
		ItemPointer iptr;
		HeapTuple   tuple;

		iptr = ifxGetRowIdForTuple(state);
		tuple = ifxFdwMakeTuple(state, rel, iptr, tupleSlot);

		tuple->t_self = *iptr;

		/* PostgreSQL 12 has changed the heapam API a little */
#if PG_VERSION_NUM >= 120000
		ExecStoreHeapTuple(tuple,
				   tupleSlot,
				   false);
#else
		ExecStoreTuple(tuple,
					   tupleSlot,
					   InvalidBuffer,
					   false);
#endif
	}
	else
		ExecStoreVirtualTuple(tupleSlot);

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
	memset(coninfo->conname, '\0', IFX_CONNAME_LEN + 1);
	ifxConnInfoSetDefaults(coninfo);
	ifxGetOptions(foreignTableOid, coninfo);

	buf = ifxGenerateConnName(coninfo);
	strncpy(coninfo->conname, buf->data, IFX_CONNAME_LEN);

	dsn = ifxGetDatabaseString(coninfo);
	coninfo->dsn = pstrdup(dsn->data);

	return coninfo;
}

/*
 * ifxFilterQuals
 *
 * Walk through all FDW-related predicate expressions passed
 * by baserel->restrictinfo and examine them for pushdown.
 *
 * Any predicates able to be pushed down are converted into a
 * character string, suitable to be passed directly as SQL to
 * an informix server. An empty string is returned in case
 * no predicates are found.
 *
 * NOTE: excl_restrictInfo is a List, holding all rejected RestrictInfo
 * structs found not able to be pushed down.
 */
static char * ifxFilterQuals(PlannerInfo *planInfo,
							 RelOptInfo *baserel,
							 List **excl_restrictInfo,
							 Oid foreignTableOid)
{
	IfxPushdownOprContext pushdownCxt;
	ListCell             *cell;
	StringInfoData       *buf;
	char                 *oprStr;
	int i;

	Assert(foreignTableOid != InvalidOid);

	pushdownCxt.foreign_relid = foreignTableOid;
	pushdownCxt.foreign_rtid  = baserel->relid;
	pushdownCxt.predicates    = NIL;
	pushdownCxt.count         = 0;
	pushdownCxt.count_removed = 0;
	pushdownCxt.has_or_expr   = false;

	/* Be paranoid, excluded RestrictInfo list initialized to be empty */
	*excl_restrictInfo = NIL;

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
		int found;

		info = (RestrictInfo *) lfirst(cell);

		found = pushdownCxt.count;
		ifx_predicate_tree_walker((Node *)info->clause, &pushdownCxt);

		if (found == pushdownCxt.count)
		{
			elog(DEBUG2, "RestrictInfo doesn't hold anything interesting, skipping");
			*excl_restrictInfo = lappend(*excl_restrictInfo, info);
		}

		/*
		 * Each list element from baserestrictinfo is AND'ed together.
		 * Record a corresponding IfxPushdownOprInfo structure in
		 * the context, so that it get decoded properly below.
		 */
		if (PG_LIST_NEXT_ITEM(baserel->baserestrictinfo, cell) != NULL)
		{
			IfxPushdownOprInfo *pushAndInfo;

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
	 * Check wether ifx_predicate_tree_walker() encountered a OR'ed
	 * expression and removed some clauses...we can't rely on the returned
	 * predicate string, so assume we can't push down any. This won't be safe,
	 * since we might push down a partial OR expression which would lead
	 * to wrong results. We might try to be a little smarter here, but leave
	 * that to future improvements...
	 */
	if (pushdownCxt.has_or_expr
		&& ( pushdownCxt.count_removed > 0))
		return "";

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

		/* ignore filtered expressions */
		if (info->type == IFX_OPR_NOT_SUPPORTED)
		{
			continue;
		}

		switch (info->type)
		{
			case IFX_OPR_OR:
			case IFX_OPR_AND:
			case IFX_OPR_NOT:
				/* save current boolean opr context */
				oprStr = text_to_cstring(info->expr_string);
				break;
			case IFX_IS_NULL:
			case IFX_IS_NOT_NULL:
				/* fall through, no special action necessary */
			default:
				appendStringInfo(buf, " %s %s",
								 (i > 1) ? oprStr : "",
								 text_to_cstring(info->expr_string));
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
 * that we are eventually required to do it twice,
 * once in ifxPlanForeignScan() and in ifxBeginForeignScan().
 * When doing a scan, we  need the query plan from
 * the DECLARE CURSOR statement in ifxPlanForeignScan()
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
	info->stmt_name = ifxGenStatementName(info->refid);

	/*
	 * An identifier for the dynamically allocated
	 * DESCRIPTOR area.
	 */
	info->descr_name = ifxGenDescrName(info->refid);

	/*
	 * ...and finally the cursor name.
	 */
	info->cursor_name = ifxGenCursorName(info->refid);

	/* Prepare the query. */
	elog(DEBUG1, "prepare query \"%s\"", info->query);
	ifxPrepareQuery(info->query,
					info->stmt_name);
	ifxCatchExceptions(info, IFX_STACK_PREPARE);

	/*
	 * Declare the cursor for the prepared
	 * statement. Check out, if we need to switch the cursor
	 * type depending on special datatypes first.
	 */
	if (coninfo->enable_blobs)
	{
		elog(NOTICE, "informix_fdw: enable_blobs specified, forcing NO SCROLL cursor");

		if (!coninfo->tx_enabled)
			ereport(WARNING,
					(errcode(ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION),
					 errmsg("informix_fdw: using NO SCROLL cursor without transactions")));

		info->cursorUsage = IFX_DEFAULT_CURSOR;
	}

	elog(DEBUG1, "declare cursor \"%s\"", info->cursor_name);
	ifxDeclareCursorForPrepared(info->stmt_name,
								info->cursor_name,
								info->cursorUsage);
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
	IfxPlanData           planData;

	festate = (IfxFdwExecutionState *) node->fdw_state;

	/*
	 * XXX: We need to get the info from the cached connection!
	 */
	plan_values = PG_SCANSTATE_PRIVATE_P(node);
	ifxDeserializeFdwData(festate, plan_values);
	ifxDeserializePlanData(&planData, plan_values);

	/* Give some possibly useful info about startup costs */
	if (es->costs)
	{
#if PG_VERSION_NUM >= 110000
		ExplainPropertyFloat("Informix costs", NULL, planData.costs, 2, es);
#else
		ExplainPropertyFloat("Informix costs", planData.costs, 2, es);
#endif

		/* print planned foreign query */
		ExplainPropertyText("Informix query", festate->stmt_info.query, es);
	}
}


static void ifxConnInfoSetDefaults(IfxConnectionInfo *coninfo)
{
	Assert(coninfo != NULL);

	if (coninfo == NULL)
		return;

	/* Assume non-tx enabled database, determined later */
	coninfo->tx_enabled = 0;

	/*
	 * Save the current nest level of transactions.
	 */
	coninfo->xact_level = GetCurrentTransactionNestLevel();

	/* Assume non-ANSI database */
	coninfo->db_ansi = 0;

    /* enable predicate pushdown */
	coninfo->predicate_pushdown = 1;

	/* disable enable_blobs per default */
	coninfo->enable_blobs = 0;

	/* default is no DELIMIDENT set */
	coninfo->delimident = 0;

	/*
	 * Use rowid for DML per default.
	 */
	coninfo->disable_rowid = 0;

	coninfo->gl_date       = IFX_ISO_DATE;
	coninfo->gl_datetime   = IFX_ISO_TIMESTAMP;
	coninfo->db_locale     = NULL;
	coninfo->db_monetary   = NULL;
	coninfo->client_locale = NULL;
	coninfo->query         = NULL;
	coninfo->tablename     = NULL;
	coninfo->username      = "\0";
	coninfo->password      = "\0";

	/*
	 * per default assume non-SE informix instance.
	 */
	coninfo->is_obsolete = 0;

	/* default scan mode */
	coninfo->scan_mode     = IFX_PLAN_SCAN;
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
			&& strcmp(ifxopt->optname, option) == 0)
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
ifxCloseConnection(PG_FUNCTION_ARGS)
{
	IfxCachedConnection *conn_cached;
	char                *conname;
	bool                 found;

	/*
	 * Check if connection cache is already
	 * initialized. If not, we don't have anything
	 * to do and can exit immediately.
	 */
	if (!IfxCacheIsInitialized)
		elog(ERROR, "informix connection cache not yet initialized");

	/* Get connection name from argument */
	conname = text_to_cstring(PG_GETARG_TEXT_P(0));
	elog(DEBUG1, "connection identifier \"%s\"",
		 conname);
	Assert(conname);

	/*
	 * Lookup connection.
	 *
	 * We can't remove it immediately from the cache, since we
	 * really don't know wether the connection has opened transactions.
	 * This usually means a local transaction with a foreign scan is
	 * in progress, so abort here in case this is true.
	 *
	 */
	conn_cached = ifxConnCache_exists(conname, &found);

	/* Check wether the handle was valid, first */
	if (!found)
	{
		elog(ERROR, "unknown informix connection name: \"%s\"",
			 conname);
		PG_RETURN_VOID();
	}

	/*
	 * If the connection is within a transaction, this means that
	 * a current scan is in progress. Don't allow to close this
	 * connection, but give the user a hint what happened.
	 */
	if (conn_cached->con.tx_in_progress > 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("connection \"%s\" has opened transactions",
						conname),
				 errdetail("commit or rollback the local transaction first")));
	}

    /*
	 * We remove the connection handle from the cache first,
	 * closing it afterwards then. This is assumed to be safe,
	 * even when the function is used in a query predicate
	 * where the connection itself is used again. Subsequent
	 * references to this connection will find the cache returning
	 * NULL when requesting the connection identifier and will
	 * reconnect again implicitely.
	 */
	conn_cached = ifxConnCache_rm(conname, &found);

	/* Sanity check */
	Assert(conn_cached != NULL);

	/* okay, we have a valid connection handle...close it */
	ifxDisconnectConnection(conname);

    /* Check for any Informix exceptions */
	if (ifxGetSqlStateClass() == IFX_ERROR)
	{
		IfxSqlStateMessage message;

		ifxGetSqlStateMessage(1, &message);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not close specified connection \"%s\"",
						conname),
				 errdetail("informix error: %s, SQLSTATE %s",
						   message.text, message.sqlstate)));
	}

	PG_RETURN_VOID();
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
		Datum                values[14];
		bool                 nulls[14];
		HeapTuple            tuple;
		Datum                result;

		call_data = (struct ifx_sp_call_data *) fcontext->user_fctx;
		Assert(call_data != NULL);
		conn_cached = (IfxCachedConnection *) hash_seq_search(call_data->hash_status);

		/*
		 * Values array. This will hold the values to be returned.
		 */
		elog(DEBUG2, "connection name %s", conn_cached->con.ifx_connection_name);
		values[0] = PointerGetDatum(cstring_to_text(conn_cached->con.ifx_connection_name));
		values[1] = Int32GetDatum(conn_cached->establishedByOid);
		values[2] = PointerGetDatum(cstring_to_text(conn_cached->con.servername));
		values[3] = PointerGetDatum(cstring_to_text(conn_cached->con.informixdir));
		values[4] = PointerGetDatum(cstring_to_text(conn_cached->con.database));
		values[5] = PointerGetDatum(cstring_to_text(conn_cached->con.username));
		values[6] = Int32GetDatum(conn_cached->con.usage);

		nulls[0] = false;
		nulls[1] = false;
		nulls[2] = false;
		nulls[3] = false;
		nulls[4] = false;
		nulls[5] = false;
		nulls[6] = false;

		/* db_locale and client_locale might be undefined */

		if (conn_cached->con.db_locale != NULL)
		{
			values[7] = PointerGetDatum(cstring_to_text(conn_cached->con.db_locale));
			nulls[7] = false;
		}
		else
		{
			nulls[7] = true;
			values[7] = PointerGetDatum(NULL);
		}

		if (conn_cached->con.client_locale != NULL)
		{
			values[8] = PointerGetDatum(cstring_to_text(conn_cached->con.client_locale));
			nulls[8] = false;
		}
		else
		{
			nulls[8] = true;
			values[8] = PointerGetDatum(NULL);
		}

		/*
		 * Show transaction usage.
		 */
		values[9] = Int32GetDatum(conn_cached->con.tx_enabled);
		nulls[9]  = false;

		/*
		 * Transaction in progress...
		 */
		values[10] = Int32GetDatum(conn_cached->con.tx_in_progress);
		nulls[10]  = false;

		/*
		 * Show wether database is ANSI enabled or not.
		 */
		values[11] = Int32GetDatum(conn_cached->con.db_ansi);
		nulls[11]  = false;

		/*
		 * Additional stats columns...
		 */
		values[12] = Int32GetDatum(conn_cached->con.tx_num_commit);
		nulls[12]  = false;

		values[13] = Int32GetDatum(conn_cached->con.tx_num_rollback);
		nulls[13]  = false;

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
		 *
		 * For pre PG11 releases, we are somehow paranoid and force
		 * max_calls to be larger or equal to 0, but starting with PG11
		 * this isn't necessary anymore, since max_calls is an unsigned
		 * datatype.
		 */
#if PG_VERSION_NUM < 110000
		if ((fcontext->max_calls >= 0) && IfxCacheIsInitialized)
#else
		if (IfxCacheIsInitialized)
#endif
			hash_seq_term(call_data->hash_status);

		SRF_RETURN_DONE(fcontext);
	}
}

/*
 * ifxXactFinalize()
 *
 * Commits or rollbacks a transaction on the remote
 * server, depending on the specified IfxXactAction.
 *
 * Internally, this function makes the specified informix
 * connection current and depending on the specified action
 * commits or rolls back the current transaction. The caller
 * should make sure, that there's really a transaction in
 * progress.
 *
 * If connection_error_ok is true, an error is thrown
 * if the specified cached informix connection can't be made
 * current. Otherwise the loglevel is decreased to a WARNING,
 * indicating the exact SQLSTATE and error message what happened.
 */
static int ifxXactFinalize(IfxCachedConnection *cached,
						   IfxXactAction action,
						   bool connection_error_ok)
{
	int result = -1;
	IfxSqlStateMessage message;

	/*
	 * Make this connection current (otherwise we aren't able to commit
	 * anything.
	 */
	if ((result = ifxSetConnectionIdent(cached->con.ifx_connection_name)) < 0)
	{
		/*
		 * Can't make this connection current, so throw an
		 * ERROR. This will return to this callback by
		 * XACT_EVENT_ABORT and do all necessary cleanup.
		 */
		ifxGetSqlStateMessage(1, &message);

		elog(((connection_error_ok) ? ERROR : WARNING),
			  "informix_fdw: error committing transaction: \"%s\", SQLSTATE %s",
			  message.text, message.sqlstate);
	}

	if (action == IFX_TX_COMMIT)
	{
		/*
		 * Commit the transaction
		 */
		if ((result = ifxCommitTransaction(&cached->con, 0)) < 0)
		{
			/* oops, something went wrong ... */
			ifxGetSqlStateMessage(1, &message);

			/*
			 * Error out in case we can't commit this transaction.
			 */
			elog(ERROR, "informix_fdw: error committing transaction: \"%s\", SQLSTATE %s",
				 message.text, message.sqlstate);
		}
	}
	else if (action == IFX_TX_ROLLBACK)
	{
		/* Rollback current transaction */
		if (ifxRollbackTransaction(&cached->con, 0) < 0)
		{
			/* oops, something went wrong ... */
			ifxGetSqlStateMessage(1, &message);

			/*
			 * Don't throw an error, but emit a warning something went
			 * wrong on the remote server with the SQLSTATE error message.
			 * Otherwise we end up in an endless loop.
			 */
			elog(WARNING, "informix_fdw: error committing transaction: \"%s\"",
				 message.text);
		}
	}

	return result;
}

/*
 * Internal function for ifx_fdw_xact_callback().
 *
 * Depending on the specified XactEvent, rolls a transaction back
 * or commits it on the remote server.
 */
static void ifx_fdw_xact_callback_internal(IfxCachedConnection *cached,
										   XactEvent event)
{
	switch(event)
	{
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
#endif
#if PG_VERSION_NUM >= 90300
		case XACT_EVENT_PRE_COMMIT:
		{
            ifxXactFinalize(cached, IFX_TX_COMMIT, true);
			break;
		}
		case XACT_EVENT_PRE_PREPARE:
		{
			/*
			 * Not supported.
			 *
			 * NOTE: I had a hard time to figure out how this works correctly,
			 *       but fortunately the postgres_fdw shows an example on how to
			 *       do this right: when an ERROR is thrown here, we come back
			 *       later with XACT_EVENT_ABORT, which will then do the whole
			 *       cleanup stuff.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("informix_fdw: cannot prepare a transaction")));
			break;
		}
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_COMMIT:
#endif
		case XACT_EVENT_COMMIT:
#else
        case XACT_EVENT_COMMIT:
		{
            ifxXactFinalize(cached, IFX_TX_COMMIT, true);
			break;
		}
#endif
		case XACT_EVENT_PREPARE:
			/* Not reach, since pre-commit does everything required. */
			elog(ERROR, "missed cleaning up connection during pre-commit");
			break;
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_ABORT:
#endif
		case XACT_EVENT_ABORT:
		{
			/*
			 * Beware that we can't throw an error here, since this would bring
			 * us into an endless loop by subsequent triggering XACT_EVENT_ABORT.
			 */
            ifxXactFinalize(cached, IFX_TX_ROLLBACK, false);
		}
	}
}

static void ifx_fdw_xact_callback(XactEvent event, void *arg

#ifdef __USE_EDB_API__
								  ,bool spl_context
#endif

	)
{
	HASH_SEQ_STATUS      hsearch_status;
	IfxCachedConnection *cached;

	/*
	 * No-op if this backend has no in-progress transactions in Informix.
	 */
	if (ifxXactInProgress < 1)
		return;

	/*
	 * We need to scan through all cached connections to check
	 * wether they have in-progress transactions.
	 */
	hash_seq_init(&hsearch_status, ifxCache.connections);
	while ((cached = (IfxCachedConnection *) hash_seq_search(&hsearch_status)))
	{
		/*
		 * No transaction in progress? If true, get to next...
		 */
		if (cached->con.tx_in_progress < 1)
			continue;

		elog(DEBUG3, "informix_fdw: xact_callback on connection \"%s\"",
			 cached->con.ifx_connection_name);

		/*
		 * Execute required actions...
		 */
		ifx_fdw_xact_callback_internal(cached, event);
	}
}

static void ifx_fdw_subxact_callback(SubXactEvent event,
									 SubTransactionId subId,
									 SubTransactionId parentId,
									 void *arg

#ifdef __USE_EDB_API__
									 ,bool spl_context
#endif

	)
{
	HASH_SEQ_STATUS      hsearch_status;
	IfxCachedConnection *cached;
	int                  curlevel;

	/*
	 * No-op if no transaction in progress.
	 */
	if (ifxXactInProgress < 1)
		return;

	/*
	 * Nothing to do on subtransaction start or abort.
	 */
	if (!(event == IFX_PGFDWAPI_SUBXACT_COMMIT ||
		  event == SUBXACT_EVENT_ABORT_SUB))
		return;

	/*
	 * We scan all current active Informix connections to find
	 * any with active subtransactions. We are only interested in
	 * nested transactions of the same nest level.
	 */
	curlevel = GetCurrentTransactionNestLevel();
	hash_seq_init(&hsearch_status, ifxCache.connections);
	while ((cached = (IfxCachedConnection *) hash_seq_search(&hsearch_status)))
	{
		/*
		 * Nothing found
		 */
		if (cached == NULL)
			continue;

		/*
		 * If the current nest level is higher than the
		 * nest level of the current connection handle, do nothing.
		 */
		if (cached->con.tx_in_progress < curlevel)
			continue; /* next one */

		/*
		 * If we encounter a cached connection with a nest level
		 * still higher what we are currently are locally, we did something
		 * wrong and missed this SAVEPOINT entirely. Give a warning that something
		 * fishy is going on...
		 */
		if (cached->con.tx_in_progress > curlevel)
		{
			elog(WARNING, "informix_fdw: leaked savepoint detected on connection \"%s\", level %d",
				 cached->con.ifx_connection_name,
				 cached->con.tx_in_progress);
		}

		if (event == IFX_PGFDWAPI_SUBXACT_COMMIT)
		{
			/* Handle subxact commit */

			elog(DEBUG3, "informix_fdw: commit xact level %d", curlevel);

			/* Release/Commit the SAVEPOINT */
			if (ifxCommitTransaction(&cached->con, curlevel) < 0)
			{
				ereport(WARNING,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("informix_fdw: cannot commit xact level %d", curlevel),
						 errhint("commit error on informix connection \"%s\"",
								 cached->con.ifx_connection_name)));
			}
		}
		else
		{
			/* This is subxact rollback action */
			elog(DEBUG3, "informix_fdw: rollback xact level %d", curlevel);

			if (ifxRollbackTransaction(&cached->con, curlevel) < 0)
			{
				ereport(WARNING,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("informix_fdw: cannot rollback xact level %d", curlevel),
						 errhint("rollback error on informix connection \"%s\"",
								 cached->con.ifx_connection_name)));
			}
		}
	}

}

void _PG_init()
{
	RegisterXactCallback(ifx_fdw_xact_callback, NULL);
	RegisterSubXactCallback(ifx_fdw_subxact_callback, NULL);
}
