/*-------------------------------------------------------------------------
 *
 * ifx_fdw.h
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAVE_IFX_FDW_H
#define HAVE_IFX_FDW_H

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
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "postgres_ext.h"

#include "ifx_type_compat.h"

/*
 * Defines actions for remote informix transactions.
 */
typedef enum
{
	IFX_TX_COMMIT,
	IFX_TX_ROLLBACK }
IfxXactAction;

/*
 * Query information pushed down
 * from the planner state to the executor
 */
typedef struct IfxQueryData
{
	char *query;
	char *stmt_name;
	char *cursor_name;
} IfxQueryData;

/*
 * PgAttrDef
 *
 * Holds PostgreSQL foreign table column type
 * and other attributes.
 */
typedef struct PgAttrDef
{
	int16 attnum;
	int16 ifx_attnum;
	int16 param_id; /* id of param in SQLDA structure (UPDATE only)
					   otherwise -1 */
	Oid   atttypid;
	int   atttypmod;
	char* attname;
	bool  attnotnull;
} PgAttrDef;

/*
 * IfxPGAttrDef
 *
 * Holds Informix values converted into PostgreSQL datums.
 *
 * NOTE: def ist normally just a pointer into
 *       IfxStatementInfo und should not be deallocated
 *       directly.
 */
typedef struct IfxValue
{
	/*
	 * Allocated column definition retrieved from
	 * Informix...normally this is just a pointer
	 * into the IfxStatementInfo structure to the
	 * column holding the datum.
	 */
	IfxAttrDef *def;

	/*
	 * PostgreSQL datum, converted Informix column
	 * value.
	 */
	Datum val;
} IfxValue;

/*
 * Stores FDW-specific properties during execution.
 *
 * Effectively this is a super class which combines
 * informix structs into a single class, which can then
 * be used by PostgreSQL properly. This makes sure we don't
 * need to include colliding PostgreSQL definitions into
 * the informix ESQL/C namespace.
 */
typedef struct IfxFdwExecutionState
{
	IfxStatementInfo stmt_info;

	/*
	 * Number of dropped columns of foreign table.
	 */
	int pgDroppedAttrCount;

	/*
	 * Number of attributes in the foreign table.
	 * Should match pgAttrDefs.
	 */
	int pgAttrCount;

	/*
	 * If a rescan occurs, set to true.
	 */
	bool rescan;

	/*
	 * Use rowid in modify actions. This is the default
	 * and is set during ifxBeginForeignModify().
	 */
	bool use_rowid;

	/*
	 * Holds the attribute number of the ROWID
	 * resjunk columns, if use_rowid is set to true.
	 */
	AttrNumber rowid_attno;

	/*
	 * Dynamic list of foreign table attribute
	 * definitions.
	 */
	PgAttrDef *pgAttrDefs;

	/*
	 * Dynamic list of column values retrieved per iteration
	 * from the foreign table.
	 */
	IfxValue *values;

	/*
	 * List of attribute numbers affected by a modify statement
	 * against the foreign table. Not used during normal scans.
	 */
	List *affectedAttrNums;

	/*
	 * AFTER EACH ROW triggers present. This will always be false
	 * on PostgreSQL versions <= 9.3, but i think it's
	 * okay to waste some bytes in this case.
	 */
	bool has_after_row_triggers;

} IfxFdwExecutionState;

#if PG_VERSION_NUM >= 90200

/*
 * PostgreSQL > 9.2 uses a much smarter planning infrastructure which
 * requires us to submit state structures to different callbacks. Unify
 * them to a single structure to ease passing them around.
 */
typedef struct IfxFdwPlanState
{
	IfxConnectionInfo *coninfo;
	IfxFdwExecutionState *state;

	/*
	 * Excluded RestrictInfo after pushdown analysis.
	 */
	List *excl_restrictInfo;
} IfxFdwPlanState;

#endif

/*
 * PostgreSQL operator types supported for pushdown
 * to an Informix database.
 */
typedef enum IfxOprType
{
	IFX_OPR_EQUAL,
	IFX_OPR_NEQUAL,
	IFX_OPR_LE,
	IFX_OPR_GE,
	IFX_OPR_GT,
	IFX_OPR_LT,
	IFX_OPR_LIKE,
	IFX_OPR_AND,
	IFX_OPR_OR,
	IFX_OPR_NOT,
	IFX_OPR_NOT_SUPPORTED,
	IFX_IS_NULL,
	IFX_IS_NOT_NULL,
	IFX_OPR_UNKNOWN
} IfxOprType;

/*
 * Type of deparsed predicate.
 *
 * Currently we distinguish between deparsed predicate expression
 * directly derived from deparse_expression() and cooked expression
 * which are rewritten to match the expectation from Informix.
 */
typedef enum IfxDeparseType
{
	IFX_DEPARSED_EXPR,    /* compatible expression */
	IFX_MAKE_COOKED_EXPR, /* expression cooking not yet completed */
	IFX_COOKED_EXPR       /* generated expression to match Informix */
} IfxDeparseType;

/*
 * Info structure for pushdown operators.
 */
typedef struct IfxPushdownOprInfo
{
	IfxOprType     type;
	IfxDeparseType deparsetype;   /* type of deparsed expressions */
	int16          num_args;      /* total number of operands (we support
									 binary operators currently only) */
	int16          arg_idx;       /* current index number of operand */
	Expr          *expr;          /* pointer to operator expression */
	text          *expr_string;   /* decoded string representation of expr */
} IfxPushdownOprInfo;

/*
 * Pushdown context structure for generating
 * pushed down query predicates. Actually used
 * by ifx_predicate_tree_walker()
 */
typedef struct IfxPushdownOprContext
{
	Oid   foreign_relid; /* OID of foreign table */
	Index foreign_rtid;  /* range table index of foreign table */
	List *predicates;    /* list of IfxPushDownOprInfo */
	int   count;         /* number of elements in predicates list */
	int   count_removed; /* number of removed predicates for FDW pushdown */
	bool  has_or_expr;
} IfxPushdownOprContext;

#if PG_VERSION_NUM >= 90500

typedef struct IfxImportTableDef
{
	int   tabid;        /* unique id of the table */
	char *owner;        /* schema name */
	char *tablename;    /* name of the table, unquoted but maybe case sensitive */
	short special_cols; /* Flags describing special column types */
	List *columnDef;    /* List of IfxAttrDef structures describing the
						   foreign table columns */
} IfxImportTableDef;

#endif

/*
 * Number of required connection parameters
 */
#define IFX_REQUIRED_CONN_KEYWORDS 4

/*
 * Helper macros to access various struct members.
 */
#define PG_ATTRTYPE_P(x, y) (x)->pgAttrDefs[(y)].atttypid
#define PG_ATTRTYPEMOD_P(x, y) (x)->pgAttrDefs[(y)].atttypmod

/*
 * Maps the local attribute number to the remote table.
 *
 * NOTE: One word about attribute number mapping:
 *
 *       The Informix FDW API doesn't rely on column names, instead we
 *       try to map the local definition of a remote table (or query,
 *       depending which was specified during CREATE FOREIGN TABLE). Since
 *       column can be dropped and readded it might happen that we get
 *       *holes* in the definition of a local table. We try to address this
 *       with mapping PG and IFX column attributes, but it should be clear
 *       that this whole magic doesn't cover all cases. PostgreSQL currently
 *       doesn't support *logical* column orders (or reording columns
 *       accordingly), so in case of a static table definition this doesn't
 *       work in all cases. For SELECT definitions of a remote table (when
 *       specified the 'query' option to CREATE FOREIGN TABLE) it might be
 *       easier to re-specify the 'query' option with an adjusted SQL instead
 *       of fiddling with column orders on local table definitions.
 *
 * IMPORTANT:
 *
 *       Each access to IfxFdwExecutionState structures and their member
 *       fields in IfxStatementInfo should go through the PG_MAPPED_IFX_ATTNUM()
 *       macro, otherwise it is not guaranteed that you will get proper
 *       values back from the values datum list. The IFX_*() macros all
 *       encapsulate their accesses through PG_MAPPED_IFX_ATTNUM(), so there is
 *       no need to pass the attnum through PG_MAPPED_IFX_ATTNUM() explicitly when
 *       using them. You have to think in *PostgreSQL attribute number* logic
 *       when using those macros!
 */
#define PG_MAPPED_IFX_ATTNUM(x, y) ((x)->pgAttrDefs[(y)].ifx_attnum - 1)

/*
 * Number of valid (means visible) columns on foreign table.
 * Excludes dropped columns for example.
 */
#define PG_VALID_COLS_COUNT(x) ((x)->pgAttrCount - (x)->pgDroppedAttrCount)

/*
 * In case we use a ROWID to modify the remote Informix table,
 * reserve an extra slot, which is required to fetch the ID later.
 *
 * NOTE: We *don't* reflect the extra slot within pgAttrCount, since
 *       this will confuse the attribute number mapping code.
 */
#define IFX_PGATTRCOUNT(a) (((a)->use_rowid) ? (a)->pgAttrCount + 1 : (a)->pgAttrCount)

/*
 * Returns the param id for a prepared informix statement and its
 * offset into the sqlvar array.
 */
#define IFX_ATTR_PARAM_ID(x, y) (x)->pgAttrDefs[(y)].param_id

#define IFX_ATTRTYPE_P(x, y) (x)->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM((x), (y))].type
#define IFX_SETVAL_P(x, y, z) (x)->values[PG_MAPPED_IFX_ATTNUM((x), (y))].val = (z)
#define IFX_GETVAL_P(x, y) (x)->values[PG_MAPPED_IFX_ATTNUM((x), (y))].val
#define IFX_ATTR_ISNULL_P(x, y) ((x)->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM((x), (y))].indicator == INDICATOR_NULL)
#define IFX_ATTR_SETNOTVALID_P(x, y) (x)->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM((x), (y))].indicator = INDICATOR_NOT_VALID
#define IFX_ATTR_IS_VALID_P(x, y) ((x)->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM((x), (y))].indicator != INDICATOR_NOT_VALID)
#define IFX_ATTR_ALLOC_SIZE_P(x, y) (x)->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM((x), (y))].mem_allocated
#define IFX_SET_INDICATOR_P(x, y, z) ((x)->stmt_info.ifxAttrDefs[PG_MAPPED_IFX_ATTNUM((x), (y))].indicator = (z))

/*
 * Datatype conversion routines.
 */
Datum convertIfxFloat(IfxFdwExecutionState *state, int attnum);
Datum convertIfxTimestamp(IfxFdwExecutionState *state, int attnum);
Datum convertIfxInt(IfxFdwExecutionState *state, int attnum);
Datum convertIfxCharacterString(IfxFdwExecutionState *state, int attnum);
Datum convertIfxBoolean(IfxFdwExecutionState *state, int attnum);
Datum convertIfxDateString(IfxFdwExecutionState *state, int attnum);
Datum convertIfxTimestampString(IfxFdwExecutionState *state, int attnum);
Datum convertIfxInterval(IfxFdwExecutionState *state, int attnum);
void ifxRewindCallstack(IfxStatementInfo *info);
IfxOprType mapPushdownOperator(Oid oprid, IfxPushdownOprInfo *pushdownInfo);
Datum convertIfxSimpleLO(IfxFdwExecutionState *state, int attnum);
Datum convertIfxDecimal(IfxFdwExecutionState *state, int attnum);
void setIfxInteger(IfxFdwExecutionState *state,
				   TupleTableSlot *slot,
				   int attnum);
void setIfxText(IfxFdwExecutionState *state,
				TupleTableSlot *slot,
				int attnum);
void setIfxCharString(IfxFdwExecutionState *state,
					  int                   attnum,
					  char                 *val,
					  int                   len);
void setIfxDateTimestamp(IfxFdwExecutionState *state,
						 TupleTableSlot       *slot,
						 int                   attnum);
void setIfxDate(IfxFdwExecutionState *state,
				TupleTableSlot       *slot,
				int                   attnum);
void setIfxInterval(IfxFdwExecutionState *state,
					TupleTableSlot       *slot,
					int                   attnum);
void setIfxDecimal(IfxFdwExecutionState *state,
				   TupleTableSlot       *slot,
				   int attnum);
void setIfxFloat(IfxFdwExecutionState *state,
				 TupleTableSlot       *slot,
				 int                   attnum);

/*
 * Internal API for PostgreSQL 9.3 and above.
 */

#if PG_VERSION_NUM >= 90300
char *dispatchColumnIdentifier(int varno, int varattno, PlannerInfo *root);
void ifxGenerateDeleteSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo);
void ifxGenerateInsertSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo,
						  PlannerInfo *root,
						  Index        rtindex);
#endif

/*
 * Node support helper functions
 */
bool ifx_predicate_tree_walker(Node *node, struct IfxPushdownOprContext *context);

#endif

