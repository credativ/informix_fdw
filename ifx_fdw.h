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
	int2 attnum;
	Oid  atttypid;
	int  atttypmod;
	char* attname;
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
	 * Number of attributes in the foreign table.
	 * Should match pgAttrDefs.
	 */
	int pgAttrCount;

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
	IFX_OPR_UNKNOWN
} IfxOprType;

/*
 * Info structure for pushdown operators.
 */
typedef struct IfxPushdownOprInfo
{
	IfxOprType type;
	OpExpr    *expr;          /* pointer to operator expression */
	text      *expr_string;   /* decoded string representation of expr */
	AttrNumber pg_attnum;     /* attribute number of foreign table */
} IfxPushdownOprInfo;

/*
 * Pushdown context structure for generating
 * pushed down query predicates. Actually used
 * by ifx_predicate_tree_walker()
 */
typedef struct IfxPushdownOprContext
{
	Oid   foreign_relid; /* OID of foreign table */
	List *predicates;    /* list of IfxPushDownOprInfo */
	int   count;         /* number of elements in predicates list */
	int   num_scan_elems; /* Number of new elements per scan of
						     ifx_predicate_tree_walker */
} IfxPushdownOprContext;

/*
 * Number of required connection parameters
 */
#define IFX_REQUIRED_CONN_KEYWORDS 3

/*
 * Helper macros to access various struct members.
 */
#define PG_ATTRTYPE_P(x, y) (x)->pgAttrDefs[(y)].atttypid
#define PG_ATTRTYPEMOD_P(x, y) (x)->pgAttrDefs[(y)].atttypmod
#define IFX_ATTRTYPE_P(x, y) (x)->stmt_info.ifxAttrDefs[(y)].type
#define IFX_SETVAL_P(x, y, z) (x)->values[(y)].val = (z)
#define IFX_GETVAL_P(x, y) (x)->values[(y)].val
#define IFX_ATTR_ISNULL_P(x, y) ((x)->stmt_info.ifxAttrDefs[(y)].indicator == INDICATOR_NULL)
#define IFX_ATTR_SETNOTVALID_P(x, y) (x)->stmt_info.ifxAttrDefs[(y)].indicator = INDICATOR_NOT_VALID
#define IFX_ATTR_IS_VALID_P(x, y) ((x)->stmt_info.ifxAttrDefs[(y)].indicator != INDICATOR_NOT_VALID)
#define IFX_ATTR_ALLOC_SIZE_P(x, y) (x)->stmt_info.ifxAttrDefs[(y)].mem_allocated

/*
 * Datatype conversion routines.
 */
Datum convertIfxTimestamp(IfxFdwExecutionState *state, int attnum);
Datum convertIfxInt(IfxFdwExecutionState *state, int attnum);
Datum convertIfxCharacterString(IfxFdwExecutionState *state, int attnum);
Datum convertIfxBoolean(IfxFdwExecutionState *state, int attnum);
Datum convertIfxDateString(IfxFdwExecutionState *state, int attnum);
Datum convertIfxTimestampString(IfxFdwExecutionState *state, int attnum);
void ifxRewindCallstack(IfxStatementInfo *info);
IfxOprType mapPushdownOperator(Oid oprid, IfxPushdownOprInfo *pushdownInfo);
Datum convertIfxSimpleLO(IfxFdwExecutionState *state, int attnum);
Datum convertIfxDecimal(IfxFdwExecutionState *state, int attnum);

/*
 * Node support helper functions
 */
bool ifx_predicate_tree_walker(Node *node, struct IfxPushdownOprContext *context);

#endif

