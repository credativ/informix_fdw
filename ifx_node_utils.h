/*-------------------------------------------------------------------------
 *
 * ifx_node_utils.h
 *                Utility functions for Informix FDW and node support.
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_node_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAVE_IFX_NODE_UTILS_H
#define HAVE_IFX_NODE_UTILS_H

#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodes.h"

/*******************************************************************************
 * Helper macros.
 */


#define makeFdwStringConst(_field_)\
	makeConst(TEXTOID, -1, InvalidOid, -1,\
			  PointerGetDatum(cstring_to_text(_field_)),	\
              false, false)

#define makeFdwInt2Const(_field_)\
	makeConst(INT2OID, -1, InvalidOid, sizeof(int2),\
              Int16GetDatum(_field_),\
              false, true)

#define makeFdwInt4Const(_field_)\
	makeConst(INT4OID, -1, InvalidOid, sizeof(int4),\
              Int32GetDatum(_field_),\
              false, true)

/*
 * Number of serialized Const nodes passed
 * from ifxPlanForeignScan()
 */
#define N_SERIALIZED_FIELDS 6

/*
 * Identifier for serialized fields
 */
#define SERIALIZED_PLAN_DATA   0
#define SERIALIZED_QUERY       1
#define SERIALIZED_STMT_NAME   2
#define SERIALIZED_CURSOR_NAME 3
#define SERIALIZED_CALLSTACK   4
#define SERIALIZED_QUALS       5

#define SERIALIZED_DATA(_vals_) Const * (_vals_)[N_SERIALIZED_FIELDS]

/*******************************************************************************
 * Node helper functions.
 */

List * ifxSerializePlanData(IfxConnectionInfo *coninfo,
							IfxFdwExecutionState *state,
							PlannerInfo *plan);
void ifxDeserializeFdwData(IfxFdwExecutionState *state,
								  FdwPlan *plan);
int2 ifxGetSerializedInt16Field(List *list, int ident);
int ifxGetSerializedInt32Field(List *list, int ident);
char * ifxGetSerializedStringField(List *list, int ident);
Datum ifxSetSerializedInt32Field(List *list, int ident, int value);
Datum ifxSetSerializedInt16Field(List *list, int ident, int2 value);

bytea *
ifxFdwPlanDataAsBytea(IfxConnectionInfo *coninfo);

#endif
