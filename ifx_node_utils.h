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
#include "utils/bytea.h"

/*******************************************************************************
 * Helper macros.
 */


#define makeFdwStringConst(_field_)\
	makeConst(TEXTOID, -1, InvalidOid, -1,\
			  PointerGetDatum(cstring_to_text(_field_)),	\
              false, false)

#define makeFdwInt16Const(_field_)\
	makeConst(INT2OID, -1, InvalidOid, sizeof(int16),\
              Int16GetDatum(_field_),\
              false, true)

#define makeFdwInt32Const(_field_)\
	makeConst(INT4OID, -1, InvalidOid, sizeof(int32),\
              Int32GetDatum(_field_),\
              false, true)

/*
 * Number of serialized Const nodes passed
 * from ifxPlanForeignScan()
 */
#define N_SERIALIZED_FIELDS 9

/*
 * Identifier for serialized Const fields
 *
 * Don't forget to set N_SERIALIZED_FIELDS accordingly
 * to the number of fields if adding/removing one!
 */
#define SERIALIZED_PLAN_DATA    0
#define SERIALIZED_QUERY        1
#define SERIALIZED_STMT_NAME    2
#define SERIALIZED_CURSOR_NAME  3
#define SERIALIZED_CALLSTACK    4
#define SERIALIZED_QUALS        5
#define SERIALIZED_CURSOR_TYPE  6
#define SERIALIZED_SPECIAL_COLS 7
#define SERIALIZED_REFID        8

#define SERIALIZED_DATA(_vals_) Const * (_vals_)[N_SERIALIZED_FIELDS]
#define AFFECTED_ATTR_NUMS_IDX (N_SERIALIZED_FIELDS)

/*******************************************************************************
 * Node helper functions.
 */

List * ifxSerializePlanData(IfxConnectionInfo *coninfo,
							IfxFdwExecutionState *state,
							PlannerInfo *plan);
void
ifxDeserializePlanData(IfxPlanData *planData,
					   void *fdw_private);
void ifxDeserializeFdwData(IfxFdwExecutionState *state,
						   void *fdw_private);
int16 ifxGetSerializedInt16Field(List *list, int ident);
int ifxGetSerializedInt32Field(List *list, int ident);
char * ifxGetSerializedStringField(List *list, int ident);
Datum ifxSetSerializedInt32Field(List *list, int ident, int value);
Datum ifxSetSerializedInt16Field(List *list, int ident, int16 value);
void ifxGenerateUpdateSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo,
						  PlannerInfo          *root,
						  Index                 rtindex);
void ifxGenerateInsertSql(IfxFdwExecutionState *state,
						  IfxConnectionInfo    *coninfo,
						  PlannerInfo *root,
						  Index        rtindex);
#endif
