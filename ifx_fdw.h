/*-------------------------------------------------------------------------
 *
 * ifx_fdw.h
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAVE_IFX_FDW_H
#define HAVE_IFX_FDW_H

#include "ifx_type_compat.h"
#include "postgres_ext.h"

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
 * Inherits the definition of IfxAttrDef and extends
 * the type to hold PostgreSQL datums.
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

#endif

