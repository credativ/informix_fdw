/*-------------------------------------------------------------------------
 *
 * ifx_fdw.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_connection.ec
 *
 *-------------------------------------------------------------------------
 */
#include <string.h>

#include "ifx_type_compat.h"

EXEC SQL include sqltypes;

/*
 * Establish a named INFORMIX database connection with transactions
 */
void ifxCreateConnectionXact(IfxConnectionInfo *coninfo)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifxdsn;
	char *ifxconname;
	char *ifxuser;
	char *ifxpass;
	EXEC SQL END DECLARE SECTION;

	ifxdsn     = coninfo->dsn;
	ifxconname = coninfo->conname;
	ifxuser    = coninfo->username;
	ifxpass    = coninfo->password;

	EXEC SQL CONNECT TO :ifxdsn AS :ifxconname
		USER :ifxuser USING :ifxpass WITH CONCURRENT TRANSACTION;

}

void ifxSetConnection(IfxConnectionInfo *coninfo)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifxconname;
	EXEC SQL END DECLARE SECTION;

	ifxconname = coninfo->conname;
	EXEC SQL SET CONNECTION :ifxconname;
}

void ifxPrepareQuery(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_query;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	ifx_query = state->query;
	ifx_stmt_name = state->stmt_name;

	EXEC SQL PREPARE :ifx_stmt_name FROM :ifx_query;
}

void ifxAllocateDescriptor(char *descr_name)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_descriptor;
	EXEC SQL END DECLARE SECTION;

	ifx_descriptor = descr_name;

	EXEC SQL ALLOCATE DESCRIPTOR :ifx_descriptor;
}

void ifxDescribeAllocatorByName(char *stmt_name, char *descr_name)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_descriptor;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	ifx_stmt_name  = stmt_name;
	ifx_descriptor = descr_name;

	EXEC SQL DESCRIBE :ifx_stmt_name
	         USING SQL DESCRIPTOR :ifx_descriptor;
}

int ifxDescriptorColumnCount(char *descr_name)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_descr_name;
	int   num_attr;
	EXEC SQL END DECLARE SECTION;

	int i;

	/*
	 * Number of attributes for current descriptor
	 * area.
	 */
	ifx_descr_name = descr_name;
	EXEC SQL GET DESCRIPTOR :ifx_descr_name :num_attr = COUNT;

	return num_attr;
}

void ifxOpenCursorForPrepared(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	ifx_cursor_name = state->cursor_name;
	EXEC SQL OPEN :ifx_cursor_name;
}

void ifxDeclareCursorForPrepared(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_stmt_name;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	/*
	 * Check if cursor support is really available.
	 */
	if (state->cursorUsage != IFX_DEFAULT_CURSOR)
		return;

	ifx_stmt_name = ifx_cursor_name = state->stmt_name;
	EXEC SQL DECLARE :ifx_cursor_name
		CURSOR FOR :ifx_stmt_name ;
}

void ifxDestroyConnection(char *conname)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifxconname;
	EXEC SQL END DECLARE SECTION;

	ifxconname = conname;

	EXEC SQL DISCONNECT :ifxconname;
}

void ifxFetchRowFromCursor(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_descr;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	ifx_descr = state->stmt_name;
	ifx_cursor_name = state->cursor_name;

	EXEC SQL FETCH NEXT :ifx_cursor_name USING SQL DESCRIPTOR :ifx_descr;
}

/*
 * Callback error handler.
 *
 * This function is intended to be called directly
 * after an ESQL call to check for an SQLSTATE error
 * condition. Once SQLSTATE is
 * set to an error code, the function returns the
 * SQLSTATE string with the statement info structure.
 */
IfxSqlStateClass ifxSetException(IfxStatementInfo *state)
{
	IfxSqlStateClass errclass = IFX_RT_ERROR;

	/*
	 * Save the SQLSTATE
	 */
	strncpy(state->sqlstate, SQLSTATE, 5);

	if (SQLSTATE[0] == '0')
	{
		/* We'd like to see here the following exception
		 * classes:
		 *
		 * = 00: IFX_SUCCESS
		 * = 01: IFX_WARNING
		 * = 02: IFX_NOT_FOUND
		 * > 02: IFX_ERROR
		 */
		switch (SQLSTATE[1])
		{
			case '0':
				errclass = IFX_SUCCESS;
				break;
			case '1':
				errclass = IFX_WARNING;
				break;
			case '2':
				errclass = IFX_NOT_FOUND;
				break;
			default:
				/* error */
				break;
		}
	}

	/*
	 * Save number of possible sub-exceptions, so
	 * they can be retrieved additionally.
	 */
	state->exception_count = ifxExceptionCount();

	return errclass;
}

/*
 * ifxCatchConnectionError
 *
 * This function can be used to obtain connection
 * errors or warnings after a CONNECT or SET CONNECTION.
 */
IfxSqlStateClass ifxConnectionStatus()
{
	/*
	 * Informix might set SQLSTATE to class 01
	 * after CONNECT or SET CONNECTION.
	 *
	 * SQLSTATE class 02 means connection error.
	 */

	if (strncmp(SQLSTATE, "02", 2) == 0)
		return IFX_CONNECTION_ERROR;

	if (strncmp(SQLSTATE, "01", 2) == 0)
		return IFX_CONNECTION_WARN;

	if (strncmp(SQLSTATE, "00", 2) == 0)
		return IFX_CONNECTION_OK;

	return IFX_STATE_UNKNOWN;
}

/*
 * ifxExceptionCount
 *
 * Returns the number of exceptions returned
 * by the last command.
 */
int ifxExceptionCount()
{
	EXEC SQL BEGIN DECLARE SECTION;
	int ifx_excp_count;
	EXEC SQL END DECLARE SECTION;

	int result;

	EXEC SQL GET DIAGNOSTICS :ifx_excp_count = NUMBER;

	/*
	 * We don't want to return a ESQL host variable,
	 * so copy it into a new result variable.
	 */
	result = ifx_excp_count;
	return result;
}

int ifxGetInt(IfxStatementInfo *state, int attnum)
{
	EXEC SQL BEGIN DECLARE SECTION;
	mint  ifx_value;
	short ifx_indicator;
	char *ifx_descr;
	int   ifx_attnum;
	EXEC SQL END DECLARE SECTION;

	int result;

	ifx_descr  = state->stmt_name;
	ifx_attnum = attnum;

	EXEC SQL GET DESCRIPTOR :ifx_descr VALUE :ifx_attnum
		:ifx_indicator = INDICATOR,
		:ifx_value     = DATA;

	/*
	 * Copy the data into a non-host variable. We don't want
	 * to reuse variables in PostgreSQL that were part of ESQL/C
	 * formerly.
	 */
	if (ifx_indicator == -1)
		/* NULL value encountered */
		state->ifxAttrDefs[ifx_attnum]->indicator = INDICATOR_NULL;
	else
	{
		state->ifxAttrDefs[ifx_attnum]->indicator = INDICATOR_NOT_NULL;
		/* fixed size value */
		state->ifxAttrDefs[ifx_attnum]->len = IFX_FIXED_SIZE_VALUE;
	}

	return result;
}

int ifxGetColumnAttributes(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	short ifx_type;
	int   ifx_attnum;
	char *ifx_descr;
	EXEC SQL END DECLARE SECTION;

	ifx_descr = state->stmt_name;

	for (ifx_attnum = 0; ifx_attnum < state->ifxAttrCount - 1; ifx_attnum++)
	{
		EXEC SQL GET DESCRIPTOR :ifx_descr VALUE :ifx_attnum
			:ifx_type = TYPE;

		/*
		 * Record the source type, so that we can translate it to
		 * the corresponding PostgreSQL type later. Currently, for most
		 * of the type we don't need any special handling, but we cannot
		 * use the Informix type directly: sqltypes.h can't be included
		 * in PostgreSQL modules, since it redefines int2 and int4.
		 */
		switch(ifx_type) {
			case SQLCHAR:
			case SQLSMINT:
			case SQLINT:
			case SQLFLOAT:
			case SQLSMFLOAT:
			case SQLDECIMAL:
			case SQLSERIAL:
			case SQLDATE:
			case SQLMONEY:
			case SQLNULL:
			case SQLDTIME:
			case SQLBYTES:
			case SQLTEXT:
			case SQLVCHAR:
			case SQLINTERVAL:
			case SQLNCHAR:
			case SQLNVCHAR:
			case SQLINT8:
			case SQLSERIAL8:
			case SQLSET:
			case SQLMULTISET:
			case SQLLIST:
			case SQLROW:
			case SQLCOLLECTION:
			case SQLROWREF:
				state->ifxAttrDefs[ifx_attnum]->type = (IfxSourceType) ifx_type;
				break;
			default:
				return -1;
		}
	}
}
