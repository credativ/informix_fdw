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
#include <stdlib.h>

#include "ifx_type_compat.h"

EXEC SQL include sqltypes;
EXEC SQL include sqlda;

static void ifxSetEnv(IfxConnectionInfo *coninfo);

/*
 * Establish a named INFORMIX database connection with transactions
 */
extern void ifxCreateConnectionXact(IfxConnectionInfo *coninfo)
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

	/*
	 * Set specific Informix environment
	 */
	ifxSetEnv(coninfo);

	EXEC SQL CONNECT TO :ifxdsn AS :ifxconname
		USER :ifxuser USING :ifxpass WITH CONCURRENT TRANSACTION;

}

void ifxSetEnv(IfxConnectionInfo *coninfo)
{
	setenv("INFORMIXDIR", coninfo->informixdir, 1);
	setenv("INFORMIXSERVER", coninfo->servername, 1);
}

void ifxSetConnection(IfxConnectionInfo *coninfo)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifxconname;
	EXEC SQL END DECLARE SECTION;

	/*
	 * Set specific Informix environment
	 */
	ifxSetEnv(coninfo);

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

void ifxCloseCursor(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	ifx_cursor_name = state->cursor_name;

	EXEC SQL CLOSE :ifx_cursor_name;
}

int ifxFreeResource(IfxStatementInfo *state,
					 int stackentry)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_id;
	EXEC SQL END DECLARE SECTION;

	switch (stackentry)
	{
		case IFX_STACK_PREPARE:
			ifx_id = state->stmt_name;
			break;
		case IFX_STACK_DECLARE:
			ifx_id = state->cursor_name;
		default:
			/* should not happen */
			return -1;
	}

	EXEC SQL FREE :ifx_id;

	return stackentry;
}

void ifxAllocateDescriptor(char *descr_name, int num_items)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_descriptor;
	int ifx_max;
	EXEC SQL END DECLARE SECTION;

	ifx_descriptor = descr_name;
	ifx_max = num_items;

	EXEC SQL ALLOCATE DESCRIPTOR :ifx_descriptor WITH MAX :ifx_max;
}

void ifxDeallocateDescriptor(char *descr_name)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_descr_name;
	EXEC SQL END DECLARE SECTION;

	ifx_descr_name = descr_name;

	EXEC SQL DEALLOCATE DESCRIPTOR :ifx_descr_name;
}

void ifxDescribeAllocatorByName(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda *ifx_sqlda;

	ifx_stmt_name  = state->stmt_name;

	EXEC SQL DESCRIBE :ifx_stmt_name
	         INTO ifx_sqlda;

	state->sqlda = (void *)ifx_sqlda;
}

void ifxSetDescriptorCount(char *descr_name, int count)
{
	EXEC SQL BEGIN DECLARE SECTION;
	int ifx_count;
	char *ifx_descriptor;
	EXEC SQL END DECLARE SECTION;

	ifx_count = count;
	ifx_descriptor = descr_name;
	EXEC SQL SET DESCRIPTOR :ifx_descriptor COUNT = :ifx_count;
}

int ifxDescriptorColumnCount(IfxStatementInfo *state)
{
	struct sqlda *sqptr = (struct sqlda *)state->sqlda;
	return sqptr->sqld;
}

void ifxOpenCursorForPrepared(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda * ifx_sqlda;

	ifx_sqlda = state->sqlda;
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
	/* if (state->cursorUsage != IFX_DEFAULT_CURSOR) */
	/* 	return; */

	ifx_stmt_name = state->stmt_name;
	ifx_cursor_name = state->cursor_name;

	EXEC SQL DECLARE :ifx_cursor_name
		SCROLL CURSOR FOR :ifx_stmt_name;

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
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda *ifx_sqlda;

	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_cursor_name = state->cursor_name;

	EXEC SQL FETCH NEXT :ifx_cursor_name USING DESCRIPTOR ifx_sqlda;
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
	 * XXX: Note that a failed GET DIAGNOSTICS command
	 * will always set SQLSTATE to IX000 or IX001.
	 * In this case it is reasonable to return
	 * a IFX_RT_ERROR code, to indicate a generic
	 * runtime error for now.
	 */

	/*
	 * Save number of possible sub-exceptions, so
	 * they can be retrieved additionally.
	 */
	state->exception_count = ifxExceptionCount();

	return errclass;
}

/*
 * ifxGetSqlStateMessage
 *
 * Returns the specified SQLSTATE id within
 * the current informix connection context.
 */
void ifxGetSqlStateMessage(int id, IfxSqlStateMessage *message)
{
	EXEC SQL BEGIN DECLARE SECTION;
	int   ifx_msg_id;
	int   ifx_msg_len;
	char  ifx_message[255];
	EXEC SQL END DECLARE SECTION;

	ifx_msg_id  = id;
	bzero(ifx_message, 255);
	bzero(message->text, 255);
	ifx_msg_len = message->len = 0;

	/* Save current SQLCODE and SQLSTATE */
	message->sqlcode = ifxGetSqlCode();
	strncpy(message->sqlstate, SQLSTATE, 6);

	/* Obtain error message */
	EXEC SQL GET DIAGNOSTICS EXCEPTION :ifx_msg_id
		:ifx_message = MESSAGE_TEXT,
		:ifx_msg_len = MESSAGE_LENGTH;

	/* copy fields to struct and we're done */
	message->id      = id;
	message->len     = ifx_msg_len;
	strncpy(message->text, ifx_message, ifx_msg_len);
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
	 * Informix might set SQLSTATE to class '01'
	 * after CONNECT or SET CONNECTION to tell
	 * some specific connection characteristics.
	 *
	 * There's also the '08' explicit connection
	 * exception class which needs to be handled
	 * accordingly. This always should be handled
	 * as an ERROR within pgsql backends.
	 *
	 * Handle Informix exception class 'IX' accordingly
	 * in case someone has installation errors with his
	 * CSDK (e.g. no INFORMIXDIR set).
	 */

	if ((strncmp(SQLSTATE, "08", 2) == 0)
		|| (strncmp(SQLSTATE, "IX", 2) == 0))
		return IFX_CONNECTION_ERROR;

	if (strncmp(SQLSTATE, "01", 2) == 0)
		return IFX_CONNECTION_WARN;

	if (strncmp(SQLSTATE, "00", 2) == 0)
		return IFX_CONNECTION_OK;

	return IFX_STATE_UNKNOWN;
}

/*
 * ifxGetSqlCode
 *
 * Return the SQLCODE identifier for
 * the last executed ESQL/C command.
 */
int ifxGetSqlCode()
{
	return SQLCODE;
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

int ifxGetInt(IfxStatementInfo *state, int ifx_attnum)
{
	int result;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	IfxIndicatorValue ifx_indicator;

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	/*
	 * Copy the data into a non-host variable. We don't want
	 * to reuse variables in PostgreSQL that were part of ESQL/C
	 * formerly.
	 */
	if ((*ifx_value->sqlind) == -1)
		/* NULL value encountered */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NULL;
	else
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_NULL;
	}

	result = (int) (*ifx_value->sqldata);

	return result;
}

/*
 * ifxGetColumnAttributes()
 *
 * Initializes an IfxStatementInfo according to the
 * attributes defined by the returning columns of a previously
 * prepared and described statement.
 *
 * ifxGetColumnAttributes() initializes column descriptors
 * so it can be used later to transform informix datatypes into
 * corresponding PostgreSQL types.
 *
 * The function returns -1 in case an error occurs or the
 * row size required to be allocated to hold all values
 * returned by a row.
 */
int ifxGetColumnAttributes(IfxStatementInfo *state)
{
	struct sqlvar_struct *column_data;
	struct sqlda *ifx_sqlda;
	int ifx_attnum;
	int row_size;
	int ifx_offset;

	ifx_sqlda   = (struct sqlda *)state->sqlda;
	column_data = ifx_sqlda->sqlvar;
	row_size    = -1;
	ifx_offset  = 0;

	/*
	 * Loop over all ifxAttrCount columns.
	 */
	for (ifx_attnum = 0; ifx_attnum < state->ifxAttrCount; ifx_attnum++)
	{
		short ifx_type;

		column_data->sqldata = NULL;
		column_data->sqlind  = NULL;

		/*
		 * Record the source type, so that we can translate it to
		 * the corresponding PostgreSQL type later. Currently, for most
		 * of the type we don't need any special handling, but we cannot
		 * use the Informix type directly: sqltypes.h can't be included
		 * in PostgreSQL modules, since it redefines int2 and int4.
		 */
		ifx_type = column_data->sqltype;

		/* Raw size of the column reported by the database */
		state->ifxAttrDefs[ifx_attnum].len = column_data->sqllen;

		/*
		 * Memory aligned offset into data buffer
		 */
		ifx_offset = rtypalign(ifx_offset, column_data->sqltype);
		state->ifxAttrDefs[ifx_attnum].offset = ifx_offset;

		/* Size used by the corresponding C datatype */
		column_data->sqllen = rtypmsize(column_data->sqltype, column_data->sqllen);
		state->ifxAttrDefs[ifx_attnum].mem_allocated = column_data->sqllen;
		row_size += state->ifxAttrDefs[ifx_attnum].mem_allocated;

		/*
		 * Save current offset position.
		 */
		ifx_offset += column_data->sqllen;

		/* Store the corresponding informix data type identifier. This is later
		 * used to identify the PostgreSQL target type we need to convert to. */
		state->ifxAttrDefs[ifx_attnum].type = (IfxSourceType) ifx_type;

		/*
		 * Switch to the ESQL/C host variable type we want to assign
		 * the value.
		 */
		switch(ifx_type) {
			case SQLCHAR:
				column_data->sqltype = CCHARTYPE;
				break;
			case SQLSMINT:
				column_data->sqltype = CSHORTTYPE;
				break;
			case SQLINT:
				column_data->sqltype = CINTTYPE;
				break;
			case SQLFLOAT:
			case SQLSMFLOAT:
				column_data->sqltype = CFLOATTYPE;
				break;
			case SQLDECIMAL:
				column_data->sqltype = CDECIMALTYPE;
				break;
			case SQLDATE:
				column_data->sqltype = CDATETYPE;
				break;
			case SQLMONEY:
				column_data->sqltype = CMONEYTYPE;
				break;
			case SQLNULL:
				/* XXX: How is this going to be used ??? */
				break;
			case SQLDTIME:
				column_data->sqltype = CDTIMETYPE;
				break;
			case SQLBYTES:
			case SQLTEXT:
				/* BLOB datatypes. These are handled via loc_t
				 * locator descriptor and require special handling */
				column_data->sqltype = CLOCATORTYPE;
				break;
			case SQLVCHAR:
			case SQLNCHAR:
			case SQLNVCHAR:
				column_data->sqltype = CSTRINGTYPE;
				break;
			case SQLINTERVAL:
				column_data->sqltype = CINVTYPE;
				break;
			case SQLSERIAL:
			case SQLINT8:
			case SQLSERIAL8:
				column_data->sqltype = CINT8TYPE;
				break;
			case SQLSET:
			case SQLMULTISET:
			case SQLLIST:
				break;
			case SQLROW:
				column_data->sqltype = CROWTYPE;
				break;
			case SQLCOLLECTION:
				column_data->sqltype = CCOLLTYPE;
				break;
			case SQLROWREF:
				break;
			default:
				return -1;
		}

		column_data++;
	}

	return row_size;
}

void ifxSetupDataBufferAligned(IfxStatementInfo *state)
{
	int ifx_attnum;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *column_data;

	/*
	 * Initialize stuff.
	 */
	ifx_sqlda = (struct sqlda *) state->sqlda;
	column_data = ifx_sqlda->sqlvar;

	for (ifx_attnum = 0; ifx_attnum < state->ifxAttrCount; ifx_attnum++)
	{
		column_data->sqldata = &state->data[state->ifxAttrDefs[ifx_attnum].offset];
		column_data->sqlind  = &state->indicator[ifx_attnum];

		/*
		 * Next one...
		 */
		column_data++;
	}
}
