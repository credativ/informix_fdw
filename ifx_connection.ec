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
EXEC SQL include "int8.h";

static void ifxSetEnv(IfxConnectionInfo *coninfo);
static inline IfxIndicatorValue ifxSetIndicator(IfxAttrDef *def,
												struct sqlvar_struct *ifx_value);

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

int ifxGetSQLCAErrd(signed short ca)
{
	return sqlca.sqlerrd[ca];
}

char ifxGetSQLCAWarn(signed short int warn)
{
	switch (warn)
	{
		case 0:
			return SQLCA_WARN(0);
		case 1:
			return SQLCA_WARN(1);
		case 2:
			return SQLCA_WARN(2);
		case 3:
			return SQLCA_WARN(3);
		case 4:
			return SQLCA_WARN(4);
		case 5:
			return SQLCA_WARN(5);
		case 6:
			return SQLCA_WARN(6);
		case 7:
			return SQLCA_WARN(7);
		default:
			return -1;
	}
}

void ifxSetEnv(IfxConnectionInfo *coninfo)
{
	setenv("INFORMIXDIR", coninfo->informixdir, 1);
	setenv("INFORMIXSERVER", coninfo->servername, 1);

	/*
	 * Set output format for DATE and DATETIME.
	 */
	setenv("GL_DATE", coninfo->gl_date, 1);
	setenv("GL_DATETIME", coninfo->gl_datetime, 1);

	/*
	 * Set CLIENT_LOCALE or leave it empty in case
	 * no value is specified.
	 */
	if (coninfo->client_locale != NULL)
		setenv("CLIENT_LOCALE", coninfo->client_locale, 1);

	/*
	 * Set DB_LOCALE or leave it empty in case
	 * no value is specified.
	 */
	if (coninfo->db_locale != NULL)
		setenv("DB_LOCALE", coninfo->db_locale, 1);
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

void ifxPrepareQuery(char *query, char *stmt_name)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_query;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	ifx_query = query;
	ifx_stmt_name = stmt_name;

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

	ifx_cursor_name = state->cursor_name;

	EXEC SQL OPEN :ifx_cursor_name;
}

void ifxDeclareCursorForPrepared(char *stmt_name,
								 char *cursor_name)
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

	ifx_stmt_name = stmt_name;
	ifx_cursor_name = cursor_name;

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

void ifxDeallocateSQLDA(IfxStatementInfo *state)
{
	/*
	 * Don't free sqlvar structs and sqlind indicator
	 * area here!. This is expected to be done by
	 * the PostgreSQL backend via pfree() later!
	 */
	if (state->sqlda != NULL)
	{
		free(state->sqlda);
		state->sqlda = NULL;
	}
}

/*
 * Sets the indicator value of the specified
 * IfxAttrDef according the the retrieved information
 * from the sqlvar. Currently, this is NULL or NOT NULL only.
 */
static inline IfxIndicatorValue ifxSetIndicator(IfxAttrDef *def,
												struct sqlvar_struct *ifx_value)
{
	if ((*ifx_value->sqlind) == -1)
	{
		/* NULL value found */
		def->indicator = INDICATOR_NULL;
	}
	else
		def->indicator = INDICATOR_NOT_NULL;

	return def->indicator;
}

/*
 * Retrieves the sqlvar value for the specified
 * attribute number as an character array.
 *
 * Returns a null pointer in case a NULL value was encountered.
 */
char *ifxGetText(IfxStatementInfo *state, int ifx_attnum)
{
	char *result;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	if (ifxSetIndicator(&state->ifxAttrDefs[ifx_attnum],
						ifx_value) == INDICATOR_NULL)
		return NULL;

	result = (char *) (ifx_value->sqldata);
	return result;
}

/*
 * Retrieves a DATETIME value as a string
 * from the given tuple.
 */
char *ifxGetTimestampAsString(IfxStatementInfo *state, int ifx_attnum,
							  char *buf)
{
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	dtime_t val;

	/*
	 * Init stuff...
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	if (buf == NULL)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return NULL;
	}

	if (ifxSetIndicator(&state->ifxAttrDefs[ifx_attnum],
						ifx_value) == INDICATOR_NULL)
		return NULL;

	/*
	 * Retrieve the DATETIME value
	 */
	memcpy(&val, (dtime_t *)ifx_value->sqldata,
		   state->ifxAttrDefs[ifx_attnum].mem_allocated);

	/*
	 * Convert into a C string.
	 */
	if (dttoasc(&val, buf) != 0)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return NULL;
	}

	return buf;
}

/*
 * Retrieves the sqlvar value for the
 * specified attribute number as a character array.
 */
char *ifxGetDateAsString(IfxStatementInfo *state, int ifx_attnum,
						 char *buf)
{
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	int val;

	if (!buf)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return NULL;
	}

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	if (ifxSetIndicator(&state->ifxAttrDefs[ifx_attnum],
						ifx_value) == INDICATOR_NULL)
		return NULL;

	memcpy(&val, (int *)ifx_value->sqldata, IFX_DATE_BINARY_SIZE);

	if (rdatestr(val, buf) != 0)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return NULL;
	}

	return buf;
}

/*
 * ifxGetBool
 *
 * Returns a BOOLEAN value from the given
 * informix attribute.
 *
 * Returns an integer representation of the BOOLEAN
 * value actually stored in the given attribute number.
 */
char ifxGetBool(IfxStatementInfo *state, int ifx_attnum)
{
	char result;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuff...
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result = 0;

	ifxSetIndicator(&state->ifxAttrDefs[ifx_attnum],
					ifx_value);

	memcpy(&result, ifx_value->sqldata, 1);
	return result;
}

/*
 * ifxGetInt2
 *
 * Retrieves a smallint value for the specified
 * attribute number.
 */
int2 ifxGetInt2(IfxStatementInfo *state, int ifx_attnum)
{
	int2 result;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuf...
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result = 0;

	/*
	 * Copy the data into from the sqlvar data area.
	 * Check for NULL-ness before and mark the value
	 * accordingly.
	 */
	if ((*ifx_value->sqlind) == -1)
	{
		/* NULL value encountered */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NULL;
		return result;
	}
	else
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_NULL;
	}

	memcpy(&result, ifx_value->sqldata,
		   state->ifxAttrDefs[ifx_attnum].mem_allocated);
	return result;
}

/*
 * ifxGetInt4
 *
 * Retrieves the sqlvar value for the specified
 * attribute number as an int4 value.
 */
int ifxGetInt4(IfxStatementInfo *state, int ifx_attnum)
{
	int result;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result = 0;

	/*
	 * Copy the data into a non-host variable. We don't want
	 * to reuse variables in PostgreSQL that were part of ESQL/C
	 * formerly.
	 */
	if ((*ifx_value->sqlind) == -1)
	{
		/* NULL value encountered */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NULL;
		return result;
	}
	else
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_NULL;
	}

	/*result = (int) (*ifx_value->sqldata);*/
	memcpy(&result, ifx_value->sqldata,
		   state->ifxAttrDefs[ifx_attnum].mem_allocated);

	return result;
}

/*
 * ifxGetInt8
 *
 * Retrieves the sqlvar value for the specified
 * attribute number as a character string. The native
 * ESQL/C int8 type is converted to a character array, since
 * PostgreSQL needs to read a compatible datatype representation.
 *
 * Returns a null pointer in case a NULL value
 * was encountered.
 *
 * If retrieved a valid value from the database, the
 * function returns to buf.
 *
 * buf is required to be an initialized character string with
 * enough room to hold an int64 value.
 */
char *ifxGetInt8(IfxStatementInfo *state, int ifx_attnum, char *buf)
{
	ifx_int8_t            val;
	char                 *result;
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result = NULL;

	/*
	 * Check for NULL values.
	 */
	if ((*ifx_value->sqlind) == -1)
	{
		/* NULL value */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NULL;
		return result;
	}
	else
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_NULL;
	}

	/*
	 * Copy the data into an int8 host variable.
	 */
	memcpy(&val, ifx_value->sqldata, sizeof(ifx_int8_t));

	/*
	 * Convert the int8 value into a character string.
	 */
	if (ifx_int8toasc(&val, buf, IFX_INT8_CHAR_LEN) == 0)
		result = buf;

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
 * The function returns 0 in case an error occurs or the
 * row size required to be allocated to hold all values
 * returned by a row.
 */
size_t ifxGetColumnAttributes(IfxStatementInfo *state)
{
	struct sqlvar_struct *column_data;
	struct sqlda *ifx_sqlda;
	int ifx_attnum;
	int row_size;
	int ifx_offset;

	ifx_sqlda   = (struct sqlda *)state->sqlda;
	column_data = ifx_sqlda->sqlvar;
	row_size    = 0;
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

		/*
		 * Raw size of the column reported by the database.
		 * NOTE: Never ever confuse this value with the real
		 * memory size occupied by the corresponding column type.
		 * DATETIME and INTERVAL for example report here their
		 * value *qualifier*, not the memory required by its value.
		 * See mem_allocated of the informix value specification later
		 * to get the real memory requirement of this value.
		 *
		 * We save this value for later use, since we need to remember
		 * the original qualifier for later formatting purposes.
		 *
		 * The handling of sqllen in sqlvar structs in Informix
		 * ESQL/C is highly confusing. We are required to make sure
		 * we distinguish types which require their memory allocation
		 * size reflected in sqllen or special types like DTIME, which
		 * require their qualifier only.
		 */
		state->ifxAttrDefs[ifx_attnum].len = column_data->sqllen;

		/*
		 * Memory aligned offset into data buffer
		 */
		ifx_offset = rtypalign(ifx_offset, column_data->sqltype);
		state->ifxAttrDefs[ifx_attnum].offset = ifx_offset;

		/*
		 * Memory size occupied by the Informix ESQL/C type.
		 */
		state->ifxAttrDefs[ifx_attnum].mem_allocated = rtypmsize(column_data->sqltype,
																 column_data->sqllen);

		/*
		 * Switch to the ESQL/C host variable type we want to assign
		 * the value. Also, make sure we store the correct sqllen value
		 * correspoding to the choosen ESQL/C host variable type.
		 */
		switch(ifx_type) {
			case SQLCHAR:
				column_data->sqltype = CCHARTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLSMINT:
				column_data->sqltype = CSHORTTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLINT:
				column_data->sqltype = CINTTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLFLOAT:
			case SQLSMFLOAT:
				column_data->sqltype = CFLOATTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLDECIMAL:
				column_data->sqltype = CDECIMALTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLDATE:
				column_data->sqltype = CDATETYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLMONEY:
				column_data->sqltype = CMONEYTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLNULL:
				/* XXX: How is this going to be used ??? */
				break;
			case SQLDTIME:
				column_data->sqltype = CDTIMETYPE;

				/*
				 * Keep sqllen *as is*. Informix wants to have
				 * the DTIME qualifier, *not* the memory allocation size.
				 */
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
			case SQLLVARCHAR:
				column_data->sqltype = CSTRINGTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLINTERVAL:
				column_data->sqltype = CINVTYPE;

				/*
				 * Keep sqllen *as is*. Informix wants to have
				 * the INTV qualifier, *not* the memory allocation size.
				 */
				break;
			case SQLSERIAL:
			case SQLINT8:
			case SQLSERIAL8:
			case SQLINFXBIGINT:
			case SQLBIGSERIAL:
				/* summarize all to INT8 */
				state->ifxAttrDefs[ifx_attnum].type = SQLINT8;
				column_data->sqltype = CINT8TYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLSET:
			case SQLMULTISET:
			case SQLLIST:
				break;
			case SQLROW:
				column_data->sqltype = CROWTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLCOLLECTION:
				column_data->sqltype = CCOLLTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLROWREF:
				break;
			case SQLBOOL:
				column_data->sqltype = CBOOLTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			default:
				return 0;
		}

		/*
		 * Sum up total row size.
		 */
		row_size += state->ifxAttrDefs[ifx_attnum].mem_allocated;

		/*
		 * Save current offset position.
		 */
		ifx_offset += state->ifxAttrDefs[ifx_attnum].mem_allocated;

		/* Store the corresponding informix data type identifier. This is later
		 * used to identify the PostgreSQL target type we need to convert to. */
		state->ifxAttrDefs[ifx_attnum].type = (IfxSourceType) ifx_type;


		column_data++;
	}

	return row_size;
}

/*
 * Setup the data buffer for the sqlvar structs and
 * initialize all structures according the memory layout.
 */
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
		 * Setup qualifier for DATETIME and INTERVAL
		 * data types. This is required for subsequent
		 * FETCH calls to force the database server to set
		 * the qualifier automatically (thus we use the qualifier
		 * value 0).
		 */
		if (column_data->sqltype == CDTIMETYPE)
			((dtime_t *) column_data->sqldata)->dt_qual = 0;

		if (column_data->sqltype == CINVTYPE)
			((intrvl_t *) column_data->sqldata)->in_qual = 0;

		/*
		 * Next one...
		 */
		column_data++;
	}
}
