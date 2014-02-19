/*-------------------------------------------------------------------------
 *
 * ifx_fdw.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * NOTES:
 *
 *   One word about datum conversion routines: The various getter
 *   and setter functions vary in their usage of the specified
 *   Informix attribute number: getter functions expect the attribute
 *   number to match it's offset into the table, whereas setter functions
 *   need to know the attribute number into the sqlvar struct to bind
 *   any new values (thus, the order of the parametrized columns in the
 *   DML query).
 *
 *   For all setter functions it is also absolutely mandatory to set
 *   valid options into the specific stmt_info->ifxAttrDefs array item,
 *   otherwise the functions won't be able to set correct values into
 *   the specific sqlvar struct item. For example, IfxAttrDef.indicator
 *   need to be set to a valid value to reflect wether the column will get
 *   a NULL or NOT NULL datum.
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_connection.ec
 *
 *-------------------------------------------------------------------------
 */
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "ifx_type_compat.h"

EXEC SQL include sqltypes;
EXEC SQL include sqlda;
EXEC SQL include "int8.h";
EXEC SQL include "decimal.h";

/* Don't trap errors. Default, but force it explicitely */
EXEC SQL WHENEVER SQLERROR CONTINUE;

/*
 * Number of current transactions
 * in progress per backend.
 */
unsigned int ifxXactInProgress = 0;

static void ifxSetEnv(IfxConnectionInfo *coninfo);
static inline IfxIndicatorValue ifxSetIndicator(IfxAttrDef *def,
												struct sqlvar_struct *ifx_value);
static void ifxReleaseSavepoint(int level);
static void ifxRollbackSavepoint(int level);
static void ifxSavepoint(IfxPGCachedConnection *cached,
						 IfxConnectionInfo *coninfo);

/*
 * Establish a named INFORMIX database connection with transactions
 *
 * This also initializes various database properties of the
 * coninfo structure, e.g. wether the database supports transactions.
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

	/*
	 * Set specific Informix environment
	 */
	ifxSetEnv(coninfo);

	EXEC SQL CONNECT TO :ifxdsn AS :ifxconname
		USER :ifxuser USING :ifxpass WITH CONCURRENT TRANSACTION;

	if (ifxGetSQLCAWarn(SQLCA_WARN_TRANSACTIONS) == 'W')
	{
		/* save state into connection info */
		coninfo->tx_enabled = 1;
	}

	if (ifxGetSQLCAWarn(SQLCA_WARN_ANSI) == 'W')
		coninfo->db_ansi = 1;
}

int ifxStartTransaction(IfxPGCachedConnection *cached, IfxConnectionInfo *coninfo)
{
	/*
	 * No-op if non-logged database or parent transaction
	 * already in progress...
	 */
	if (coninfo->tx_enabled == 1)
	{

		/*
		 * If we already have started a transaction, we use a
		 * SAVEPOINT instead.
		 */
		if (cached->tx_in_progress < 1)
		{
			EXEC SQL BEGIN WORK;
			EXEC SQL SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

			if (ifxGetSqlStateClass() != IFX_ERROR)
			{
				cached->tx_in_progress = 1;
				++ifxXactInProgress;
			}
			else
				return -1;
		}

		/*
		 * If the nested level didn't change, this is a no-op. No further
		 * nested transaction started so far.
		 */
		if (cached->tx_in_progress == coninfo->xact_level)
			return 0;

		/*
		 * Okay, looks like this connection already has established
		 * a parent transaction and a new nested subtransaction.
		 * Since the xact level increased in the meantime, we need
		 * to create a corresponding SAVEPOINT for the
		 * current nest level on the remote server as well.
		 */
		while(cached->tx_in_progress < coninfo->xact_level)
		{
			ifxSavepoint(cached, coninfo);

			if (ifxGetSqlStateClass() != IFX_ERROR)
			{
				cached->tx_in_progress = coninfo->xact_level;
				++ifxXactInProgress;
			}
			else
			{
				/*
				 * Indicate something went wrong, leave it up
				 * to the caller to inspect SQLSTATE
				 */
				return -1;
			}
		}
	}

	/* no-op treated as success! */
	return 0;
}

/*
 * ifxReleaseSavepoint()
 *
 * Release the given savepoint identified by level.
 *
 * NOTE: This function isn't intended to be called directly, the caller
 *       needs to make sure the transaction state is actually
 *       adjusted in the cached connection handle. See ifxCommitTransaction()
 *       or ifxRollbackTransaction() for details.
 */
static void ifxReleaseSavepoint(int level)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char ifx_sql[256];
	EXEC SQL END DECLARE SECTION;

	bzero(ifx_sql, sizeof(ifx_sql));
	snprintf(ifx_sql, sizeof(ifx_sql), "RELEASE SAVEPOINT ifxfdw_svpt%d",
			 level);

	EXEC SQL EXECUTE IMMEDIATE :ifx_sql;
}

/*
 * ifxRollbackSavepoint()
 *
 * Rollback the specified savepoint identified by level.
 *
 * NOTE: This function isn't intended to be called directly, the caller
 *       needs to make sure the transaction state is actually
 *       adjusted in the cached connection handle. See ifxCommitTransaction()
 *       or ifxRollbackTransaction() for details.
 */
static void ifxRollbackSavepoint(int level)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char ifx_sql[256];
	EXEC SQL END DECLARE SECTION;

	bzero(ifx_sql, sizeof(ifx_sql));
	snprintf(ifx_sql, sizeof(ifx_sql), "ROLLBACK TO SAVEPOINT ifxfdw_svpt%d",
			 level);

	EXEC SQL EXECUTE IMMEDIATE :ifx_sql;
}

/*
 * ifxSavepoint()
 *
 * Creates a savepoint within the current nested xact level.
 *
 * NOTE: This function isn't intended to be called directly, the caller
 *       needs to make sure the transaction state is actually
 *       adjusted in the cached connection handle. See ifxCommitTransaction()
 *       or ifxRollbackTransaction() for details.
 */
static void ifxSavepoint(IfxPGCachedConnection *cached, IfxConnectionInfo *coninfo)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char ifx_sql[256];
	EXEC SQL END DECLARE SECTION;

	/*
	 * Generate the SAVEPOINT SQL command sent to the Informix server.
	 * NOTE: Since we rely on the caller, we don't increment the
	 *       nest level here directly and leave this duty to the caller.
	 */
	bzero(ifx_sql, sizeof(ifx_sql));
	snprintf(ifx_sql, sizeof(ifx_sql), "SAVEPOINT ifxfdw_svpt%d",
			 cached->tx_in_progress + 1);

	EXEC SQL EXECUTE IMMEDIATE :ifx_sql;
}

/*
 * Rollback a parent transaction or SAVEPOINT.
 *
 * If subXactLevel is specified and the current cached connection handle
 * is within a nested xact level, the transaction is rolled back to the
 * SAVEPOINT identified by subXactLevel.
 *
 * Specify 0 within subXactLevel to ROLLBACK the whole parent transaction.
 */
int ifxRollbackTransaction(IfxPGCachedConnection *cached, int subXactLevel)
{
	/*
	 * No-op, if no transaction in progress.
	 * We don't treat this as an error currently, since we
	 * might talk with a remote Informix database with no logging.
	 */
	if (cached->tx_in_progress <= 0)
		return 0;

	/*
	 * Rollback transaction.
	 */
	if ((cached->tx_in_progress == 1)
		|| (cached->tx_in_progress > 1 && subXactLevel == 0))
	{
		EXEC SQL ROLLBACK WORK;

		if (ifxGetSqlStateClass() != IFX_ERROR)
		{
			--cached->tx_in_progress;
			--ifxXactInProgress;
			++cached->tx_num_rollback;
		}
		else
		{
			/*
			 * Indicate something went wrong, leave it up
			 * to the caller to inspect SQLSTATE
			 */
			return -1;
		}
	}
	else
	{
		/*
		 * If tx_in_progress is larger than 1, then we know that
		 * we are currently nested within several subtransactions
		 * locally. Rollback to the savepoint specified by the
		 * subXactLevel parameter...
		 */
		ifxRollbackSavepoint(subXactLevel);

		if (ifxGetSqlStateClass() == IFX_ERROR)
			return -1;

		/*
		 * ...and finally release the savepoint.
		 */
		ifxReleaseSavepoint(subXactLevel);

		if (ifxGetSqlStateClass() != IFX_ERROR)
		{
			/* decrease the nest level */
			--cached->tx_in_progress;
			--ifxXactInProgress;

			/*
			 * NOTE: We count parent transactions in the stats
			 * only!
			 */
		}
		else
		{
			/*
			 * Indicate something went wrong, leave it up
			 * to the caller to inspect SQLSTATE.
			 */
			return -1;
		}
	}

	/* only reached in case of success */
	return 0;
}

/*
 * Commit a transaction or release a SAVEPOINT identified by
 * subXactLevel.
 */
int ifxCommitTransaction(IfxPGCachedConnection *cached, int subXactLevel)
{
	if (cached->tx_in_progress == 1)
	{
		EXEC SQL COMMIT WORK;

		if (ifxGetSqlStateClass() != IFX_ERROR)
		{
			cached->tx_in_progress = 0;
			--ifxXactInProgress;
			++cached->tx_num_commit;
		}
		else
		{
			/*
			 * Indicate something went wrong, leave it up
			 * to the caller to inspect SQLSTATE
			 */
			return -1;
		}
	}
	else if (cached->tx_in_progress > 1)
	{
		/*
		 * Commit the current savepoint.
		 */
		ifxReleaseSavepoint(subXactLevel);

		if (ifxGetSqlStateClass() != IFX_ERROR)
		{
			--cached->tx_in_progress;
			--ifxXactInProgress;

			/*
			 * NOTE: We count parent transactions in the stats
			 * only!
			 */
		}
	}

	/* no-op treated as success! */
	return 0;
}

void ifxDisconnectConnection(char *conname)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_conname;
	EXEC SQL END DECLARE SECTION;

	ifx_conname = conname;

	EXEC SQL DISCONNECT :ifx_conname;
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

/*
 * Sets the connection specified by conname.
 */
int ifxSetConnectionIdent(char *conname)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifxconname;
	EXEC SQL END DECLARE SECTION;

	ifxconname = conname;
	EXEC SQL SET CONNECTION :ifxconname;

	/*
	 * In case we can't make this connection current abort
	 * immediately, but let the caller know that something went
	 * wrong by returning -1. It's up to him to examine SQLSTATE
	 * then. Note that we only react on an SQLSTATE representing an
	 * ERROR, warnings or other SQLSTATE classes are ignored.
	 */
	if (ifxGetSqlStateClass() == IFX_ERROR)
		return -1;
	else
		return 0;
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

	if (ifxGetSQLCAWarn(SQLCA_WARN_TRANSACTIONS) == 'W')
	{
		/* save state into connection info */
		coninfo->tx_enabled = 1;
	}

	if (ifxGetSQLCAWarn(SQLCA_WARN_ANSI) == 'W')
		coninfo->db_ansi = 1;
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

void ifxDescribeStmtInput(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda *ifx_sqlda;

	ifx_stmt_name = state->stmt_name;

	EXEC SQL DESCRIBE INPUT :ifx_stmt_name INTO ifx_sqlda;

	state->sqlda = (void *) ifx_sqlda;
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

/*
 * Execute a prepared statement assigned to the
 * specified execution state without a given
 * SQLDA structure.
 */
void ifxExecuteStmt(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	ifx_stmt_name = state->stmt_name;
	EXEC SQL EXECUTE :ifx_stmt_name;
}

/*
 * Execute a prepared statement assigned to the
 * specified execution state with the attached SQLDA
 * structure. The caller is responsible to make sure
 * the state is a valid execution state with a fully
 * initialized SQLDA structure attached to it.
 */
void ifxExecuteStmtSqlda(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_stmt_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda *sqptr = (struct sqlda *)state->sqlda;

	ifx_stmt_name =  state->stmt_name;

	EXEC SQL EXECUTE :ifx_stmt_name USING DESCRIPTOR sqptr;
}

void ifxPutValuesInPrepared(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda *sqptr = (struct sqlda *)state->sqlda;

	ifx_cursor_name = state->cursor_name;

	EXEC SQL PUT :ifx_cursor_name USING DESCRIPTOR sqptr;
}

void ifxFlushCursor(IfxStatementInfo *info)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	ifx_cursor_name = info->cursor_name;

	EXEC SQL FLUSH :ifx_cursor_name;
}

void ifxDeclareCursorForPrepared(char *stmt_name, char *cursor_name,
								 IfxCursorUsage cursorType)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_stmt_name;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	/*
	 * Check if cursor support is really available.
	 */
	if (cursorType == IFX_NO_CURSOR)
		return;

	ifx_stmt_name = stmt_name;
	ifx_cursor_name = cursor_name;

	if (cursorType == IFX_SCROLL_CURSOR)
	{
		EXEC SQL DECLARE :ifx_cursor_name
			SCROLL CURSOR FOR :ifx_stmt_name;
	}
	else
	{
		/*
		 * IFX_UPDATE_CURSOR,
		 * IFX_DEFAULT_CURSOR,
		 * IFX_INSERT_CURSOR
		 */
		EXEC SQL DECLARE :ifx_cursor_name
			CURSOR FOR :ifx_stmt_name;
	}
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

void ifxFetchFirstRowFromCursor(IfxStatementInfo *state)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_cursor_name;
	EXEC SQL END DECLARE SECTION;

	struct sqlda *ifx_sqlda;

	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_cursor_name = state->cursor_name;

	EXEC SQL FETCH FIRST :ifx_cursor_name USING DESCRIPTOR ifx_sqlda;
}

/*
 * Returns the class of the current SQLSTATE value.
 */
IfxSqlStateClass ifxGetSqlStateClass(void)
{
	IfxSqlStateClass errclass = IFX_RT_ERROR;

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
				errclass = IFX_ERROR;
				break;
		}
	}
	else if (SQLSTATE[0] == '4')
	{
		/*
		 * Map SQL errors
		 *
		 * Currently we pay special attention to
		 * SQLSTATE 42000 (syntax error) and 4040
		 * (invalid transaction state).
		 */
		if ((SQLSTATE[1] == '0')
			|| (SQLSTATE[1] == '2'))
		{
			errclass = IFX_ERROR;
		}
	}
	else if (SQLSTATE[0] == 'S')
	{
		/*
		 * S0 SQLSTATE messages carry object errors
		 * or conflicts, such as tables already existing
		 * or missing or invalid names.
		 * Trap them accordingly and map them
		 * to a specific error code.
		 */
		if (strncmp(SQLSTATE, "S0002", 5) == 0)
			errclass = IFX_ERROR_TABLE_NOT_FOUND;

		if (strncmp(SQLSTATE, "S0000", 5) == 0)
			errclass = IFX_ERROR_INVALID_NAME;
	}

	/*
	 * XXX: Note that a failed GET DIAGNOSTICS command
	 * will always set SQLSTATE to IX000 or IX001.
	 * In this case it is reasonable to return
	 * a IFX_RT_ERROR code, to indicate a generic
	 * runtime error for now.
	 */
	return errclass;
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

	/*
	 * Examine in which category the current
	 * SQLSTATE belongs.
	 */
	errclass = ifxGetSqlStateClass();

	/*
	 * Save number of possible sub-exceptions, so
	 * they can be retrieved additionally.
	 */
	state->exception_count = ifxExceptionCount();

	/* and we're done */
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
	char  ifx_class_origin[255];
	char  ifx_subclass_origin[255];
	EXEC SQL END DECLARE SECTION;


	ifx_msg_id  = id;
	bzero(ifx_message, 255);
	bzero(ifx_class_origin, 255);
	bzero(ifx_subclass_origin, 255);
	bzero(message->text, 255);
	bzero(message->class_origin, 255);
	bzero(message->subclass_origin, 255);
	ifx_msg_len = message->len = 0;

	/* Save current SQLCODE and SQLSTATE */
	message->sqlcode = ifxGetSqlCode();
	strncpy(message->sqlstate, SQLSTATE, 6);

	/* Obtain error message */
	EXEC SQL GET DIAGNOSTICS EXCEPTION :ifx_msg_id
		:ifx_message = MESSAGE_TEXT,
		:ifx_msg_len = MESSAGE_LENGTH,
		:ifx_subclass_origin = SUBCLASS_ORIGIN,
		:ifx_class_origin    = CLASS_ORIGIN;

	/* copy fields to struct and we're done */
	message->id      = id;
	message->len     = ifx_msg_len;

	strncpy(message->text, ifx_message, ifx_msg_len);
	strncpy(message->subclass_origin,
			ifx_subclass_origin,
			sizeof(message->subclass_origin));
	strncpy(message->class_origin,
			ifx_class_origin,
			sizeof(message->class_origin));
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
inline int ifxGetSqlCode()
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

/*
 * Deallocate SQLDA structure from the current statement
 * info structure.
 */
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

IfxIndicatorValue ifxSetSqlVarIndicator(IfxStatementInfo *info, int ifx_attnum, IfxIndicatorValue value)
{
	struct sqlvar_struct *ifx_value;

	ifx_value = ((struct sqlda *)info->sqlda)->sqlvar + ifx_attnum;

	/* no-op if INDICATOR_NOT_VALID */
	switch (value)
	{
		case INDICATOR_NOT_NULL:
			*(ifx_value->sqlind) = 0;
			break;
		case INDICATOR_NULL:
			*(ifx_value->sqlind) = -1;
			break;
		default:
			break;
	}

	return value;
}

/*
 * Copy the specified 4 byte integer into the specified
 * attribute number of the current SQLDA structure.
 */
void ifxSetInteger(IfxStatementInfo *info, int ifx_attnum, int value)
{
	struct sqlda *sqlda = (struct sqlda *)info->sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	/*
	 * Copy the value.
	 */
	ifx_value = sqlda->sqlvar + ifx_attnum;
	memcpy(ifx_value->sqldata, &value, info->ifxAttrDefs[ifx_attnum].mem_allocated);
}

/*
 * Copy the specified ifx_int8 value into the specified
 * attribute number of the current SQLDA structure.
 *
 * NOTE:
 *
 * We transport the INT8 value in its *character* representation, since
 * we don't have any binary compatible representation available. value
 * must be a null terminated character array holding the corresponding
 * string representation of the to-be converted value.
 *
 * If the character string cannot be converted into a valid INT8 binary
 * value, ifxSetInt8() will set the indicator variable of the attribute
 * within the specified IfxStatementInfo structure to INDICATOR_NOT_VALID.
 * The caller is responsible to abort the action then.
 */
void ifxSetInt8(IfxStatementInfo *info, int ifx_attnum, char *value)
{
	ifx_int8_t binval;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	/*
	 * Convert string into a valid Informix ifx_int8_t value.
	 */
	if (ifx_int8cvasc(value, strlen(value), &binval) < 0)
	{
		info->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return;
	}

	/*
	 * Copy into sqlvar struct.
	 */
	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	memcpy(ifx_value->sqldata, &binval, info->ifxAttrDefs[ifx_attnum].mem_allocated);
}

/*
 * Copy the specified Informix BIGINT value into the specified
 * attribute number of the current SQLDA structure.
 *
 * NOTE:
 *
 * We transport the BIGINT value in its *character* representation, since
 * we don't have any binary compatible representation available. value
 * must be a null terminated character array holding the corresponding
 * string representation of the to-be converted value.
 *
 * If the character string cannot be converted into a valid BIGINT binary
 * value, ifxSetBigint() will set the indicator variable of the attribute
 * within the specified IfxStatementInfo structure to INDICATOR_NOT_VALID.
 * The caller is responsible to abort the action then.
 */
void ifxSetBigint(IfxStatementInfo *info,
				  int ifx_attnum,
				  char *value)
{
	bigint binval;
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	/*
	 * Convert string to a valid Informix BIGINT value.
	 */
	if (bigintcvasc(value, strlen(value), &binval) < 0)
	{
		info->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return;
	}

	/*
	 * Copy into sqlvar struct.
	 */
	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	memcpy(ifx_value->sqldata, &binval, info->ifxAttrDefs[ifx_attnum].mem_allocated);
}

/*
 * Copy the specified smallint value into the specified
 * attribute number of the current SQLDA structure.
 */
void ifxSetInt2(IfxStatementInfo *info, int ifx_attnum, short value)
{
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	/*
	 * Copy the value into the sqlvar structure.
	 */
	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	memcpy(ifx_value->sqldata, &value, info->ifxAttrDefs[ifx_attnum].mem_allocated);
}

/*
 * Copy the specified character value into the specified
 * attribute number of the current SQLDA structure.
 */
void ifxSetText(IfxStatementInfo *info, int ifx_attnum, char *value)
{
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	memcpy(ifx_value->sqldata, value, info->ifxAttrDefs[ifx_attnum].mem_allocated);

	/*
	 * Set length
	 */
	ifx_value->sqllen = info->ifxAttrDefs[ifx_attnum].mem_allocated;
}

/*
 * Copy the specified buffer into a BYTE or TEXT
 * BLOB column.
 */
void ifxSetSimpleLO(IfxStatementInfo *info, int ifx_attnum, char *buf,
					int buflen)
{
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	ifx_loc_t *loc;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;


	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	loc = (ifx_loc_t *) ifx_value->sqldata;

	loc->loc_indicator = *(ifx_value->sqlind);

	/*
	 * Use LOCMEMORY flag to tell Informix that we're
	 * maintaining our own buffer.
	 */
	loc->loc_loctype   = LOCMEMORY;

	/*
	 * XXX: not sure why we have to set both, but nevertheless
	 *      ESQL/C complains of loc_size and loc_bufsize aren't
	 *      initialized properly.
	 */
	loc->loc_bufsize   = buflen;
	loc->loc_size      = buflen;
	loc->loc_buffer    = buf;
}

/*
 * Copy a INTERVAL value into the specified attribute
 * number of the current SQLDA structure.
 *
 * The specified INTERVAL value must be compatible with
 * the ANSI format of the interval column and its
 * qualifier. See ifx_conv.c:setIfxInterval for details.
 */
void ifxSetIntervalFromString(IfxStatementInfo *info,
							  int ifx_attnum,
							  char *format,
							  char *instring)
{
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	int       converrcode;
	intrvl_t *intrvl;

	/*
	 * Init...
	 */
	ifx_sqlda = (struct sqlda *) info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	/*
	 * Depending on the qualifier of the target column
	 * we need a corresponding formatted value. Retrieve
	 * a corresponding format value depending on the current
	 * interval range.
	 */
	intrvl = (intrvl_t *)(ifx_value->sqldata);
	intrvl->in_qual = ifx_value->sqllen;

	if ((converrcode = incvfmtasc(instring, format, intrvl)) != 0)
	{
		/* An error ocurred, tell the caller something went wrong
		 * with this conversion */
		info->ifxAttrDefs[ifx_attnum].indicator   = INDICATOR_NOT_VALID;
		info->ifxAttrDefs[ifx_attnum].converrcode = converrcode;
	}

	/*
	 * Conversion looks good, proceed...
	 */
}

/*
 * Copy a DATETIME value into the specified
 * attribute number of the current SQLDA structure.
 *
 * The specified DATETIME value must be compatible with the
 * IFX_ISO_TIMESTAMP format value. Otherwise we might not be
 * able to convert into the target dtime value.
 */
void ifxSetTimestampFromString(IfxStatementInfo *info, int ifx_attnum,
							   char *dtstring)
{
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	mint                  converrcode;

	/*
	 * Set NULL indicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	/*
	 * Take care for the datetime qualifier we need to use.
	 */
	((dtime_t *)(ifx_value->sqldata))->dt_qual = ifx_value->sqllen;

	/*
	 * We get the DATETIME value as an ANSI string formatted value
	 * Convert it back into an Informix dtime_t value.
	 */
	if ((converrcode = dtcvasc(dtstring,
							   (dtime_t *)ifx_value->sqldata)) < 0)
	{
		/* An error ocurred, tell the caller something went wrong
		 * with this conversion */
		info->ifxAttrDefs[ifx_attnum].indicator   = INDICATOR_NOT_VALID;
		info->ifxAttrDefs[ifx_attnum].converrcode = converrcode;
	}
}

/*
 * Convert a time string given in the format
 * hh24:mi:ss into an Informix DATETIME value.
 */
void ifxSetTimeFromString(IfxStatementInfo *info,
						  int ifx_attnum,
						  char *timestr)
{
	EXEC SQL BEGIN DECLARE SECTION;

	/*
	 * We assume a PostgreSQL TIMEOID datum is suitable to
	 * fit into a corresponding date value. Set the datetime
	 * qualifier to be suitable to hold a HH24:MI:SS.nnnnn value.
	 */
	datetime hour to fraction(5) *timeval;

	EXEC SQL END DECLARE SECTION;

	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	mint                  converrcode;

	/*
	 * Set NULL inidicator
	 */
	if (ifxSetSqlVarIndicator(info,
							  ifx_attnum,
							  info->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	ifx_sqlda = (struct sqlda *)info->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	/*
	 * Convert the value, but set the dtime_t qualifier before.
	 */
	timeval = (dtime_t *)(ifx_value->sqldata);
	timeval->dt_qual = TU_DTENCODE(TU_HOUR, TU_F5);

	if ((converrcode = dtcvfmtasc(timestr, "%H:%M:%S", timeval)) < 0)
	{
		/* An error ocurred, tell the caller somethin went wrong
		 * with this conversion */
		info->ifxAttrDefs[ifx_attnum].indicator   = INDICATOR_NOT_VALID;
		info->ifxAttrDefs[ifx_attnum].converrcode = converrcode;
	}
}

/*
 * Returns the start and end of a given informix attribute value
 * in case its an interval or datetime type.
 * The caller is responsible to pass a valid attribute number,
 * no further checking done.
 *
 * The returned IfxTemporalRange reports the TU_START and
 * TU_END of the given temporal type. The precision is always
 * initialized to be equal to TU_END.
 */
IfxTemporalRange ifxGetTemporalQualifier(IfxStatementInfo *state,
										 int ifx_attnum)
{
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	IfxTemporalRange      range;

	ifx_sqlda = (struct sqlda *)state->sqlda;

	/*
	 * Extract the qualifier, determine starting and
	 * end of the qual.
	 */
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	range.start = TU_START(ifx_value->sqllen);
	range.end   = TU_END(ifx_value->sqllen);
	range.precision = TU_END(ifx_value->sqllen);

	return range;
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
	int     converrcode;

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
	if ((converrcode = dttoasc(&val, buf)) != 0)
	{
		/* An error ocurred, tell the caller something went wrong
		 * with this conversion */
		state->ifxAttrDefs[ifx_attnum].indicator   = INDICATOR_NOT_VALID;
		state->ifxAttrDefs[ifx_attnum].converrcode = converrcode;
		return NULL;
	}

	return buf;
}

/*
 * Retrieves an INTERVAL value as a string from
 * the given tuple.
 *
 * buf must be a valid allocated string buffer suitable
 * to hold the string value.
 */
char *ifxGetIntervalAsString(IfxStatementInfo *state, int ifx_attnum,
							 char *buf)
{
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	intrvl_t              val;
	int                   converrcode;

	if (!buf)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return NULL;
	}

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *) state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	/*
	 * If we got a NULL value, nothing more to do...
	 */
	if (ifxSetIndicator(&state->ifxAttrDefs[ifx_attnum],
						ifx_value) == INDICATOR_NULL)
		return NULL;

	memcpy(&val, (int *)ifx_value->sqldata,
		   state->ifxAttrDefs[ifx_attnum].mem_allocated);

	/*
	 * Convert the interval value into a string suitable to be
	 * processed by PostgreSQL
	 */
	if ((converrcode = intoasc(&val, buf)) < 0)
	{
		/* An error ocurred, tell the caller something went wrong
		 * with this conversion */
		state->ifxAttrDefs[ifx_attnum].indicator   = INDICATOR_NOT_VALID;
		state->ifxAttrDefs[ifx_attnum].converrcode = converrcode;
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
 * ifxGetDecimal()
 *
 * Retrieves a decimal value from the specified
 * attribute.
 *
 * The decimal value is returned as a character string. The buffer
 * passed to the buf parameter must have enough room (IFX_DECIMAL_BUF_LEN)
 * to hold the decimal value.
 * The informix dectoasc() function will convert the decimal
 * character representation into exponential notation, in case the buffer
 * is too small. If this doesn't fit even, NULL is returned.
 */
char *ifxGetDecimal(IfxStatementInfo *state, int ifx_attnum, char *buf)
{
	char         *result;
	char          dec_buf[IFX_DECIMAL_BUF_LEN + 1];
	struct sqlda *ifx_sqlda;
	struct sqlvar_struct *ifx_value;
	dec_t val;

	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result = NULL;

	ifxSetIndicator(&state->ifxAttrDefs[ifx_attnum],
					ifx_value);

	/*
	 * Check for NULL values. Don't proceed in case
	 * the retrieved value is a SQL NULL datum.
	 */
	if (state->ifxAttrDefs[ifx_attnum].indicator == INDICATOR_NULL)
	{
		return result;
	}

	/*
	 * Copy the value...
	 */
	memcpy(&val, ifx_value->sqldata,
		   state->ifxAttrDefs[ifx_attnum].mem_allocated);

	/*
	 * Convert the decimal value into its character
	 * representation.
	 */
	if (dectoasc(&val, dec_buf, sizeof(dec_buf), -1) < 0)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return result;
	}
	else
	{
		dec_buf[sizeof(dec_buf) - 1] = '\0';
		strncpy(buf, dec_buf, IFX_DECIMAL_BUF_LEN);
		result = buf;
	}

	return result;
}

/*
 * Assigns the specified decimal value string into
 * a binary dec_t datatype and stores it into the
 * specified attribute. The character string must be a valid
 * representation suitable to be converted into an Informix
 * dec_t binary representation. converrcode will be set if any
 * conversion error is encountered.
 */
void ifxSetDecimal(IfxStatementInfo *state, int ifx_attnum, char *value)
{
	dec_t binval;
	int   converrcode;
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Set NULL inidicator
	 */
	if (ifxSetSqlVarIndicator(state,
							  ifx_attnum,
							  state->ifxAttrDefs[ifx_attnum].indicator) != INDICATOR_NOT_NULL)
		return;

	ifx_sqlda = (struct sqlda *) state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;

	/*
	 * Try to convert the given character string into
	 * a valid dec_t binary value. Set converrcode if
	 * any error occured.
	 */
	if ((converrcode = deccvasc(value, strlen(value), &binval)) < 0)
	{
		/* Something went wrong. Safe the error code and
		 * return to the caller, telling him that this
		 * attribute is not valid
		 */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		state->ifxAttrDefs[ifx_attnum].converrcode = converrcode;
		return;
	}

	/*
	 * Store the value into SQLDA
	 */
	memcpy(ifx_value->sqldata, &binval,
		   state->ifxAttrDefs[ifx_attnum].mem_allocated);

	/* ...and we're done */
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
	 * Copy the data into from sqlvar data area.
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
 * ifxGetBigInt
 *
 * Retrieves the sqlvar value for the specified
 * attribute number in case it is a BIGINT value.
 *
 * Returns the character representation of the
 * value or NULL, in case the BIGINT value is
 * NULL.
 */
char *ifxGetBigInt(IfxStatementInfo *state, int ifx_attnum, char *buf)
{
	bigint                val;
	char                 *result;
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result    = NULL;

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
	memcpy(&val, ifx_value->sqldata, sizeof(bigint));

	/*
	 * Convert the int8 value into a character string.
	 */
	if (biginttoasc(val, buf, IFX_INT8_CHAR_LEN, 10) == 0)
		result = buf;
	else
		/* other values returned are < 0, meaning conversion has failed */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;

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
	else
		/* other values returned are < 0, meaning conversion has failed */
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;

	return result;
}

/*
 * ifxGetTextFromLocator
 *
 * Extracts a character string from the locator type
 * bundled with the specified attribute. The resulting
 * character array could also be used to extract a byte
 * stream from a simple large object, so be prepared
 * to deal with embedded null bytes in this case.
 *
 * The result character string is *not* copied, instead
 * a pointer to the locator structure is returned, thus the
 * caller must not free it itself.
 *
 * If an error occurred, the return value is NULL and
 * the indicator value is set to INDICATOR_NOT_VALID.
 *
 * loc_buf_len will store the buffer size of the current locator.
 */
char *ifxGetTextFromLocator(IfxStatementInfo *state, int ifx_attnum,
							long *loc_buf_len)
{
	ifx_loc_t            *loc;
	char                 *result;
	struct sqlda         *ifx_sqlda;
	struct sqlvar_struct *ifx_value;

	/*
	 * Init stuff.
	 */
	ifx_sqlda = (struct sqlda *)state->sqlda;
	ifx_value = ifx_sqlda->sqlvar + ifx_attnum;
	result = NULL;
	*loc_buf_len   = 0;

	loc = (ifx_loc_t *) ifx_value->sqldata;

	/*
	 * Check wether informix was able to allocate a buffer
	 * for us. See
	 *
	 * http://publib.boulder.ibm.com/infocenter/idshelp/v115/index.jsp?topic=%2Fcom.ibm.esqlc.doc%2Fsii-07-37658.htm
	 *
	 * for a detailed description of the error conditions here. It looks
	 * to me that catching everything below 0 is enough...anyways, it might
	 * be necessary to extend the indicator value to reflect further
	 * error conditions in the future (e.g. out of memory, ...).
	 */
	if (loc->loc_status < 0)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_VALID;
		return result;
	}

	/*
	 * Check for NULL...we can't use ifxSetIndicatorValue() here,
	 * since we need to get the value from the locator directly.
	 */
	if (loc->loc_indicator == -1)
	{
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NULL;
		return result;
	}
	else
		state->ifxAttrDefs[ifx_attnum].indicator = INDICATOR_NOT_NULL;

	*loc_buf_len = loc->loc_size;
	result = (char *) loc->loc_buffer;
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
			case SQLSERIAL:
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
			{
				/* BLOB datatypes. These are handled via loc_t
				 * locator descriptor and require special handling
				 *
				 * NOTE: ifxSetupDataBufferAligned() is responsible to
				 *       setup the locator structure properly.
				 */

				column_data->sqltype = CLOCATORTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;

				/*
				 * Remember any encountered BLOB type. The caller can react on
				 * any special blob types accordingly then.
				 */
				state->special_cols |= IFX_HAS_BLOBS;
				break;
			}
			case SQLVCHAR:
			case SQLNCHAR:
			case SQLNVCHAR:
				column_data->sqltype = CSTRINGTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLLVARCHAR:
				state->special_cols |= IFX_HAS_OPAQUE;
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
			case SQLINFXBIGINT:
				column_data->sqltype = CBIGINTTYPE;
				column_data->sqllen = state->ifxAttrDefs[ifx_attnum].mem_allocated;
				break;
			case SQLINT8:
			case SQLSERIAL8:
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
				state->special_cols |= IFX_HAS_OPAQUE;
				break;
			default:
				return 0;
		}

		/*
		 * Save current offset position.
		 */
		ifx_offset += state->ifxAttrDefs[ifx_attnum].mem_allocated;

		/*
		 * Get total row size. Rely on the *next* aligned offset, otherwise
		 * we get a wrong number of total bytes to allocate.
		 */
		row_size = rtypalign(ifx_offset,
							 column_data->sqltype);

		/* Store the corresponding informix data type identifier. This is later
		 * used to identify the PostgreSQL target type we need to convert to. */
		state->ifxAttrDefs[ifx_attnum].type = (IfxSourceType) ifx_type;

		column_data++;
	}

	return row_size;
}

/*
 * Get statistic information from Informix. These might
 * not be accurate atm, but we need them for ANALYZE.
 */
void ifxGetSystableStats(char *tablename, IfxPlanData *planData)
{
	EXEC SQL BEGIN DECLARE SECTION;
	char *ifx_tablename;
	double ifx_npused;
	double ifx_nrows;
	short ifx_row_size;
	short ifx_pagesize;
	EXEC SQL END DECLARE SECTION;

	ifx_tablename = tablename;

	EXEC SQL
		SELECT npused, nrows, rowsize, pagesize INTO :ifx_npused,
               :ifx_nrows, :ifx_row_size, :ifx_pagesize
		FROM systables
		WHERE tabname = :ifx_tablename;

	planData->nrows = ifx_nrows;
	planData->npages = ifx_npused;
	planData->row_size = ifx_row_size;
	planData->pagesize = ifx_pagesize;

	return;
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
		 * Setup locator type. LOC_ALLOC specified within
		 * mflags tells ESQL/C to maintain the locator buffer
		 * itself...
		 *
		 * Please note that we always try to allocate the
		 * LOB buffer in memory (LOCMEMORY).
		 */
		if (column_data->sqltype == CLOCATORTYPE)
		{
			ifx_loc_t *loc;

			loc = (ifx_loc_t *)(column_data->sqldata);
			loc->loc_loctype = LOCMEMORY;
			loc->loc_bufsize = -1;
			loc->loc_oflags  = 0;
			loc->loc_mflags  = LOC_ALLOC;
		}

		/*
		 * Next one...
		 */
		column_data++;
	}
}

