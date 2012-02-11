/*-------------------------------------------------------------------------
 *
 * ifx_type_compat.h
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Describes the API for accessing the INFORMIX module without
 * the need to include PostgreSQL-related header files to avoid
 * conflicts.
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef HAVE_IFX_TYPE_COMPAT_H
#define HAVE_IFX_TYPE_COMPAT_H

/*
 * Max length of an identifier and connection name.
 *
 * Informix allows up to 128 Bytes per identifier in newer
 * releases, so leave room for the user/server identifier.
 */
#define IFX_IDENT_MAX_LEN 128
#define IFX_CONNAME_LEN (2 * IFX_IDENT_MAX_LEN)

/*
 * Fixed size INFORMIX value are always treated to
 * have length -1.
 */
#define IFX_FIXED_SIZE_VALUE -1

/*
 * Number of required connection parameters
 */
#define IFX_REQUIRED_CONN_KEYWORDS 2

/*
 * Which kind of CURSOR to use.
 */
typedef enum IfxCursorUsage
{
	IFX_DEFAULT_CURSOR,
	IFX_NO_CURSOR
} IfxCursorUsage;

/*
 * Informix SQLSTATE classes.
 */
typedef enum IfxSqlStateClass
{
	IFX_RT_ERROR = -1,
	IFX_SUCCESS = 0,
	IFX_WARNING = 1,
	IFX_ERROR   = 2,

	IFX_NOT_FOUND = 100,

	IFX_CONNECTION_OK = 200,
	IFX_CONNECTION_WARN = 201,
	IFX_CONNECTION_ERROR = 202,

	IFX_STATE_UNKNOWN = 1000
} IfxSqlStateClass;

/*
 * IfxSqlStateMessage
 *
 * Message from a SQLSTATE exception
 */
typedef struct IfxSqlStateMessage
{
	int   id;
	int   len;
	char  text[255];
} IfxSqlStateMessage;

/*
 * Define supported source types which can
 * be converted into a specific PostgreSQL type.
 *
 * This table is translated from INFORMIXDIR/incl/sqltypes.h. We
 * can't use it directly, since Informix redefines int2 and int4
 * in ifxtypes.h, which is included by sqltypes.h implicitely. So
 * maintain our own lookup types for compatiblity.
 */

typedef enum IfxSourceType
{

	IFX_CHARACTER = 0,
	IFX_SMALLINT  = 1,
	IFX_INTEGER   = 2,
	IFX_FLOAT     = 3,
	IFX_SMFLOAT   = 4,
	IFX_DECIMAL   = 5,
	IFX_SERIAL    = 6,
	IFX_DATE      = 7,
	IFX_MONEY     = 8,
	IFX_NULL      = 9,
	IFX_DTIME     = 10,
	IFX_BYTES     = 11,
	IFX_TEXT      = 12,
	IFX_VCHAR     = 13,
	IFX_INTERVAL  = 14,
	IFX_NCHAR     = 15,
	IFX_NVCHAR    = 16,
	IFX_INT8      = 17,
	IFX_SERIAL8   = 18,
	IFX_SET       = 19,
	IFX_MULTISET  = 20,
	IFX_LIST      = 21,
	IFX_ROW       = 22,
	IFX_COLLECTION = 23,
	IFX_ROWREF     = 24

} IfxSourceType;

typedef enum IfxIndicatorValue
{
	INDICATOR_NULL,
	INDICATOR_NOT_NULL
} IfxIndicatorValue;

/*
 * IfxAttrDef
 *
 * Holds Informix column type definitions
 * retrieved by an query descriptor.
 */
typedef struct IfxAttrDef
{
	IfxSourceType  type;
	int            len;
	char          *name;
	IfxIndicatorValue indicator;
} IfxAttrDef;

/*
 * Stores plan data, e.g. row and cost estimation.
 */
typedef struct IfxPlanData
{
	/*
	 * Cost parameters
	 */
	double estimated_rows;
	double connection_costs;

} IfxPlanData;

/*
 * Informix connection
 */
typedef struct IfxConnectionInfo
{
	char *servername;
	char *informixdir;
	char *username;
	char *password;
	char *database;

	/*
	 * once generated, this holds the connection
	 * name and connection string
	 */
	char conname[IFX_CONNAME_LEN + 1];
	char *dsn;

	/*
	 * Table to access with query
	 */
	char *tablename;
	char *query;

	/* plan data */
	IfxPlanData planData;

} IfxConnectionInfo;

/*
 * IfxStatementInfo
 *
 * Transports state information during a FDW scan.
 */
typedef struct IfxStatementInfo
{
	/*
	 * Links the informix database connection to
	 * this statement.
	 */
	char conname[IFX_CONNAME_LEN + 1];

	/*
	 * Which kind of Informix Cursor to use.
	 * Currently this is always set to IFX_DEFAULT_CURSOR.
	 */
	IfxCursorUsage cursorUsage;

	/*
	 * SQLSTATE value retrieved by the last action.
	 *
	 * This value shouldn't be set directly. Instead
	 * use ifxSetError() (see ifx_connection.ec for details).
	 */
	char sqlstate[6];

	/*
	 * Number of exceptions per ESQL call.
	 */
	int exception_count;

	/*
	 * Query text.
	 */
	char *query;

	/*
	 * Name of an associated cursor.
	 */
	char *cursor_name;

	/*
	 * Name of the prepared statement.
	 */
	char *stmt_name;

	/*
	 * Size of the informix column descriptor list.
     * Should match number of IfxAttrDef elements in ifxAttrDefs.
	 */
	int ifxAttrCount;

	/*
	 * Size of the foreign table column descriptor
	 * list. Should match the number of columns of
	 * the foreign table defined in pgAttrDefs.
	 */

	/*
	 * Dynamic list of attribute definitions
	 */
	IfxAttrDef **ifxAttrDefs;

} IfxStatementInfo;

extern void ifxCreateConnectionXact(IfxConnectionInfo *coninfo);
void ifxSetConnection(IfxConnectionInfo *coninfo);
void ifxDestroyConnection(char *conname);
void ifxPrepareQuery(IfxStatementInfo *state);
void ifxAllocateDescriptor(char *descr_name);
void ifxDescribeAllocatorByName(char *stmt_name, char *descr_name);
int ifxDescriptorColumnCount(char *descr_name);
void ifxDeclareCursorForPrepared(IfxStatementInfo *state);
void ifxOpenCursorForPrepared(IfxStatementInfo *state);
int ifxGetColumnAttributes(IfxStatementInfo *state);
void ifxFetchRowFromCursor(IfxStatementInfo *state);

/*
 * Error handling
 */
IfxSqlStateClass ifxSetException(IfxStatementInfo *state);
IfxSqlStateClass ifxConnectionStatus();
int ifxExceptionCount();
void ifxGetSqlStateMessage(int id, IfxSqlStateMessage *message);
int ifxGetSqlCode();

/*
 * Functions to access specific datatypes
 * within result sets
 */
int ifxGetInt(IfxStatementInfo *state, int attnum);

#endif
