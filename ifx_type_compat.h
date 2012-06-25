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
 * Maximum length of a signed int8 value
 * in its character representation.
 */
#define IFX_INT8_CHAR_LEN 19

/*
 * Flags to identify current state
 * of informix calls.
 */
#define IFX_STACK_EMPTY    0
#define IFX_STACK_PREPARE  1
#define IFX_STACK_DECLARE  2
#define IFX_STACK_ALLOCATE 4
#define IFX_STACK_DESCRIBE 8
#define IFX_STACK_OPEN     16

/*
 * IS8601 compatible DATE and DATETIME
 * output formats for Informix.
 */
#define IFX_DBDATE_FORMAT "Y4MD-"
#define IFX_ISO_DATE "%iY-%m-%d"
#define IFX_ISO_TIMESTAMP "%iY-%m-%d %H:%M:%S"

/*
 * Binary size of informix DATE value
 */
#define IFX_DATE_BINARY_SIZE 4

/*
 * Default buffer length for
 * DATE character strings.
 */
#define IFX_DATE_BUFFER_LEN 20

/*
 * Default buffer length for
 * DATETIME character strings.
 */
#define IFX_DATETIME_BUFFER_LEN 26

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
	char  sqlcode;
	char  sqlstate[6];
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
	IFX_ROWREF     = 24,

	/*
	 * Special ESQL/C types.
	 */
	IFX_LVARCHAR  = 43,
	IFX_BOOLEAN   = 45,
	IFX_INFX_INT8 = 52

} IfxSourceType;


/*
 * Defines Informix indicator values. Currently,
 * NULL and NOT NULL values are supported.
 */
typedef enum IfxIndicatorValue
{
	INDICATOR_NULL,
	INDICATOR_NOT_NULL,
	INDICATOR_NOT_VALID
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
	size_t            mem_allocated; /* memory allocated for data */
	int               offset;        /* offset into the data memory buffer */
} IfxAttrDef;

/*
 * Stores plan data, e.g. row and cost estimation.
 * Pushed down from the planner stage to ifxBeginForeignScan().
 * Stays here only because it's currently used in IfxConnectionInfo
 * (XXX: need to change that, not really required) :(
 */
typedef struct IfxPlanData
{
	/*
	 * Cost parameters, this might be
	 * passed through FDW options.
	 */
	double estimated_rows;
	double connection_costs;

	/*
	 * Table statistics derived
	 * from Informix.
	 */
	double nrows;
	double npages;
	double costs;
	short row_size;

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

	/*
	 * Environment options, e.g. GL_DATE, ...
	 */
	char *gl_date;
	char *gl_datetime;
	char *client_locale;
	char *db_locale;
	short tx_enabled; /* 0 = n tx, 1 = tx enabled */
	short predicate_pushdown; /* 0 = disabled, 1 = enabled */

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
	 * Call stack. Used to track
	 * the current state of informix calls.
	 */
	unsigned short call_stack;

	/*
	 * Number of exceptions per ESQL call.
	 */
	int exception_count;

	/*
	 * Query text.
	 */
	char *query;

	/*
	 * Predicate string to be pushed down.
	 * This is the string representation of the WHERE
	 * expressions examined during the planning phase...
	 */
	char *predicate;

	/*
	 * Name of an associated cursor.
	 */
	char *cursor_name;

	/*
	 * Name of the prepared statement.
	 */
	char *stmt_name;

	/*
	 * Named descriptor area.
	 */
	char *descr_name;

	/*
	 * Size of the informix column descriptor list.
     * Should match number of IfxAttrDef elements in ifxAttrDefs.
	 */
	int ifxAttrCount;

	/*
	 * Size of the foreign table column descriptor
	 * list. Should match the number of columns of
	 * the foreign table defined in pgAttrDefs.
	 *
	 * XXX: Required here?
	 *
	 * XXX: Pointer to internal informix SQLDA structure
	 */
	void *sqlda;

	/*
	 * Allocated row size. Should be > 0 in case any allocations
	 * to SQLDA and sqldata within sqlvar structs occur.
	 */
	size_t row_size;

	/*
	 * Memory area for sqlvar structs to store values.
	 */
	char *data;

	/*
	 * Memory area for SQLDA indicator values.
	 */
	short *indicator;

	/*
	 * Dynamic list of attribute definitions
	 */
	IfxAttrDef *ifxAttrDefs;

} IfxStatementInfo;

extern void ifxCreateConnectionXact(IfxConnectionInfo *coninfo);
void ifxSetConnection(IfxConnectionInfo *coninfo);
void ifxDestroyConnection(char *conname);
void ifxPrepareQuery(char *query, char *stmt_name);
void ifxAllocateDescriptor(char *descr_name, int num_items);
void ifxDescribeAllocatorByName(IfxStatementInfo *state);
int ifxDescriptorColumnCount(IfxStatementInfo *state);
void ifxDeclareCursorForPrepared(char *stmt_name, char *cursor_name);
void ifxOpenCursorForPrepared(IfxStatementInfo *state);
size_t ifxGetColumnAttributes(IfxStatementInfo *state);
void ifxFetchRowFromCursor(IfxStatementInfo *state);
void ifxDeallocateSQLDA(IfxStatementInfo *state);
void ifxSetupDataBufferAligned(IfxStatementInfo *state);
void ifxCloseCursor(IfxStatementInfo *state);
int ifxFreeResource(IfxStatementInfo *state,
					int stackentry);
void ifxDeallocateDescriptor(char *descr_name);
char ifxGetSQLCAWarn(signed short warn);
int ifxGetSQLCAErrd(signed short ca);

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
char *ifxGetInt8(IfxStatementInfo *state, int attnum, char *buf);
char *ifxGetDateAsString(IfxStatementInfo *state, int ifx_attnum,
						 char *buf);
char *ifxGetTimestampAsString(IfxStatementInfo *state, int ifx_attnum,
							  char *buf);
char ifxGetBool(IfxStatementInfo *state, int ifx_attnum);
int2 ifxGetInt2(IfxStatementInfo *state, int attnum);
int ifxGetInt4(IfxStatementInfo *state, int attnum);
char *ifxGetText(IfxStatementInfo *state, int attnum);

/*
 * Helper macros.
 */
#define SQLCA_WARN_SET 0
#define SQLCA_WARN_TRANSACTIONS 1
#define SQLCA_WARN_ANSI 2
#define SQLCA_WARN_NO_IFX_SE 3
#define SQLCA_WARN_FLOAT_IS_DEC 4
#define SQLCA_WARN_RESERVED 5 /* not used */
#define SQLCA_WARN_REPLICATED_DB 6
#define SQLCA_WARN_DB_LOCALE_MISMATCH 7
#define SQLCA_WARN(a) sqlca.sqlwarn.sqlwarn##a

#define SQLCA_NROWS_PROCESSED 0
#define SQLCA_NROWS_WEIGHT    3

#endif
