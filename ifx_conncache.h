/*-------------------------------------------------------------------------
 *
 * ifx_conncache.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2012, credativ GmbH
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_conncache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAVE_IFX_CONNCACHE_H
#define HAVE_IFX_CONNCACHE_H

#include "ifx_fdw.h"

#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/dynahash.h"

/*
 * Cached information for an INFORMIX
 * foreign table.
 */
typedef struct IfxFTCacheItem
{
	/*
	 * ID of the associated INFORMIX database
	 * connection.
	 */
	char ifx_connection_name[IFX_CONNAME_LEN];

	Oid foreignTableOid;

	/*
	 * XXX: Cached cost estimates for this foreign table
	 */
} IfxFTCacheItem;

/*
 * Cached informix database connection.
 */
typedef struct IfxCachedConnection
{
	char ifx_connection_name[IFX_CONNAME_LEN];
	char *servername;
	char *informixdir;
	char *username;
	char *database;
	char *db_locale;
	char *client_locale;
	int usage;
	int tx_enabled;
	int db_ansi;
	Oid establishedByOid;
} IfxCachedConnection;

/*
 * Caches INFORMIX database connections and foreign
 * table informations.
 */
typedef struct InformixCache
{
  HTAB *connections;
  HTAB *tables;
} InformixCache;

/*
 * Global module options
 *
 * IfxCacheIsInitialized: indicates wether the cache structure
 *                        was already initialized.
 * ifxCache: INFORMIX connection and foreign table properties cache
 */
bool IfxCacheIsInitialized;
InformixCache ifxCache;

void InformixCacheInit(void);

/*
 * Register a new INFORMIX foreign table to the cache.
 */
IfxFTCacheItem *ifxFTCache_add(Oid foreignTableOid, char *conname);
IfxCachedConnection *ifxConnCache_add(Oid foreignTableOid,
									  IfxConnectionInfo *coninfo,
                                      bool *found);
IfxCachedConnection *ifxConnCache_rm(char *conname,
                                     bool *found);

#endif
