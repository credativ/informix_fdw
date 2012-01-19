/*-------------------------------------------------------------------------
 *
 * ifx_conncache.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_conncache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAVE_IFX_CONNCACHE_H
#define HAVE_IFX_CONNCACHE_H

#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/dynahash.h"

#include "ifx_fdw.h"

/*
 * Cached information for an INFORMIX
 * foreign table.
 */
typedef struct IfxFTCacheItem
{
  Oid foreignTableOid;

  /*
   * ID of the associated INFORMIX database
   * connection.
   */
  char ifx_connection_name[IFX_CONNAME_LEN];

  /*
   * Cached cost estimates for this foreign table
   */
} IfxFTCacheItem;

/*
 * Cached informix database connection.
 */
typedef struct IfxCachedConnection
{
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

void InformixCacheInit();

/*
 * Register a new INFORMIX foreign table to the cache.
 */
IfxFTCacheItem *ifxFTCache_add(Oid foreignTableOid, char *conname);
IfxCachedConnection *ifxConnCache_add(Oid foreignTableOid, char *conname,
                                      bool *found);

#endif
