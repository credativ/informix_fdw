/*-------------------------------------------------------------------------
 *
 * ifx_conncache.c
 *		  foreign-data wrapper for IBM INFORMIX databases
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  informix_fdw/ifx_conncache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/memutils.h"

#include "ifx_conncache.h"

/*
 * Expected size of the foreign table cache. This might not
 * be very large, but it is depending on the number
 * of distinct FDW tables actually used. So assume a moderate
 * number of fixed slots.
 */
#define IFX_FTCACHE_SIZE 32

/*
 * Expected number of cached connections.
 */
#define IFX_CONNCACHE_SIZE 16

/*
 * Name of the connection hash table
 */
#define IFX_CONNCACHE_HASHTABLE "IFX_CONN_CACHE"

/*
 * Name of the foreign table hash table
 */
#define IFX_FT_HASHTABLE "IFX_FT_CACHE"

static void ifxFTCache_init();
static void ifxConnCache_init();

extern bool IfxCacheIsInitialized;
extern InformixCache ifxCache;

void InformixCacheInit()
{
	if (!IfxCacheIsInitialized)
	{
		ifxFTCache_init();
		ifxConnCache_init();
		IfxCacheIsInitialized = true;
	}
}

/*
 * Initialize INFORMIX connection cache. Each connection
 * to an INFORMIX server is explicitely named and cached
 * within a backend-local list.
 */
static void ifxConnCache_init()
{
	HASHCTL hash_ctl;
	MemoryContext old_ctxt;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = IFX_CONNAME_LEN;
	hash_ctl.entrysize = sizeof(IfxCachedConnection);

	/*
	 * We need to allocate within the backend's
	 * memory context, otherwise we will loose all allocated
	 * objects when the transaction ends.
	 */
	hash_ctl.hcxt = TopMemoryContext;

	old_ctxt = MemoryContextSwitchTo(TopMemoryContext);
	ifxCache.connections = hash_create(IFX_CONNCACHE_HASHTABLE,
									   IFX_CONNCACHE_SIZE,
									   &hash_ctl,
									   HASH_ELEM | HASH_CONTEXT);

	/*
	 * Back to old context.
	 */
	MemoryContextSwitchTo(old_ctxt);

}

/*
 * Initialize the foreign table cache. This cache
 * stashes information of a used foreign table away, such as
 * cost estimates and other information.
 */
static void ifxFTCache_init()
{
	HASHCTL hash_ctl;
	MemoryContext old_ctxt;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(IfxFTCacheItem);
    hash_ctl.hash = oid_hash;

	/*
	 * Seems HTAB always allocates in TopMemoryContext if no
	 * other context is requested, but assign it explicitely
	 * anyways.
	 */
	hash_ctl.hcxt = TopMemoryContext;

	/*
	 * Since the cache is initialized the first time and is
	 * required to be stay alive the whole lifetime of the current
	 * backend, we allocate all cache-related objects in the TopMemoryContext.
	 *
	 * Please note that we don't delete them afterwards,
	 * the cached objects are freed on backend termination automatically.
	 */

	old_ctxt = MemoryContextSwitchTo(TopMemoryContext);

	ifxCache.tables = hash_create(IFX_FT_HASHTABLE, IFX_FTCACHE_SIZE,
								  &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/*
	 * Back to old context
	 */
	MemoryContextSwitchTo(old_ctxt);
}

/*
 * Add a new INFORMIX connection to the connection cache.
 */
IfxCachedConnection *
ifxConnCache_add(Oid foreignTableOid, IfxConnectionInfo *coninfo, bool *found)
{
	IfxCachedConnection *item;

	Assert(IfxCacheIsInitialized);

	/*
	 * Lookup the connection name. If it is *not*
	 * already registered, create a new cached entry, otherwise
	 * return the cached item.
	 */
	item = hash_search(ifxCache.connections, (void *) coninfo->conname,
					   HASH_ENTER, found);

	/*
	 * Connection already cached?
	 */
	if (!*found)
	{
		MemoryContext old_cxt;

		/*
		 * Make sure, cached connection information is
		 * allocated within the memory context actually
		 * used by the connection cache.
		 */
		old_cxt = MemoryContextSwitchTo(TopMemoryContext);

		item->establishedByOid = foreignTableOid;
		item->servername       = pstrdup(coninfo->servername);
		item->informixdir      = pstrdup(coninfo->informixdir);
		item->username         = pstrdup(coninfo->username);
		item->database         = pstrdup(coninfo->database);

		/* can be NULL */
		if (coninfo->db_locale != NULL)
			item->db_locale        = pstrdup(coninfo->db_locale);
		else
			item->db_locale = NULL;

		if (coninfo->client_locale != NULL)
			item->client_locale    = pstrdup(coninfo->client_locale);
		else
			item->client_locale = NULL;

		/* also initialize usage counter */
		item->usage = 1;

		MemoryContextSwitchTo(old_cxt);
	}
	else
	{
		item->usage++;
	}

	return item;
}

/*
 * Remove an existing connection handle from the cache.
 * If the requested connection doesn't exist yet, NULL
 * is returned.
 */
IfxCachedConnection *
ifxConnCache_rm(char *conname, bool *found)
{
	IfxCachedConnection *item;

	Assert(IfxCacheIsInitialized);

	/*
	 * Lookup the connection name. If found, the entry is
	 * removed from the cache, but returned to the caller.
	 */
	item = hash_search(ifxCache.connections, (void *) conname,
					   HASH_REMOVE, found);

	/*
	 * If something found, return it, otherwise
	 * NULL is returned.
	 */
	return item;
}

/*
 * Registers or updates the given foreign table (FT) in the
 * local backend cache. Returns a pointer to the cached FT structure.
 *
 * This function assumes we never get an InvalidOid here, so the caller
 * might be advised to check the Oid before.
 */
IfxFTCacheItem *ifxFTCache_add(Oid foreignTableOid, char *conname)
{
	IfxFTCacheItem *item;
	bool found;

	/*
	 * Lookup the OID of this foreign table. If it is *not*
	 * already registered, create a new cached entry. We assume
	 * we never get an InvalidOid here.
	 */
	item = hash_search(ifxCache.tables, (void *) &foreignTableOid,
					   HASH_ENTER, &found);

	/*
	 * If this is a new entry, initialize all required values
	 */
	if (!found)
	{
		/* TO DO: initialize cached table properties */
		item->foreignTableOid = foreignTableOid;
		bzero(item->ifx_connection_name, IFX_CONNAME_LEN);
		StrNCpy(item->ifx_connection_name, conname, strlen(conname));
	}

	return item;
}
