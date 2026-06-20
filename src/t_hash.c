/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 * 
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "server.h"
#include "redisassert.h"
#include "ebuckets.h"
#include "entry.h"
#include "cluster_asm.h"
#include "vector.h"
#include <math.h>

/* Threshold for HEXPIRE and HPERSIST to be considered whether it is worth to
 * update the expiration time of the hash object in global HFE DS. */
#define HASH_NEW_EXPIRE_DIFF_THRESHOLD max(4000, 1<<EB_BUCKET_KEY_PRECISION)

/* Reserve 2 bits out of hash-field expiration time for possible future lightweight
 * indexing/categorizing of fields. It can be achieved by hacking HFE as follows:
 *
 *    HPEXPIREAT key [ 2^47 + USER_INDEX ] FIELDS numfields field [field …]
 *
 * Redis will also need to expose kind of HEXPIRESCAN and HEXPIRECOUNT for this
 * idea. Yet to be better defined.
 *
 * HFE_MAX_ABS_TIME_MSEC constraint must be enforced only at API level. Internally,
 * the expiration time can be up to EB_EXPIRE_TIME_MAX for future readiness.
 */
#define HFE_MAX_ABS_TIME_MSEC (EB_EXPIRE_TIME_MAX >> 2)

typedef enum GetFieldRes {
    /* common (Used by hashTypeGet* value family) */
    GETF_OK = 0,            /* The field was found. */
    GETF_NOT_FOUND,         /* The field was not found. */
    GETF_EXPIRED,           /* Logically expired (Might be lazy deleted or not) */
    GETF_EXPIRED_HASH,      /* Delete hash since retrieved field was expired and
                             * it was the last field in the hash. */
} GetFieldRes;

typedef listpackEntry CommonEntry; /* extend usage beyond lp */

#define FIELDS_STACK_SIZE 16

/* A vec with an embedded stack buffer, used to collect field robj pointers
 * for subkey notifications without heap allocation in the common case. */
typedef struct fieldvec { vec v; void *buf[FIELDS_STACK_SIZE]; } fieldvec;

static inline vec *fieldvecInit(fieldvec *fv, size_t cap) {
    vecInit(&fv->v, fv->buf, FIELDS_STACK_SIZE);
    vecReserve(&fv->v, cap);
    return &fv->v;
}

static inline hashTemplate *hashTypeGetTemplate(robj *o) {
    return (o->encoding == OBJ_ENCODING_TMPL_LP) ?
        hashTemplateLpGetTemplate(o->ptr) :
        ((hashTemplateArray *)o->ptr)->tmpl;
}

/* hash field expiration (HFE) funcs */
static ExpireAction onFieldExpire(eItem item, void *ctx);
static ExpireMeta* hentryGetExpireMeta(const eItem field);
static void hexpireGenericCommand(client *c, long long basetime, int unit);
static void hfieldPersist(robj *hashObj, Entry *entry);
static void propagateHashFieldDeletion(redisDb *db, sds key, char *field, size_t fieldLen);

/* hash dictType funcs */
static void dictEntryDestructor(dict *d, void *entry);
static size_t hashDictMetadataBytes(dict *d);
static size_t hashDictWithExpireMetadataBytes(dict *d);
static void hashDictWithExpireOnRelease(dict *d);
static kvobj* hashTypeLookupWriteOrCreate(client *c, robj *key);
static int hashTypeCanConvertTmplToListpack(robj *o);
static void hashTypeConvertTmplGeneric(robj *o, int target_enc, int with_hfe);
static void hashTypeConvertTmplToListpackOrHT(robj *o, int lp_enc, int with_hfe);

/*-----------------------------------------------------------------------------
 * Define dictType of hash
 *
 * - Stores fields as entry objects (field-value pairs) with optional expiration
 * - Note that small hashes are represented with listpacks
 * - Once expiration is set for a field, the dict instance and corresponding
 *   dictType are replaced with a dict containing metadata for Hash Field
 *   Expiration (HFE) and using dictType `entryHashDictTypeWithHFE`
 * - Dict uses no_value=1 since entry contains both field and value
 *----------------------------------------------------------------------------*/
dictType entryHashDictType = {
    dictSdsHash,                                /* lookup hash function */
    NULL,                                       /* key dup */
    NULL,                                       /* val dup */
    dictSdsKeyCompare,                          /* lookup key compare */
    dictEntryDestructor,                       /* key destructor */
    NULL,                                       /* val destructor (value is in entry) */
    .dictMetadataBytes = hashDictMetadataBytes,
    .no_value = 1,                              /* entry contains both field and value */
    .keys_are_odd = 1,                          /* entry pointers (SDS) are always odd */
};

/* Define alternative dictType of hash with hash-field expiration (HFE) support */
dictType entryHashDictTypeWithHFE = {
    dictSdsHash,                                /* lookup hash function */
    NULL,                                       /* key dup */
    NULL,                                       /* val dup */
    dictSdsKeyCompare,                          /* lookup key compare */
    dictEntryDestructor,                       /* key destructor */
    NULL,                                       /* val destructor (value is in entry) */
    .dictMetadataBytes = hashDictWithExpireMetadataBytes,
    .onDictRelease = hashDictWithExpireOnRelease,
    .no_value = 1,                              /* entry contains both field and value */
    .keys_are_odd = 1,                          /* entry pointers (SDS) are always odd */
};

/*-----------------------------------------------------------------------------
 * Hash Field Expiration (HFE) Feature
 *
 * Each hash instance maintains its own set of hash field expiration within its
 * private ebuckets DS. In order to support HFE active expire cycle across hash
 * instances, hashes with associated HFE will be also registered in a global
 * ebuckets DS with expiration time value that reflects their next minimum
 * time to expire (db->subexpires). The global HFE Active expiration will be
 * triggered from activeExpireCycle() function and in turn will invoke "local"
 * HFE Active sub-expiration for each hash instance that has expired fields.
 *----------------------------------------------------------------------------*/
EbucketsType subexpiresBucketsType = {
    .onDeleteItem = NULL,
    .getExpireMeta = hashGetExpireMeta,   /* get ExpireMeta attached to each hash */
    .itemsAddrAreOdd = 0,                 /* Addresses of dict are even */
};

/* htExpireMetadata - ebuckets-type for hash fields with time-Expiration. ebuckets
 * instance Will be attached to each hash that has at least one field with expiry
 * time. */
EbucketsType hashFieldExpireBucketsType = {
    .onDeleteItem = NULL,
    .getExpireMeta = hentryGetExpireMeta, /* get ExpireMeta attached to each field */
    .itemsAddrAreOdd = 1,                 /* Addresses of hfield (entry/sds) are odd!! */
};

/* OnFieldExpireCtx passed to OnFieldExpire() */
typedef struct OnFieldExpireCtx {
    robj *hashObj;
    redisDb *db;
    int activeEx; /* 1 for active expire, 0 for lazy expire */
    vec *vexpired; /* Expired fields vector */
} OnFieldExpireCtx;

/* The implementation of hashes by dict was modified from storing fields as sds
 * strings to store "entry" objects (field-value pairs with optional expiration).
 * The entry structure unifies field and value into a single allocation, with
 * optional expiration metadata. This is simpler than the previous mstr approach
 * and provides better memory locality.
 */

/* Used by hpersistCommand() */
typedef enum SetPersistRes {
    HFE_PERSIST_NO_FIELD =     -2,   /* No such hash-field */
    HFE_PERSIST_NO_TTL =       -1,   /* No TTL attached to the field */
    HFE_PERSIST_OK =            1
} SetPersistRes;

static inline int isDictWithMetaHFE(dict *d) {
    return d->type == &entryHashDictTypeWithHFE;
}

/*-----------------------------------------------------------------------------
 * setex* - Set field's expiration
 *
 * Setting expiration time to fields might be time-consuming and complex since
 * each update of expiration time, not only updates `ebuckets` of corresponding
 * hash, but also might update `ebuckets` of global HFE DS. It is required to opt
 * sequence of field updates with expirartion for a given hash, such that only
 * once done, the global HFE DS will get updated.
 *
 * To do so, follow the scheme:
 * 1. Call hashTypeSetExInit() to initialize the HashTypeSetEx struct.
 * 2. Call hashTypeSetEx() one time or more, for each field/expiration update.
 * 3. Call hashTypeSetExDone() for notification and update of global HFE.
 *----------------------------------------------------------------------------*/

/* Returned value of hashTypeSetEx() */
typedef enum SetExRes {
    HSETEX_OK =                1,   /* Expiration time set/updated as expected */
    HSETEX_NO_FIELD =         -2,   /* No such hash-field */
    HSETEX_NO_CONDITION_MET =  0,   /* Specified NX | XX | GT | LT condition not met */
    HSETEX_DELETED =           2,   /* Field deleted because the specified time is in the past */
} SetExRes;

/* Used by httlGenericCommand() */
typedef enum GetExpireTimeRes {
    HFE_GET_NO_FIELD =          -2, /* No such hash-field */
    HFE_GET_NO_TTL =            -1, /* No TTL attached to the field */
} GetExpireTimeRes;

typedef enum ExpireSetCond {
    HFE_NX = 1<<0,
    HFE_XX = 1<<1,
    HFE_GT = 1<<2,
    HFE_LT = 1<<3
} ExpireSetCond;

/* Used by hashTypeSetEx() for setting fields or their expiry  */
typedef struct HashTypeSetEx {

    /*** config ***/
    ExpireSetCond expireSetCond;        /* [XX | NX | GT | LT] */

    /*** metadata ***/
    uint64_t minExpire;                 /* if uninit EB_EXPIRE_TIME_INVALID */
    redisDb *db;
    robj *key, *hashObj;
    uint64_t minExpireFields;           /* Trace updated fields and their previous/new
                                         * minimum expiration time. If minimum recorded
                                         * is above minExpire of the hash, then we don't
                                         * have to update global HFE DS */

    /* Optionally provide client for notification */
    client *c;
    const char *cmd;
} HashTypeSetEx;

int hashTypeSetExInit(robj *key, kvobj *kvo, client *c, redisDb *db,
                      ExpireSetCond expireSetCond, HashTypeSetEx *ex);

SetExRes hashTypeSetEx(robj *o, sds field, uint64_t expireAt, HashTypeSetEx *exInfo);

void hashTypeSetExDone(HashTypeSetEx *e);

/*-----------------------------------------------------------------------------
 * Accessor functions for dictType of hash
 *----------------------------------------------------------------------------*/

static void dictEntryDestructor(dict *d, void *entry) {
    size_t usable;
    size_t *alloc_size = htGetMetadataSize(d);

    /* If attached TTL to the field, then remove it from hash's private ebuckets. */
    if (entryGetExpiry(entry) != EB_EXPIRE_TIME_INVALID) {
        htMetadataEx *dictExpireMeta = htGetMetadataEx(d);
        ebRemove(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, entry);
    }

    entryFree(entry, &usable);
    *alloc_size -= usable;

    /* Don't have to update global HFE DS. It's unnecessary. Implementing this
     * would introduce significant complexity and overhead for an operation that
     * isn't critical. In the worst case scenario, the hash will be efficiently
     * updated later by an active-expire operation, or it will be removed by the
     * hash's dbGenericDelete() function. */
}

static size_t hashDictMetadataBytes(dict *d) {
    UNUSED(d);
    return sizeof(size_t);
}

static size_t hashDictWithExpireMetadataBytes(dict *d) {
    UNUSED(d);
    /* expireMeta of the hash, ref to ebuckets and pointer to hash's key */
    return sizeof(htMetadataEx);
}

static void hashDictWithExpireOnRelease(dict *d) {
    /* for sure allocated with metadata. Otherwise, this func won't be registered */
    htMetadataEx *dictExpireMeta = htGetMetadataEx(d);
    ebDestroy(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, NULL);
}

/*-----------------------------------------------------------------------------
 * listpackEx functions
 *----------------------------------------------------------------------------*/
/*
 * If any of hash field expiration command is called on a listpack hash object
 * for the first time, we convert it to OBJ_ENCODING_LISTPACK_EX encoding.
 * We allocate "struct listpackEx" which holds listpack pointer and expiry
 * metadata. In the listpack string, we append another TTL entry for each field
 * value pair. From now on, listpack will have triplets in it: field-value-ttl.
 * If TTL is not set for a field, we store 'zero' as the TTL value. 'zero' is
 * encoded as two bytes in the listpack. Memory overhead of a non-existing TTL
 * will be two bytes per field.
 *
 * Fields in the listpack will be ordered by TTL. Field with the smallest expiry
 * time will be the first item. Fields without TTL will be at the end of the
 * listpack. This way, it is easier/faster to find expired items.
 */

#define HASH_LP_NO_TTL 0

struct listpackEx *listpackExCreate(void) {
    listpackEx *lpt = zcalloc(sizeof(*lpt));
    lpt->meta.trash = 1;
    lpt->lp = NULL;
    return lpt;
}

static void listpackExFree(listpackEx *lpt) {
    lpFree(lpt->lp);
    zfree(lpt);
}

#define HASH_TMPL_STACK_ENTRIES 128

/* Global template registry - also accessible via htemplates */
static hashTemplates *htemplates = NULL;

/* Allocate the smallest available template ID and register in by_id.
 * Main-thread only (template creation), but a BIO lazyfree thread may read by_id
 * concurrently (hashTemplateLpFree), so the array is grown/written under the
 * lock that also guards reclaim_ids. */
static uint64_t allocateTemplateId(hashTemplate *tmpl) {
    pthread_mutex_lock(&htemplates->lock);
    /* Scan for first free slot. TODO: need a faster way? */
    for (size_t i = 0; i < htemplates->by_id_cap; i++) {
        if (htemplates->by_id[i] == NULL) {
            htemplates->by_id[i] = tmpl;
            pthread_mutex_unlock(&htemplates->lock);
            return i;
        }
    }
    /* All slots full - grow array. */
    uint64_t id = htemplates->by_id_cap;
    htemplates->by_id_cap = htemplates->by_id_cap ? htemplates->by_id_cap * 2 : 16;
    htemplates->by_id = zrealloc(htemplates->by_id, sizeof(*htemplates->by_id) * htemplates->by_id_cap);
    memset(htemplates->by_id + id, 0, sizeof(hashTemplate *) * (htemplates->by_id_cap - id));
    htemplates->by_id[id] = tmpl;
    pthread_mutex_unlock(&htemplates->lock);
    return id;
}

/* Recycle a template ID when template is freed. Main-thread only, but taken
 * under the lock since a BIO lazyfree thread may be reading by_id. */
static void recycleTemplateId(uint64_t id) {
    pthread_mutex_lock(&htemplates->lock);
    htemplates->by_id[id] = NULL;

    /* This runs from the registry's key destructor, which dictGenericDelete()
     * invokes before decrementing the table's used count, so the template being
     * removed is still counted: dictSize()==1 means it is the last one and the
     * by_id index can be released. */
    if (dictSize(htemplates->registry) == 1) {
        zfree(htemplates->by_id);
        htemplates->by_id = NULL;
        htemplates->by_id_cap = 0;
    }
    pthread_mutex_unlock(&htemplates->lock);
}

/* Lookup template by ID. Returns NULL if invalid. */
hashTemplate *hashTemplateGetById(uint64_t id) {
    if (id >= htemplates->by_id_cap) return NULL;
    return htemplates->by_id[id];
}

/* Compute SipHash for a single field. */
static uint64_t computeFieldHash(sds field) {
    return dictGenHashFunction(field, sdslen(field));
}

/* Compute commutative hash for a set of fields.
 * Uses sum of per-field SipHashes so fields can be
 * incrementally added/removed:
 *   add:    hash += computeFieldHash(new_field)
 *   remove: hash -= computeFieldHash(removed_field) */
static uint64_t computeFieldsHash(sds *fields, unsigned long long field_count) {
    uint64_t hash = 0;
    for (unsigned long long i = 0; i < field_count; i++)
        hash += computeFieldHash(fields[i]);
    return hash;
}

/* Template registry dict callbacks. Key is hashTemplate*, value is same. */
static uint64_t templateRegistryHashFunc(const void *key) {
    const hashTemplate *tmpl = key;
    return tmpl->hash;
}

static int templateRegistryKeyCompare(dictCmpCache *cache, const void *k1,
                                       const void *k2) {
    UNUSED(cache);
    const hashTemplate *t1 = k1;
    const hashTemplate *t2 = k2;

    if (t1->hash != t2->hash) return 0;
    if (t1->field_count != t2->field_count) return 0;

    for (unsigned long long i = 0; i < t1->field_count; i++) {
        if (sdscmplen(t1->fields[i], t2->fields[i]) != 0)
            return 0;
    }
    return 1;
}

static void templateRegistryKeyDestructor(dict *d, void *key) {
    UNUSED(d);
    hashTemplate *tmpl = key;
    /* propargv is freed in hashTemplateDecrHoldRef when the last non-key
     * holder releases the template. */
    serverAssert(tmpl->propargv == NULL);
    recycleTemplateId(tmpl->id);
    for (unsigned long long i = 0; i < tmpl->field_count; i++)
        sdsfree(tmpl->fields[i]);
    zfree(tmpl->fields);
    zfree(tmpl);
}

static dictType templateRegistryDictType = {
    templateRegistryHashFunc,      /* hash function */
    NULL,                          /* key dup */
    NULL,                          /* val dup */
    templateRegistryKeyCompare,    /* key compare */
    templateRegistryKeyDestructor, /* key destructor (key=tmpl) */
    NULL,                          /* val destructor (val=same as key) */
    NULL                           /* allow to expand */
};

/* Initialize hash templates registry. */
void hashTemplatesInit(void) {
    if (htemplates) return;
    htemplates = zcalloc(sizeof(hashTemplates));
    htemplates->registry = dictCreate(&templateRegistryDictType);
    pthread_mutex_init(&htemplates->lock, NULL);
    server.htemplates = htemplates;
}

/* Create a new hash tmpl (internal, not from registry).
 * fields must be pre-sorted. */
static hashTemplate *hashTemplateCreateInternal(uint64_t hash, sds *fields, unsigned long long field_count) {
    hashTemplate *tmpl = zmalloc(sizeof(*tmpl));
    tmpl->hash = hash;
    tmpl->hold_refcount = 0;
    atomicSet(tmpl->key_refcount, 0);
    tmpl->field_count = field_count;
    tmpl->fields = zmalloc(sizeof(sds) * field_count);
    tmpl->propargv = NULL; /* Lazy: built on first HIMPORT SET / HSETC. */
    for (unsigned long long i = 0; i < field_count; i++)
        tmpl->fields[i] = sdsdup(fields[i]);

    tmpl->id = allocateTemplateId(tmpl);
    return tmpl;
}

/* Get or create a tmpl. fields must be pre-sorted.
 * If hash is 0, it will be computed from fields. */
hashTemplate *hashTemplateGetOrCreate(sds *fields, unsigned long long field_count) {
    return hashTemplateGetOrCreateWithHash(
        computeFieldsHash(fields, field_count), fields, field_count);
}

/* Get or create a tmpl with pre-computed hash.
 * Used for incremental hash updates (HDEL/HSET). */
hashTemplate *hashTemplateGetOrCreateWithHash(uint64_t hash, sds *fields,
                                              unsigned long long field_count) {
    hashTemplate query = {
        .hash = hash,
        .field_count = field_count,
        .fields = fields
    };

    dictEntry *de = dictFind(htemplates->registry, &query);
    if (de) return dictGetKey(de);

    hashTemplate *tmpl = hashTemplateCreateInternal(hash, fields, field_count);
    dictAdd(htemplates->registry, tmpl, NULL);
    return tmpl;
}

/* Increment key_refcount (called when creating a hash key). Always on the main
 * thread, but the counter is atomic because a BIO lazyfree thread may decrement
 * a (different key of the) same template concurrently. */
void hashTemplateIncrKeyRef(hashTemplate *tmpl) {
    serverAssert(pthread_equal(pthread_self(), server.main_thread_id));
    atomicIncr(tmpl->key_refcount, 1);
    atomicIncr(htemplates->total_key_refs, 1);
}

/* Increment hold_refcount while non-key state references the template. */
void hashTemplateIncrHoldRef(hashTemplate *tmpl) {
    serverAssert(pthread_equal(pthread_self(), server.main_thread_id));
    tmpl->hold_refcount++;
}

/* Enqueue a template id for the main thread to reclaim. Pushed by whichever
 * thread brought the template to a fully-unreferenced state; the actual removal
 * happens in hashTemplateDrainPendingFree after re-validating the refcounts. The
 * queue holds at most one live id per template between drains, so it is bounded
 * by the number of templates, not the number of keys freed. */
static void hashTemplateEnqueueReclaim(uint64_t id) {
    pthread_mutex_lock(&htemplates->lock);
    if (htemplates->reclaim_count == htemplates->reclaim_cap) {
        htemplates->reclaim_cap = htemplates->reclaim_cap ?
            htemplates->reclaim_cap * 2 : 16;
        htemplates->reclaim_ids = zrealloc(htemplates->reclaim_ids,
            sizeof(*htemplates->reclaim_ids) * htemplates->reclaim_cap);
    }
    htemplates->reclaim_ids[htemplates->reclaim_count++] = id;
    pthread_mutex_unlock(&htemplates->lock);
}

/* Decrement hold_refcount. Main-thread only (clients and RDB load), so
 * hold_refcount and propargv need no locking. When the last non-key holder is
 * gone and no key references remain either, free the template inline. */
void hashTemplateDecrHoldRef(hashTemplate *tmpl) {
    serverAssert(pthread_equal(pthread_self(), server.main_thread_id));
    serverAssert(tmpl->hold_refcount > 0);
    tmpl->hold_refcount--;
    if (tmpl->hold_refcount > 0) return;

    /* No non-key holder is left, so the cached propagation argv is dead weight. */
    if (tmpl->propargv) {
        /* Field robjs live at propargv[2 .. 2+field_count). */
        for (unsigned long long i = 0; i < tmpl->field_count; i++)
            decrRefCount(tmpl->propargv[2 + i]);
        zfree(tmpl->propargv);
        tmpl->propargv = NULL;
    }

    unsigned long long key_refs;
    atomicGet(tmpl->key_refcount, key_refs);
    if (key_refs == 0) dictDelete(htemplates->registry, tmpl);
}

/* Decrement key_refcount. Thread-safe: called from the hash object-free paths,
 * which may run on the main thread or in a BIO lazyfree thread.
 *
 * On the main thread the template is freed inline once it is fully unreferenced,
 * exactly like before, so transient templates produced by incremental field
 * updates never accumulate in the registry. In a BIO lazyfree thread the registry
 * must not be mutated, so only the decrement that brings key_refcount to zero
 * enqueues the template id for the main thread to reclaim - bounding the queue by
 * the number of templates, not the number of keys freed.
 *
 * tmpl->id is read before the decrement: once key_refcount hits zero the main
 * thread may free the struct concurrently (via DecrHoldRef), so the BIO path must
 * not dereference tmpl afterwards. */
void hashTemplateDecrKeyRef(hashTemplate *tmpl) {
    uint64_t id = tmpl->id;
    unsigned long long new_count;
    atomicDecr(htemplates->total_key_refs, 1);
    atomicIncrGet(tmpl->key_refcount, new_count, -1);
    serverAssert(new_count != (unsigned long long)-1); /* underflow guard */
    if (new_count != 0) return;

    if (pthread_equal(pthread_self(), server.main_thread_id)) {
        /* Safe to touch tmpl: only the main thread frees templates, and no key
         * ref remains for a BIO thread to be racing on. */
        if (tmpl->hold_refcount == 0) dictDelete(htemplates->registry, tmpl);
    } else {
        hashTemplateEnqueueReclaim(id);
    }
}

/* Drain queued template reclaims. Main-thread only (serverCron). Each queued id
 * is re-validated: a template is removed from the registry only if it is still
 * fully unreferenced. Duplicate or stale ids (already freed, or revived) resolve
 * to NULL or fail the re-check and are skipped, so the pass is idempotent. */
void hashTemplateDrainPendingFree(void) {
    pthread_mutex_lock(&htemplates->lock);
    uint64_t *queue = htemplates->reclaim_ids;
    size_t count = htemplates->reclaim_count;
    htemplates->reclaim_ids = NULL;
    htemplates->reclaim_count = 0;
    htemplates->reclaim_cap = 0;
    pthread_mutex_unlock(&htemplates->lock);

    for (size_t i = 0; i < count; i++) {
        hashTemplate *tmpl = hashTemplateGetById(queue[i]);
        if (tmpl == NULL) continue; /* already reclaimed (duplicate id) */
        unsigned long long key_refs;
        atomicGet(tmpl->key_refcount, key_refs);
        if (key_refs == 0 && tmpl->hold_refcount == 0)
            dictDelete(htemplates->registry, tmpl);
    }
    zfree(queue);
}

/* Get number of templates in the registry. */
size_t hashTemplateRegistrySize(void) {
    if (!htemplates->registry) return 0;
    return dictSize(htemplates->registry);
}

/* Get total number of template-based keys (sum of all key_refcounts). */
size_t hashTemplateKeyCount(void) {
    if (!htemplates) return 0;
    size_t count;
    atomicGet(htemplates->total_key_refs, count);
    return count;
}

/* Lazy-build the propargv. Field robjs are stored contiguously at
 * propargv[2 .. 2+field_count), making them directly usable both for
 * propagation and for keyspace subkey notifications. Returns the field
 * segment (&propargv[2]) for caller convenience. */
static robj **hashTemplateGetPropargvFields(hashTemplate *tmpl) {
    if (!tmpl->propargv) {
        unsigned long long n = tmpl->field_count;
        tmpl->propargv = zmalloc(sizeof(robj *) * (2 + n * 2));
        tmpl->propargv[0] = shared.hsetc;
        for (unsigned long long i = 0; i < n; i++) {
            sds field = tmpl->fields[i];
            tmpl->propargv[2 + i] = createStringObject(field, sdslen(field));
        }
    }
    return &tmpl->propargv[2];
}

/* Find field index in tmpl using binary search (fields are sorted).
 * Returns the index (>= 0) if found, otherwise -(insert_pos + 1) where
 * insert_pos is the position to splice field into to keep fields sorted. */
int hashTemplateFieldIndex(hashTemplate *tmpl, sds field) {
    int lo = 0, hi = (int)tmpl->field_count - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        int cmp = sdscmplen(field, tmpl->fields[mid]);
        if (cmp == 0) return mid;
        if (cmp < 0) hi = mid - 1;
        else lo = mid + 1;
    }
    return -(lo + 1);
}

/* Return 1 if fields are strictly ascending (sorted, no duplicates), else 0. */
int hashTemplateValidateFields(sds *fields, unsigned long long field_count) {
    for (unsigned long long i = 1; i < field_count; i++)
        if (sdscmplen(fields[i - 1], fields[i]) >= 0)
            return 0;
    return 1;
}

/* Return the equivalent non-template encoding name (listpack or hashtable)
 * for a template-encoded hash, by checking field/value sizes and count against
 * listpack limits. Used by the hash-template-mask-encoding compat shim so the
 * existing test suite can run with the feature enabled.
 * TODO: Remove before merge. */
char *hashTemplateEquivalentEncoding(robj *o) {
    hashTemplate *tmpl = hashTypeGetTemplate(o);

    if (tmpl->field_count > server.hash_max_listpack_entries)
        return "hashtable";

    for (unsigned long long i = 0; i < tmpl->field_count; i++) {
        if (sdslen(tmpl->fields[i]) > server.hash_max_listpack_value)
            return "hashtable";
    }

    if (o->encoding == OBJ_ENCODING_TMPL_LP) {
        unsigned char *lp = o->ptr;
        unsigned char *p = lpFirst(lp);
        p = lpNext(lp, p); /* skip template ID */
        for (unsigned long long i = 0; i < tmpl->field_count && p; i++) {
            unsigned int vlen;
            long long vll;
            unsigned char *vstr = lpGetValue(p, &vlen, &vll);
            if (vstr && vlen > server.hash_max_listpack_value)
                return "hashtable";
            p = lpNext(lp, p);
        }
    } else {
        hashTemplateArray *hta = o->ptr;
        for (unsigned long long i = 0; i < tmpl->field_count; i++) {
            if (hta->values[i] && sdslen(hta->values[i]) > server.hash_max_listpack_value)
                return "hashtable";
        }
    }

    return "listpack";
}


/*-----------------------------------------------------------------------------
 * HashTemplateLp functions (OBJ_ENCODING_TMPL_LP)
 *
 * Listpack format: [template_id (varint)][value1][value2]...
 * o->ptr points directly to the listpack - no struct wrapper.
 *----------------------------------------------------------------------------*/

/* Get template from listpack (first entry is template ID).
 * The first entry of every TMPL_LP listpack is written as an integer by
 * hashTemplateLpCreate(); a non-NULL vstr or an unknown ID indicates
 * either internal corruption or a mismatch with the template registry. */
/* Read the immutable template ID stored as the first listpack entry. Safe to
 * call off the main thread: the ID never changes for a given listpack. */
uint64_t hashTemplateLpGetTemplateId(unsigned char *lp) {
    unsigned char *p = lpFirst(lp);
    long long id;
    unsigned char *vstr = lpGetValue(p, NULL, &id);
    if (vstr != NULL)
        serverPanic("TMPL_LP listpack header is not an integer template ID");
    return (uint64_t)id;
}

hashTemplate *hashTemplateLpGetTemplate(unsigned char *lp) {
    hashTemplate *tmpl = hashTemplateGetById(hashTemplateLpGetTemplateId(lp));
    if (tmpl == NULL)
        serverPanic("TMPL_LP listpack references unknown template ID");
    return tmpl;
}

/* Replace template ID in listpack. Does NOT update refcounts. */
unsigned char *hashTemplateLpSetTemplate(unsigned char *lp, hashTemplate *tmpl) {
    unsigned char *p = lpFirst(lp);
    return lpReplaceInteger(lp, &p, (long long)tmpl->id);
}

/* Get pointer to first value entry (skip template ID). */
static unsigned char *hashTemplateLpFirstValue(unsigned char *lp) {
    unsigned char *p = lpFirst(lp);  /* template ID entry */
    return lpNext(lp, p);            /* first value */
}

/* Create listpack with template ID and values. Increments key_refcount. */
unsigned char *hashTemplateLpCreate(hashTemplate *tmpl, sds *values) {
    hashTemplateIncrKeyRef(tmpl);

    unsigned long long n = tmpl->field_count;
    /* +1 for template ID entry */
    listpackEntry stack_entries[HASH_TMPL_STACK_ENTRIES + 1];
    listpackEntry *entries = (n + 1 <= HASH_TMPL_STACK_ENTRIES + 1) ?
                             stack_entries :
                             zmalloc(sizeof(listpackEntry) * (n + 1));

    /* First entry: template ID */
    entries[0].lval = (long long)tmpl->id;
    entries[0].sval = NULL;

    /* Remaining entries: values */
    for (unsigned long long i = 0; i < n; i++) {
        entries[i + 1].sval = (unsigned char *)values[i];
        entries[i + 1].slen = sdslen(values[i]);
    }

    unsigned char *lp = lpBatchInsert(NULL, NULL, LP_BEFORE, entries, n + 1, NULL);
    if (entries != stack_entries) zfree(entries);

    return lp;
}

/* Free a template listpack: release its key-ref and free the listpack. May run
 * in a BIO lazyfree thread, so the id->template lookup is done under the lock
 * (the main thread may be growing by_id concurrently). The template stays valid
 * while this outstanding key-ref exists, so the pointer is safe to decrement. */
void hashTemplateLpFree(unsigned char *lp) {
    uint64_t id = hashTemplateLpGetTemplateId(lp);
    pthread_mutex_lock(&htemplates->lock);
    hashTemplate *tmpl = hashTemplateGetById(id);
    pthread_mutex_unlock(&htemplates->lock);
    if (tmpl == NULL)
        serverPanic("TMPL_LP listpack references unknown template ID");
    hashTemplateDecrKeyRef(tmpl);
    lpFree(lp);
}

/*-----------------------------------------------------------------------------
 * hashTemplateArray functions (OBJ_ENCODING_TMPL_ARRAY)
 *----------------------------------------------------------------------------*/

/* Create a new hashTemplateArray. Increments key_refcount.
 * If take is set, takes ownership of the SDS strings in values array.
 * Otherwise copies them with sdsdup. */
hashTemplateArray *hashTemplateArrayCreate(hashTemplate *tmpl, sds *values, int take) {
    unsigned long long n = tmpl->field_count;
    hashTemplateArray *hta = zmalloc(sizeof(*hta) + sizeof(sds) * n);
    hta->tmpl = tmpl;

    for (unsigned long long i = 0; i < n; i++)
        hta->values[i] = take ? values[i] : sdsdup(values[i]);

    hashTemplateIncrKeyRef(tmpl);
    return hta;
}

/* Free a hashTemplateArray (release key-ref and free data). May run in a BIO
 * lazyfree thread; the template pointer and its id/field_count are immutable
 * and stay valid while this key-ref is outstanding. */
void hashTemplateArrayFree(hashTemplateArray *hta) {
    for (unsigned long long i = 0; i < hta->tmpl->field_count; i++)
        sdsfree(hta->values[i]);
    hashTemplateDecrKeyRef(hta->tmpl);
    zfree(hta);
}

struct lpFingArgs {
    uint64_t max_to_search; /* [in] Max number of tuples to search */
    uint64_t expire_time;   /* [in] Find the tuple that has a TTL larger than expire_time */
    unsigned char *p;       /* [out] First item of the tuple that has a TTL larger than expire_time */
    int expired;            /* [out] Number of tuples that have TTLs less than expire_time */
    int index;              /* Internally used */
    unsigned char *fptr;    /* Internally used, temp ptr */
};

/* Callback for lpFindCb(). Used to find number of expired fields as part of
 * active expiry or when trying to find the position for the new field according
 * to its expiry time.*/
static int cbFindInListpack(const unsigned char *lp, unsigned char *p,
                            void *user, unsigned char *s, long long slen)
{
    (void) lp;
    struct lpFingArgs *r = user;

    r->index++;

    if (r->max_to_search == 0)
        return 0; /* Break the loop and return */

    if (r->index % 3 == 1) {
        r->fptr = p;  /* First item of the tuple. */
    } else if (r->index % 3 == 0) {
        serverAssert(!s);

        /* Third item of a tuple is expiry time */
        if (slen == HASH_LP_NO_TTL || (uint64_t) slen >= r->expire_time) {
            r->p = r->fptr;
            return 0; /* Break the loop and return */
        }
        r->expired++;
        r->max_to_search--;
    }

    return 1;
}

/* Returns number of expired fields. */
static uint64_t listpackExExpireDryRun(const robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);

    listpackEx *lpt = o->ptr;

    struct lpFingArgs r = {
        .max_to_search = UINT64_MAX,
        .expire_time = commandTimeSnapshot(),
    };

    lpFindCb(lpt->lp, NULL, &r, cbFindInListpack, 0);
    return r.expired;
}

/* Returns the expiration time of the item with the nearest expiration. */
static uint64_t listpackExGetMinExpire(robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);

    long long expireAt;
    unsigned char *fptr;
    listpackEx *lpt = o->ptr;

    /* As fields are ordered by expire time, first field will have the smallest
     * expiry time. Third element is the expiry time of the first field */
    fptr = lpSeek(lpt->lp, 2);
    if (fptr != NULL) {
        serverAssert(lpGetIntegerValue(fptr, &expireAt));

        /* Check if this is a non-volatile field. */
        if (expireAt != HASH_LP_NO_TTL)
            return expireAt;
    }

    return EB_EXPIRE_TIME_INVALID;
}

/* Walk over fields and delete the expired ones. */
void listpackExExpire(redisDb *db, kvobj *kv, ExpireInfo *info) {
    OnFieldExpireCtx *ctx = info->ctx;
    serverAssert(kv->encoding == OBJ_ENCODING_LISTPACK_EX);
    uint64_t expired = 0, min = EB_EXPIRE_TIME_INVALID;
    unsigned char *ptr;
    listpackEx *lpt = kv->ptr;

    ptr = lpFirst(lpt->lp);

    sds key = kvobjGetKey(kv);

    while (ptr != NULL && (info->itemsExpired < info->maxToExpire)) {
        long long val;
        int64_t flen;
        unsigned char intbuf[LP_INTBUF_SIZE], *fref;

        fref = lpGet(ptr, &flen, intbuf);

        ptr = lpNext(lpt->lp, ptr);
        serverAssert(ptr);
        ptr = lpNext(lpt->lp, ptr);
        serverAssert(ptr && lpGetIntegerValue(ptr, &val));

        /* Fields are ordered by expiry time. If we reached to a non-expired
         * or a non-volatile field, we know rest is not yet expired. */
        if (val == HASH_LP_NO_TTL || (uint64_t) val > info->now)
            break;

        /* Collect expired field for subkey notification. */
        if (ctx->vexpired) {
            char *fstr = (char *)(fref ? fref : intbuf);
            vecPush(ctx->vexpired, createStringObject(fstr, flen));
        }

        propagateHashFieldDeletion(db, key, (char *)((fref) ? fref : intbuf), flen);
        server.stat_expired_subkeys++;
        if (ctx->activeEx) server.stat_expired_subkeys_active++;

        ptr = lpNext(lpt->lp, ptr);

        info->itemsExpired++;
        expired++;
    }

    if (expired) {
        size_t oldsize = 0;
        if (server.memory_tracking_enabled)
            oldsize = kvobjAllocSize(kv);
        lpt->lp = lpDeleteRange(lpt->lp, 0, expired * 3);
        if (server.memory_tracking_enabled)
            updateSlotAllocSize(db, getKeySlot(key), kv, oldsize, kvobjAllocSize(kv));

        /* update keysizes */
        unsigned long l = lpLength(lpt->lp) / 3;
        updateKeysizesHist(db, OBJ_HASH, l + expired, l);
    }

    min = hashTypeGetMinExpire(kv, 1 /*accurate*/);
    info->nextExpireTime = min;
}

static void listpackExAddInternal(robj *o, listpackEntry ent[3]) {
    listpackEx *lpt = o->ptr;

    /* Shortcut, just append at the end if this is a non-volatile field. */
    if (ent[2].lval == HASH_LP_NO_TTL) {
        lpt->lp = lpBatchAppend(lpt->lp, ent, 3);
        return;
    }

    struct lpFingArgs r = {
            .max_to_search = UINT64_MAX,
            .expire_time = ent[2].lval,
    };

    /* Check if there is a field with a larger TTL. */
    lpFindCb(lpt->lp, NULL, &r, cbFindInListpack, 0);

    /* If list is empty or there is no field with a larger TTL, result will be
     * NULL. Otherwise, just insert before the found item.*/
    if (r.p)
        lpt->lp = lpBatchInsert(lpt->lp, r.p, LP_BEFORE, ent, 3, NULL);
    else
        lpt->lp = lpBatchAppend(lpt->lp, ent, 3);
}

/* Add new field ordered by expire time. */
void listpackExAddNew(robj *o, char *field, size_t flen,
                      char *value, size_t vlen, uint64_t expireAt) {
    listpackEntry ent[3] = {
        {.sval = (unsigned char*) field, .slen = flen},
        {.sval = (unsigned char*) value, .slen = vlen},
        {.lval = expireAt}
    };

    listpackExAddInternal(o, ent);
}

/* If expiry time is changed, this function will place field into the correct
 * position. First, it deletes the field and re-inserts to the listpack ordered
 * by expiry time. */
static void listpackExUpdateExpiry(robj *o, sds field,
                                   unsigned char *fptr,
                                   unsigned char *vptr,
                                   uint64_t expire_at) {
    unsigned int slen = 0;
    long long val = 0;
    unsigned char tmp[512] = {0};
    unsigned char *valstr;
    sds tmpval = NULL;
    listpackEx *lpt = o->ptr;

    /* Copy value */
    valstr = lpGetValue(vptr, &slen, &val);
    if (valstr) {
        /* Normally, item length in the listpack is limited by
         * 'hash-max-listpack-value' config. It is unlikely, but it might be
         * larger than sizeof(tmp). */
        if (slen > sizeof(tmp))
            tmpval = sdsnewlen(valstr, slen);
        else
            memcpy(tmp, valstr, slen);
    }

    /* Delete field name, value and expiry time */
    lpt->lp = lpDeleteRangeWithEntry(lpt->lp, &fptr, 3);

    listpackEntry ent[3] = {{0}};

    ent[0].sval = (unsigned char*) field;
    ent[0].slen = sdslen(field);

    if (valstr) {
        ent[1].sval = tmpval ? (unsigned char *) tmpval : tmp;
        ent[1].slen = slen;
    } else {
        ent[1].lval = val;
    }
    ent[2].lval = expire_at;

    listpackExAddInternal(o, ent);
    sdsfree(tmpval);
}

/* Update field expire time. */
SetExRes hashTypeSetExpiryListpack(HashTypeSetEx *ex, sds field,
                                   unsigned char *fptr, unsigned char *vptr,
                                   unsigned char *tptr, uint64_t expireAt)
{
    long long expireTime;
    uint64_t prevExpire = EB_EXPIRE_TIME_INVALID;

    serverAssert(lpGetIntegerValue(tptr, &expireTime));

    if (expireTime != HASH_LP_NO_TTL) {
        prevExpire = (uint64_t) expireTime;
    }

    /* Special value of EXPIRE_TIME_INVALID indicates field should be persisted.*/
    if (expireAt == EB_EXPIRE_TIME_INVALID) {
        /* Return error if already there is no ttl. */
        if (prevExpire == EB_EXPIRE_TIME_INVALID)
            return HSETEX_NO_CONDITION_MET;
        listpackExUpdateExpiry(ex->hashObj, field, fptr, vptr, HASH_LP_NO_TTL);
        return HSETEX_OK;
    }

    if (prevExpire == EB_EXPIRE_TIME_INVALID) {
        /* For fields without expiry, LT condition is considered valid */
        if (ex->expireSetCond & (HFE_XX | HFE_GT))
            return HSETEX_NO_CONDITION_MET;
    } else {
        if (((ex->expireSetCond == HFE_GT) && (prevExpire >= expireAt)) ||
            ((ex->expireSetCond == HFE_LT) && (prevExpire <= expireAt)) ||
            (ex->expireSetCond == HFE_NX) )
            return HSETEX_NO_CONDITION_MET;

        /* Track of minimum expiration time (only later update global HFE DS) */
        if (ex->minExpireFields > prevExpire)
            ex->minExpireFields = prevExpire;
    }

    /* If expired, then delete the field and propagate the deletion.
     * If replica, continue like the field is valid */
    if (unlikely(checkAlreadyExpired(expireAt))) {
        propagateHashFieldDeletion(ex->db, ex->key->ptr, field, sdslen(field));
        hashTypeDelete(ex->hashObj, field);
        server.stat_expired_subkeys++;
        return HSETEX_DELETED;
    }

    if (ex->minExpireFields > expireAt)
        ex->minExpireFields = expireAt;

    listpackExUpdateExpiry(ex->hashObj, field, fptr, vptr, expireAt);
    return HSETEX_OK;
}

/* Returns 1 if expired */
int hashTypeIsExpired(const robj *o, uint64_t expireAt) {
    if (server.allow_access_expired) return 0;

    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        if (expireAt == HASH_LP_NO_TTL)
            return 0;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (expireAt == EB_EXPIRE_TIME_INVALID)
            return 0;
    } else {
        serverPanic("Unknown encoding: %d", o->encoding);
    }

    return (mstime_t) expireAt < commandTimeSnapshot();
}

/* Returns listpack pointer of the object. */
unsigned char *hashTypeListpackGetLp(robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK)
        return o->ptr;
    else if (o->encoding == OBJ_ENCODING_LISTPACK_EX)
        return ((listpackEx*)o->ptr)->lp;

    serverPanic("Unknown encoding: %d", o->encoding);
}

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

int hashTypeCanCreateTmplLp(unsigned long long count, sds *values) {
    if ((size_t)count > server.hash_max_listpack_entries)
        return 0;

    size_t sum = 0;
    for (unsigned long long i = 0; i < count; i++) {
        size_t len = sdslen(values[i]);
        if (len > server.hash_max_listpack_value)
            return 0;
        sum += len;
    }

    return lpSafeToAdd(NULL, sum);
}


/* Build a hash object that shares `tmpl` and stores the given values.
 * Two template-backed encodings exist:
 *   TMPL_LP    - values packed in a listpack (compact, small hashes).
 *   TMPL_ARRAY - values stored as an sds array (used when listpack limits
 *                would be exceeded, or when many fields make per-field
 *                listpack walks expensive). */
robj *createHashObjectFromTemplate(hashTemplate *tmpl, sds *values) {
    robj *o;

    if (hashTypeCanCreateTmplLp(tmpl->field_count, values)) {
        o = createObject(OBJ_HASH, hashTemplateLpCreate(tmpl, values));
        o->encoding = OBJ_ENCODING_TMPL_LP;
    } else {
        o = createObject(OBJ_HASH, hashTemplateArrayCreate(tmpl, values, 0));
        o->encoding = OBJ_ENCODING_TMPL_ARRAY;
    }
    return o;
}

/* Check the length of a number of objects to see if we need to convert a
 * listpack to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
void hashTypeTryConversion(redisDb *db, kvobj *o, robj **argv, int start, int end) {
    int tmpl_lp = (o->encoding == OBJ_ENCODING_TMPL_LP);
    int target_enc = tmpl_lp ? OBJ_ENCODING_TMPL_ARRAY : OBJ_ENCODING_HT;

    /* TMPL_ARRAY and HT don't need conversion checks. */
    if (o->encoding != OBJ_ENCODING_LISTPACK &&
        o->encoding != OBJ_ENCODING_LISTPACK_EX &&
        o->encoding != OBJ_ENCODING_TMPL_LP)
    {
        return;
    }

    /* Determine target encoding for conversion. */
    if (!tmpl_lp) {
        /* Check field count limit for regular listpack. */
        size_t new_fields = (end - start + 1) / 2;
        if (new_fields > server.hash_max_listpack_entries) {
            hashTypeConvert(db, o, OBJ_ENCODING_HT);
            dictExpand(o->ptr, new_fields);
            return;
        }
    }

    /* Check value sizes (and field sizes for non-TMPL_LP). */
    size_t sum = 0;
    for (int i = start; i <= end; i++) {
        if (tmpl_lp && ((i - start) % 2 == 0))
            continue;
        if (!sdsEncodedObject(argv[i])) 
            continue;
        size_t len = sdslen(argv[i]->ptr);
        if (len > server.hash_max_listpack_value) {
            hashTypeConvert(db, o, target_enc);
            return;
        }
        sum += len;
    }
    /* o is one of LISTPACK / LISTPACK_EX / TMPL_LP here (guarded above). The
     * raw listpack lives in o->ptr for LISTPACK and TMPL_LP; LISTPACK_EX wraps
     * it. We don't use hashTypeListpackGetLp() so that helper can stay strict
     * and keep panicking for TMPL_LP at every other call site. */
    unsigned char *lp = (o->encoding == OBJ_ENCODING_LISTPACK_EX) ?
                        ((listpackEx*)o->ptr)->lp : o->ptr;
    if (!lpSafeToAdd(lp, sum))
        hashTypeConvert(db, o, target_enc);
}

/* Get the value from a listpack encoded hash, identified by field. */
GetFieldRes hashTypeGetFromListpack(robj *o, sds field,
                            unsigned char **vstr,
                            unsigned int *vlen,
                            long long *vll,
                            uint64_t *expiredAt)
{
    *expiredAt = EB_EXPIRE_TIME_INVALID;
    unsigned char *zl, *fptr = NULL, *vptr = NULL;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = lpNext(zl, fptr);
                serverAssert(vptr != NULL);
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        long long expire;
        unsigned char *h;
        listpackEx *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                vptr = lpNext(lpt->lp, fptr);
                serverAssert(vptr != NULL);

                h = lpNext(lpt->lp, vptr);
                serverAssert(h && lpGetIntegerValue(h, &expire));
                if (expire != HASH_LP_NO_TTL)
                    *expiredAt = expire;
            }
        }
    } else {
        serverPanic("Unknown hash encoding: %d", o->encoding);
    }

    if (vptr != NULL) {
        *vstr = lpGetValue(vptr, vlen, vll);
        return GETF_OK;
    }

    return GETF_NOT_FOUND;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
GetFieldRes hashTypeGetFromHashTable(robj *o, sds field, sds *value, uint64_t *expiredAt) {
    dictEntry *de;

    *expiredAt = EB_EXPIRE_TIME_INVALID;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);

    if (de == NULL)
        return GETF_NOT_FOUND;

    Entry *entry = dictGetKey(de);
    *expiredAt = entryGetExpiry(entry);
    *value = entryGetValue(entry);
    return GETF_OK;
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field.
 * Arguments:
 * hfeFlags      - Lookup for HFE_LAZY_* flags
 *
 * Returned:
 * GetFieldRes  - Result of get operation
 * vstr, vlen   - if string, ref in either *vstr and *vlen if it's
 *                returned in string form,
 * vll          - or stored in *vll if it's returned as a number.
 *                If *vll is populated *vstr is set to NULL, so the caller can
 *                always check the function return by checking the return value
 *                for GETF_OK and checking if vll (or vstr) is NULL.
 * expiredAt    - if the field has an expiration time, it will be set to the expiration 
 *                time of the field. Otherwise, will be set to EB_EXPIRE_TIME_INVALID.
 */
GetFieldRes hashTypeGetValue(redisDb *db, kvobj *o, sds field, unsigned char **vstr,
                                   unsigned int *vlen, long long *vll, 
                                   int hfeFlags, uint64_t *expiredAt)
{
    sds key = kvobjGetKey(o);
    GetFieldRes res;
    uint64_t dummy;
    size_t oldsize = 0;
    if (expiredAt == NULL) expiredAt = &dummy;
    if (o->encoding == OBJ_ENCODING_LISTPACK ||
        o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        *vstr = NULL;
        res = hashTypeGetFromListpack(o, field, vstr, vlen, vll, expiredAt);

        if (res == GETF_NOT_FOUND)
            return GETF_NOT_FOUND;

    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value = NULL;
        if (server.memory_tracking_enabled && !(hfeFlags & HFE_LAZY_NO_UPDATE_ALLOCSIZES))
            oldsize = kvobjAllocSize(o);
        res = hashTypeGetFromHashTable(o, field, &value, expiredAt);
        if (server.memory_tracking_enabled && !(hfeFlags & HFE_LAZY_NO_UPDATE_ALLOCSIZES))
            updateSlotAllocSize(db, getKeySlot(key), o, oldsize, kvobjAllocSize(o));

        if (res == GETF_NOT_FOUND)
            return GETF_NOT_FOUND;

        *vstr = (unsigned char*) value;
        *vlen = sdslen(value);
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP) {
        unsigned char *lp = o->ptr;
        hashTemplate *tmpl = hashTemplateLpGetTemplate(lp);
        int idx = hashTemplateFieldIndex(tmpl, field);
        if (idx < 0) return GETF_NOT_FOUND;

        /* Get value at index (idx+1 to skip template ID entry). */
        unsigned char *p = lpSeek(lp, idx + 1);
        if (!p) return GETF_NOT_FOUND;

        *vstr = lpGetValue(p, vlen, vll);
        *expiredAt = EB_EXPIRE_TIME_INVALID;
        res = GETF_OK;
    } else if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = o->ptr;
        int idx = hashTemplateFieldIndex(hta->tmpl, field);
        if (idx < 0) return GETF_NOT_FOUND;

        sds value = hta->values[idx];
        *vstr = (unsigned char*) value;
        *vlen = sdslen(value);
        *expiredAt = EB_EXPIRE_TIME_INVALID;
        res = GETF_OK;
    } else {
        serverPanic("Unknown hash encoding");
    }

    if ((server.allow_access_expired) ||
        (*expiredAt >= (uint64_t) commandTimeSnapshot()) ||
        (hfeFlags & HFE_LAZY_ACCESS_EXPIRED))
        return GETF_OK;

    if (server.masterhost || server.cluster_enabled) {
        /* If CLIENT_MASTER, assume valid as long as it didn't get delete.
         *
         * In cluster mode, we also assume valid if we are importing data
         * from the source, to avoid deleting fields that are still in use.
         * We create a fake master client for data import, which can be
         * identified using the CLIENT_MASTER flag. */
        if (server.current_client && (server.current_client->flags & CLIENT_MASTER))
            return GETF_OK;

        /* For replica, if user client, then act as if expired, but don't delete! */
        if (server.masterhost) return GETF_EXPIRED;
    }

    if ((server.loading) ||
        (hfeFlags & HFE_LAZY_AVOID_FIELD_DEL) ||
        (isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)))
        return GETF_EXPIRED;

    /* delete the field and propagate the deletion */
    if (server.memory_tracking_enabled && !(hfeFlags & HFE_LAZY_NO_UPDATE_ALLOCSIZES))
        oldsize = kvobjAllocSize(o);
    serverAssert(hashTypeDelete(o, field) == 1);
    if (server.memory_tracking_enabled && !(hfeFlags & HFE_LAZY_NO_UPDATE_ALLOCSIZES))
        updateSlotAllocSize(db, getKeySlot(key), o, oldsize, kvobjAllocSize(o));
    propagateHashFieldDeletion(db, key, field, sdslen(field));
    server.stat_expired_subkeys++;

    if (!(hfeFlags & HFE_LAZY_NO_UPDATE_KEYSIZES)) {
        uint64_t l = hashTypeLength(o, 0);
        updateKeysizesHist(db, OBJ_HASH, l+1, l);
    }

    /* If the field is the last one in the hash, then the hash will be deleted */
    res = GETF_EXPIRED;
    robj *keyObj = createStringObject(key, sdslen(key));
    unsigned long length = hashTypeLength(o, 0);
    if ((length != 0) && !(hfeFlags & HFE_LAZY_NO_NOTIFICATION)) {
        robj fobj, *farr[1] = {&fobj};
        initStaticStringObject(fobj, field);
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpired", keyObj, db->id, farr, 1);
    }
    if ((length == 0) && (!(hfeFlags & HFE_LAZY_AVOID_HASH_DEL))) {
        if (!(hfeFlags & HFE_LAZY_NO_NOTIFICATION))
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", keyObj, db->id);
        dbDelete(db,keyObj);
        o = NULL;
        res = GETF_EXPIRED_HASH;
    }
    keyModified(NULL, db, keyObj, o, !(hfeFlags & HFE_LAZY_NO_SIGNAL));
    decrRefCount(keyObj);
    return res;
}

/* Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned.
 *
 * hfeFlags      - Lookup HFE_LAZY_* flags
 * isHashDeleted - If attempted to access expired field and it's the last field
 *                 in the hash, then the hash will as well be deleted. In this case,
 *                 isHashDeleted will be set to 1.
 * val           - If the field is found, then val will be set to the value object.
 * expireTime    - If the field exists (`GETF_OK`) then expireTime will be set to  
 *                 the expiration time of the field. Otherwise, it will be set to 0.
 *                 
 * Returns 1 if the field exists, and 0 when it doesn't.
 */
int hashTypeGetValueObject(redisDb *db, kvobj *o, sds field, int hfeFlags,
                           robj **val, uint64_t *expireTime, int *isHashDeleted) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (isHashDeleted) *isHashDeleted = 0;
    if (val) *val = NULL;
    GetFieldRes res = hashTypeGetValue(db,o,field,&vstr,&vlen,&vll, 
                                                   hfeFlags, expireTime);

    if (res == GETF_OK) {
        /* expireTime set to 0 if the field has no expiration time */ 
        if (expireTime && (*expireTime == EB_EXPIRE_TIME_INVALID))
            *expireTime = 0;
        
        /* If expected to return the value, then create a new object */
        if (val) {
            if (vstr) *val = createStringObject((char *) vstr, vlen);
            else *val = createStringObjectFromLongLong(vll);
        }
        return 1;
    }

    if ((res == GETF_EXPIRED_HASH) && (isHashDeleted))
        *isHashDeleted = 1;

    /* GETF_EXPIRED_HASH, GETF_EXPIRED, GETF_NOT_FOUND */
    return 0;
}

/* Test if the specified field exists in the given hash. If the field is
 * expired (HFE), then it will be lazy deleted unless HFE_LAZY_AVOID_FIELD_DEL 
 * hfeFlags is set.
 *
 * hfeFlags      - Lookup HFE_LAZY_* flags
 * isHashDeleted - If attempted to access expired field and it is the last field
 *                 in the hash, then the hash will as well be deleted. In this case,
 *                 isHashDeleted will be set to 1.
 *
 * Returns 1 if the field exists, and 0 when it doesn't.
 */
int hashTypeExists(redisDb *db, kvobj *o, sds field, int hfeFlags, int *isHashDeleted) {
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    GetFieldRes res = hashTypeGetValue(db, o, field, &vstr, &vlen, &vll, 
                                             hfeFlags, NULL);
    if (isHashDeleted)
        *isHashDeleted = (res == GETF_EXPIRED_HASH) ? 1 : 0;
    return (res == GETF_OK) ? 1 : 0;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD  -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE  -- The SDS value ownership passes to the function.
 * HASH_SET_KEEP_TTL --  keep original TTL if field already exists
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
#define HASH_SET_TAKE_FIELD  (1<<0)
#define HASH_SET_TAKE_VALUE  (1<<1)
#define HASH_SET_KEEP_TTL (1<<2)

static_assert(HASH_SET_TAKE_VALUE == ENTRY_TAKE_VALUE, "ENTRY_TAKE_VALUE must match HASH_SET_TAKE_VALUE");

int hashTypeSet(redisDb *db, kvobj *o, sds field, sds value, int flags) {
    int update = 0;

    /* Handle template encodings. */
    if (o->encoding == OBJ_ENCODING_TMPL_LP ||
        o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplate *tmpl = hashTypeGetTemplate(o);

        /* Check if field exists in tmpl; on miss, decode sorted insert position. */
        int field_idx = hashTemplateFieldIndex(tmpl, field);
        int is_new = field_idx < 0;

        /* Single pre-mutation conversion decision: a TMPL_LP can't hold this
         * update if the value exceeds the per-value limit, the listpack would
         * overflow, or a new field pushes the entry count over the limit.
         * Promote to TMPL_ARRAY up front - mirroring how a plain listpack
         * converts to a hashtable before the value is ever added. The template
         * (and thus field_idx) survives the conversion unchanged. */
        if (o->encoding == OBJ_ENCODING_TMPL_LP &&
            (sdslen(value) > server.hash_max_listpack_value ||
             !lpSafeToAdd(o->ptr, sdslen(value)) ||
             (is_new && tmpl->field_count + 1 > server.hash_max_listpack_entries)))
        {
            hashTypeConvert(db, o, OBJ_ENCODING_TMPL_ARRAY);
        }

        if (!is_new) {
            /* Field exists - update value in place. */
            if (o->encoding == OBJ_ENCODING_TMPL_LP) {
                unsigned char *lp = o->ptr;
                /* +1 to skip template ID entry */
                unsigned char *p = lpSeek(lp, field_idx + 1);
                o->ptr = lpReplace(lp, &p, (unsigned char *)value, sdslen(value));
            } else {
                hashTemplateArray *hta = o->ptr;
                if (hta->values[field_idx]) sdsfree(hta->values[field_idx]);
                hta->values[field_idx] = sdsdup(value);
            }
            update = 1;
            goto cleanup;
        }

        /* Field not in tmpl - build sorted new_fields by splicing at insert_pos. */
        int insert_pos = -field_idx - 1;
        unsigned long long new_field_count = tmpl->field_count + 1;
        sds stack_fields[HASH_TMPL_STACK_ENTRIES];
        sds *new_fields = (new_field_count <= HASH_TMPL_STACK_ENTRIES) ?
                          stack_fields : zmalloc(sizeof(sds) * new_field_count);
        memcpy(new_fields, tmpl->fields, sizeof(sds) * insert_pos);
        new_fields[insert_pos] = field;
        memcpy(&new_fields[insert_pos + 1], &tmpl->fields[insert_pos],
               sizeof(sds) * (tmpl->field_count - insert_pos));

        /* Incremental hash: add new field's hash. */
        uint64_t new_hash = tmpl->hash + computeFieldHash(field);
        hashTemplate *new_tmpl = hashTemplateGetOrCreateWithHash(new_hash, new_fields, new_field_count);
        hashTemplateIncrKeyRef(new_tmpl);
        if (new_fields != stack_fields) zfree(new_fields);

        /* Insert value at insert_pos in existing structure. */
        if (o->encoding == OBJ_ENCODING_TMPL_LP) {
            unsigned char *lp = o->ptr;
            /* Update template ID. */
            lp = hashTemplateLpSetTemplate(lp, new_tmpl);
            /* Insert value at position (offset +1 for template ID). */
            if ((unsigned long long)insert_pos == tmpl->field_count) {
                lp = lpAppend(lp, (unsigned char *)value, sdslen(value));
            } else {
                unsigned char *p = lpSeek(lp, insert_pos + 1);
                lp = lpInsertString(lp, (unsigned char *)value, sdslen(value), p, LP_BEFORE, NULL);
            }
            hashTemplateDecrKeyRef(tmpl);
            o->ptr = lp;
        } else {
            hashTemplateArray *hta = o->ptr;
            /* Expand struct and shift elements to make room. */
            hta = zrealloc(hta, sizeof(*hta) + sizeof(sds) * new_field_count);
            if ((unsigned long long)insert_pos < tmpl->field_count) {
                memmove(&hta->values[insert_pos + 1], &hta->values[insert_pos],
                        sizeof(sds) * (tmpl->field_count - insert_pos));
            }
            hta->values[insert_pos] = sdsdup(value);
            hashTemplateDecrKeyRef(tmpl);
            hta->tmpl = new_tmpl;
            o->ptr = hta;
        }

        goto cleanup;
    }

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr, *vptr;

        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = lpNext(zl, fptr);
                serverAssert(vptr != NULL);

                /* Replace value */
                zl = lpReplace(zl, &vptr, (unsigned char*)value, sdslen(value));
                update = 1;
            }
        }

        if (!update) {
            listpackEntry entries[2] = {
                {.sval = (unsigned char*) field, .slen = sdslen(field)},
                {.sval = (unsigned char*) value, .slen = sdslen(value)},
            };

            /* Push new field/value pair onto the tail of the listpack */
            zl = lpBatchAppend(zl, entries, 2);
        }
        o->ptr = zl;

        /* Check if the listpack needs to be converted to a hash table */
        if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
            hashTypeConvert(db, o, OBJ_ENCODING_HT);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *fptr = NULL, *vptr = NULL, *tptr = NULL;
        listpackEx *lpt = o->ptr;
        long long expireTime = HASH_LP_NO_TTL;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = lpNext(lpt->lp, fptr);
                serverAssert(vptr != NULL);

                /* Replace value */
                lpt->lp = lpReplace(lpt->lp, &vptr, (unsigned char *) value, sdslen(value));
                update = 1;

                fptr = lpPrev(lpt->lp, vptr);
                serverAssert(fptr != NULL);

                tptr = lpNext(lpt->lp, vptr);
                serverAssert(tptr && lpGetIntegerValue(tptr, &expireTime));

                if (flags & HASH_SET_KEEP_TTL) {
                    /* keep old field along with TTL */
                } else if (expireTime != HASH_LP_NO_TTL) {
                    /* re-insert field and override TTL */
                    listpackExUpdateExpiry(o, field, fptr, vptr, HASH_LP_NO_TTL);
                }
            }
        }

        if (!update)
            listpackExAddNew(o, field, sdslen(field), value, sdslen(value),
                             HASH_LP_NO_TTL);

        /* Check if the listpack needs to be converted to a hash table */
        if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
            hashTypeConvert(db, o, OBJ_ENCODING_HT);

    } else if (o->encoding == OBJ_ENCODING_HT) {
        dict *ht = o->ptr;
        /* check if field already exists */
        dictEntryLink bucket, link = dictFindLink(ht, field, &bucket);
        size_t *alloc_size = htGetMetadataSize(ht);

        /* take ownership of value if requested */
        uint32_t newEntryFlags = flags & HASH_SET_TAKE_VALUE;
        flags &= ~HASH_SET_TAKE_VALUE;

        if (link == NULL) {
            /* Create entry and transfer value ownership if possible */
            size_t usable;
            Entry *newEntry = entryCreate(field, value, newEntryFlags, &usable);

            dictSetKeyAtLink(ht, newEntry, &bucket, 1);
            *alloc_size += usable;
        } else {
            /* Existing field - update value in entry */
            Entry *oldEntry = dictGetKey(*link);

            /* Check if old entry has expiration before potentially freeing it */
            uint64_t oldExpireAt = entryGetExpiry(oldEntry);
            uint64_t newExpireAt = EB_EXPIRE_TIME_INVALID;

            /* If attached TTL to the old field, then remove it from hash's
             * private ebuckets. We do this before updating the value because
             * the entry might be reallocated and freed. */
            if (oldExpireAt != EB_EXPIRE_TIME_INVALID) {
                hfieldPersist(o, oldEntry);
                if (flags & HASH_SET_KEEP_TTL) {
                    newExpireAt = oldExpireAt;
                    newEntryFlags |= ENTRY_HAS_EXPIRY;
                }
            }
            
            ssize_t usableDiff;
            Entry *newEntry = entryUpdate(oldEntry, value, newEntryFlags, &usableDiff);

            /* If entry was reallocated, update the dict key */
            if (newEntry != oldEntry) {
                /* entryUpdate already freed the old entry if needed */
                /* Update the dict to point to the new entry using dictSetKeyAtLink (no_value=1) */
                dictSetKeyAtLink(ht, newEntry, &link, 0);
            }

            /* If keeping TTL, add the (potentially new) entry back to ebuckets */
            if (newExpireAt != EB_EXPIRE_TIME_INVALID) {
                dict *d = o->ptr;
                htMetadataEx *dictExpireMeta = htGetMetadataEx(d);
                ebAdd(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, newEntry, newExpireAt);
            }

            *alloc_size += usableDiff;
            update = 1;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

cleanup:
    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);

    /* Auto-convert to template if threshold met and not already template. */
    if (!update && server.hash_min_template_entries > 0 &&
        (o->encoding == OBJ_ENCODING_LISTPACK || o->encoding == OBJ_ENCODING_HT))
    {
        hashTypeTryConvertToTemplate(o);
    }

    return update;
}

SetExRes hashTypeSetExpiryHT(HashTypeSetEx *exInfo, sds field, uint64_t expireAt) {
    dict *ht = exInfo->hashObj->ptr;
    dictEntryLink link = NULL;
    Entry *entryNew = NULL;

    link = dictFindLink(ht, field, NULL);
    if (link == NULL)
        return HSETEX_NO_FIELD;

    dictEntry *existingEntry = *link;
    Entry *oldEntry = dictGetKey(existingEntry);
    /* Special value of EXPIRE_TIME_INVALID indicates field should be persisted.*/
    if (expireAt == EB_EXPIRE_TIME_INVALID) {
        /* Return error if already there is no ttl. */
        if (entryGetExpiry(oldEntry) == EB_EXPIRE_TIME_INVALID)
            return HSETEX_NO_CONDITION_MET;

        hfieldPersist(exInfo->hashObj, oldEntry);
        return HSETEX_OK;
    }

    /* If field doesn't have expiry metadata attached */
    if (!entryHasExpiry(oldEntry)) {
        size_t *alloc_size = htGetMetadataSize(ht);

        /* For fields without expiry, LT condition is considered valid */
        if (exInfo->expireSetCond & (HFE_XX | HFE_GT))
            return HSETEX_NO_CONDITION_MET;

        ssize_t usableDiff;
        entryNew = entryUpdate(oldEntry, NULL, ENTRY_HAS_EXPIRY, &usableDiff);
        *alloc_size += usableDiff;
    } else { /* field has ExpireMeta struct attached */
        uint64_t prevExpire = entryGetExpiry(oldEntry);

        /* If field has valid expiration time, then check GT|LT|NX */
        if (prevExpire != EB_EXPIRE_TIME_INVALID) {
            if (((exInfo->expireSetCond == HFE_GT) && (prevExpire >= expireAt)) ||
                ((exInfo->expireSetCond == HFE_LT) && (prevExpire <= expireAt)) ||
                (exInfo->expireSetCond == HFE_NX) )
                return HSETEX_NO_CONDITION_MET;

            /* If expiry time is the same, then nothing to do */
            if (prevExpire == expireAt)
                return HSETEX_OK;

            /* remove old expiry time from hash's private ebuckets */
            htMetadataEx *dm = htGetMetadataEx(ht);
            ebRemove(&dm->hfe, &hashFieldExpireBucketsType, oldEntry);

            /* Track of minimum expiration time (only later update global HFE DS) */
            if (exInfo->minExpireFields > prevExpire)
                exInfo->minExpireFields = prevExpire;

        } else {
            /* field has invalid expiry. No need to ebRemove() */

            /* Check XX|LT|GT */
            if (exInfo->expireSetCond & (HFE_XX | HFE_GT))
                return HSETEX_NO_CONDITION_MET;
        }

        /* Reuse hfOld as hfNew and rewrite its expiry with ebAdd() */
        entryNew = oldEntry;
    }

    dictSetKeyAtLink(ht, entryNew, &link, 0);  /* newItem=0 for updating existing entry */


    /* If expired, then delete the field and propagate the deletion.
     * If replica, continue like the field is valid */
    if (unlikely(checkAlreadyExpired(expireAt))) {
        /* replicas should not initiate deletion of fields */
        propagateHashFieldDeletion(exInfo->db, exInfo->key->ptr, field, sdslen(field));
        hashTypeDelete(exInfo->hashObj, field);
        server.stat_expired_subkeys++;
        return HSETEX_DELETED;
    }

    if (exInfo->minExpireFields > expireAt)
        exInfo->minExpireFields = expireAt;

    htMetadataEx *dm = htGetMetadataEx(ht);
    ebAdd(&dm->hfe, &hashFieldExpireBucketsType, entryNew, expireAt);
    return HSETEX_OK;
}

/*
 * Set field expiration
 *
 * Take care to call first hashTypeSetExInit() and then call this function.
 * Finally, call hashTypeSetExDone() to notify and update global HFE DS.
 *
 * Special value of EB_EXPIRE_TIME_INVALID for 'expireAt' argument will persist
 * the field.
 */
SetExRes hashTypeSetEx(robj *o, sds field, uint64_t expireAt, HashTypeSetEx *exInfo) {
    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *fptr = NULL, *vptr = NULL, *tptr = NULL;
        listpackEx *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr)
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);

        if (!fptr)
            return HSETEX_NO_FIELD;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = lpNext(lpt->lp, fptr);
        serverAssert(vptr != NULL);

        tptr = lpNext(lpt->lp, vptr);
        serverAssert(tptr);

        /* update TTL */
        return hashTypeSetExpiryListpack(exInfo, field, fptr, vptr, tptr, expireAt);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* If needed to set the field along with expiry */
        return hashTypeSetExpiryHT(exInfo, field, expireAt);
    } else {
        serverPanic("Unknown hash encoding");
    }

    return HSETEX_OK; /* never reach here */
}

void initDictExpireMetadata(robj *o) {
    dict *ht = o->ptr;

    htMetadataEx *m = htGetMetadataEx(ht);
    m->hfe = ebCreate();     /* Allocate HFE DS */
    m->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */
}

/* Init HashTypeSetEx struct before calling hashTypeSetEx() */
int hashTypeSetExInit(robj *key, kvobj *o, client *c, redisDb *db,
                      ExpireSetCond expireSetCond, HashTypeSetEx *ex)
{
    dict *ht = o->ptr;
    ex->expireSetCond = expireSetCond;
    ex->minExpire = EB_EXPIRE_TIME_INVALID;
    ex->c = c;
    ex->db = db;
    ex->key = key;
    ex->hashObj = o;
    ex->minExpireFields = EB_EXPIRE_TIME_INVALID;

    /* Take care that HASH support expiration */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvert(c->db, o, OBJ_ENCODING_LISTPACK_EX);
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP ||
               o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        /* Prepare template hash for HFE: LISTPACK_EX if it fits, else HT-with-HFE.
         * Routed through the generic dispatcher (like the plain LISTPACK case
         * above) so every HFE conversion shares a single entry point. */
        hashTypeConvert(c->db, o, OBJ_ENCODING_LISTPACK_EX);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* Take care dict has HFE metadata */
        if (!isDictWithMetaHFE(ht)) {
            /* Realloc (only header of dict) with metadata for hash-field expiration */
            dictTypeAddMeta(&ht, &entryHashDictTypeWithHFE);
            htMetadataEx *m = htGetMetadataEx(ht);
            o->ptr = ht;

            /* Find the key in the keyspace. Need to keep reference to the key for
             * notifications or even removal of the hash */

            /* Fillup dict HFE metadata */
            m->hfe = ebCreate();     /* Allocate HFE DS */
            m->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */
        }
    }

    /* Read minExpire from attached ExpireMeta to the hash */
    ex->minExpire = hashTypeGetMinExpire(o, 0);
    return C_OK;
}

/*
 * After calling hashTypeSetEx() for setting fields or their expiry, call this
 * function to update global HFE DS.
 */
void hashTypeSetExDone(HashTypeSetEx *ex) {

    if (hashTypeLength(ex->hashObj, 0) == 0)
        return;

    /* If minimum HFE of the hash is smaller than expiration time of the
     * specified fields in the command as well as it is smaller or equal
     * than expiration time provided in the command, then the minimum
     * HFE of the hash won't change following this command. */
    if ((ex->minExpire < ex->minExpireFields))
        return;

    /* Retrieve new expired time. It might have changed. */
    uint64_t newMinExpire = hashTypeGetMinExpire(ex->hashObj, 1 /*accurate*/);

    /* Calculate the diff between old minExpire and newMinExpire. If it is
     * only few seconds, then don't have to update global HFE DS. At the worst
     * case fields of hash will be active-expired up to few seconds later.
     *
     * In any case, active-expire operation will know to update global
     * HFE DS more efficiently than here for a single item.
     */
    uint64_t diff = (ex->minExpire > newMinExpire) ?
                    (ex->minExpire - newMinExpire) : (newMinExpire - ex->minExpire);
    if (diff < HASH_NEW_EXPIRE_DIFF_THRESHOLD) return;

    int slot = getKeySlot(ex->key->ptr);
    if (ex->minExpire != EB_EXPIRE_TIME_INVALID) {
        if (newMinExpire != EB_EXPIRE_TIME_INVALID)
            estoreUpdate(ex->db->subexpires, slot, ex->hashObj, newMinExpire);
        else
            estoreRemove(ex->db->subexpires, slot, ex->hashObj);
    } else {
        if (newMinExpire != EB_EXPIRE_TIME_INVALID)
            estoreAdd(ex->db->subexpires, slot, ex->hashObj, newMinExpire);
    }
}

/* Delete an element from a hash.
 *
 * Return 1 on deleted and 0 on not found.
 * field - sds field name to delete */
int hashTypeDelete(robj *o, void *field) {
    int deleted = 0;
    int fieldLen = sdslen((sds)field);

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, fieldLen, 1);
            if (fptr != NULL) {
                /* Delete both of the key and the value. */
                zl = lpDeleteRangeWithEntry(zl,&fptr,2);
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *fptr;
        listpackEx *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, fieldLen, 2);
            if (fptr != NULL) {
                /* Delete field, value and ttl */
                lpt->lp = lpDeleteRangeWithEntry(lpt->lp, &fptr, 3);
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* dictDelete() will call dictEntryDestructor() */
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;
        }
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP ||
               o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplate *tmpl = hashTypeGetTemplate(o);
        int idx = hashTemplateFieldIndex(tmpl, field);
        if (idx >= 0) {
            int old_count = tmpl->field_count;
            int new_count = old_count - 1;

            if (new_count == 0) {
                /* Last field deleted - convert to empty listpack. */
                if (o->encoding == OBJ_ENCODING_TMPL_LP)
                    hashTemplateLpFree(o->ptr);
                else
                    hashTemplateArrayFree(o->ptr);
                o->ptr = lpNew(0);
                o->encoding = OBJ_ENCODING_LISTPACK;
            } else {
                /* Incremental hash: subtract removed field's hash. */
                uint64_t new_hash = tmpl->hash - computeFieldHash(tmpl->fields[idx]);

                /* Build fields array without deleted field (already sorted). */
                sds stack_fields[HASH_TMPL_STACK_ENTRIES];
                sds *new_fields = (new_count <= HASH_TMPL_STACK_ENTRIES) ?
                    stack_fields : zmalloc(sizeof(sds) * new_count);
                int j = 0;
                for (int i = 0; i < old_count; i++) {
                    if (i != idx) new_fields[j++] = tmpl->fields[i];
                }

                /* Lookup/create template by incremental hash. */
                hashTemplate *new_tmpl = hashTemplateGetOrCreateWithHash(
                    new_hash, new_fields, new_count);
                hashTemplateIncrKeyRef(new_tmpl);

                if (o->encoding == OBJ_ENCODING_TMPL_LP) {
                    /* Delete value at idx (index 0 is template ID). */
                    unsigned char *lp = o->ptr;
                    unsigned char *p = lpSeek(lp, idx + 1);
                    lp = lpDeleteRangeWithEntry(lp, &p, 1);
                    lp = hashTemplateLpSetTemplate(lp, new_tmpl);
                    o->ptr = lp;
                } else {
                    hashTemplateArray *hta = o->ptr;
                    sdsfree(hta->values[idx]);
                    memmove(&hta->values[idx], &hta->values[idx + 1],
                            sizeof(sds) * (old_count - idx - 1));
                    hta->tmpl = new_tmpl;
                    hta = zrealloc(hta, sizeof(*hta) + sizeof(sds) * new_count);
                    o->ptr = hta;
                }
                hashTemplateDecrKeyRef(tmpl);

                if (new_fields != stack_fields) zfree(new_fields);
            }
            deleted = 1;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Auto-convert from template to regular if below threshold. */
    if (deleted && server.hash_min_template_entries > 0 &&
        (o->encoding == OBJ_ENCODING_TMPL_LP ||
         o->encoding == OBJ_ENCODING_TMPL_ARRAY))
    {
        size_t fc = hashTypeLength(o, 0);
        if (fc < server.hash_min_template_entries) {
            /* No db in scope here, but the template->listpack path never touches
             * subexpires, so NULL is safe (same as the RDB-load call sites). */
            hashTypeConvert(NULL, o, OBJ_ENCODING_LISTPACK);
        }
    }

    return deleted;
}

/* Return the number of elements in a hash.
 *
 * Note, subtractExpiredFields=1 might be pricy in case there are many HFEs
 */
unsigned long hashTypeLength(const robj *o, int subtractExpiredFields) {
    unsigned long length = ULONG_MAX;
    /* If expired field access is allowed, don't subtract expired fields from the count. */
    if (server.allow_access_expired)
        subtractExpiredFields = 0;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        length = lpLength(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;
        length = lpLength(lpt->lp) / 3;

        if (subtractExpiredFields && lpt->meta.trash == 0)
            length -= listpackExExpireDryRun(o);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        uint64_t expiredItems = 0;
        dict *d = (dict*)o->ptr;
        if (subtractExpiredFields && isDictWithMetaHFE(d)) {
            htMetadataEx *meta = htGetMetadataEx(d);
            /* If dict registered in global HFE DS */
            if (meta->expireMeta.trash == 0)
                expiredItems = ebExpireDryRun(meta->hfe,
                                              &hashFieldExpireBucketsType,
                                              commandTimeSnapshot());
        }
        length = dictSize(d) - expiredItems;
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP) {
        /* lpLength - 1: first entry is template ID, rest are values */
        length = lpLength(o->ptr) - 1;
    } else if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = o->ptr;
        length = hta->tmpl->field_count;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

size_t hashTypeAllocSize(const robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_HASH);
    size_t size = 0;
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        size = lpBytes(o->ptr);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;
        size = sizeof(listpackEx) + lpBytes(lpt->lp);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dict *d = o->ptr;
        size += sizeof(dict) + dictMemUsage(d) + *htGetMetadataSize(d);
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP) {
        unsigned char *lp = o->ptr;
        size = lpBytes(lp);
    } else if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = o->ptr;
        size = sizeof(hashTemplateArray) + sizeof(sds) * hta->tmpl->field_count;
        for (unsigned long long i = 0; i < hta->tmpl->field_count; i++) {
            if (hta->values[i]) size += sdsAllocSize(hta->values[i]);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return size;
}

void hashTypeInitIterator(hashTypeIterator *hi, robj *subject) {
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_LISTPACK ||
        hi->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        hi->fptr = NULL;
        hi->vptr = NULL;
        hi->tptr = NULL;
        hi->expire_time = EB_EXPIRE_TIME_INVALID;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        dictInitIterator(&hi->di, subject->ptr);
    } else if (hi->encoding == OBJ_ENCODING_TMPL_LP) {
        hi->tmpl_index = -1;  /* Not started yet. */
        hi->vptr = NULL;
        hi->expire_time = EB_EXPIRE_TIME_INVALID;
        hi->tmpl = hashTemplateLpGetTemplate(subject->ptr);
    } else if (hi->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hi->tmpl_index = -1;  /* Not started yet. */
        hi->vptr = NULL;
        hi->expire_time = EB_EXPIRE_TIME_INVALID;
        hi->tmpl = ((hashTemplateArray *)subject->ptr)->tmpl;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hashTypeResetIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictResetIterator(&hi->di);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */
int hashTypeNext(hashTypeIterator *hi, int skipExpiredFields) {
    /* If expired field access is allowed, don't skip expired fields during iteration */
    if (server.allow_access_expired)
        skipExpiredFields = 0;

    hi->expire_time = EB_EXPIRE_TIME_INVALID;
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            serverAssert(vptr == NULL);
            fptr = lpFirst(zl);
        } else {
            /* Advance cursor */
            serverAssert(vptr != NULL);
            fptr = lpNext(zl, vptr);
        }
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = lpNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_LISTPACK_EX) {
        long long expire_time;
        unsigned char *zl = hashTypeListpackGetLp(hi->subject);
        unsigned char *fptr, *vptr, *tptr;

        fptr = hi->fptr;
        vptr = hi->vptr;
        tptr = hi->tptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            serverAssert(vptr == NULL);
            fptr = lpFirst(zl);
        } else {
            /* Advance cursor */
            serverAssert(tptr != NULL);
            fptr = lpNext(zl, tptr);
        }
        if (fptr == NULL) return C_ERR;

        while (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            vptr = lpNext(zl, fptr);
            serverAssert(vptr != NULL);

            tptr = lpNext(zl, vptr);
            serverAssert(tptr && lpGetIntegerValue(tptr, &expire_time));

            if (!skipExpiredFields || !hashTypeIsExpired(hi->subject, expire_time))
                break;

            fptr = lpNext(zl, tptr);
        }
        if (fptr == NULL) return C_ERR;

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
        hi->tptr = tptr;
        hi->expire_time = (expire_time != HASH_LP_NO_TTL) ? (uint64_t) expire_time : EB_EXPIRE_TIME_INVALID;
    } else if (hi->encoding == OBJ_ENCODING_HT) {

        while ((hi->de = dictNext(&hi->di)) != NULL) {
            Entry *e = dictGetKey(hi->de);
            hi->expire_time = entryGetExpiry(e);
            /* this condition still valid if expire_time equals EB_EXPIRE_TIME_INVALID */
            if (skipExpiredFields && ((mstime_t)hi->expire_time < commandTimeSnapshot()))
                continue;
            return C_OK;
        }
        return C_ERR;
    } else if (hi->encoding == OBJ_ENCODING_TMPL_LP) {
        unsigned char *lp = hi->subject->ptr;

        /* Advance to next field. lpNext returning NULL signals end. */
        hi->tmpl_index++;
        hi->vptr = (hi->tmpl_index == 0) ?
                   hashTemplateLpFirstValue(lp) : lpNext(lp, hi->vptr);
        if (!hi->vptr) return C_ERR;
    } else if (hi->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        /* Advance to next field. */
        hi->tmpl_index++;
        if ((unsigned long long)hi->tmpl_index >= hi->tmpl->field_count)
            return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a listpack. Prototype is similar to `hashTypeGetFromListpack`. */
void hashTypeCurrentFromListpack(hashTypeIterator *hi, int what,
                                 unsigned char **vstr,
                                 unsigned int *vlen,
                                 long long *vll,
                                 uint64_t *expireTime)
{
    serverAssert(hi->encoding == OBJ_ENCODING_LISTPACK ||
                 hi->encoding == OBJ_ENCODING_LISTPACK_EX);

    if (what & OBJ_HASH_KEY) {
        *vstr = lpGetValue(hi->fptr, vlen, vll);
    } else {
        *vstr = lpGetValue(hi->vptr, vlen, vll);
    }

    if (expireTime)
        *expireTime = hi->expire_time;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`.
 *
 * expireTime - If parameter is not null, then the function will return the expire
 *              time of the field. If expiry not set, return EB_EXPIRE_TIME_INVALID
 */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, char **str, size_t *len, uint64_t *expireTime) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);
    Entry *e = dictGetKey(hi->de);

    if (what & OBJ_HASH_KEY) {
        sds field = entryGetField(e);
        *str = field;
        *len = sdslen(field);
    } else {
        sds val = entryGetValue(e);
        *str = val;
        *len = sdslen(val);
    }

    if (expireTime)
        *expireTime = hi->expire_time;
}

/* Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. */
void hashTypeCurrentObject(hashTypeIterator *hi,
                           int what,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll,
                           uint64_t *expireTime)
{
    if (hi->encoding == OBJ_ENCODING_LISTPACK ||
        hi->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        *vstr = NULL;
        hashTypeCurrentFromListpack(hi, what, vstr, vlen, vll, expireTime);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        char *ele;
        size_t eleLen;
        hashTypeCurrentFromHashTable(hi, what, &ele, &eleLen, expireTime);
        *vstr = (unsigned char*) ele;
        *vlen = eleLen;
    } else if (hi->encoding == OBJ_ENCODING_TMPL_LP) {
        if (what & OBJ_HASH_KEY) {
            /* Return field name from tmpl. */
            sds field = hi->tmpl->fields[hi->tmpl_index];
            *vstr = (unsigned char*) field;
            *vlen = sdslen(field);
        } else {
            /* Return value from listpack. */
            *vstr = lpGetValue(hi->vptr, vlen, vll);
        }
        if (expireTime) *expireTime = EB_EXPIRE_TIME_INVALID;
    } else if (hi->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = hi->subject->ptr;
        if (what & OBJ_HASH_KEY) {
            /* Return field name from tmpl. */
            sds field = hi->tmpl->fields[hi->tmpl_index];
            *vstr = (unsigned char*) field;
            *vlen = sdslen(field);
        } else {
            /* Return value from array. */
            sds val = hta->values[hi->tmpl_index];
            *vstr = (unsigned char*) val;
            *vlen = sdslen(val);
        }
        if (expireTime) *expireTime = EB_EXPIRE_TIME_INVALID;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Return the key or value at the current iterator position as a new
 * SDS string. */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll, NULL);
    if (vstr) return sdsnewlen(vstr,vlen);
    return sdsfromlonglong(vll);
}

/* Return the key at the current iterator position as a new entry. */
Entry *hashTypeCurrentObjectNewEntry(hashTypeIterator *hi, size_t *usable) {
    char fieldBuf[LONG_STR_SIZE], valueBuf[LONG_STR_SIZE];
    unsigned char *fieldStr, *valueStr;
    unsigned int fieldLen, valueLen;
    long long fieldLl, valueLl;
    Entry *entry;

    /* Get field */
    hashTypeCurrentObject(hi, OBJ_HASH_KEY, &fieldStr, &fieldLen, &fieldLl, NULL);
    if (!fieldStr) {
        fieldLen = ll2string(fieldBuf, sizeof(fieldBuf), fieldLl);
        fieldStr = (unsigned char *) fieldBuf;
    }
    sds field = sdsnewlen(fieldStr, fieldLen);

    /* Get value */
    hashTypeCurrentObject(hi, OBJ_HASH_VALUE, &valueStr, &valueLen, &valueLl, NULL);
    if (!valueStr) {
        valueLen = ll2string(valueBuf, sizeof(valueBuf), valueLl);
        valueStr = (unsigned char *) valueBuf;
    }
    sds value = sdsnewlen(valueStr, valueLen);
    int hasExpiry = (hi->expire_time != EB_EXPIRE_TIME_INVALID);

    /* Create entry with field and value, using iterator's expire_time */
    uint32_t entryFlags = ENTRY_TAKE_VALUE | ((hasExpiry) ? ENTRY_HAS_EXPIRY : 0); 
    entry = entryCreate(field, value, entryFlags, usable);
    sdsfree(field);  /* entryCreate() doesn't take ownership of field */

    return entry;
}

static kvobj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    dictEntryLink link;
    kvobj *kv = lookupKeyWriteWithLink(c->db, key, &link);
    if (checkType(c, kv, OBJ_HASH)) return NULL;

    if (kv == NULL) {
        robj *o = createHashObject();
        kv = dbAddByLink(c->db, key, &o, &link);
    }
    return kv;
}

/* Can a TMPL_LP / TMPL_ARRAY hash be re-encoded as a plain listpack?
 * Generic over both template encodings: checks field-name limits, plus the
 * separately-stored values for TMPL_ARRAY (TMPL_LP holds them inline). */
static int hashTypeCanConvertTmplToListpack(robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_TMPL_LP ||
                 o->encoding == OBJ_ENCODING_TMPL_ARRAY);
    hashTemplate *tmpl = hashTypeGetTemplate(o);

    if (tmpl->field_count > server.hash_max_listpack_entries)
        return 0;

    size_t total_size = 0;
    for (unsigned long long i = 0; i < tmpl->field_count; i++) {
        size_t flen = sdslen(tmpl->fields[i]);
        if (flen > server.hash_max_listpack_value)
            return 0;
        total_size += flen;
    }

    if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = o->ptr;
        for (unsigned long long i = 0; i < tmpl->field_count; i++) {
            size_t vlen = sdslen(hta->values[i]);
            if (vlen > server.hash_max_listpack_value)
                return 0;
            total_size += vlen;
        }
        return lpSafeToAdd(NULL, total_size) ? 1 : 0;
    }

    /* TMPL_LP: values already live in the listpack blob. */
    return lpSafeToAdd(NULL, lpBytes((unsigned char *)o->ptr) + total_size) ? 1 : 0;
}

/* Convert a TMPL_LP / TMPL_ARRAY hash to LISTPACK, LISTPACK_EX or HT. Fields
 * and values are read through the generic hash iterator, which already
 * abstracts over both template encodings, so one body covers every
 * source/target pair. 'with_hfe' selects an HFE-capable hashtable and only
 * applies when target_enc is HT (LISTPACK_EX always carries TTL slots). */
static void hashTypeConvertTmplGeneric(robj *o, int target_enc, int with_hfe) {
    serverAssert(o->encoding == OBJ_ENCODING_TMPL_LP ||
                 o->encoding == OBJ_ENCODING_TMPL_ARRAY);
    int src_enc = o->encoding;
    void *old_ptr = o->ptr;

    hashTypeIterator hi;
    hashTypeInitIterator(&hi, o);

    void *new_ptr;
    if (target_enc == OBJ_ENCODING_HT) {
        dict *d = dictCreate(with_hfe ? &entryHashDictTypeWithHFE
                                      : &entryHashDictType);
        dictExpand(d, hashTypeLength(o, 0));

        size_t usable, *alloc_size;
        if (with_hfe) {
            htMetadataEx *meta = htGetMetadataEx(d);
            meta->hfe = ebCreate();
            meta->expireMeta.trash = 1;
            alloc_size = &meta->alloc_size;
        } else {
            alloc_size = htGetMetadataSize(d);
        }

        while (hashTypeNext(&hi, 0) != C_ERR) {
            Entry *entry = hashTypeCurrentObjectNewEntry(&hi, &usable);
            int ret = dictAdd(d, entry, NULL);
            serverAssert(ret == DICT_OK);
            *alloc_size += usable;
        }
        new_ptr = d;
    } else {
        /* LISTPACK or LISTPACK_EX: rebuild field/value pairs (EX appends a
         * no-TTL marker after each value). */
        int ex = (target_enc == OBJ_ENCODING_LISTPACK_EX);
        unsigned char *new_lp = lpNew(0);
        while (hashTypeNext(&hi, 0) != C_ERR) {
            unsigned char *vstr;
            unsigned int vlen;
            long long vll;

            hashTypeCurrentObject(&hi, OBJ_HASH_KEY, &vstr, &vlen, &vll, NULL);
            new_lp = lpAppend(new_lp, vstr, vlen);

            hashTypeCurrentObject(&hi, OBJ_HASH_VALUE, &vstr, &vlen, &vll, NULL);
            if (vstr)
                new_lp = lpAppend(new_lp, vstr, vlen);
            else
                new_lp = lpAppendInteger(new_lp, vll);

            if (ex)
                new_lp = lpAppendInteger(new_lp, HASH_LP_NO_TTL);
        }
        if (ex) {
            listpackEx *lpt = listpackExCreate();
            lpt->lp = new_lp;
            new_ptr = lpt;
        } else {
            new_ptr = new_lp;
        }
    }

    hashTypeResetIterator(&hi);

    /* Release the source template structure (drops its template ref). */
    if (src_enc == OBJ_ENCODING_TMPL_LP)
        hashTemplateLpFree(old_ptr);
    else
        hashTemplateArrayFree(old_ptr);

    o->ptr = new_ptr;
    o->encoding = (target_enc == OBJ_ENCODING_LISTPACK_EX) ? OBJ_ENCODING_LISTPACK_EX :
                  (target_enc == OBJ_ENCODING_LISTPACK)    ? OBJ_ENCODING_LISTPACK    :
                                                             OBJ_ENCODING_HT;
}

/* Re-encode a template hash to a listpack-family target ('lp_enc' is LISTPACK
 * or LISTPACK_EX) when it fits listpack limits, otherwise fall back to HT.
 * 'with_hfe' applies HFE metadata to the HT fallback (LISTPACK_EX carries TTL
 * slots inline, so the listpack path needs no extra flag). */
static void hashTypeConvertTmplToListpackOrHT(robj *o, int lp_enc, int with_hfe) {
    if (hashTypeCanConvertTmplToListpack(o))
        hashTypeConvertTmplGeneric(o, lp_enc, 0);
    else
        hashTypeConvertTmplGeneric(o, OBJ_ENCODING_HT, with_hfe);
}

/* TMPL_LP -> TMPL_ARRAY */
static void hashTypeConvertTmplLpToArray(robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_TMPL_LP);

    unsigned char *lp = o->ptr;
    hashTemplate *tmpl = hashTemplateLpGetTemplate(lp);
    unsigned long long field_count = tmpl->field_count;

    sds *values = zmalloc(sizeof(sds) * field_count);
    unsigned char *p = hashTemplateLpFirstValue(lp);
    for (unsigned long long i = 0; i < field_count; i++) {
        unsigned int vlen;
        long long vll;
        unsigned char *vstr = lpGetValue(p, &vlen, &vll);
        if (vstr)
            values[i] = sdsnewlen(vstr, vlen);
        else
            values[i] = sdsfromlonglong(vll);
        p = lpNext(lp, p);
    }

    hashTemplateArray *hta =
        hashTemplateArrayCreate(tmpl, values, 1);
    zfree(values);
    hashTemplateLpFree(lp);

    o->encoding = OBJ_ENCODING_TMPL_ARRAY;
    o->ptr = hta;
}

void hashTypeConvertListpack(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK);

    if (enc == OBJ_ENCODING_LISTPACK) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *p;

        /* Append HASH_LP_NO_TTL to each field name - value pair. */
        p = lpFirst(o->ptr);
        while (p != NULL) {
            p = lpNext(o->ptr, p);
            serverAssert(p);

            o->ptr = lpInsertInteger(o->ptr, HASH_LP_NO_TTL, p, LP_AFTER, &p);
            p = lpNext(o->ptr, p);
        }

        listpackEx *lpt = listpackExCreate();
        lpt->lp = o->ptr;
        o->encoding = OBJ_ENCODING_LISTPACK_EX;
        o->ptr = lpt;
    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator hi;
        dict *dict;
        int ret;

        hashTypeInitIterator(&hi, o);
        dict = dictCreate(&entryHashDictType);

        /* Presize the dict to avoid rehashing */
        dictExpand(dict,hashTypeLength(o, 0));

        size_t usable, *alloc_size = htGetMetadataSize(dict);
        while (hashTypeNext(&hi, 0) != C_ERR) {
            Entry *entry = hashTypeCurrentObjectNewEntry(&hi, &usable);
            ret = dictAdd(dict, entry, NULL);
            if (ret != DICT_OK) {
                entryFree(entry, NULL); /* Needed for gcc ASAN */
                hashTypeResetIterator(&hi);  /* Needed for gcc ASAN */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                    o->ptr,lpBytes(o->ptr));
                serverPanic("Listpack corruption detected");
            }
            *alloc_size += usable;
        }
        hashTypeResetIterator(&hi);
        zfree(o->ptr);
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* db can be NULL to avoid registration in subexpires */
void hashTypeConvertListpackEx(redisDb *db, robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);

    if (enc == OBJ_ENCODING_LISTPACK_EX) {
        return;
    } else if (enc == OBJ_ENCODING_HT) {
        uint64_t minExpire = EB_EXPIRE_TIME_INVALID;
        int ret, slot = -1;
        hashTypeIterator hi;
        dict *dict;
        htMetadataEx *dictExpireMeta;
        listpackEx *lpt = o->ptr;

        if (db && lpt->meta.trash != 1) {
            minExpire = hashTypeGetMinExpire(o, 0);
            slot = getKeySlot(kvobjGetKey(o));
            estoreRemove(db->subexpires, slot, o);
        }

        dict = dictCreate(&entryHashDictTypeWithHFE);
        dictExpand(dict,hashTypeLength(o, 0));
        dictExpireMeta = htGetMetadataEx(dict);

        /* Fillup dict HFE metadata */
        dictExpireMeta->hfe = ebCreate();     /* Allocate HFE DS */
        dictExpireMeta->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */

        hashTypeInitIterator(&hi, o);

        size_t usable, *alloc_size = &dictExpireMeta->alloc_size;
        while (hashTypeNext(&hi, 0) != C_ERR) {
            /* Create entry with both field and value */
            Entry *entry = hashTypeCurrentObjectNewEntry(&hi, &usable);
            ret = dictAdd(dict, entry, NULL);
            if (ret != DICT_OK) {
                entryFree(entry, NULL); /* Needed for gcc ASAN */
                hashTypeResetIterator(&hi);  /* Needed for gcc ASAN */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                                 lpt->lp,lpBytes(lpt->lp));
                serverPanic("Listpack corruption detected");
            }
            *alloc_size += usable;

            if (hi.expire_time != EB_EXPIRE_TIME_INVALID)
                ebAdd(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, entry, hi.expire_time);
        }
        hashTypeResetIterator(&hi);
        listpackExFree(lpt);

        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;

        if (minExpire != EB_EXPIRE_TIME_INVALID)
            estoreAdd(db->subexpires, slot, o, minExpire);
    } else {
        serverPanic("Unknown hash encoding: %d", enc);
    }
}

/* Convert TMPL_LP to target encoding. HT is not a valid direct target: a
 * template reaches HT only as the no-fit fallback inside the LISTPACK /
 * LISTPACK_EX path (see hashTypeConvertTmplToListpackOrHT). */
void hashTypeConvertTmplLp(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_TMPL_LP);

    if (enc == OBJ_ENCODING_TMPL_LP) {
        /* Nothing to do. */
    } else if (enc == OBJ_ENCODING_LISTPACK || enc == OBJ_ENCODING_LISTPACK_EX) {
        /* LISTPACK_EX implies HFE, so the HT fallback must keep HFE metadata. */
        hashTypeConvertTmplToListpackOrHT(o, enc, enc == OBJ_ENCODING_LISTPACK_EX);
    } else if (enc == OBJ_ENCODING_TMPL_ARRAY) {
        hashTypeConvertTmplLpToArray(o);
    } else {
        serverPanic("Invalid conversion from TMPL_LP to %d", enc);
    }
}

/* Convert TMPL_ARRAY to target encoding. HT is not a valid direct target: a
 * template reaches HT only as the no-fit fallback inside the LISTPACK /
 * LISTPACK_EX path (see hashTypeConvertTmplToListpackOrHT). */
void hashTypeConvertTmplArray(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_TMPL_ARRAY);

    if (enc == OBJ_ENCODING_TMPL_ARRAY) {
        /* Nothing to do. */
    } else if (enc == OBJ_ENCODING_LISTPACK || enc == OBJ_ENCODING_LISTPACK_EX) {
        /* LISTPACK_EX implies HFE, so the HT fallback must keep HFE metadata. */
        hashTypeConvertTmplToListpackOrHT(o, enc, enc == OBJ_ENCODING_LISTPACK_EX);
    } else {
        serverPanic("Invalid conversion from TMPL_ARRAY to %d", enc);
    }
}

void hashTypeConvert(redisDb *db, robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvertListpack(o, enc);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        hashTypeConvertListpackEx(db, o, enc);
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP) {
        hashTypeConvertTmplLp(o, enc);
    } else if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTypeConvertTmplArray(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* qsort comparator for {field, value} pairs sorted by field name. */
typedef struct { sds field; sds value; } hashTypeFvPair;
static int hashTypeTryConvertCmpPair(const void *a, const void *b) {
    return sdscmplen(((const hashTypeFvPair *)a)->field,
                     ((const hashTypeFvPair *)b)->field);
}

/* Try to convert a hash to template-based encoding.
 * LP → TMPL_LP
 * HT (no HFE) → TMPL_ARRAY
 * Returns 1 if converted, 0 otherwise.
 * Does not convert if:
 * - encoding is LP_EX (has HFE)
 * - encoding is HT with HFE
 * - field count < min_fields
 * - template threshold not met */
int hashTypeTryConvertToTemplate(robj *o) {
    size_t min_fields = server.hash_min_template_entries;

    /* min_fields == 0 means the feature is disabled (the default). */
    if (min_fields == 0) return 0;

    /* Only LP and HT (without HFE) can be converted. */
    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) return 0;
    if (o->encoding == OBJ_ENCODING_TMPL_LP) return 0;
    if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) return 0;
    if (o->encoding == OBJ_ENCODING_HT && isDictWithMetaHFE(o->ptr)) return 0;

    /* Check field count threshold. */
    size_t num_fields = hashTypeLength(o, 0);
    if (num_fields < min_fields) return 0;

    /* Extract field/value pairs so we can sort them by field name
     * before handing them to hashTemplateGetOrCreate (which requires
     * pre-sorted fields). */
    hashTypeFvPair *pairs = zmalloc(sizeof(*pairs) * num_fields);

    hashTypeIterator hi;
    hashTypeInitIterator(&hi, o);
    size_t i = 0;
    while (hashTypeNext(&hi, 0) != C_ERR) {
        pairs[i].field = hashTypeCurrentObjectNewSds(&hi, OBJ_HASH_KEY);
        pairs[i].value = hashTypeCurrentObjectNewSds(&hi, OBJ_HASH_VALUE);
        i++;
    }
    hashTypeResetIterator(&hi);

    qsort(pairs, num_fields, sizeof(*pairs), hashTypeTryConvertCmpPair);

    /* Materialize separate fields/values arrays from sorted pairs. */
    sds *fields = zmalloc(sizeof(sds) * num_fields);
    sds *values = zmalloc(sizeof(sds) * num_fields);
    for (size_t j = 0; j < num_fields; j++) {
        fields[j] = pairs[j].field;
        values[j] = pairs[j].value;
    }
    zfree(pairs);

    /* Get or create template. */
    hashTemplate *tmpl = hashTemplateGetOrCreate(fields, num_fields);
    int took_values; /* TMPL_ARRAY takes ownership of values; TMPL_LP copies. */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        /* LP → TMPL_LP */
        unsigned char *new_lp = hashTemplateLpCreate(tmpl, values);
        zfree(o->ptr);
        o->ptr = new_lp;
        o->encoding = OBJ_ENCODING_TMPL_LP;
        took_values = 0;
    } else {
        /* HT → TMPL_ARRAY */
        hashTemplateArray *hta = hashTemplateArrayCreate(tmpl, values, 1);
        dictRelease(o->ptr);
        o->ptr = hta;
        o->encoding = OBJ_ENCODING_TMPL_ARRAY;
        took_values = 1;
    }

    /* Free temporary arrays. Fields are always copied by the template; values
     * are taken by TMPL_ARRAY but copied by TMPL_LP, so free them only when the
     * new encoding did not take ownership. */
    for (size_t j = 0; j < num_fields; j++)
        sdsfree(fields[j]);
    if (!took_values) {
        for (size_t j = 0; j < num_fields; j++)
            sdsfree(values[j]);
    }
    zfree(fields);
    zfree(values);

    return 1;
}

/* This is a helper function for the COPY command.
 * Duplicate a hash object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
robj *hashTypeDup(kvobj *o, uint64_t *minHashExpire) {
    robj *hobj;
    hashTypeIterator hi;

    serverAssert(o->type == OBJ_HASH);

    if(o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = o->ptr;
        size_t sz = lpBytes(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        hobj = createObject(OBJ_HASH, new_zl);
        hobj->encoding = OBJ_ENCODING_LISTPACK;
    } else if(o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;

        if (lpt->meta.trash == 0)
            *minHashExpire = ebGetMetaExpTime(&lpt->meta);

        listpackEx *dup = listpackExCreate();

        size_t sz = lpBytes(lpt->lp);
        dup->lp = lpNew(sz);
        memcpy(dup->lp, lpt->lp, sz);

        hobj = createObject(OBJ_HASH, dup);
        hobj->encoding = OBJ_ENCODING_LISTPACK_EX;
    } else if(o->encoding == OBJ_ENCODING_HT) {
        htMetadataEx *dictExpireMetaSrc, *dictExpireMetaDst = NULL;
        dict *d;

        /* If dict doesn't have HFE metadata, then create a new dict without it */
        if (!isDictWithMetaHFE(o->ptr)) {
            d = dictCreate(&entryHashDictType);
        } else {
            /* Create a new dict with HFE metadata */
            d = dictCreate(&entryHashDictTypeWithHFE);
            dictExpireMetaSrc = htGetMetadataEx((dict *) o->ptr);
            dictExpireMetaDst = htGetMetadataEx(d);
            dictExpireMetaDst->hfe = ebCreate();     /* Allocate HFE DS */
            dictExpireMetaDst->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */

            /* Extract the minimum expire time of the source hash (Will be used by caller
             * to register the new hash in the global subexpires DB) */
            if (dictExpireMetaSrc->expireMeta.trash == 0)
                *minHashExpire = ebGetMetaExpTime(&dictExpireMetaSrc->expireMeta);
        }
        dictExpand(d, dictSize((const dict*)o->ptr));

        size_t usable, *alloc_size = htGetMetadataSize(d);
        hashTypeInitIterator(&hi, o);
        while (hashTypeNext(&hi, 0) != C_ERR) {
            Entry *newEntry;
            uint64_t expireTime;
            /* Extract a field-value pair from an original hash object.*/
            char *field, *value;
            size_t fieldLen, valueLen;
            hashTypeCurrentFromHashTable(&hi, OBJ_HASH_KEY, &field, &fieldLen, &expireTime);
            hashTypeCurrentFromHashTable(&hi, OBJ_HASH_VALUE, &value, &valueLen, NULL);

            /* Create new entry with field and value */
            sds newFieldSds = sdsnewlen(field, fieldLen);
            sds newValueSds = sdsnewlen(value, valueLen);
            /* Create new entry with field and value, optional expiry. */
            if (expireTime == EB_EXPIRE_TIME_INVALID) {
                newEntry = entryCreate(newFieldSds, newValueSds, 
                                       ENTRY_TAKE_VALUE, &usable);
            } else {
                newEntry = entryCreate(newFieldSds, newValueSds, 
                                       ENTRY_TAKE_VALUE | ENTRY_HAS_EXPIRY, &usable);
                ebAdd(&dictExpireMetaDst->hfe, &hashFieldExpireBucketsType, newEntry, expireTime);
            }
            sdsfree(newFieldSds); /* (Only value ownership transferred to entry) */

            /* Add entry to new hash object. */
            dictAdd(d, newEntry, NULL);  /* no_value=1, so value is NULL */
            *alloc_size += usable;
        }
        hashTypeResetIterator(&hi);

        hobj = createObject(OBJ_HASH, d);
        hobj->encoding = OBJ_ENCODING_HT;
    } else if (o->encoding == OBJ_ENCODING_TMPL_LP) {
        unsigned char *old_lp = o->ptr;
        hashTemplate *tmpl = hashTemplateLpGetTemplate(old_lp);

        /* Create new listpack copy. */
        size_t sz = lpBytes(old_lp);
        unsigned char *new_lp = zmalloc(sz);
        memcpy(new_lp, old_lp, sz);

        /* Increment refcount for new key. */
        hashTemplateIncrKeyRef(tmpl);

        hobj = createObject(OBJ_HASH, new_lp);
        hobj->encoding = OBJ_ENCODING_TMPL_LP;
    } else if (o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = o->ptr;
        unsigned long long n = hta->tmpl->field_count;

        /* Create new array structure with duplicated values. */
        hashTemplateArray *new_hta = zmalloc(sizeof(*new_hta) + sizeof(sds) * n);
        new_hta->tmpl = hta->tmpl;
        hashTemplateIncrKeyRef(new_hta->tmpl);
        for (unsigned long long i = 0; i < n; i++) {
            new_hta->values[i] = sdsdup(hta->values[i]);
        }

        hobj = createObject(OBJ_HASH, new_hta);
        hobj->encoding = OBJ_ENCODING_TMPL_ARRAY;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hobj;
}

/* Create a new sds string from the listpack entry. */
sds hashSdsFromListpackEntry(listpackEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* Reply with bulk string from the listpack entry. */
void hashReplyFromListpackEntry(client *c, listpackEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}

/* Return random element from a non empty hash.
 * 'key' and 'val' will be set to hold the element.
 * The memory in them is not to be freed or modified by the caller.
 * 'val' can be NULL in which case it's not extracted. */
void hashTypeRandomElement(robj *hashobj, unsigned long hashsize, CommonEntry *key, CommonEntry *val) {
    if (hashobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetFairRandomKey(hashobj->ptr);
        Entry *entry = dictGetKey(de);
        sds field = entryGetField(entry);
        key->sval = (unsigned char*) field;
        key->slen = sdslen(field);
        if (val) {
            sds s = entryGetValue(entry);
            val->sval = (unsigned char*)s;
            val->slen = sdslen(s);
        }
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK) {
        lpRandomPair(hashobj->ptr, hashsize, (listpackEntry *) key, (listpackEntry *) val, 2);
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        lpRandomPair(hashTypeListpackGetLp(hashobj), hashsize, (listpackEntry *) key,
                     (listpackEntry *) val, 3);
    } else if (hashobj->encoding == OBJ_ENCODING_TMPL_LP) {
        unsigned char *lp = hashobj->ptr;
        hashTemplate *tmpl = hashTemplateLpGetTemplate(lp);
        int idx = rand() % tmpl->field_count;

        /* Get field from tmpl. */
        sds field = tmpl->fields[idx];
        key->sval = (unsigned char *)field;
        key->slen = sdslen(field);

        if (val) {
            /* Get value at index (idx+1 to skip template ID entry). */
            unsigned char *p = lpSeek(lp, idx + 1);
            unsigned int vlen;
            long long vll;
            val->sval = lpGetValue(p, &vlen, &vll);
            if (val->sval) {
                val->slen = vlen;
            } else {
                val->lval = vll;
            }
        }
    } else if (hashobj->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplateArray *hta = hashobj->ptr;
        int idx = rand() % hta->tmpl->field_count;

        /* Get field from tmpl. */
        sds field = hta->tmpl->fields[idx];
        key->sval = (unsigned char *)field;
        key->slen = sdslen(field);

        if (val) {
            /* Get value from array at index. */
            sds v = hta->values[idx];
            val->sval = (unsigned char *)v;
            val->slen = sdslen(v);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Delete all expired fields from the hash and delete the hash if left empty.
 *
 * updateSubexpires - If the hash should be updated in the subexpires DB with new
 *                   expiration time in case expired fields were deleted.
 *
 * Return next Expire time of the hash
 * - 0 if hash got deleted
 * - EB_EXPIRE_TIME_INVALID if no more fields to expire
 */
uint64_t hashTypeExpire(redisDb *db, kvobj *o, uint32_t *quota, int updateSubexpires, int activeEx) {
    uint64_t noExpireLeftRes = EB_EXPIRE_TIME_INVALID;

    /* Collect expired field names for batched subkey notification.
     * Skip allocation entirely when subkey notifications are disabled. */
    fieldvec fvexpired;
    vec *vexpired = isSubkeyNotifyEnabled(NOTIFY_HASH) ?
                        fieldvecInit(&fvexpired, FIELDS_STACK_SIZE) : NULL;

    OnFieldExpireCtx onFieldExpireCtx = { .hashObj = o, .db = db, .activeEx = activeEx, .vexpired = vexpired };
    ExpireInfo info = (ExpireInfo) {
                .maxToExpire = *quota,
                .now = commandTimeSnapshot(),
                .ctx = &onFieldExpireCtx,
                .itemsExpired = 0};

    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackExExpire(db, o, &info);
    } else {
        serverAssert(o->encoding == OBJ_ENCODING_HT);

        dict *d = o->ptr;
        htMetadataEx *dictExpireMeta = htGetMetadataEx(d);

        info.onExpireItem = onFieldExpire;
        ebExpire(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, &info);
    }

    /* Update quota left */
    *quota -= info.itemsExpired;

    /* In some cases, a field might have been deleted without updating the global DS.
     * As a result, active-expire might not expire any fields, in such cases,
     * we don't need to send notifications or perform other operations for this key. */
    if (info.itemsExpired) {
        sds keystr = kvobjGetKey(o);
        robj *key = createStringObject(keystr, sdslen(keystr));

        /* Send subkey notification with all expired fields */
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpired", key, db->id,
            vexpired ? (robj**)vecData(vexpired) : NULL, vexpired ? vecSize(vexpired) : 0);

        int slot;
        int deleted = 0;

        if (updateSubexpires) {
            slot = getKeySlot(keystr);
            estoreRemove(db->subexpires, slot, o);
        }

        if (hashTypeLength(o, 0) == 0) {
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, db->id);
            dbDelete(db, key);
            noExpireLeftRes = 0;
            deleted = 1;
        } else {
            if ((updateSubexpires) && (info.nextExpireTime != EB_EXPIRE_TIME_INVALID))
                estoreAdd(db->subexpires, slot, o, info.nextExpireTime);
        }

        keyModified(NULL, db, key, deleted ? NULL : o, 1);
        decrRefCount(key);
    }

    /* Free collected expired fields */
    if (vexpired) {
        for (size_t i = 0; i < vecSize(vexpired); i++) {
            decrRefCount(vecGet(vexpired, i));
        }
        vecRelease(vexpired);
    }

    /* return 0 if hash got deleted, EB_EXPIRE_TIME_INVALID if no more fields
     * with expiration. Else return next expiration time */
    return (info.nextExpireTime == EB_EXPIRE_TIME_INVALID) ? noExpireLeftRes : info.nextExpireTime;
}

/* Delete all expired fields in hash if needed (Currently used only by HRANDFIELD)
 *
 * NOTICE: If we call this function in other places, we should consider the slot
 * migration scenario, where we don't want to delete expired fields. See also
 * expireIfNeeded().
 *
 * Return 1 if the entire hash was deleted, 0 otherwise.
 * This function might be pricy in case there are many expired fields.
 */
static int hashTypeExpireIfNeeded(redisDb *db, kvobj *o) {
    uint64_t nextExpireTime;
    uint64_t minExpire = hashTypeGetMinExpire(o, 1 /*accurate*/);

    /* Nothing to expire */
    if ((mstime_t) minExpire >= commandTimeSnapshot())
        return 0;

    /* Follow expireIfNeeded() conditions of when not lazy-expire */
    if ( (server.loading) ||
         (server.allow_access_expired) ||
         (server.masterhost) ||  /* master-client or user-client, don't delete */
         (isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)))
        return 0;

    /* Take care to expire all the fields */
    uint32_t quota = UINT32_MAX;
    nextExpireTime = hashTypeExpire(db, o, &quota, 1, 0);
    /* return 1 if the entire hash was deleted */
    return nextExpireTime == 0;
}

/* Return the next/minimum expiry time of the hash-field.
 * accurate=1 - Return the exact time by looking into the object DS.
 * accurate=0 - Return the minimum expiration time maintained in expireMeta
 *              (Verify it is not trash before using it) which might not be
 *              accurate due to optimization reasons.
 *
 * If not found, return EB_EXPIRE_TIME_INVALID
 */
uint64_t hashTypeGetMinExpire(robj *o, int accurate) {
    ExpireMeta *expireMeta = NULL;

    /* TMPL_* encodings don't support field expiration. */
    if (o->encoding == OBJ_ENCODING_TMPL_LP ||
        o->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        return EB_EXPIRE_TIME_INVALID;
    }

    if (!accurate) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            return EB_EXPIRE_TIME_INVALID;
        } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = o->ptr;
            expireMeta = &lpt->meta;
        } else {
            serverAssert(o->encoding == OBJ_ENCODING_HT);

            dict *d = o->ptr;
            if (!isDictWithMetaHFE(d))
                return EB_EXPIRE_TIME_INVALID;

            expireMeta = &htGetMetadataEx(d)->expireMeta;
        }

        /* Keep aside next hash-field expiry before updating HFE DS. Verify it is not trash */
        if (expireMeta->trash == 1)
            return EB_EXPIRE_TIME_INVALID;

        return ebGetMetaExpTime(expireMeta);
    }

    /* accurate == 1 */

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return EB_EXPIRE_TIME_INVALID;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        return listpackExGetMinExpire(o);
    } else {
        serverAssert(o->encoding == OBJ_ENCODING_HT);

        dict *d = o->ptr;
        if (!isDictWithMetaHFE(d))
            return EB_EXPIRE_TIME_INVALID;

        htMetadataEx *expireMeta = htGetMetadataEx(d);
        return ebGetNextTimeToExpire(expireMeta->hfe, &hashFieldExpireBucketsType);
    }
}

int hashTypeIsFieldsWithExpire(robj *o) {
    /* TMPL_* and plain LISTPACK encodings never carry field expiration (HFE
     * forces a conversion away from these before any TTL is set). */
    if (o->encoding == OBJ_ENCODING_TMPL_LP ||
        o->encoding == OBJ_ENCODING_TMPL_ARRAY ||
        o->encoding == OBJ_ENCODING_LISTPACK) {
        return 0;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        return EB_EXPIRE_TIME_INVALID != listpackExGetMinExpire(o);
    } else { /* o->encoding == OBJ_ENCODING_HT */
        dict *d = o->ptr;
        /* If dict doesn't holds HFE metadata */
        if (!isDictWithMetaHFE(d))
            return 0;
        htMetadataEx *meta = htGetMetadataEx(d);
        return ebGetTotalItems(meta->hfe, &hashFieldExpireBucketsType) != 0;
    }
}

void hashTypeFree(robj *o) {
    switch (o->encoding) {
        case OBJ_ENCODING_HT:
            /* Verify hash is not registered in global HFE ds */
            if (isDictWithMetaHFE((dict*)o->ptr)) {
                htMetadataEx *m = htGetMetadataEx((dict*)o->ptr);
                serverAssert(m->expireMeta.trash == 1);
            }
#ifdef DEBUG_ASSERTIONS
            dictEmpty(o->ptr, NULL);
            debugServerAssert(*htGetMetadataSize(o->ptr) == 0);
#endif
            dictRelease((dict*) o->ptr);
            break;
        case OBJ_ENCODING_LISTPACK:
            lpFree(o->ptr);
            break;
        case OBJ_ENCODING_LISTPACK_EX:
            /* Verify hash is not registered in global HFE ds */
            serverAssert(((listpackEx *) o->ptr)->meta.trash == 1);
            listpackExFree(o->ptr);
            break;
        case OBJ_ENCODING_TMPL_LP:
            hashTemplateLpFree(o->ptr);
            break;
        case OBJ_ENCODING_TMPL_ARRAY:
            hashTemplateArrayFree(o->ptr);
            break;
        default:
            serverPanic("Unknown hash encoding type");
            break;
    }
}

ebuckets *hashTypeGetDictMetaHFE(dict *d) {
    htMetadataEx *dictExpireMeta = htGetMetadataEx(d);
    return &dictExpireMeta->hfe;
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

void hsetnxCommand(client *c) {
    unsigned long hlen;
    int isHashDeleted;
    size_t oldsize = 0;
    robj *kv = hashTypeLookupWriteOrCreate(c,c->argv[1]);
    if (kv == NULL) return;

    if (hashTypeExists(c->db, kv, c->argv[2]->ptr, HFE_LAZY_EXPIRE, &isHashDeleted)) {
        addReply(c, shared.czero);
        return;
    }

    /* Field expired and in turn hash deleted. Create new one! */
    if (isHashDeleted) {
        robj *o = createHashObject();
        kv = dbAdd(c->db,c->argv[1],&o);
    }

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(kv);
    hashTypeTryConversion(c->db, kv, c->argv, 2, 3);
    hashTypeSet(c->db, kv, c->argv[2]->ptr, c->argv[3]->ptr, HASH_SET_COPY);
    addReply(c, shared.cone);
    keyModified(c,c->db,c->argv[1], kv, 1);
    hlen = hashTypeLength(kv, 0);
    updateKeysizesHist(c->db, OBJ_HASH, hlen - 1, hlen);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), kv, oldsize, kvobjAllocSize(kv));
    notifyKeyspaceEventWithSubkeys(NOTIFY_HASH,"hset",c->argv[1],c->db->id,&c->argv[2],1);
    KSN_INVALIDATE_KVOBJ(kv);
    server.dirty++;
}

void hsetCommand(client *c) {
    int i, created = 0;
    size_t oldsize = 0;
    kvobj *kv;

    if ((c->argc % 2) == 1) {
        addReplyErrorArity(c);
        return;
    }

    if ((kv = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(kv);
    hashTypeTryConversion(c->db, kv, c->argv, 2, c->argc-1);

    for (i = 2; i < c->argc; i += 2)
        created += !hashTypeSet(c->db, kv, c->argv[i]->ptr, c->argv[i+1]->ptr, HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        addReply(c, shared.ok);
    }
    keyModified(c,c->db,c->argv[1],kv,1);
    unsigned long l = hashTypeLength(kv, 0);
    updateKeysizesHist(c->db, OBJ_HASH, l - created, l);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), kv, oldsize, kvobjAllocSize(kv));

    /* Collect field pointers for subkey notification. Fields are at argv[2,4,6...]. */
    int numfields = (c->argc - 2) / 2;
    fieldvec fvset;
    vec *vset = fieldvecInit(&fvset, numfields);
    for (i = 0; i < numfields; i++) {
        vecPush(vset, c->argv[2 + i * 2]);
    }
    notifyKeyspaceEventWithSubkeys(NOTIFY_HASH,"hset",c->argv[1],c->db->id,(robj**)vecData(vset),numfields);
    vecRelease(vset);
    KSN_INVALIDATE_KVOBJ(kv);
    server.dirty += (c->argc - 2)/2;
}

/* Per-client fieldset: a session-local binding from a user-chosen name to a
 * shared hashTemplate, populated by HIMPORT PREPARE and consumed by HIMPORT
 * SET. Exists to make HIMPORT SET cheap on the hot write path.
 *
 * HIMPORT PREPARE does the expensive work once:
 *   - sorts the field names
 *   - computes value_order (user argv index for each sorted field position)
 *   - looks up the global template registry and creates the hashTemplate if
 *     the layout is new, then takes a hold reference on it
 *   - stores name + tmpl + value_order as a himportFieldset on the client
 *
 * HIMPORT SET then just looks up the fieldset by name and writes the key
 * using the cached tmpl/value_order: no registry lookup, no field sorting,
 * no per-call allocation for the layout.
 *
 * The binding lives until HIMPORT DISCARD, HIMPORT DISCARDALL, or client
 * disconnect; the underlying hashTemplate stays in the registry while any
 * client or hash key still references it. */
typedef struct himportFieldset {
    sds name;           /* Fieldset name. */
    hashTemplate *tmpl; /* Template that matches the fieldset. */
    int *value_order;   /* Maps tmpl index -> user argv field index (pre-computed). */
} himportFieldset;

/* Container for client's HIMPORT fieldsets (sorted by name). */
typedef struct himportFieldsetList {
    int count;
    int cap;
    himportFieldset *arr;
} himportFieldsetList;

/* Free a fieldset. */
static void himportFieldsetFree(himportFieldset *fs) {
    if (!fs) return;
    if (fs->name) sdsfree(fs->name);
    if (fs->tmpl) hashTemplateDecrHoldRef(fs->tmpl);
    if (fs->value_order) zfree(fs->value_order);
}

/* Binary search for fieldset by name. Returns index if found,
 * or -(insertion_point + 1) if not found. */
static int himportFieldsetSearch(himportFieldsetList *list, sds name) {
    int lo = 0, hi = list->count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = sdscmplen(list->arr[mid].name, name);
        if (cmp == 0) return mid;
        if (cmp < 0) lo = mid + 1;
        else hi = mid - 1;
    }
    return -(lo + 1);
}

/* Get fieldset by name from client's fieldset list. */
static himportFieldset *himportFieldsetGet(client *c, sds name) {
    himportFieldsetList *list = c->himport_fieldsets;
    if (!list) return NULL;

    int idx = himportFieldsetSearch(list, name);
    return (idx >= 0) ? &list->arr[idx] : NULL;
}

/* Add or replace a fieldset in client's fieldset list. */
static void himportFieldsetAdd(client *c, sds name,
                               hashTemplate *tmpl,
                               int *value_order)
{
    himportFieldsetList *list = c->himport_fieldsets;
    himportFieldset newfs = {name, tmpl, value_order};

    if (!list) {
        list = zmalloc(sizeof(himportFieldsetList));
        list->cap = 4;
        list->arr = zmalloc(sizeof(himportFieldset) * list->cap);
        list->arr[0] = newfs;
        list->count = 1;
        c->himport_fieldsets = list;
        return;
    }

    int idx = himportFieldsetSearch(list, name);
    if (idx >= 0) {
        /* Overwrite existing. */
        himportFieldsetFree(&list->arr[idx]);
        list->arr[idx] = newfs;
        return;
    }

    /* Insert at sorted position. */
    int pos = -(idx + 1);
    if (list->count >= list->cap) {
        list->cap *= 2;
        list->arr = zrealloc(list->arr, sizeof(himportFieldset) * list->cap);
    }
    if (pos < list->count) {
        memmove(&list->arr[pos + 1], &list->arr[pos],
                sizeof(himportFieldset) * (list->count - pos));
    }
    list->arr[pos] = newfs;
    list->count++;
}

/* Remove a fieldset by name. Returns 1 if removed, 0 if not found. */
static int himportFieldsetRemove(client *c, sds name) {
    int idx = -1;
    himportFieldsetList *list = c->himport_fieldsets;

    if (!list || (idx = himportFieldsetSearch(list, name)) < 0)
        return 0;

    himportFieldsetFree(&list->arr[idx]);
    if (idx < list->count - 1) {
        memmove(&list->arr[idx], &list->arr[idx + 1],
                sizeof(himportFieldset) * (list->count - 1 - idx));
    }

    list->count--;
    if (list->count == 0) {
        zfree(list->arr);
        zfree(list);
        c->himport_fieldsets = NULL;
    }
    return 1;
}

/* Free client's fieldset list. Called from freeClient().
 * Returns the number of fieldsets that were removed. */
int himportFieldsetFreeList(client *c) {
    himportFieldsetList *list = c->himport_fieldsets;
    if (!list) return 0;

    int removed = list->count;
    for (int i = 0; i < list->count; i++)
        himportFieldsetFree(&list->arr[i]);

    zfree(list->arr);
    zfree(list);
    c->himport_fieldsets = NULL;
    return removed;
}

/* Per-client memory overhead of this client's HIMPORT fieldset bindings.
 * Accounts only client-owned allocations: the list, its reserved array, and
 * each fieldset's name and value_order map. The templates referenced via
 * hold-ref are shared state in the global registry and are not attributed
 * here (mirrors multiStateMemOverhead's treatment of watched keys). */
size_t himportFieldsetMemOverhead(client *c) {
    himportFieldsetList *list = c->himport_fieldsets;
    if (!list) return 0;

    size_t mem = sizeof(himportFieldsetList);
    mem += (size_t)list->cap * sizeof(himportFieldset);
    for (int i = 0; i < list->count; i++) {
        himportFieldset *fs = &list->arr[i];
        if (fs->name) mem += sdsZmallocSize(fs->name);
        if (fs->tmpl) mem += fs->tmpl->field_count * sizeof(int);
    }
    return mem;
}

/* Context for himportCmpFieldIdx, set before qsort call. */
static robj **himport_cmp_argv = NULL;

/* Compare function for sorting field indexes by field name. */
static int himportCmpFieldIdx(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    return sdscmplen(himport_cmp_argv[ia]->ptr,
                     himport_cmp_argv[ib]->ptr);
}

/* HIMPORT PREPARE <fieldset_name> <field1> [field2 ...]
 *
 * Register a named fieldset on this client so subsequent HIMPORT SET calls
 * only have to pass values (not field names). Sorts the fields alphabetically
 * and looks up / creates the matching template in the registry, taking a
 * hold reference on it. The original user-provided field order is preserved
 * as field_order[] so HIMPORT SET can map its positional values back into
 * template-sorted order. Rejects duplicate field names in the fieldset. */
void himportPrepareCommand(client *c) {
    sds fieldset_name = c->argv[2]->ptr;
    int field_count = c->argc - 3;
    robj **field_argv = &c->argv[3];  /* Fields start at argv[3]. */

    /* Create field_order: maps 'field index in fieldset' -> 'user argv index'.
     * Fieldset fields are sorted alphabetically, so we sort indexes by
     * field name and store the mapping. */
    int *field_order = zmalloc(sizeof(int) * field_count);
    for (int i = 0; i < field_count; i++)
        field_order[i] = i;

    himport_cmp_argv = field_argv;
    qsort(field_order, field_count, sizeof(int), himportCmpFieldIdx);

    /* Build sorted fields array for tmpl lookup. */
    sds *sorted_fields = zmalloc(sizeof(sds) * field_count);
    for (int i = 0; i < field_count; i++)
        sorted_fields[i] = field_argv[field_order[i]]->ptr;

    /* Reject duplicate field names. After sort, duplicates are adjacent. */
    for (int i = 1; i < field_count; i++) {
        if (sdscmplen(sorted_fields[i - 1], sorted_fields[i]) == 0) {
            zfree(sorted_fields);
            zfree(field_order);
            addReplyError(c, "duplicate field name in fieldset");
            return;
        }
    }

    /* Get or create tmpl with sorted fields and hold a reference. */
    hashTemplate *tmpl = hashTemplateGetOrCreate(sorted_fields, field_count);
    hashTemplateIncrHoldRef(tmpl);
    zfree(sorted_fields);

    himportFieldsetAdd(c, sdsdup(fieldset_name), tmpl, field_order);
    addReply(c, shared.ok);
}

/* HIMPORT SET <key> <fieldset> <value1> [value2 ...] */
void himportSetCommand(client *c) {
    /* Lookup fieldset. */
    sds fieldset_name = c->argv[3]->ptr;
    himportFieldset *fs = himportFieldsetGet(c, fieldset_name);
    if (!fs) {
        addReplyError(c, "no such fieldset");
        return;
    }

    hashTemplate *tmpl = fs->tmpl;
    unsigned long long field_count = tmpl->field_count;
    int valuec = c->argc - 4;

    /* Number of values must match fieldset field count. */
    if ((unsigned long long)valuec != field_count) {
        addReplyError(c, "value count does not match fieldset field count");
        return;
    }

    /* Build values array using a stack buffer for small fieldsets.
     * value_order[i] maps fieldset index i to user's argv index. */
    int *value_order = fs->value_order;
    sds stack_values[HASH_TMPL_STACK_ENTRIES];
    sds *values = (field_count <= HASH_TMPL_STACK_ENTRIES) ?
                  stack_values : zmalloc(sizeof(sds) * field_count);

    /* Ensure propargv is built; field robjs live at propargv[2 .. 2+N). */
    robj **fields_robj = hashTemplateGetPropargvFields(tmpl);
    robj **propargv = tmpl->propargv;
    propargv[1] = c->argv[2];

    for (unsigned long long i = 0; i < field_count; i++) {
        robj *valobj = c->argv[4 + value_order[i]];
        values[i] = valobj->ptr;
        propargv[2 + field_count + i] = valobj;
    }

    robj *o = createHashObjectFromTemplate(tmpl, values);

    /* Set key (overwrites existing key of any type). */
    setKey(c, c->db, c->argv[2], &o, 0);

    /* Notify keyspace listeners with the per-field subkeys, matching HSET's
     * semantics. The field segment of propargv is contiguous. */
    notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hset", c->argv[2], c->db->id,
                                   fields_robj, (int) field_count);
    server.dirty++;

    /* Propagate as HSETC. */
    alsoPropagate(c->db->id, propargv, 2 + field_count * 2, PROPAGATE_AOF | PROPAGATE_REPL);

    /* Free heap-allocated values buffer if used. */
    if (values != stack_values) zfree(values);

    /* Prevent HIMPORT SET from being propagated - we propagated HSETC instead. */
    preventCommandPropagation(c);
    addReply(c, shared.ok);
}

/* HIMPORT DISCARD <fieldset> */
void himportDiscardCommand(client *c) {
    addReplyLongLong(c, himportFieldsetRemove(c, c->argv[2]->ptr));
}

/* HIMPORT DISCARDALL */
void himportDiscardallCommand(client *c) {
    addReplyLongLong(c, himportFieldsetFreeList(c));
}

/* Per-client cache of the last HSETC template — when consecutive calls share
 * the same field schema (the common case during replication/AOF replay) we
 * skip computeFieldsHash + dictFind. The cached tmpl holds a reference,
 * so tmpl->fields stays alive and serves as the comparison snapshot. */
void hsetcCacheFree(client *c) {
    hashTemplate *tmpl = c->hsetc_cache;
    if (!tmpl) return;
    hashTemplateDecrHoldRef(tmpl);
    c->hsetc_cache = NULL;
}

static int hsetcCacheMatch(hashTemplate *tmpl, client *c, unsigned long long field_count) {
    if (!tmpl || tmpl->field_count != field_count)
        return 0;
    for (unsigned long long i = 0; i < field_count; i++)
        if (sdscmplen(tmpl->fields[i], c->argv[2 + i]->ptr) != 0)
            return 0;
    return 1;
}

/* HSETC key f0 f1 ... fN-1 v0 v1 ... vN-1
 *
 * Internal command (CMD_INTERNAL) used to replicate template-encoded hashes:
 * resolves the template for the given fields, builds the value object and
 * overwrites key in one shot. The per-client hsetc_cache fast-paths the
 * common case where consecutive calls share the same field set. */
void hsetcCommand(client *c) {
    if (c->argc < 4 || (c->argc % 2) != 0) {
        addReplyErrorArity(c);
        return;
    }

    unsigned long long field_count = (c->argc - 2) / 2;
    hashTemplate *tmpl = c->hsetc_cache;
    /* Skip tmpl lookup if the cached tmpl matches the field set. */
    if (!hsetcCacheMatch(tmpl, c, field_count)) {
        /* Unwrap robj* into sds* and accumulate the fields hash in the same
         * pass so hashTemplateGetOrCreateWithHash skips a second iteration. */
        sds stack_fields[HASH_TMPL_STACK_ENTRIES];
        sds *fields = (field_count <= HASH_TMPL_STACK_ENTRIES) ?
                      stack_fields : zmalloc(sizeof(sds) * field_count);
        uint64_t hash = 0;
        for (unsigned long long i = 0; i < field_count; i++) {
            fields[i] = c->argv[2 + i]->ptr;
            hash += computeFieldHash(fields[i]);
        }
        tmpl = hashTemplateGetOrCreateWithHash(hash, fields, field_count);
        if (fields != stack_fields) zfree(fields);

        hsetcCacheFree(c);
        hashTemplateIncrHoldRef(tmpl);
        c->hsetc_cache = tmpl;
    }

    sds stack_values[HASH_TMPL_STACK_ENTRIES];
    sds *values = (field_count <= HASH_TMPL_STACK_ENTRIES) ?
                  stack_values : zmalloc(sizeof(sds) * field_count);
    for (unsigned long long i = 0; i < field_count; i++)
        values[i] = c->argv[2 + field_count + i]->ptr;

    robj *o = createHashObjectFromTemplate(tmpl, values);
    if (values != stack_values) zfree(values);

    setKey(c, c->db, c->argv[1], &o, 0);

    /* Notify keyspace listeners with per-field subkeys, matching HSET's
     * semantics. The field segment of propargv is contiguous. */
    notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hset", c->argv[1], c->db->id,
                                   hashTemplateGetPropargvFields(tmpl), (int) field_count);

    server.dirty++;
    addReply(c, shared.ok);
}

/* Parse expire time from argument and do boundary checks. */
static int parseExpireTime(client *c, robj *o, int unit, long long basetime,
                           long long *expire)
{
    long long val;

    /* Read the expiry time from command */
    if (getLongLongFromObjectOrReply(c, o, &val, NULL) != C_OK)
        return C_ERR;

    if (val < 0) {
        addReplyError(c,"invalid expire time, must be >= 0");
        return C_ERR;
    }

    if (unit == UNIT_SECONDS) {
        if (val > (long long) HFE_MAX_ABS_TIME_MSEC / 1000) {
            addReplyErrorExpireTime(c);
            return C_ERR;
        }
        val *= 1000;
    }

    if (val > (long long) HFE_MAX_ABS_TIME_MSEC - basetime) {
        addReplyErrorExpireTime(c);
        return C_ERR;
    }
    val += basetime;
    *expire = val;
    return C_OK;
}

/* Flags that are used as part of HGETEX and HSETEX commands. */
#define HFE_EX       (1<<0) /* Expiration time in seconds */
#define HFE_PX       (1<<1) /* Expiration time in milliseconds */
#define HFE_EXAT     (1<<2) /* Expiration time in unix seconds */
#define HFE_PXAT     (1<<3) /* Expiration time in unix milliseconds */
#define HFE_PERSIST  (1<<4) /* Persist fields */
#define HFE_KEEPTTL  (1<<5) /* Do not discard field ttl on set op */
#define HFE_FXX      (1<<6) /* Set fields if all the fields already exist */
#define HFE_FNX      (1<<7) /* Set fields if none of the fields exist */

/* Command types for unified hash argument parser */
#define HASH_CMD_HGETEX 0
#define HASH_CMD_HSETEX 1

/* Parse hash field expiration command arguments for both HGETEX and HSETEX.
 * HGETEX <key> [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]
 *              FIELDS <numfields> field [field ...]
 * HSETEX <key> [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|KEEPTTL]
 *              [FXX|FNX] FIELDS <numfields> field value [field value ...]
 */
static int parseHashFieldExpireArgs(client *c, int *flags,
                                    long long *expire_time, int *expire_time_pos,
                                    int *first_field_pos, int *field_count,
                                    int command_type) {
    *flags = 0;
    *first_field_pos = -1;
    *field_count = -1;
    *expire_time_pos = -1;

    for (int i = 2; i < c->argc; i++) {
        if (!strcasecmp(c->argv[i]->ptr, "fields")) {
            /* Ensure only one FIELDS argument is provided */
            if (*first_field_pos != -1) {
                addReplyError(c, "FIELDS keyword specified multiple times");
                return C_ERR;
            }

            int args_per_field = (command_type == HASH_CMD_HSETEX) ? 2 : 1;
            long val;
            /* Ensure we have at least the numfields argument */
            if (i + 1 >= c->argc) {
                addReplyErrorArity(c);
                return C_ERR;
            }

            if (getRangeLongFromObjectOrReply(c, c->argv[i + 1], 1, INT_MAX, &val,
                                              "invalid number of fields") != C_OK)
                return C_ERR;

            *first_field_pos = i + 2;
            *field_count = (int) val;

            /* Validate field count based on command type */
            long long required_args = *first_field_pos + ((long long)*field_count * args_per_field);
            if (required_args > c->argc) {
                addReplyError(c, "wrong number of arguments");
                return C_ERR;
            }

            /* Skip over numfields and all field-value pairs
             * Set i to the last position of the FIELDS block, loop will increment past it */
            i = *first_field_pos + (*field_count * args_per_field) - 1;
            continue;
        } else if (!strcasecmp(c->argv[i]->ptr, "EX")) {
            if (*flags & (HFE_EX | HFE_EXAT | HFE_PX | HFE_PXAT | HFE_KEEPTTL | HFE_PERSIST))
                goto err_expiration;

            if (i >= c->argc - 1)
                goto err_missing_expire;

            *flags |= HFE_EX;
            i++;
            if (parseExpireTime(c, c->argv[i], UNIT_SECONDS,
                                commandTimeSnapshot(), expire_time) != C_OK)
                return C_ERR;

            *expire_time_pos = i;
        } else if (!strcasecmp(c->argv[i]->ptr, "PX")) {
            if (*flags & (HFE_EX | HFE_EXAT | HFE_PX | HFE_PXAT | HFE_KEEPTTL | HFE_PERSIST))
                goto err_expiration;

            if (i >= c->argc - 1)
                goto err_missing_expire;

            *flags |= HFE_PX;
            i++;
            if (parseExpireTime(c, c->argv[i], UNIT_MILLISECONDS,
                                commandTimeSnapshot(), expire_time) != C_OK)
                return C_ERR;

            *expire_time_pos = i;
        } else if (!strcasecmp(c->argv[i]->ptr, "EXAT")) {
            if (*flags & (HFE_EX | HFE_EXAT | HFE_PX | HFE_PXAT | HFE_KEEPTTL | HFE_PERSIST))
                goto err_expiration;

            if (i >= c->argc - 1)
                goto err_missing_expire;

            *flags |= HFE_EXAT;
            i++;
            if (parseExpireTime(c, c->argv[i], UNIT_SECONDS, 0, expire_time) != C_OK)
                return C_ERR;

            *expire_time_pos = i;
        } else if (!strcasecmp(c->argv[i]->ptr, "PXAT")) {
            if (*flags & (HFE_EX | HFE_EXAT | HFE_PX | HFE_PXAT | HFE_KEEPTTL | HFE_PERSIST))
                goto err_expiration;

            if (i >= c->argc - 1)
                goto err_missing_expire;

            *flags |= HFE_PXAT;
            i++;
            if (parseExpireTime(c, c->argv[i], UNIT_MILLISECONDS, 0,
                                expire_time) != C_OK)
                return C_ERR;

            *expire_time_pos = i;
        } else if (command_type == HASH_CMD_HGETEX && !strcasecmp(c->argv[i]->ptr, "PERSIST")) {
            if (*flags & (HFE_EX | HFE_EXAT | HFE_PX | HFE_PXAT | HFE_PERSIST))
                goto err_expiration;
            *flags |= HFE_PERSIST;
        } else if (command_type == HASH_CMD_HSETEX && !strcasecmp(c->argv[i]->ptr, "KEEPTTL")) {
            if (*flags & (HFE_EX | HFE_EXAT | HFE_PX | HFE_PXAT | HFE_KEEPTTL))
                goto err_expiration;
            *flags |= HFE_KEEPTTL;
        } else if (command_type == HASH_CMD_HSETEX && !strcasecmp(c->argv[i]->ptr, "FXX")) {
            if (*flags & (HFE_FXX | HFE_FNX))
                goto err_condition;
            *flags |= HFE_FXX;
        } else if (command_type == HASH_CMD_HSETEX && !strcasecmp(c->argv[i]->ptr, "FNX")) {
            if (*flags & (HFE_FXX | HFE_FNX))
                goto err_condition;
            *flags |= HFE_FNX;
        } else {
            addReplyErrorFormat(c, "unknown argument: %s", (char*) c->argv[i]->ptr);
            return C_ERR;
        }
    }

    /* Ensure FIELDS is specified */
    if (*first_field_pos == -1) {
        addReplyError(c, "missing FIELDS argument");
        return C_ERR;
    }

    return C_OK;

err_missing_expire:
    addReplyError(c, "missing expire time");
    return C_ERR;
err_condition:
    addReplyError(c, "Only one of FXX or FNX arguments can be specified");
    return C_ERR;
err_expiration:
    if (command_type == HASH_CMD_HSETEX) {
        addReplyError(c, "Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified");
    } else {
        addReplyError(c, "Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified");
    }
    return C_ERR;
}

/* Set the value of one or more fields of a given hash key, and optionally set
 * their expiration.
 *
 * HSETEX key
 *  [FNX | FXX]
 *  [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
 *  FIELDS <numfields> field value [field value...]
 *
 * Reply:
 *   Integer reply: 0 if no fields were set (due to FXX/FNX args)
 *   Integer reply: 1 if all the fields were set
 */
void hsetexCommand(client *c) {
    int flags = 0, first_field_pos = 0, field_count = 0, expire_time_pos = -1;
    int set_expiry;
    long long expire_time = EB_EXPIRE_TIME_INVALID;
    int64_t oldlen, newlen;
    HashTypeSetEx setex;
    dictEntryLink link;
    size_t oldsize = 0;

    if (parseHashFieldExpireArgs(c, &flags, &expire_time, &expire_time_pos,
                                 &first_field_pos, &field_count, HASH_CMD_HSETEX) != C_OK)
        return;

    kvobj *o = lookupKeyWriteWithLink(c->db, c->argv[1], &link);
    if (checkType(c, o, OBJ_HASH))
        return;

    if (!o) {
        if (flags & HFE_FXX) {
            addReplyLongLong(c, 0);
            return;
        }
        o = createHashObject();
        dbAddByLink(c->db, c->argv[1], &o, &link);
    }
    oldlen = (int64_t) hashTypeLength(o, 0);
    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);

    /* Track fields for subkey notifications by event type. */
    fieldvec fvexpired, fvset, fvdeleted, fvupdated;
    vec *vexpired = fieldvecInit(&fvexpired, field_count);
    vec *vset = fieldvecInit(&fvset, field_count);
    vec *vdeleted = fieldvecInit(&fvdeleted, field_count);
    vec *vupdated = fieldvecInit(&fvupdated, field_count);

    if (flags & (HFE_FXX | HFE_FNX)) {
        int found = 0;
        for (int i = 0; i < field_count; i++) {
            sds field = c->argv[first_field_pos + (i * 2)]->ptr;
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;
            const int opt = HFE_LAZY_NO_NOTIFICATION |
                            HFE_LAZY_NO_SIGNAL |
                            HFE_LAZY_AVOID_HASH_DEL |
                            HFE_LAZY_NO_UPDATE_KEYSIZES |
                            HFE_LAZY_NO_UPDATE_ALLOCSIZES;

            GetFieldRes res = hashTypeGetValue(c->db, o, field, &vstr, &vlen, &vll, opt, NULL);
            int exists = (res == GETF_OK);
            if (res == GETF_EXPIRED) {
                vecPush(vexpired, c->argv[first_field_pos + (i * 2)]);
            }
            found += exists;

            /* Check for early exit if the condition is already invalid. */
            if (((flags & HFE_FXX) && !exists) ||
                ((flags & HFE_FNX) && exists))
                break;
        }

        int all_exists = (found == field_count);
        int non_exists = (found == 0);

        if (((flags & HFE_FNX) && !non_exists) ||
            ((flags & HFE_FXX) && !all_exists))
        {
            addReplyLongLong(c, 0);
            goto out;
        }
    }
    hashTypeTryConversion(c->db, o,c->argv, first_field_pos, c->argc - 1);

    /* Check if we will set the expiration time. */
    set_expiry = flags & (HFE_EX | HFE_PX | HFE_EXAT | HFE_PXAT);
    if (set_expiry)
        hashTypeSetExInit(c->argv[1], o, c, c->db, 0, &setex);

    for (int i = 0; i < field_count; i++) {
        sds field = c->argv[first_field_pos + (i * 2)]->ptr;
        sds value = c->argv[first_field_pos + (i * 2) + 1]->ptr;

        int opt = HASH_SET_COPY;
        /* If we are going to set the expiration time later, no need to discard
         * it as part of set operation now. */
        if (flags & (HFE_EX | HFE_PX | HFE_EXAT | HFE_PXAT | HFE_KEEPTTL))
            opt |= HASH_SET_KEEP_TTL;

        hashTypeSet(c->db, o, field, value, opt);
        vecPush(vset, c->argv[first_field_pos + (i * 2)]);
        /* Update the expiration time. */
        if (set_expiry) {
            int ret = hashTypeSetEx(o, field, expire_time, &setex);
            if (ret == HSETEX_OK) {
                vecPush(vupdated, c->argv[first_field_pos + (i * 2)]);
            } else if (ret == HSETEX_DELETED) {
                vecPush(vdeleted, c->argv[first_field_pos + (i * 2)]);
            }
        }
    }

    if (set_expiry)
        hashTypeSetExDone(&setex);

    server.dirty += field_count;

    if (vecSize(vdeleted)) {
        /* If fields are deleted due to timestamp is being in the past, hdel's
         * are already propagated. No need to propagate the command itself. */
        preventCommandPropagation(c);
    } else if (set_expiry && !(flags & HFE_PXAT)) {
        /* Propagate as 'HSETEX <key> PXAT ..' if there is EX/EXAT/PX flag*/

        /* Replace EX/EXAT/PX with PXAT */
        rewriteClientCommandArgument(c, expire_time_pos - 1, shared.pxat);
        /* Replace timestamp with unix timestamp milliseconds. */
        robj *expire = createStringObjectFromLongLong(expire_time);
        rewriteClientCommandArgument(c, expire_time_pos, expire);
        decrRefCount(expire);
    }

    addReplyLongLong(c, 1);

out:
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));
    /* Emit keyspace notifications based on field expiry, mutation, or key deletion */
    if (vecSize(vset) || vecSize(vexpired)) {
        newlen = (int64_t) hashTypeLength(o, 0); 
        keyModified(c, c->db, c->argv[1], o, 1);
        if (vecSize(vexpired)) {
            notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpired", c->argv[1],
                                           c->db->id, (robj**)vecData(vexpired), vecSize(vexpired));
        }
        if (vecSize(vset)) {
            notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hset", c->argv[1],
                                           c->db->id, (robj**)vecData(vset), vecSize(vset));
            if (vecSize(vdeleted)) {
                notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hdel", c->argv[1],
                                               c->db->id, (robj**)vecData(vdeleted), vecSize(vdeleted));
            } else if (vecSize(vupdated)) {
                notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpire", c->argv[1],
                                               c->db->id, (robj**)vecData(vupdated), vecSize(vupdated));
            }
        }
        
        KSN_INVALIDATE_KVOBJ(o);
        
        /* Key may become empty due to lazy expiry in hashTypeGetValue()
         * or the new expiration time is in the past.*/
        if (newlen == 0) {
            newlen = -1;
            /* Del key but don't update KEYSIZES. else it will decr wrong bin in histogram */
            dbDeleteSkipKeysizesUpdate(c->db, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        }
        if (oldlen != newlen)
            updateKeysizesHist(c->db, OBJ_HASH, oldlen, newlen);
    }

    vecRelease(vexpired);
    vecRelease(vset);
    vecRelease(vdeleted);
    vecRelease(vupdated);
}

void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    kvobj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
    size_t oldsize = 0;

    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    GetFieldRes res = hashTypeGetValue(c->db,o,c->argv[2]->ptr,&vstr,&vlen,&value,
                                       HFE_LAZY_EXPIRE, NULL);
    if (res == GETF_OK) {
        if (vstr) {
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else if ((res == GETF_NOT_FOUND) || (res == GETF_EXPIRED)) {
        value = 0;
        unsigned long l = hashTypeLength(o, 0);
        updateKeysizesHist(c->db, OBJ_HASH, l, l + 1);
    } else {
        /* Field expired and in turn hash deleted. Create new one! */
        o = createHashObject();
        dbAdd(c->db,c->argv[1],&o);
        value = 0;
        updateKeysizesHist(c->db, OBJ_HASH, 0, 1);
    }

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;
    new = sdsfromlonglong(value);
    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);

    robj obj, *argv[2] = {c->argv[2], &obj};
    initStaticStringObject(obj, new);
    hashTypeTryConversion(c->db, o, argv, 0, 1);

    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));
    addReplyLongLong(c,value);
    keyModified(c,c->db,c->argv[1], o, 1);
    notifyKeyspaceEventWithSubkeys(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id,&c->argv[2],1);
    KSN_INVALIDATE_KVOBJ(o);
    server.dirty++;
}

void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    kvobj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
    size_t oldsize = 0;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if (isnan(incr) || isinf(incr)) {
        addReplyError(c,"value is NaN or Infinity");
        return;
    }
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    GetFieldRes res = hashTypeGetValue(c->db, o,c->argv[2]->ptr,&vstr,&vlen,&ll,
                                       HFE_LAZY_EXPIRE, NULL);
    if (res == GETF_OK) {
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            value = (long double)ll;
        }
    } else if ((res == GETF_NOT_FOUND) || (res == GETF_EXPIRED)) {
        value = 0;
        unsigned long l = hashTypeLength(o, 0);
        updateKeysizesHist(c->db, OBJ_HASH, l, l + 1);
    } else {
        /* Field expired and in turn hash deleted. Create new one! */
        o = createHashObject();
        dbAdd(c->db, c->argv[1], &o);
        value = 0;
        updateKeysizesHist(c->db, OBJ_HASH, 0, 1);
    }

    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,LD_STR_HUMAN);
    new = sdsnewlen(buf,len);
    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);

    robj obj, *argv[2] = {c->argv[2], &obj};
    initStaticStringObject(obj, new);
    hashTypeTryConversion(c->db, o, argv, 0, 1);

    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));
    addReplyBulkCBuffer(c,buf,len);
    keyModified(c,c->db,c->argv[1],o,1);
    notifyKeyspaceEventWithSubkeys(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id,&c->argv[2],1);
    KSN_INVALIDATE_KVOBJ(o);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSETEX command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart.
     * The KEEPTTL flag is used to make sure the field TTL is preserved. */
    robj *newobj;
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandVector(c, 7, shared.hsetex, c->argv[1], shared.keepttl,
                        shared.fields, shared.integers[1], c->argv[2], newobj);
    decrRefCount(newobj);
}

static GetFieldRes addHashFieldToReply(client *c, kvobj *o, sds field, int hfeFlags) {
    if (o == NULL) {
        addReplyNull(c);
        return GETF_NOT_FOUND;
    }

    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    GetFieldRes res = hashTypeGetValue(c->db, o, field, &vstr, &vlen, &vll, hfeFlags, NULL);
    if (res == GETF_OK) {
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }
    } else {
        addReplyNull(c);
    }
    return res;
}

void hgetCommand(client *c) {
    kvobj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addHashFieldToReply(c, o, c->argv[2]->ptr, HFE_LAZY_EXPIRE);
}

void hmgetCommand(client *c) {
    GetFieldRes res = GETF_OK;
    int i, deleted = 0;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    kvobj *o = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c,o,OBJ_HASH)) return;

    /* Track expired fields for subkey notification. */
    fieldvec fvexpired;
    vec *vexpired = fieldvecInit(&fvexpired, c->argc-2);

    addReplyArrayLen(c, c->argc-2);
    for (i = 2; i < c->argc ; i++) {
        if (!deleted) {
            res = addHashFieldToReply(c, o, c->argv[i]->ptr, HFE_LAZY_NO_NOTIFICATION);
            if (res == GETF_EXPIRED) {
                vecPush(vexpired, c->argv[i]);
            }
            deleted += (res == GETF_EXPIRED_HASH);
        } else {
            /* If hash got lazy expired since all fields are expired (o is invalid),
             * then fill the rest with trivial nulls and return. */
            addReplyNull(c);
        }
    }

    if (vecSize(vexpired)) {
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpired", c->argv[1],
                                       c->db->id, (robj**)vecData(vexpired), vecSize(vexpired));
    }
    if (deleted)
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);

    vecRelease(vexpired);
}

/* Get and delete the value of one or more fields of a given hash key.
 * HGETDEL <key> FIELDS <numfields> field1 field2 ...
 * Reply: list of the value associated with each field or nil if the field
 *        doesn’t exist.
 */
void hgetdelCommand(client *c) {
    int res = 0, hfe = 0;
    int64_t oldlen = -1; /* not exists as long as it is not set */
    long num_fields = 0;
    size_t oldsize = 0;

    kvobj *o = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, o, OBJ_HASH))
        return;

    if (strcasecmp(c->argv[2]->ptr, "FIELDS") != 0) {
        addReplyError(c, "Mandatory argument FIELDS is missing or not at the right position");
        return;
    }

    /* Read number of fields */
    if (getRangeLongFromObjectOrReply(c, c->argv[3], 1, LONG_MAX, &num_fields,
                                      "Number of fields must be a positive integer") != C_OK)
        return;

    /* Verify `numFields` is consistent with number of arguments */
    if (num_fields != c->argc - 4) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }

    /* Hash field expiration is optimized to avoid frequent update global HFE DS
     * for each field deletion. Eventually active-expiration will run and update
     * or remove the hash from global HFE DS gracefully. Nevertheless, statistic
     * "subexpiry" might reflect wrong number of hashes with HFE to the user if
     * it is the last field with expiration. The following logic checks if this
     * is the last field with expiration and removes it from global HFE DS. */
    if (o) {
        hfe = hashTypeIsFieldsWithExpire(o);
        oldlen = hashTypeLength(o, 0);
        if (server.memory_tracking_enabled)
            oldsize = kvobjAllocSize(o);
    }

    /* Track fields for subkey notifications. */
    fieldvec fvexpired, fvdeleted;
    vec *vexpired = fieldvecInit(&fvexpired, num_fields);
    vec *vdeleted = fieldvecInit(&fvdeleted, num_fields);

    addReplyArrayLen(c, num_fields);
    for (int i = 4; i < c->argc; i++) {
        const int flags = HFE_LAZY_NO_NOTIFICATION |
                          HFE_LAZY_NO_SIGNAL |
                          HFE_LAZY_AVOID_HASH_DEL |
                          HFE_LAZY_NO_UPDATE_KEYSIZES |
                          HFE_LAZY_NO_UPDATE_ALLOCSIZES;
        res = addHashFieldToReply(c, o, c->argv[i]->ptr, flags);
        if (res == GETF_EXPIRED) {
            vecPush(vexpired, c->argv[i]);
        }
        /* Try to delete only if it's found and not expired lazily. */
        if (res == GETF_OK) {
            vecPush(vdeleted, c->argv[i]);
            serverAssert(hashTypeDelete(o, c->argv[i]->ptr) == 1);
        }
    }

    /* Return if no modification has been made. */
    if (vecSize(vexpired) == 0 && vecSize(vdeleted) == 0) {
        vecRelease(vexpired);
        vecRelease(vdeleted);
        return;
    }

    int64_t newlen = (int64_t) hashTypeLength(o, 0);
    /* del key if become empty */
    int delete_key = (newlen == 0);
    /* update new len for keysizes histogram */
    int64_t hist_newlen = delete_key ? -1 : newlen;
    if (oldlen != hist_newlen)
        updateKeysizesHist(c->db, OBJ_HASH, oldlen, hist_newlen);
    /* update memory tracking */
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));
    /* is it last HFE */
    if (!delete_key && hfe && (hashTypeIsFieldsWithExpire(o) == 0))
        estoreRemove(c->db->subexpires, getKeySlot(c->argv[1]->ptr), o);
    
    keyModified(c, c->db, c->argv[1], o, 1);

    if (vecSize(vexpired)) {
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpired", c->argv[1],
                                       c->db->id, (robj**)vecData(vexpired), vecSize(vexpired));
    }
    if (vecSize(vdeleted)) {
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hdel", c->argv[1],
                                       c->db->id, (robj**)vecData(vdeleted), vecSize(vdeleted));
        server.dirty += vecSize(vdeleted);

        /* Propagate as HDEL command.
         * Orig: HGETDEL <key> FIELDS <numfields> field1 field2 ...
         * Repl: HDEL <key> field1 field2 ... */
        rewriteClientCommandArgument(c, 0, shared.hdel);
        rewriteClientCommandArgument(c, 2, NULL);  /* Delete FIELDS arg */
        rewriteClientCommandArgument(c, 2, NULL);  /* Delete <numfields> arg */
    }

    vecRelease(vexpired);
    vecRelease(vdeleted);
    KSN_INVALIDATE_KVOBJ(o);

    /* Key may have become empty because of deleting fields or lazy expire. */
    if (delete_key) {
        /* Del key but don't update KEYSIZES. else it will decr wrong bin in histogram */
        dbDeleteSkipKeysizesUpdate(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }
}

/* Get the value of one or more fields of a given hash key and optionally set 
 * their expiration.
 *
 * HGETEX <key>
 *   [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
 *   FIELDS <numfields> field1 field2 ...
 *
 * Reply: list of the value associated with each field or nil if the field
 *        doesn’t exist.
 */
void hgetexCommand(client *c) {
    int parse_flags = 0, expire_time_pos = -1, first_field_pos = -1, num_fields = -1;
    long long expire_time = 0;
    int64_t oldlen = 0, newlen = -1;
    HashTypeSetEx setex;
    size_t oldsize = 0;

    kvobj *o = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, o, OBJ_HASH))
        return;

    /* Parse arguments using flexible parser */
    if (parseHashFieldExpireArgs(c, &parse_flags, &expire_time, &expire_time_pos, &first_field_pos, &num_fields, HASH_CMD_HGETEX) != C_OK)
        return;

    /* Non-existing keys and empty hashes are the same thing. Reply null if the
     * key does not exist.*/
    if (!o) {
        addReplyArrayLen(c, num_fields);
        for (int i = 0; i < num_fields; i++)
            addReplyNull(c);
        return;
    }

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);
    oldlen = hashTypeLength(o, 0);
    if (parse_flags)
        hashTypeSetExInit(c->argv[1], o, c, c->db, 0, &setex);

    /* Track fields for subkey notifications by event type. */
    fieldvec fvexpired, fvdeleted, fvupdated;
    vec *vexpired = fieldvecInit(&fvexpired, num_fields);
    vec *vdeleted = fieldvecInit(&fvdeleted, num_fields);
    vec *vupdated = fieldvecInit(&fvupdated, num_fields);

    addReplyArrayLen(c, num_fields);
    for (int i = first_field_pos; i < first_field_pos + num_fields; i++) {
        const int flags = HFE_LAZY_NO_NOTIFICATION |
                          HFE_LAZY_NO_SIGNAL |
                          HFE_LAZY_AVOID_HASH_DEL |
                          HFE_LAZY_NO_UPDATE_KEYSIZES |
                          HFE_LAZY_NO_UPDATE_ALLOCSIZES;
        sds field = c->argv[i]->ptr;
        int res = addHashFieldToReply(c, o, c->argv[i]->ptr, flags);
        if (res == GETF_EXPIRED) {
            vecPush(vexpired, c->argv[i]);
        }

        /* Set expiration only if the field exists and not expired lazily. */
        if (res == GETF_OK && parse_flags) {
            if (parse_flags & HFE_PERSIST)
                expire_time = EB_EXPIRE_TIME_INVALID;

            res = hashTypeSetEx(o, field, expire_time, &setex);
            if (res == HSETEX_DELETED) {
                vecPush(vdeleted, c->argv[i]);
            } else if (res == HSETEX_OK) {
                vecPush(vupdated, c->argv[i]);
            }
        }
    }

    if (parse_flags)
        hashTypeSetExDone(&setex);

    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));

    /* Exit early if no modification has been made. */
    if (vecSize(vexpired) == 0 && vecSize(vdeleted) == 0 && vecSize(vupdated) == 0) {
        vecRelease(vexpired);
        vecRelease(vdeleted);
        vecRelease(vupdated);
        return;
    }

    server.dirty += vecSize(vdeleted) + vecSize(vupdated);
    keyModified(c, c->db, c->argv[1], o, 1);

    /* This command will never be propagated as it is. It will be propagated as
     * HDELs when fields are lazily expired or deleted, if the new timestamp is
     * in the past. HDEL's will be emitted as part of addHashFieldToReply()
     * or hashTypeSetEx() in this case.
     *
     * If PERSIST flags is used, it will be propagated as HPERSIST command.
     * IF EX/EXAT/PX/PXAT flags are used, it will be replicated as HPEXPRITEAT.
     */
    if (vecSize(vexpired)) {
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpired", c->argv[1],
                                       c->db->id, (robj**)vecData(vexpired), vecSize(vexpired));
    }
    if (vecSize(vupdated)) {
        /* Build canonical command for propagation */
        int canonical_argc;
        robj **canonical_argv;
        int idx = 0;

        if (parse_flags & HFE_PERSIST) {
            notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hpersist", c->argv[1],
                                           c->db->id, (robj**)vecData(vupdated), vecSize(vupdated));
            /* Build canonical HPERSIST command: HPERSIST key FIELDS numfields field1 field2 ... */
            canonical_argc = 4 + num_fields;
            canonical_argv = zmalloc(sizeof(robj*) * canonical_argc);
            canonical_argv[idx++] = shared.hpersist;
            incrRefCount(shared.hpersist);
            canonical_argv[idx++] = c->argv[1]; /* key */
            incrRefCount(c->argv[1]);
        } else {
            notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpire", c->argv[1],
                                           c->db->id, (robj**)vecData(vupdated), vecSize(vupdated));
            /* Build canonical HPEXPIREAT command: HPEXPIREAT key timestamp FIELDS numfields field1 field2 ... */
            canonical_argc = 5 + num_fields;
            canonical_argv = zmalloc(sizeof(robj*) * canonical_argc);
            canonical_argv[idx++] = shared.hpexpireat;
            incrRefCount(shared.hpexpireat);
            canonical_argv[idx++] = c->argv[1]; /* key */
            incrRefCount(c->argv[1]);
            canonical_argv[idx++] = createStringObjectFromLongLong(expire_time); /* timestamp */
        }

        canonical_argv[idx++] = shared.fields;
        incrRefCount(shared.fields);
        canonical_argv[idx++] = createStringObjectFromLongLong(num_fields);
        for (int i = 0; i < num_fields; i++) {
            canonical_argv[idx++] = c->argv[first_field_pos + i];
            incrRefCount(c->argv[first_field_pos + i]);
        }

        replaceClientCommandVector(c, canonical_argc, canonical_argv);
    } else if (vecSize(vdeleted)) {
        /* If we are here, fields are deleted because new timestamp was in the
         * past. HDELs are already propagated as part of hashTypeSetEx(). */
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hdel", c->argv[1],
                                       c->db->id, (robj**)vecData(vdeleted), vecSize(vdeleted));
        preventCommandPropagation(c);
    }

    vecRelease(vexpired);
    vecRelease(vdeleted);
    vecRelease(vupdated);

    /* Key may become empty due to lazy expiry in addHashFieldToReply()
     * or the new expiration time is in the past.*/
    newlen = hashTypeLength(o, 0);

    updateKeysizesHist(c->db, OBJ_HASH, oldlen, newlen);
    if (newlen == 0) {
        dbDelete(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }
}

void hdelCommand(client *c) {
    kvobj *o;
    int j, keyremoved = 0;
    size_t oldsize = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    int64_t oldLen = (int64_t) hashTypeLength(o, 0);
    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);

    /* Hash field expiration is optimized to avoid frequent update global HFE DS for
     * each field deletion. Eventually active-expiration will run and update or remove
     * the hash from global HFE DS gracefully. Nevertheless, statistic "subexpiry"
     * might reflect wrong number of hashes with HFE to the user if it is the last
     * field with expiration. The following logic checks if this is indeed the last
     * field with expiration and removes it from global HFE DS. */
    int isHFE = hashTypeIsFieldsWithExpire(o);

    /* Track which fields were actually deleted for subkey notification. */
    fieldvec fvdeleted;
    vec *vdeleted = fieldvecInit(&fvdeleted, c->argc - 2);

    if (o->encoding == OBJ_ENCODING_HT)
        dictPauseAutoResize((dict*)o->ptr);
    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            vecPush(vdeleted, c->argv[j]);
            if (hashTypeLength(o, 0) == 0) {
                keyremoved = 1;
                break;
            }
        }
    }
    
    if (!keyremoved && o->encoding == OBJ_ENCODING_HT) {
        dictResumeAutoResize((dict*)o->ptr);
        dictShrinkIfNeeded((dict*)o->ptr);
    }
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));
    if (vecSize(vdeleted)) {
        /* Update keysizes histogram */
        int64_t newLen = (int64_t) hashTypeLength(o, 0);
        updateKeysizesHist(c->db, OBJ_HASH, oldLen, keyremoved ? -1 : newLen);
        
        if (keyremoved) {
            /* del key but don't update KEYSIZES. Else it will decr wrong bin in histogram */
            dbDeleteSkipKeysizesUpdate(c->db, c->argv[1]);
        } else {
            /* is it last HFE */
            if (isHFE && (hashTypeIsFieldsWithExpire(o) == 0))
                estoreRemove(c->db->subexpires, getKeySlot(c->argv[1]->ptr), o);
        }

        /* Signal key modification */
        keyModified(c, c->db, c->argv[1], keyremoved ? NULL : o, 1);
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH,"hdel",c->argv[1],c->db->id,(robj**)vecData(vdeleted),vecSize(vdeleted));
        
        KSN_INVALIDATE_KVOBJ(o); /* Invalidate local kvobj pointer */
        
        /* Notify del event if key was deleted */
        if (keyremoved) notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty += vecSize(vdeleted);
    }
    addReplyLongLong(c,vecSize(vdeleted));
    vecRelease(vdeleted);
}

void hlenCommand(client *c) {
    kvobj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addReplyLongLong(c,hashTypeLength(o, 0));
}

void hstrlenCommand(client *c) {
    kvobj *o;
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    GetFieldRes res = hashTypeGetValue(c->db, o, c->argv[2]->ptr, &vstr,
                                       &vlen, &vll, HFE_LAZY_EXPIRE, NULL);

    if (res == GETF_NOT_FOUND || res == GETF_EXPIRED || res == GETF_EXPIRED_HASH) {
        addReply(c, shared.czero);
        return;
    }

    size_t len = vstr ? vlen : sdigits10(vll);
    addReplyLongLong(c,len);
}

static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_LISTPACK ||
        hi->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromListpack(hi, what, &vstr, &vlen, &vll, NULL);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        char *value;
        size_t len;
        hashTypeCurrentFromHashTable(hi, what, &value, &len, NULL);
        addReplyBulkCBuffer(c, value, len);
    } else if (hi->encoding == OBJ_ENCODING_TMPL_LP ||
               hi->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentObject(hi, what, &vstr, &vlen, &vll, NULL);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(client *c, int flags) {
    kvobj *o;
    hashTypeIterator hi;
    int length, count = 0;
    size_t oldsize = 0;

    robj *emptyResp = (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) ?
        shared.emptymap[c->resp] : shared.emptyarray;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyResp))
        == NULL || checkType(c,o,OBJ_HASH)) return;

    /* We return a map if the user requested keys and values, like in the
     * HGETALL case. Otherwise to use a flat array makes more sense. */
    if ((length = hashTypeLength(o, 1 /*subtractExpiredFields*/)) == 0) {
        addReply(c, emptyResp);
        return;
    }

    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) {
        addReplyMapLen(c, length);
    } else {
        addReplyArrayLen(c, length);
    }

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);

    /* Fast path: batched prefetch for hashtable-encoded HGETALL.
     * Collect a batch of dict entries, prefetch their Entry structs and
     * value SDS data, then emit replies while the data is cache-warm.
     * This hides the latency of pointer chasing through scattered
     * heap allocations (dictEntry → Entry → value SDS). */
#define HGETALL_BATCH 16
    if (o->encoding == OBJ_ENCODING_HT) {
        int skip_expired = !server.allow_access_expired;
        dict *d = o->ptr;
        dictIterator di;
        dictInitSafeIterator(&di, d);
        Entry *batch_entry[HGETALL_BATCH];
        sds batch_val[HGETALL_BATCH];

        while (1) {
            /* Phase 1: pull a batch of entries from the dict iterator and
             * prefetch their Entry structs. Pure pointer-fetch — we don't
             * dereference Entry here so the prefetch is effective. */
            int batch_count = 0;
            while (batch_count < HGETALL_BATCH) {
                dictEntry *de = dictNext(&di);
                if (!de) break;
                Entry *e = dictGetKey(de);
                batch_entry[batch_count++] = e;
                redis_prefetch_read(e);
            }
            if (batch_count == 0) break;

            /* Phase 2: Entry structs are warm — check expiry, extract value,
             * and prefetch the value SDS. Expired entries are dropped from
             * the batch by compacting in place. */
            int valid_count = 0;
            for (int i = 0; i < batch_count; i++) {
                Entry *e = batch_entry[i];
                if (skip_expired) {
                    uint64_t expire_time = entryGetExpiry(e);
                    if (expire_time != EB_EXPIRE_TIME_INVALID && (mstime_t)expire_time < commandTimeSnapshot())
                        continue;
                }
                batch_entry[valid_count] = e;
                if (flags & OBJ_HASH_VALUE) {
                    sds val = entryGetValue(e);
                    batch_val[valid_count] = val;
                    redis_prefetch_read(val);
                }
                valid_count++;
            }

            /* Phase 3: emit replies — field + value data is cache-warm. */
            for (int i = 0; i < valid_count; i++) {
                if (flags & OBJ_HASH_KEY) {
                    sds field = entryGetField(batch_entry[i]);
                    addReplyBulkCBuffer(c, field, sdslen(field));
                    count++;
                }
                if (flags & OBJ_HASH_VALUE) {
                    sds val = batch_val[i];
                    addReplyBulkCBuffer(c, val, sdslen(val));
                    count++;
                }
            }
        }
        dictResetIterator(&di);
        goto done;
    }

    hashTypeInitIterator(&hi, o);

    while (hashTypeNext(&hi, 1 /*skipExpiredFields*/) != C_ERR) {
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, &hi, OBJ_HASH_KEY);
            count++;
        }
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, &hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeResetIterator(&hi);

done:
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));

    /* Make sure we returned the right number of elements. */
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) count /= 2;
    serverAssert(count == length);
}

void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

void hexistsCommand(client *c) {
    kvobj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addReply(c,hashTypeExists(c->db,o,c->argv[2]->ptr,HFE_LAZY_EXPIRE, NULL) ?
                                shared.cone : shared.czero);
}

void hscanCommand(client *c) {
    kvobj *o;
    unsigned long long cursor;
    size_t oldsize = 0;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(o);
    scanGenericCommand(c,o,cursor);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), o, oldsize, kvobjAllocSize(o));
}

static void hrandfieldReplyWithListpack(client *c, unsigned int count, listpackEntry *keys, listpackEntry *vals) {
    for (unsigned long i = 0; i < count; i++) {
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        if (vals) {
            if (vals[i].sval)
                addReplyBulkCBuffer(c, vals[i].sval, vals[i].slen);
            else
                addReplyBulkLongLong(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the hash compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define HRANDFIELD_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
#define HRANDFIELD_RANDOM_SAMPLE_LIMIT 1000

void hrandfieldWithCountCommand(client *c, long l, int withvalues) {
    unsigned long count, size;
    int uniq = 1;
    kvobj *hash;
    size_t oldsize = 0;

    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,hash,OBJ_HASH)) return;

    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* Delete all expired fields. If the entire hash got deleted then return empty array. */
    if (hashTypeExpireIfNeeded(c->db, hash)) {
        addReply(c, shared.emptyarray);
        return;
    }

    /* Delete expired fields */
    size = hashTypeLength(hash, 0);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(hash);

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        if (withvalues && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);
        if (hash->encoding == OBJ_ENCODING_HT) {
            while (count--) {
                dictEntry *de = dictGetFairRandomKey(hash->ptr);
                Entry *entry = dictGetKey(de);
                sds fieldStr = entryGetField(entry);
                if (withvalues && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, fieldStr, sdslen(fieldStr));
                if (withvalues) {
                    sds value = entryGetValue(entry);
                    addReplyBulkCBuffer(c, value, sdslen(value));
                }
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
        } else if (hash->encoding == OBJ_ENCODING_LISTPACK ||
                   hash->encoding == OBJ_ENCODING_LISTPACK_EX)
        {
            listpackEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            unsigned char *lp = hashTypeListpackGetLp(hash);
            int tuple_len = hash->encoding == OBJ_ENCODING_LISTPACK ? 2 : 3;

            limit = count > HRANDFIELD_RANDOM_SAMPLE_LIMIT ? HRANDFIELD_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(listpackEntry)*limit);
            if (withvalues)
                vals = zmalloc(sizeof(listpackEntry)*limit);
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomPairs(lp, sample_count, keys, vals, tuple_len);
                hrandfieldReplyWithListpack(c, sample_count, keys, vals);
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
            zfree(keys);
            zfree(vals);
        } else if (hash->encoding == OBJ_ENCODING_TMPL_LP ||
                   hash->encoding == OBJ_ENCODING_TMPL_ARRAY) {
            /* For tmpl-based hashes, use hashTypeRandomElement. */
            while (count--) {
                listpackEntry key, val;
                hashTypeRandomElement(hash, size, &key, withvalues ? &val : NULL);
                if (withvalues && c->resp > 2)
                    addReplyArrayLen(c, 2);
                if (key.sval)
                    addReplyBulkCBuffer(c, key.sval, key.slen);
                else
                    addReplyBulkLongLong(c, key.lval);
                if (withvalues) {
                    if (val.sval)
                        addReplyBulkCBuffer(c, val.sval, val.slen);
                    else
                        addReplyBulkLongLong(c, val.lval);
                }
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
        }
        goto out;
    }

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    long reply_size = count < size ? count : size;
    if (withvalues && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the hash: simply return the whole hash. */
    if(count >= size) {
        hashTypeIterator hi;
        hashTypeInitIterator(&hi, hash);
        while (hashTypeNext(&hi, 0) != C_ERR) {
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addHashIteratorCursorToReply(c, &hi, OBJ_HASH_KEY);
            if (withvalues)
                addHashIteratorCursorToReply(c, &hi, OBJ_HASH_VALUE);
        }
        hashTypeResetIterator(&hi);
        goto out;
    }

    /* CASE 2.5 listpack only. Sampling unique elements, in non-random order.
     * Listpack encoded hashes are meant to be relatively small, so
     * HRANDFIELD_SUB_STRATEGY_MUL isn't necessary and we rather not make
     * copies of the entries. Instead, we emit them directly to the output
     * buffer.
     *
     * And it is inefficient to repeatedly pick one random element from a
     * listpack in CASE 4. So we use this instead. */
    if (hash->encoding == OBJ_ENCODING_LISTPACK ||
        hash->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        unsigned char *lp = hashTypeListpackGetLp(hash);
        int tuple_len = hash->encoding == OBJ_ENCODING_LISTPACK ? 2 : 3;
        listpackEntry *keys, *vals = NULL;
        keys = zmalloc(sizeof(listpackEntry)*count);
        if (withvalues)
            vals = zmalloc(sizeof(listpackEntry)*count);
        serverAssert(lpRandomPairsUnique(lp, count, keys, vals, tuple_len) == count);
        hrandfieldReplyWithListpack(c, count, keys, vals);
        zfree(keys);
        zfree(vals);
        goto out;
    }

    /* CASE 2.5b Template-based hashes. Pick unique random indexes. */
    if (hash->encoding == OBJ_ENCODING_TMPL_LP ||
        hash->encoding == OBJ_ENCODING_TMPL_ARRAY)
    {
        hashTemplate *tmpl = hashTypeGetTemplate(hash);
        int fc = (int)tmpl->field_count;
        if (count > (unsigned long)fc) count = fc;

        /* Pick unique random indexes using Fisher-Yates partial shuffle. */
        int stack_idx[HASH_TMPL_STACK_ENTRIES];
        int *idx = (fc <= HASH_TMPL_STACK_ENTRIES) ?
                   stack_idx : zmalloc(sizeof(int) * fc);
        for (int i = 0; i < fc; i++) idx[i] = i;
        for (unsigned long i = 0; i < count; i++) {
            int j = i + (rand() % (fc - i));
            int tmp = idx[i]; idx[i] = idx[j]; idx[j] = tmp;
        }

        for (unsigned long i = 0; i < count; i++) {
            int fi = idx[i];
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c, 2);
            addReplyBulkCBuffer(c, tmpl->fields[fi], sdslen(tmpl->fields[fi]));
            if (withvalues) {
                if (hash->encoding == OBJ_ENCODING_TMPL_LP) {
                    unsigned char *p = lpSeek(hash->ptr, fi + 1);
                    unsigned int vlen;
                    long long vll;
                    unsigned char *vstr = lpGetValue(p, &vlen, &vll);
                    if (vstr)
                        addReplyBulkCBuffer(c, vstr, vlen);
                    else
                        addReplyBulkLongLong(c, vll);
                } else {
                    hashTemplateArray *hta = hash->ptr;
                    addReplyBulkCBuffer(c, hta->values[fi], sdslen(hta->values[fi]));
                }
            }
        }
        if (idx != stack_idx) zfree(idx);
        goto out;
    }

    /* CASE 3:
     * The number of elements inside the hash of type dict is not greater than
     * HRANDFIELD_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create an array of dictEntry pointers from the original hash,
     * and subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the hash, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count*HRANDFIELD_SUB_STRATEGY_MUL > size) {
        /* Hashtable encoding (generic implementation) */
        dict *ht = hash->ptr;
        dictIterator di;
        dictEntry *de;
        unsigned long idx = 0;

        /* Allocate a temporary array of pointers to stored key-values in dict and
         * assist it to remove random elements to reach the right count. */
        struct FieldValPair {
            sds field;
            sds value;
        } *pairs = zmalloc(sizeof(struct FieldValPair) * size);

        /* Add all the elements into the temporary array. */
        dictInitIterator(&di, ht);
        while((de = dictNext(&di)) != NULL) {
            Entry *e = dictGetKey(de);
            pairs[idx++] = (struct FieldValPair) {entryGetField(e), entryGetValue(e)};
        }
        dictResetIterator(&di);

        /* Remove random elements to reach the right count. */
        while (size > count) {
            unsigned long toDiscardIdx = rand() % size;
            pairs[toDiscardIdx] = pairs[--size];
        }

        /* Reply with what's in the array */
        for (idx = 0; idx < size; idx++) {
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkCBuffer(c, pairs[idx].field, sdslen(pairs[idx].field));
            if (withvalues)
                addReplyBulkCBuffer(c, pairs[idx].value, sdslen(pairs[idx].value));
        }

        zfree(pairs);
    }

    /* CASE 4: We have a big hash compared to the requested number of elements.
     * In this case we can simply get random elements from the hash and add
     * to the temporary hash, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        /* Allocate temporary dictUnique to find unique elements. Just keep ref
         * to key-value from the original hash. This dict relaxes hash function
         * to be based on field's pointer */
        dictType uniqueDictType = { .hashFunction =  dictPtrHash };
        dict *dictUnique = dictCreate(&uniqueDictType);
        dictExpand(dictUnique, count);

        /* Hashtable encoding (generic implementation) */
        unsigned long added = 0;

        while(added < count) {
            dictEntry *de = dictGetFairRandomKey(hash->ptr);
            serverAssert(de != NULL);
            Entry *e = dictGetKey(de);
            sds field = entryGetField(e);
            sds value = entryGetValue(e);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            if (dictAdd(dictUnique, field, value) != DICT_OK)
                continue;

            added++;

            /* We can reply right away, so that we don't need to store the value in the dict. */
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);

            addReplyBulkCBuffer(c, field, sdslen(field));
            if (withvalues)
                addReplyBulkCBuffer(c, value, sdslen(value));
        }

        /* Release memory */
        dictRelease(dictUnique);
    }
out:
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), hash, oldsize, kvobjAllocSize(hash));
}

/*
 * HRANDFIELD - Return a random field from the hash value stored at key.
 * CLI usage: HRANDFIELD key [<count> [WITHVALUES]]
 *
 * Considerations for the current imp of HRANDFIELD & HFE feature:
 *  HRANDFIELD might access any of the fields in the hash as some of them might
 *  be expired. And so the Implementation of HRANDFIELD along with HFEs
 *  might be one of the two options:
 *  1. Expire hash-fields before diving into handling HRANDFIELD.
 *  2. Refine HRANDFIELD cases to deal with expired fields.
 *
 *  Regarding the first option, as reference, the command RANDOMKEY also declares
 *  on O(1) complexity, yet might be stuck on a very long (but not infinite) loop
 *  trying to find non-expired keys. Furthermore RANDOMKEY also evicts expired keys
 *  along the way even though it is categorized as a read-only command. Note that
 *  the case of HRANDFIELD is more lightweight versus RANDOMKEY since HFEs have
 *  much more effective and aggressive active-expiration for fields behind.
 *
 *  The second option introduces additional implementation complexity to HRANDFIELD.
 *  We could further refine HRANDFIELD cases to differentiate between scenarios
 *  with many expired fields versus few expired fields, and adjust based on the
 *  percentage of expired fields. However, this approach could still lead to long
 *  loops or necessitate expiring fields before selecting them. For the “lightweight”
 *  cases it is also expected to have a lightweight expiration.
 *
 *  Considering the pros and cons, and the fact that HRANDFIELD is an infrequent
 *  command (particularly with HFEs) and the fact we have effective active-expiration
 *  behind for hash-fields, it is better to keep it simple and choose the option #1.
 */
void hrandfieldCommand(client *c) {
    long l;
    int withvalues = 0;
    kvobj *hash;
    CommonEntry ele;
    size_t oldsize = 0;

    if (c->argc >= 3) {
        if (getRangeLongFromObjectOrReply(c,c->argv[2],-LONG_MAX,LONG_MAX,&l,NULL) != C_OK) return;
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withvalues"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4) {
            withvalues = 1;
            if (l < -LONG_MAX/2 || l > LONG_MAX/2) {
                addReplyError(c,"value is out of range");
                return;
            }
        }
        hrandfieldWithCountCommand(c, l, withvalues);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,hash,OBJ_HASH)) {
        return;
    }

    /* Delete all expired fields. If the entire hash got deleted then return null. */
    if (hashTypeExpireIfNeeded(c->db, hash)) {
        addReply(c,shared.null[c->resp]);
        return;
    }

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(hash);
    hashTypeRandomElement(hash,hashTypeLength(hash, 0),&ele,NULL);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), hash, oldsize, kvobjAllocSize(hash));

    if (ele.sval)
        addReplyBulkCBuffer(c, ele.sval, ele.slen);
    else
        addReplyBulkLongLong(c, ele.lval);
}

/*-----------------------------------------------------------------------------
 * Hash Field with optional expiry (based on entry)
 *----------------------------------------------------------------------------*/

static ExpireMeta* hentryGetExpireMeta(const eItem e) {
    /* extract the expireMeta from the field (entry) */
    return entryRefExpiryMeta((Entry *)e);
}

/* Remove TTL from the field. Assumed ExpireMeta is attached and has valid value */
static void hfieldPersist(robj *hashObj, Entry *entry) {
    uint64_t fieldExpireTime = entryGetExpiry(entry);
    if (fieldExpireTime == EB_EXPIRE_TIME_INVALID)
        return;

    /* if field is set with expire, then dict must has HFE metadata attached */
    dict *d = hashObj->ptr;
    htMetadataEx *dictExpireMeta = htGetMetadataEx(d);

    /* Remove field from private HFE DS */
    ebRemove(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, entry);

    /* Don't have to update global HFE DS. It's unnecessary. Implementing this
     * would introduce significant complexity and overhead for an operation that
     * isn't critical. In the worst case scenario, the hash will be efficiently
     * updated later by an active-expire operation, or it will be removed by the
     * hash's dbGenericDelete() function. */
}

/*-----------------------------------------------------------------------------
 * Hash Field Expiration (HFE)
 *----------------------------------------------------------------------------*/
/*  Can be called either by active-expire cron job or query from the client */
static void propagateHashFieldDeletion(redisDb *db, sds key, char *field, size_t fieldLen) {
    robj *argv[] = {
        shared.hdel,
        createStringObject((char*) key, sdslen(key)),
        createStringObject(field, fieldLen)
    };

    enterExecutionUnit(1, 0);
    int prev_replication_allowed = server.replication_allowed;
    server.replication_allowed = 1;
    alsoPropagate(db->id,argv, 3, PROPAGATE_AOF|PROPAGATE_REPL);
    server.replication_allowed = prev_replication_allowed;
    exitExecutionUnit();

    /* Propagate the HDEL command */
    postExecutionUnitOperations();

    decrRefCount(argv[1]);
    decrRefCount(argv[2]);
}

/* Called during active expiration of hash-fields. Propagate to replica & Delete. */
static ExpireAction onFieldExpire(eItem item, void *ctx) {
    OnFieldExpireCtx *expCtx = ctx;
    Entry *e = item;
    kvobj *kv = expCtx->hashObj;
    size_t oldsize = 0;
    sds key = kvobjGetKey(kv);

    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(kv);
    sds field = entryGetField(e);

    /* Collect expired field for subkey notification (before deletion) */
    if (expCtx->vexpired)
        vecPush(expCtx->vexpired, createStringObject(field, sdslen(field)));

    propagateHashFieldDeletion(expCtx->db, key, field, sdslen(field));

    /* update keysizes */
    unsigned long l = hashTypeLength(expCtx->hashObj, 0);
    updateKeysizesHist(expCtx->db, OBJ_HASH, l, l - 1);

    serverAssert(hashTypeDelete(expCtx->hashObj, field) == 1);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(expCtx->db, getKeySlot(key), kv, oldsize, kvobjAllocSize(kv));
    server.stat_expired_subkeys++;
    if (expCtx->activeEx)
        server.stat_expired_subkeys_active++;
    return ACT_REMOVE_EXP_ITEM;
}

/* Retrieve the ExpireMeta associated with the hash.
 * The caller is responsible for ensuring that it is indeed attached. */
ExpireMeta *hashGetExpireMeta(const eItem hash) {
    robj *hashObj = (robj *)hash;
    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = hashObj->ptr;
        return &lpt->meta;
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;
        htMetadataEx *dictExpireMeta = htGetMetadataEx(d);
        return &dictExpireMeta->expireMeta;
    } else {
        serverPanic("Unknown encoding: %d", hashObj->encoding);
    }
}

/* Generic structure to hold parsed arguments for all hash field commands */
typedef struct {
    /* FIELDS arguments */
    int fieldsPos;          /* Position of FIELDS keyword (-1 if not found) */
    int numFieldsPos;       /* Position of numfields argument */
    int firstFieldPos;      /* Position of first field */
    int fieldCount;         /* Number of fields */

    /* HEXPIRE family arguments */
    int expireTimePos;      /* Position of expire time argument */
    long long expireTime;   /* Parsed expire time */
    int expireCondition;    /* HFE_NX, HFE_XX, HFE_GT, HFE_LT */
} HashCommandArgs;

/* Parser for HEXPIRE family commands with flexible keyword ordering.
 * Returns C_OK on success, C_ERR on error (with reply sent). */
static int parseHashCommandArgs(client *c, HashCommandArgs *args,
                                long long basetime, int unit)
{
    memset(args, 0, sizeof(*args));
    args->fieldsPos = -1;
    args->expireTimePos = 2;

    if (parseExpireTime(c, c->argv[2], unit, basetime, &args->expireTime) != C_OK) {
        return C_ERR;
    }

    /* Parse remaining arguments starting from position 3 */
    for (int i = 3; i < c->argc; i++) {
        char *arg = c->argv[i]->ptr;

        /* FIELDS keyword - supported by ALL hash field commands */
        if (!strcasecmp(arg, "FIELDS")) {
            if (args->fieldsPos != -1) {
                addReplyError(c, "FIELDS keyword specified multiple times");
                return C_ERR;
            }

            if (i >= c->argc - 2) {
                addReplyError(c, "FIELDS requires at least numfields and one field argument");
                return C_ERR;
            }

            args->fieldsPos = i;
            args->numFieldsPos = i + 1;
            long numFields;
            if (getRangeLongFromObjectOrReply(c, c->argv[args->numFieldsPos], 1, INT_MAX,
                                              &numFields, "Parameter `numFields` should be greater than 0") != C_OK)
                return C_ERR;

            args->firstFieldPos = i + 2;

            /* Check bounds - we must have exactly the right number of fields */
            if (numFields > c->argc - args->firstFieldPos) {
                addReplyError(c, "wrong number of arguments");
                return C_ERR;
            }

            args->fieldCount = (int)numFields;

            /* Skip over the field arguments */
            i = args->firstFieldPos + args->fieldCount - 1;
            continue;
        }

        /* Expiration condition keywords - validation moved outside loop for performance */
        if (!strcasecmp(arg, "NX")) {
            args->expireCondition |= HFE_NX;
            continue;
        } else if (!strcasecmp(arg, "XX")) {
            args->expireCondition |= HFE_XX;
            continue;
        } else if (!strcasecmp(arg, "GT")) {
            args->expireCondition |= HFE_GT;
            continue;
        } else if (!strcasecmp(arg, "LT")) {
            args->expireCondition |= HFE_LT;
            continue;
        }

        addReplyErrorFormat(c, "unknown argument: %s", (char*) c->argv[i]->ptr);
        return C_ERR;
    }

    /* Ensure FIELDS is specified */
    if (args->fieldsPos == -1) {
        addReplyError(c, "missing FIELDS argument");
        return C_ERR;
    }

    if (__builtin_popcount(args->expireCondition & (HFE_NX|HFE_XX|HFE_GT|HFE_LT)) > 1) {
        addReplyError(c, "Multiple condition flags specified");
        return C_ERR;
    }

    return C_OK;
}

/* HTTL key <FIELDS count field [field ...]>  */
static void httlGenericCommand(client *c, const char *cmd, long long basetime, int unit){
    UNUSED(cmd);
    kvobj *hashObj;
    long numFields = 0, numFieldsAt = 3;

    /* Read the hash object */
    hashObj = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c, hashObj, OBJ_HASH))
        return;

    if (strcasecmp(c->argv[numFieldsAt-1]->ptr, "FIELDS")) {
        addReplyError(c, "Mandatory argument FIELDS is missing or not at the right position");
        return;
    }

    /* Read number of fields */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Number of fields must be a positive integer") != C_OK)
        return;

    /* Verify `numFields` is consistent with number of arguments */
    if (numFields != (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }

    /* Non-existing keys and empty hashes are the same thing. It also means
     * fields in the command don't exist in the hash key. */
    if (!hashObj) {
        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            addReplyLongLong(c, HFE_GET_NO_FIELD);
        }
        return;
    }

    /* Template encodings don't support HFE. Report field presence
     * with no TTL, or missing field. */
    if (hashObj->encoding == OBJ_ENCODING_TMPL_LP ||
        hashObj->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplate *tmpl = hashTypeGetTemplate(hashObj);

        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            if (hashTemplateFieldIndex(tmpl, field) >= 0)
                addReplyLongLong(c, HFE_GET_NO_TTL);
            else
                addReplyLongLong(c, HFE_GET_NO_FIELD);
        }
        return;
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        void *lp = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            void *fptr = lpFirst(lp);
            if (fptr != NULL)
                fptr = lpFind(lp, fptr, (unsigned char *) field, sdslen(field), 1);

            if (!fptr)
                addReplyLongLong(c, HFE_GET_NO_FIELD);
            else
                addReplyLongLong(c, HFE_GET_NO_TTL);
        }
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            long long expire;
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            void *fptr = lpFirst(lpt->lp);
            if (fptr != NULL)
                fptr = lpFind(lpt->lp, fptr, (unsigned char *) field, sdslen(field), 2);

            if (!fptr) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            fptr = lpNext(lpt->lp, fptr);
            serverAssert(fptr);
            fptr = lpNext(lpt->lp, fptr);
            serverAssert(fptr && lpGetIntegerValue(fptr, &expire));

            if (expire == HASH_LP_NO_TTL) {
                addReplyLongLong(c, HFE_GET_NO_TTL);
                continue;
            }

            if (expire <= commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            if (unit == UNIT_SECONDS)
                addReplyLongLong(c, (expire + 999 - basetime) / 1000);
            else
                addReplyLongLong(c, (expire - basetime));
        }
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;
        size_t oldsize = 0;
        if (server.memory_tracking_enabled)
            oldsize = kvobjAllocSize(hashObj);

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            dictEntry *de = dictFind(d, field);
            if (de == NULL) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            Entry *entry = dictGetKey(de);
            uint64_t expire = entryGetExpiry(entry);
            if (expire == EB_EXPIRE_TIME_INVALID) {
                addReplyLongLong(c, HFE_GET_NO_TTL); /* no ttl */
                continue;
            }

            if ( (long long) expire < commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            if (unit == UNIT_SECONDS)
                addReplyLongLong(c, (expire + 999 - basetime) / 1000);
            else
                addReplyLongLong(c, (expire - basetime));
        }
        if (server.memory_tracking_enabled)
            updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), hashObj, oldsize, kvobjAllocSize(hashObj));
        return;
    } else {
        serverPanic("Unknown encoding: %d", hashObj->encoding);
    }
}

/* This is the generic command implementation for HEXPIRE, HPEXPIRE, HEXPIREAT
 * and HPEXPIREAT. Because the command second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliseconds.
 *
 * PROPAGATE TO REPLICA:
 *   The command will be translated into HPEXPIREAT and the expiration time will be
 *   converted to absolute time in milliseconds.
 *
 *   As we need to propagate H(P)EXPIRE(AT) command to the replica, each field that
 *   is mentioned in the command should be categorized into one of the four options:
 *   1. Field’s expiration time updated successfully - Propagate it to replica as
 *      part of the HPEXPIREAT command.
 *   2. The field got deleted since the time is in the past - propagate also HDEL
 *      command to delete the field. Also remove the field from the propagated
 *      HPEXPIREAT command.
 *   3. Condition not met for the field - Remove the field from the propagated
 *      HPEXPIREAT command.
 *   4. Field doesn't exists - Remove the field from propagated HPEXPIREAT command.
 *
 *   If none of the provided fields match option #1, that is provided time of the
 *   command is in the past, then avoid propagating the HPEXPIREAT command to the
 *   replica.
 *
 *   This approach is aligned with existing EXPIRE command. If a given key has already
 *   expired, then DEL will be propagated instead of EXPIRE command. If condition
 *   not met, then command will be rejected. Otherwise, EXPIRE command will be
 *   propagated for given key.
 */
static void hexpireGenericCommand(client *c, long long basetime, int unit) {
    HashCommandArgs args;
    int fieldsNotSet = 0;
    int64_t oldlen, newlen;
    robj *keyArg = c->argv[1];
    size_t oldsize = 0;

    /* Read the hash object */
    kvobj *hashObj = lookupKeyWrite(c->db, keyArg);
    if (checkType(c, hashObj, OBJ_HASH))
        return;

    /* Parse arguments using flexible keyword-based parsing */
    if (parseHashCommandArgs(c, &args, basetime, unit) != C_OK)
        return;

    /* Non-existing keys and empty hashes are the same thing. It also means
     * fields in the command don't exist in the hash key. */
    if (!hashObj) {
        addReplyArrayLen(c, args.fieldCount);
        for (int i = 0; i < args.fieldCount; i++) {
            addReplyLongLong(c, HSETEX_NO_FIELD);
        }
        return;
    }

    oldlen = hashTypeLength(hashObj, 0);
    if (server.memory_tracking_enabled)
        oldsize = kvobjAllocSize(hashObj);

    HashTypeSetEx exCtx;
    hashTypeSetExInit(keyArg, hashObj, c, c->db, args.expireCondition, &exCtx);
    addReplyArrayLen(c, args.fieldCount);

    /* Lazy allocation of fieldsToRemove - only allocate when failures occur */
    int *fieldsToRemove = NULL;
    int removeCount = 0;

    /* Track fields for subkey notifications. */
    fieldvec fvupdated, fvdeleted;
    vec *vupdated = fieldvecInit(&fvupdated, args.fieldCount);
    vec *vdeleted = fieldvecInit(&fvdeleted, args.fieldCount);

    for (int i = 0; i < args.fieldCount; i++) {
        int fieldPos = args.firstFieldPos + i;
        sds field = c->argv[fieldPos]->ptr;
        SetExRes res = hashTypeSetEx(hashObj, field, args.expireTime, &exCtx);
        if (res == HSETEX_OK) {
            vecPush(vupdated, c->argv[fieldPos]);
        } else if (res == HSETEX_DELETED) {
            vecPush(vdeleted, c->argv[fieldPos]);
        }

        if (unlikely(res != HSETEX_OK)) {
            if (fieldsToRemove == NULL) {
                fieldsToRemove = zmalloc(sizeof(int) * (args.fieldCount > 0 ? args.fieldCount : 1));
            }
            /* Remember this field position for later removal from propagation */
            fieldsToRemove[removeCount++] = fieldPos;
            fieldsNotSet = 1;
        }

        addReplyLongLong(c, res);
    }

    hashTypeSetExDone(&exCtx);
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(c->db, getKeySlot(keyArg->ptr), hashObj, oldsize, kvobjAllocSize(hashObj));

    if (vecSize(vdeleted) + vecSize(vupdated) > 0) {
        server.dirty += vecSize(vdeleted) + vecSize(vupdated);
        keyModified(c, c->db, keyArg, hashObj, 1);
        if (vecSize(vdeleted)) notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hdel",
                                keyArg, c->db->id, (robj**)vecData(vdeleted), vecSize(vdeleted));
        if (vecSize(vupdated)) notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hexpire",
                                keyArg, c->db->id, (robj**)vecData(vupdated), vecSize(vupdated));
    }

    newlen = (int64_t) hashTypeLength(hashObj, 0);
    if (newlen == 0) {
        newlen = -1;
        /* Del key but don't update KEYSIZES. Else it will decr wrong bin in histogram */
        dbDeleteSkipKeysizesUpdate(c->db, keyArg);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", keyArg, c->db->id);
    }

    if (oldlen != newlen)
        updateKeysizesHist(c->db, OBJ_HASH, oldlen, newlen);

    /* Avoid propagating command if not even one field was updated (Either because
     * the time is in the past, and corresponding HDELs were sent, or conditions
     * not met) then it is useless and invalid to propagate command with no fields */
    if (vecSize(vupdated) == 0) {
        vecRelease(vupdated);
        vecRelease(vdeleted);
        preventCommandPropagation(c);
        zfree(fieldsToRemove);
        return;
    }

    /* Handle propagation using command rewriting
     * Rewrite to canonical HPEXPIREAT command */
    if (c->cmd->proc != hpexpireatCommand) {
        rewriteClientCommandArgument(c, 0, shared.hpexpireat);

        robj *expireTimeObj = createStringObjectFromLongLong(args.expireTime);
        rewriteClientCommandArgument(c, args.expireTimePos, expireTimeObj);
        decrRefCount(expireTimeObj);
    }

    /* For partial failures, remove failed fields from the original command */
    if (fieldsNotSet) {
        for (int i = removeCount - 1; i >= 0; i--) {
            rewriteClientCommandArgument(c, fieldsToRemove[i], NULL);
        }
        robj *newFieldCount = createStringObjectFromLongLong(vecSize(vupdated));
        rewriteClientCommandArgument(c, args.fieldsPos + 1, newFieldCount);
        decrRefCount(newFieldCount);
    }

    if (fieldsToRemove)
        zfree(fieldsToRemove);

    vecRelease(vupdated);
    vecRelease(vdeleted);
}

/* HPEXPIRE key milliseconds [ NX | XX | GT | LT] FIELDS numfields <field [field ...]> */
void hpexpireCommand(client *c) {
    hexpireGenericCommand(c,commandTimeSnapshot(),UNIT_MILLISECONDS);
}

/* HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields <field [field ...]> */
void hexpireCommand(client *c) {
    hexpireGenericCommand(c,commandTimeSnapshot(),UNIT_SECONDS);
}

/* HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields <field [field ...]> */
void hexpireatCommand(client *c) {
    hexpireGenericCommand(c,0,UNIT_SECONDS);
}

/* HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] FIELDS numfields <field [field ...]> */
void hpexpireatCommand(client *c) {
    hexpireGenericCommand(c,0,UNIT_MILLISECONDS);
}

/* for each specified field: get the remaining time to live in seconds*/
/* HTTL key FIELDS numfields <field [field ...]> */
void httlCommand(client *c) {
    httlGenericCommand(c, "httl", commandTimeSnapshot(), UNIT_SECONDS);
}

/* HPTTL key FIELDS numfields <field [field ...]> */
void hpttlCommand(client *c) {
    httlGenericCommand(c, "hpttl", commandTimeSnapshot(), UNIT_MILLISECONDS);
}

/* HEXPIRETIME key FIELDS numfields <field [field ...]> */
void hexpiretimeCommand(client *c) {
    httlGenericCommand(c, "hexpiretime", 0, UNIT_SECONDS);
}

/* HPEXPIRETIME key FIELDS numfields <field [field ...]> */
void hpexpiretimeCommand(client *c) {
    httlGenericCommand(c, "hexpiretime", 0, UNIT_MILLISECONDS);
}

/* HPERSIST key FIELDS numfields <field [field ...]> */
void hpersistCommand(client *c) {
    long numFields = 0, numFieldsAt = 3;

    /* Read the hash object */
    kvobj *hashObj = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, hashObj, OBJ_HASH))
        return;

    if (strcasecmp(c->argv[numFieldsAt-1]->ptr, "FIELDS")) {
        addReplyError(c, "Mandatory argument FIELDS is missing or not at the right position");
        return;
    }

    /* Read number of fields */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Number of fields must be a positive integer") != C_OK)
        return;

    /* Verify `numFields` is consistent with number of arguments */
    if (numFields != (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }

    /* Non-existing keys and empty hashes are the same thing. It also means
     * fields in the command don't exist in the hash key. */
    if (!hashObj) {
        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
        }
        return;
    }

    /* Template encodings don't support HFE. */
    if (hashObj->encoding == OBJ_ENCODING_TMPL_LP ||
        hashObj->encoding == OBJ_ENCODING_TMPL_ARRAY) {
        hashTemplate *tmpl = (hashObj->encoding == OBJ_ENCODING_TMPL_LP) ?
            hashTemplateLpGetTemplate(hashObj->ptr) :
            ((hashTemplateArray *)hashObj->ptr)->tmpl;

        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            if (hashTemplateFieldIndex(tmpl, field) >= 0)
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
            else
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
        }
        return;
    }

    /* Track which fields were successfully persisted for subkey notification. */
    fieldvec fvpersisted;
    vec *vpersisted = fieldvecInit(&fvpersisted, numFields);

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt + 1 + i]->ptr;
            unsigned char *fptr, *zl = hashObj->ptr;

            fptr = lpFirst(zl);
            if (fptr != NULL)
                fptr = lpFind(zl, fptr, (unsigned char *) field, sdslen(field), 1);

            if (!fptr)
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
            else
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
        }
        vecRelease(vpersisted);
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        long long prevExpire;
        unsigned char *fptr, *vptr, *tptr;
        listpackEx *lpt = hashObj->ptr;
        size_t oldsize = 0;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt + 1 + i]->ptr;

            fptr = lpFirst(lpt->lp);
            if (fptr != NULL)
                fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);

            if (!fptr) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            vptr = lpNext(lpt->lp, fptr);
            serverAssert(vptr);
            tptr = lpNext(lpt->lp, vptr);
            serverAssert(tptr && lpGetIntegerValue(tptr, &prevExpire));

            if (prevExpire == HASH_LP_NO_TTL) {
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
                continue;
            }

            if (prevExpire < commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            if (server.memory_tracking_enabled)
                oldsize = kvobjAllocSize(hashObj);
            listpackExUpdateExpiry(hashObj, field, fptr, vptr, HASH_LP_NO_TTL);
            if (server.memory_tracking_enabled)
                updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), hashObj, oldsize, kvobjAllocSize(hashObj));
            addReplyLongLong(c, HFE_PERSIST_OK);
            vecPush(vpersisted, c->argv[numFieldsAt + 1 + i]);
        }
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;
        size_t oldsize = 0;
        if (server.memory_tracking_enabled)
            oldsize = kvobjAllocSize(hashObj);

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt + 1 + i]->ptr;
            dictEntry *de = dictFind(d, field);
            if (de == NULL) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            Entry *entry = dictGetKey(de);
            uint64_t expire = entryGetExpiry(entry);
            if (expire == EB_EXPIRE_TIME_INVALID) {
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
                continue;
            }

            /* Already expired. Pretend there is no such field */
            if ( (long long) expire < commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            hfieldPersist(hashObj, entry);
            addReplyLongLong(c, HFE_PERSIST_OK);
            vecPush(vpersisted, c->argv[numFieldsAt + 1 + i]);
        }
        if (server.memory_tracking_enabled)
            updateSlotAllocSize(c->db, getKeySlot(c->argv[1]->ptr), hashObj, oldsize, kvobjAllocSize(hashObj));
    } else {
        serverPanic("Unknown encoding: %d", hashObj->encoding);
    }

    /* Generates a hpersist event if the expiry time associated with any field
     * has been successfully deleted. */
    if (vecSize(vpersisted)) {
        notifyKeyspaceEventWithSubkeys(NOTIFY_HASH, "hpersist", c->argv[1],
                                       c->db->id, (robj**)vecData(vpersisted), vecSize(vpersisted));
        keyModified(c, c->db, c->argv[1], hashObj, 1);
        server.dirty++;
    }
    vecRelease(vpersisted);
}
