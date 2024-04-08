/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "ebuckets.h"
#include <math.h>

/* hash field expiration (HFE) funcs */
static ExpireAction onFieldExpire(eItem item, void *ctx);
static ExpireMeta* hfieldGetExpireMeta(const eItem field);
static ExpireMeta *hashGetExpireMeta(const eItem item);
static void hexpireGenericCommand(client *c, const char *cmd, long long basetime, int unit);
static ExpireAction hashTypeActiveExpire(eItem hashObj, void *ctx);
static void hfieldPersist(redisDb *db, robj *hashObj, hfield field);
static uint64_t hfieldGetExpireTime(hfield field);

/* hash dictType funcs */
static int dictHfieldKeyCompare(dict *d, const void *key1, const void *key2);
static uint64_t dictMstrHash(const void *key);
static void dictHfieldDestructor(dict *d, void *val);
static size_t hashDictWithExpireMetadataBytes(dict *d);
static void hashDictWithExpireOnRelease(dict *d);
static robj* hashTypeLookupWriteOrCreate(client *c, robj *key);

/*-----------------------------------------------------------------------------
 * Define dictType of hash
 *
 * - Stores fields as mstr strings with optional metadata to attach TTL
 * - Note that small hashes are represented with listpacks
 * - Once expiration is set for a field, the dict instance and corresponding
 *   dictType are replaced with a dict containing metadata for Hash Field
 *   Expiration (HFE) and using dictType `mstrHashDictTypeWithHFE`
 *----------------------------------------------------------------------------*/
dictType mstrHashDictType = {
    dictSdsHash,                                /* lookup hash function */
    NULL,                                       /* key dup */
    NULL,                                       /* val dup */
    dictSdsMstrKeyCompare,                      /* lookup key compare */
    dictHfieldDestructor,                       /* key destructor */
    dictSdsDestructor,                          /* val destructor */
    .storedHashFunction = dictMstrHash,         /* stored hash function */
    .storedKeyCompare = dictHfieldKeyCompare,   /* stored key compare */
};

/* Define alternative dictType of hash with hash-field expiration (HFE) support */
dictType mstrHashDictTypeWithHFE = {
    dictSdsHash,                                /* lookup hash function */
    NULL,                                       /* key dup */
    NULL,                                       /* val dup */
    dictSdsMstrKeyCompare,                      /* lookup key compare */
    dictHfieldDestructor,                       /* key destructor */
    dictSdsDestructor,                          /* val destructor */
    .storedHashFunction = dictMstrHash,         /* stored hash function */
    .storedKeyCompare = dictHfieldKeyCompare,   /* stored key compare */
    .dictMetadataBytes = hashDictWithExpireMetadataBytes,
    .onDictRelease = hashDictWithExpireOnRelease,
};

/*-----------------------------------------------------------------------------
 * Hash Field Expiration (HFE) Feature
 *
 * Each hash instance maintains its own set of hash field expiration within its
 * private ebuckets DS. In order to support HFE active expire cycle across hash
 * instances, hashes with associated HFE will be also registered in a global
 * ebuckets DS with expiration time value that reflects their next minimum
 * time to expire. The global HFE Active expiration will be triggered from
 * activeExpireCycle() function and will invoke "local" HFE Active expiration
 * for each hash instance that has expired fields.
 *
 * hashExpireBucketsType - ebuckets-type to be used at the global space
 * (db->hexpires) to register hashes that have one or more fields with time-Expiration.
 * The hashes will be registered in with the expiration time of the earliest field
 * in the hash.
 *----------------------------------------------------------------------------*/
EbucketsType hashExpireBucketsType = {
    .onDeleteItem = NULL,
    .getExpireMeta = hashGetExpireMeta,   /* get ExpireMeta attached to each hash */
    .itemsAddrAreOdd = 0,                 /* Addresses of dict are even */
};

/* dictExpireMetadata - ebuckets-type for hash fields with time-Expiration. ebuckets
 * instance Will be attached to each hash that has at least one field with expiry
 * time. */
EbucketsType hashFieldExpireBucketsType = {
    .onDeleteItem = NULL,
    .getExpireMeta = hfieldGetExpireMeta, /* get ExpireMeta attached to each field */
    .itemsAddrAreOdd = 1,                 /* Addresses of hfield (mstr) are odd!! */
};

/* Each dict of hash object that has fields with time-Expiration will have the
 * following metadata attached to dict header */
typedef struct dictExpireMetadata {
    ExpireMeta expireMeta;   /* embedded ExpireMeta in dict.
                                To be used in order to register the hash in the
                                global ebuckets (i.e db->hexpires) with next,
                                minimum, hash-field to expire */
    ebuckets hfe;            /* DS of Hash Fields Expiration, associated to each hash */
    sds key;                 /* reference to the key, same one that stored in
                               db->dict. Will be used from active-expiration flow
                               for notification and deletion of the object, if
                               needed. */
} dictExpireMetadata;

/* ActiveExpireCtx passed to hashTypeActiveExpire() */
typedef struct ActiveExpireCtx {
    uint32_t fieldsToExpireQuota;
    redisDb *db;
} ActiveExpireCtx;

/* The implementation of hashes by dict was modified from storing fields as sds
 * strings to store "mstr" (Immutable string with metadata) in order to be able to
 * attach TTL (ExpireMeta) to the hash-field. This usage of mstr opens up the
 * opportunity for future features to attach additional metadata by need to the
 * fields.
 *
 * The following defines new hfield kind of mstr */
typedef enum HfieldMetaFlags {
    HFIELD_META_EXPIRE = 0,
} HfieldMetaFlags;

mstrKind mstrFieldKind = {
    .name = "hField",

    /* Taking care that all metaSize[*] values are even ensures that all
     * addresses of hfield instances will be odd. */
    .metaSize[HFIELD_META_EXPIRE] = sizeof(ExpireMeta),
};
static_assert(sizeof(struct ExpireMeta ) % 2 == 0, "must be even!");

/* Used by hpersistCommand() */
typedef enum SetPersistRes {
    HFE_PERSIST_NO_FIELD =     -2,   /* No such hash-field */
    HFE_PERSIST_NO_TTL =       -1,   /* No TTL attached to the field */
    HFE_PERSIST_OK =            1
} SetPersistRes;

/* Used by hashTypeSetExpire() */
typedef enum SetExpireTimeRes {
    HFE_SET_NO_FIELD =         -2,   /* No such hash-field */
    HFE_SET_NO_CONDITION_MET =  0,   /* Specified NX | XX | GT | LT condition not met */
    HFE_SET_OK =                1,   /* Expiration time set/updated as expected */
    HFE_SET_DELETED =           2    /* Field deleted because the specified time is in the past */
} SetExpireTimeRes;

/* Used by httlGenericCommand() */
typedef enum GetExpireTimeRes {
    HFE_GET_NO_FIELD =          -2,   /* No such hash-field */
    HFE_GET_NO_TTL =            -1,   /* No TTL attached to the field */
} GetExpireTimeRes;

#define HFE_NX (1<<0)
#define HFE_XX (1<<1)
#define HFE_GT (1<<2)
#define HFE_LT (1<<3)

static inline int isDictWithMetaHFE(dict *d) {
    return d->type == &mstrHashDictTypeWithHFE;
}

/*-----------------------------------------------------------------------------
 * Accessor functions for dictType of hash
 *----------------------------------------------------------------------------*/

static int dictHfieldKeyCompare(dict *d, const void *key1, const void *key2)
{
    int l1,l2;
    UNUSED(d);

    l1 = hfieldlen((hfield)key1);
    l2 = hfieldlen((hfield)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static uint64_t dictMstrHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, mstrlen((char*)key));
}

static void dictHfieldDestructor(dict *d, void *val) {
    /* If attached TTL to the field, then remove it from hash's private ebuckets */
    if (hfieldGetExpireTime(val) != EB_EXPIRE_TIME_INVALID) {
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
        ebRemove(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, val);
    }

    hfieldFree(val);
}

static size_t hashDictWithExpireMetadataBytes(dict *d) {
    UNUSED(d);
    /* expireMeta of the hash, ref to ebuckets and pointer to hash's key */
    return sizeof(dictExpireMetadata);
}

static void hashDictWithExpireOnRelease(dict *d) {
    /* for sure allocated with metadata. Otherwise, this func won't be registered */
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
    ebDestroy(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, NULL);
}

#define HASH_LP_NO_TTL 0

typedef struct listpackTTL {
    ExpireMeta meta;
    void *lp;
    sds key;
} listpackTTL;

int hashLpIsExpired(uint64_t ttl) {
    return ttl != HASH_LP_NO_TTL && (mstime_t) ttl < commandTimeSnapshot();
}

unsigned char *hashLpGetListpack(robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK)
        return o->ptr;
    else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL)
        return ((listpackTTL*)o->ptr)->lp;

    serverAssert(0);
}

static uint64_t hashLpExpireDryRun(const robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_TTL);

    uint64_t expired = 0;
    unsigned char *fptr;
    unsigned char *lp = ((listpackTTL*) o->ptr)->lp;

    fptr = lpFirst(lp);
    while (fptr != NULL) {
        long long val;

        fptr = lpNext(lp, fptr);
        fptr = lpNext(lp, fptr);
        lpGetValue(fptr, NULL, &val);

        if (hashLpIsExpired(val))
            expired++;

        fptr = lpNext(lp, fptr);
    }

    return expired;
}

static void hashLpExpire(robj *o, ExpireInfo *info) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_TTL);

    long long min = LLONG_MAX;
    unsigned char *fptr, *head;
    listpackTTL *lpt = o->ptr;

    fptr = lpFirst(lpt->lp);
    while (fptr != NULL) {
        long long val;

        head = fptr;
        fptr = lpNext(lpt->lp, fptr);
        fptr = lpNext(lpt->lp, fptr);

        lpGetValue(fptr, NULL, &val);

        if (hashLpIsExpired(val)) {
            server.stat_expired_hash_fields++;
            lpt->lp = lpDeleteRangeWithEntry(lpt->lp, &head, 3);
            fptr = head;
            info->itemsExpired++;
        } else {
            if (val != HASH_LP_NO_TTL)
                min = min < val ? min : val;

            fptr = lpNext(lpt->lp, fptr);
        }
    }

    info->nextExpireTime = (min == LLONG_MAX) ? 0 : min;
}

static int hashlpSetExpire(robj *o, sds field, long long expireAt, int flag) {
    long long prevExpire;
    unsigned char *fptr;
    listpackTTL *lpt = o->ptr;

    fptr = lpFirst(lpt->lp);
    if (fptr != NULL)
        fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);

    if (!fptr)
        return HFE_SET_NO_FIELD;

    fptr = lpNext(lpt->lp, fptr);
    fptr = lpNext(lpt->lp, fptr);
    lpGetValue(fptr, NULL, &prevExpire);

    if (prevExpire == HASH_LP_NO_TTL) {
        if (flag & (HFE_XX | HFE_LT | HFE_GT))
            return HFE_SET_NO_CONDITION_MET;
    } else {
        if (((flag == HFE_GT) && (prevExpire >= expireAt)) ||
            ((flag == HFE_LT) && (prevExpire <= expireAt)) ||
            (flag == HFE_NX) )
            return HFE_SET_NO_CONDITION_MET;
    }

    /* if expiration time is in the past */
    if (checkAlreadyExpired(expireAt)) {
        hashTypeDelete(o, field);
        return HFE_SET_DELETED;
    }

    /* TODO: Consider storing fields ordered by TTL. */
    lpt->lp = lpReplaceInteger(lpt->lp, &fptr, expireAt);
    return HFE_SET_OK;
}

static uint64_t hashLpGetMinExpire(robj *o) {
    long long minExpire = EB_EXPIRE_TIME_INVALID;
    unsigned char *fptr;
    listpackTTL *lpt = o->ptr;

    fptr = lpFirst(lpt->lp);
    while (fptr != NULL) {
        long long val;

        fptr = lpNext(lpt->lp, fptr);
        fptr = lpNext(lpt->lp, fptr);

        lpGetValue(fptr, NULL, &val);
        if (val != HASH_LP_NO_TTL && val < minExpire)
            minExpire = val;

        fptr = lpNext(lpt->lp, fptr);
    }

    return (uint64_t) minExpire;
}

static int hashLpPersist(robj *o, redisDb *db, sds field, long long now) {
    unsigned char *fptr;
    listpackTTL *lpt = o->ptr;

    fptr = lpFirst(lpt->lp);
    if (fptr != NULL)
        fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);

    if (!fptr)
        return HFE_PERSIST_NO_FIELD;

    long long prevExpire;

    fptr = lpNext(lpt->lp, fptr);
    fptr = lpNext(lpt->lp, fptr);
    lpGetValue(fptr, NULL, &prevExpire);

    if (prevExpire == HASH_LP_NO_TTL)
        return HFE_PERSIST_NO_TTL;

    if (prevExpire < now)
        return HFE_PERSIST_NO_FIELD;

    lpt->lp = lpReplaceInteger(lpt->lp, &fptr, HASH_LP_NO_TTL);
    ebRemove(&db->hexpires, &hashExpireBucketsType, o);

    uint64_t minExpire = hashLpGetMinExpire(o);
    if (minExpire != EB_EXPIRE_TIME_INVALID) {
        ebAdd(&db->hexpires, &hashExpireBucketsType, o, minExpire);
    }

    return HFE_SET_OK;
}

void freeHash(robj *o) {
    switch (o->encoding) {
        case OBJ_ENCODING_HT:
            dictRelease((dict*) o->ptr);
            break;
        case OBJ_ENCODING_LISTPACK:
            lpFree(o->ptr);
            break;
        case OBJ_ENCODING_LISTPACK_TTL:
            lpFree(((listpackTTL*)o->ptr)->lp);
            zfree(o->ptr);
            break;
        default:
            serverPanic("Unknown hash encoding type");
            break;
    }
}

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * listpack to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;
    size_t sum = 0;

    if (o->encoding != OBJ_ENCODING_LISTPACK &&
        o->encoding != OBJ_ENCODING_LISTPACK_TTL)
        return;

    /* We guess that most of the values in the input are unique, so
     * if there are enough arguments we create a pre-sized hash, which
     * might over allocate memory if there are duplicates. */
    size_t new_fields = (end - start + 1) / 2;
    if (new_fields > server.hash_max_listpack_entries) {
        hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
        dictExpand(o->ptr, new_fields);
        return;
    }

    for (i = start; i <= end; i++) {
        if (!sdsEncodedObject(argv[i]))
            continue;
        size_t len = sdslen(argv[i]->ptr);
        if (len > server.hash_max_listpack_value) {
            hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
            return;
        }
        sum += len;
    }
    if (!lpSafeToAdd(hashLpGetListpack(o), sum))
        hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
}

/* Get the value from a listpack encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromListpack(robj *o, sds field,
                            unsigned char **vstr,
                            unsigned int *vlen,
                            long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;

    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK ||
                 o->encoding == OBJ_ENCODING_LISTPACK_TTL);

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
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        zl = ((listpackTTL*)o->ptr)->lp;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                vptr = lpNext(zl, fptr);
                serverAssert(vptr != NULL);

                long long expire;
                unsigned char *h;

                h = lpNext(zl, vptr);
                h = lpGetValue(h, NULL, &expire);
                serverAssert(h == NULL);

                if (hashLpIsExpired(expire))
                    return -1;
            }
        }
    }

    if (vptr != NULL) {
        *vstr = lpGetValue(vptr, vlen, vll);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);

    if (de == NULL) return NULL;

    /* Check if the field is expired */
    if (hfieldIsExpired(dictGetKey(de))) return NULL;

    return dictGetVal(de);
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. */
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (o->encoding == OBJ_ENCODING_LISTPACK ||
        o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        *vstr = NULL;
        if (hashTypeGetFromListpack(o, field, vstr, vlen, vll) == 0)
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
            *vstr = (unsigned char*) value;
            *vlen = sdslen(value);
            return C_OK;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_ERR;
}

/* Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned. */
robj *hashTypeGetValueObject(robj *o, sds field) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (hashTypeGetValue(o,field,&vstr,&vlen,&vll) == C_ERR) return NULL;
    if (vstr) return createStringObject((char*)vstr,vlen);
    else return createStringObjectFromLongLong(vll);
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
size_t hashTypeGetValueLength(robj *o, sds field) {
    size_t len = 0;
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if (hashTypeGetValue(o, field, &vstr, &vlen, &vll) == C_OK)
        len = vstr ? vlen : sdigits10(vll);

    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
int hashTypeExists(robj *o, sds field) {
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    return hashTypeGetValue(o, field, &vstr, &vlen, &vll) == C_OK;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int hashTypeSet(redisDb *db, robj *o, sds field, sds value, int flags) {
    int update = 0;

    /* Check if the field is too long for listpack, and convert before adding the item.
     * This is needed for HINCRBY* case since in other commands this is handled early by
     * hashTypeTryConversion, so this check will be a NOP. */
    if (o->encoding == OBJ_ENCODING_LISTPACK ||
        o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        if (sdslen(field) > server.hash_max_listpack_value || sdslen(value) > server.hash_max_listpack_value)
            hashTypeConvert(o, OBJ_ENCODING_HT, db);
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
                update = 1;

                /* Replace value */
                zl = lpReplace(zl, &vptr, (unsigned char*)value, sdslen(value));
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the listpack */
            zl = lpAppend(zl, (unsigned char*)field, sdslen(field));
            zl = lpAppend(zl, (unsigned char*)value, sdslen(value));
        }
        o->ptr = zl;

        /* Check if the listpack needs to be converted to a hash table */
        if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT, db);

    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL){
        unsigned char *fptr, *vptr;
        listpackTTL *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = lpNext(lpt->lp, fptr);
                serverAssert(vptr != NULL);
                update = 1;

                /* Replace value */
                lpt->lp = lpReplace(lpt->lp, &vptr, (unsigned char*)value, sdslen(value));

                /* Clear TTL */
                vptr = lpNext(lpt->lp, vptr);
                serverAssert(vptr != NULL);
                lpt->lp = lpReplaceInteger(lpt->lp, &vptr, HASH_LP_NO_TTL);
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the listpack */
            lpt->lp = lpAppend(lpt->lp, (unsigned char*)field, sdslen(field));
            lpt->lp = lpAppend(lpt->lp, (unsigned char*)value, sdslen(value));
            lpt->lp = lpAppendInteger(lpt->lp, HASH_LP_NO_TTL);
        }

        /* Check if the listpack needs to be converted to a hash table */
        if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT, db);
    }  else if (o->encoding == OBJ_ENCODING_HT) {
        dict *ht = o->ptr;
        dictEntry *de, *existingEntry;
        sds storedValue;
        if (flags & HASH_SET_TAKE_VALUE) {
            storedValue = value;
            value = NULL;
        } else {
            storedValue = sdsdup(value);
        }
        /* Cannot leverage HASH_SET_TAKE_FIELD since hfield is not of type sds */
        hfield newField = hfieldNew(field, sdslen(field), 0);

        /* stored key is different than lookup key */
        dictUseStoredKeyApi(ht, 1);
        de = dictAddRaw(ht, newField, &existingEntry);
        dictUseStoredKeyApi(ht, 0);

        if (de) {
            dictSetVal(ht, de, storedValue);
        } else {
            /* If attached TTL to the old field, then remove it from hash's private ebuckets */
            hfield oldField = dictGetKey(existingEntry);
            hfieldPersist(db, o, oldField);
            sdsfree(dictGetVal(existingEntry));
            dictSetVal(ht, existingEntry, storedValue);
            update = 1;
            hfieldFree(newField);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Delete both of the key and the value. */
                zl = lpDeleteRangeWithEntry(zl,&fptr,2);
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        long long expire;
        unsigned char *fptr, *h;
        listpackTTL *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                /* Skip field and value */
                h = lpNext(lpt->lp, fptr);
                h = lpNext(lpt->lp, h);

                h = lpGetValue(h, NULL, &expire);
                serverAssert(h == NULL);

                if (hashLpIsExpired(expire))
                    return 0;

                lpt->lp = lpDeleteRangeWithEntry(lpt->lp,&fptr,3);
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Update hash on rename - in case of HFE, key is attached to dict for
 * notifications (in case of active-expiration flow) */
void hashTypeRename(robj *o, sds newName) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = o->ptr;
        lpt->key = newName;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dict *d = o->ptr;
        if (isDictWithMetaHFE(d)) {
            dictExpireMetadata *meta = (dictExpireMetadata *)dictMetadata(d);
            meta->key = newName;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Check if the hash object is empty
 *
 * If using HFE feature, there is an option to hash to still be defined with
 * items but logically be empty. Better use this func rather than checking length
 * (hashTypeLength(o,1)==0). It might be faster in case of many pending expired fields.
 */
int hashTypeIsEmpty(const robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return lpLength(o->ptr) == 0;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = o->ptr;
        if (lpt->meta.trash)
            return lpLength(lpt->lp) == 0;

        uint64_t count = hashLpExpireDryRun(o);
        return count == lpLength(lpt->lp) / 3;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        uint64_t dSize;
        dict *d = (dict*)o->ptr;
        dSize = dictSize(d);

        /* If dict doesn't have any HFE metadata or is not registered in global HFE
         * DS (i.e. expireMeta is trash). It means that there are no expired HFEs. */
        dictExpireMetadata *meta = (dictExpireMetadata *) dictMetadata(d);
        if (!isDictWithMetaHFE(d) || (meta->expireMeta.trash))
            return dSize == 0;

        /* Hash has fields with expiry. Need to take into account expired ones */

        /* If time of next hash-field to expire is greater than current time, then not empty */
        if (ebGetExpireTime(&hashExpireBucketsType, (eItem) o) >= (uint64_t)commandTimeSnapshot())
            return 0;

        /* If not all fields with expiry, then for sure not empty */
        if (likely(ebGetTotalItems(meta->hfe, &hashFieldExpireBucketsType) < dSize))
            return 0;

        /* All fields are with expiry, Check if maximum TTL is in the past */
        return ebGetMaxExpireTime(meta->hfe, &hashFieldExpireBucketsType, 1) <
                (uint64_t) commandTimeSnapshot();
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Return the number of elements in a hash.
 *
 * Note: Might be pricy in case there are many HFEs
 */
unsigned long hashTypeLength(const robj *o, int subtractExpiredFields) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        length = lpLength(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = o->ptr;
        length = lpLength(lpt->lp) / 3;

        if (subtractExpiredFields && lpt->meta.trash == 0)
           length -= hashLpExpireDryRun(o);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        uint64_t expiredItems = 0;
        dict *d = (dict*)o->ptr;
        if (subtractExpiredFields && isDictWithMetaHFE(d)) {
            dictExpireMetadata *meta = (dictExpireMetadata *) dictMetadata(d);
            /* If dict registered in global HFE DS */
            if (meta->expireMeta.trash == 0)
                expiredItems = ebExpireDryRun(meta->hfe,
                                              &hashFieldExpireBucketsType,
                                              commandTimeSnapshot());
        }
        length = dictSize(d) - expiredItems;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_LISTPACK || hi->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        hi->fptr = NULL;
        hi->vptr = NULL;
        hi->tptr = NULL;
        hi->ttl = EB_EXPIRE_TIME_INVALID;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */
int hashTypeNext(hashTypeIterator *hi, int skipExpiredFields) {
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hashLpGetListpack(hi->subject);
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
    } else if (hi->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        unsigned char *zl = hashLpGetListpack(hi->subject);
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

        long long ttl;

        while (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            vptr = lpNext(zl, fptr);
            serverAssert(vptr != NULL);

            tptr = lpNext(zl, vptr);
            serverAssert(tptr != NULL);

            lpGetValue(tptr, NULL, &ttl);

            if (!skipExpiredFields || !hashLpIsExpired(ttl)) {
                break;
            }

            fptr = lpNext(zl, tptr);
        }

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
        hi->tptr = tptr;
        hi->ttl = ttl;

    } else if (hi->encoding == OBJ_ENCODING_HT) {
        while ((hi->de = dictNext(hi->di)) != NULL) {
            if (skipExpiredFields && hfieldIsExpired(dictGetKey(hi->de)))
                continue;
            return C_OK;
        }
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
    serverAssert(hi->encoding == OBJ_ENCODING_LISTPACK || hi->encoding == OBJ_ENCODING_LISTPACK_TTL);

    if (what & OBJ_HASH_KEY) {
        *vstr = lpGetValue(hi->fptr, vlen, vll);
    } else {
        *vstr = lpGetValue(hi->vptr, vlen, vll);
    }

    if (expireTime)
        *expireTime = hi->ttl;
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
    hfield key = NULL;

    if (what & OBJ_HASH_KEY) {
        key = dictGetKey(hi->de);
        *str = key;
        *len = hfieldlen(key);
    } else {
        sds val = dictGetVal(hi->de);
        *str = val;
        *len = sdslen(val);
    }

    if (expireTime) {
        if (!key) key = dictGetKey(hi->de);
        *expireTime = hfieldGetExpireTime( key );
    }

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
    if (hi->encoding == OBJ_ENCODING_LISTPACK || hi->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        *vstr = NULL;
        hashTypeCurrentFromListpack(hi, what, vstr, vlen, vll, expireTime);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        char *ele;
        size_t eleLen;
        hashTypeCurrentFromHashTable(hi, what, &ele, &eleLen, expireTime);
        *vstr = (unsigned char*) ele;
        *vlen = eleLen;
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

/* Return the key at the current iterator position as a new hfield string. */
hfield hashTypeCurrentObjectNewHfield(hashTypeIterator *hi, int withExpireMeta) {
    char buf[LONG_STR_SIZE];
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;
    hfield hf;

    hashTypeCurrentObject(hi,OBJ_HASH_KEY,&vstr,&vlen,&vll, NULL);

    if (!vstr) {
        vlen = ll2string(buf, sizeof(buf), vll);
        vstr = (unsigned char *) buf;
    }

    hf = hfieldNew(vstr,vlen, withExpireMeta);
    return hf;
}

static robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (checkType(c,o,OBJ_HASH)) return NULL;

    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    }
    return o;
}


void hashTypeConvertListpack(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK);

    if (enc == OBJ_ENCODING_LISTPACK) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_LISTPACK_TTL) {
        void *fptr;
        long long val;
        unsigned int slen;
        listpackTTL *lpt = zcalloc(sizeof(*lpt));

        lpt->lp = lpNew(lpBytes(o->ptr) + lpLength(o->ptr));
        lpt->meta.trash = 1;

        fptr = lpFirst(o->ptr);
        while (fptr != NULL) {
            void *ptr;

            ptr = lpGetValue(fptr, &slen, &val);
            if (ptr)
                lpt->lp = lpAppend(lpt->lp, ptr, slen);
            else
                lpt->lp = lpAppendInteger(lpt->lp, val);

            fptr = lpNext(o->ptr, fptr);
            ptr = lpGetValue(fptr, &slen, &val);
            if (ptr)
                lpt->lp = lpAppend(lpt->lp, ptr, slen);
            else
                lpt->lp = lpAppendInteger(lpt->lp, val);

            lpt->lp = lpAppendInteger(lpt->lp, HASH_LP_NO_TTL);
            fptr = lpNext(o->ptr, fptr);
        }

        lpFree(o->ptr);
        o->encoding = OBJ_ENCODING_LISTPACK_TTL;
        o->ptr = lpt;
    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        hi = hashTypeInitIterator(o);
        dict = dictCreate(&mstrHashDictType);

        /* Presize the dict to avoid rehashing */
        /* TODO: activeExpire list pack. Should be small */
        dictExpand(dict,hashTypeLength(o, 0));

        while (hashTypeNext(hi, 0) != C_ERR) {

            hfield key = hashTypeCurrentObjectNewHfield(hi, 0);
            sds value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            dictUseStoredKeyApi(dict, 1);
            ret = dictAdd(dict, key, value);
            dictUseStoredKeyApi(dict, 0);
            if (ret != DICT_OK) {
                hfieldFree(key); sdsfree(value); /* Needed for gcc ASAN */
                hashTypeReleaseIterator(hi);  /* Needed for gcc ASAN */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                    o->ptr,lpBytes(o->ptr));
                serverPanic("Listpack corruption detected");
            }
        }
        hashTypeReleaseIterator(hi);
        zfree(o->ptr);
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hashTypeConvertListpackTTL(robj *o, int enc, redisDb *db) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_TTL);

    if (enc == OBJ_ENCODING_LISTPACK_TTL) {
        return;
    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;
        dictExpireMetadata *dictExpireMeta;
        listpackTTL *lpt = o->ptr;
        uint64_t minExpire = hashTypeGetMinExpire(o);

        if (db && lpt->meta.trash != 1)
            ebRemove(&db->hexpires, &hashExpireBucketsType, o);

        dict = dictCreate(&mstrHashDictTypeWithHFE);
        dictExpand(dict,hashTypeLength(o, 0));
        dictExpireMeta = (dictExpireMetadata *) dictMetadata(dict);

        /* Fillup dict HFE metadata */
        dictExpireMeta->key = lpt->key;            /* reference key in keyspace */
        dictExpireMeta->hfe = ebCreate();     /* Allocate HFE DS */
        dictExpireMeta->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */

        hi = hashTypeInitIterator(o);

        while (hashTypeNext(hi, 0) != C_ERR) {
            hfield key = hashTypeCurrentObjectNewHfield(hi, hi->ttl != HASH_LP_NO_TTL);
            sds value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            dictUseStoredKeyApi(dict, 1);
            ret = dictAdd(dict, key, value);
            dictUseStoredKeyApi(dict, 0);
            if (ret != DICT_OK) {
                hfieldFree(key); sdsfree(value); /* Needed for gcc ASAN */
                hashTypeReleaseIterator(hi);  /* Needed for gcc ASAN */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                                 o->ptr,lpBytes(o->ptr));
                serverPanic("Listpack corruption detected");
            }

            if (hi->ttl != HASH_LP_NO_TTL)
                ebAdd(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, key, hi->ttl);
        }
        hashTypeReleaseIterator(hi);
        lpFree(lpt->lp);
        zfree(lpt);

        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;

        if (minExpire != EB_EXPIRE_TIME_INVALID)
            ebAdd(&db->hexpires, &hashExpireBucketsType, o, minExpire);

    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hashTypeConvert(robj *o, int enc, redisDb *db) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvertListpack(o, enc);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        hashTypeConvertListpackTTL(o, enc, db);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a hash object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
robj *hashTypeDup(robj *o, sds newkey, uint64_t *minHashExpire) {
    robj *hobj;
    hashTypeIterator *hi;

    serverAssert(o->type == OBJ_HASH);

    if(o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = o->ptr;
        size_t sz = lpBytes(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        hobj = createObject(OBJ_HASH, new_zl);
        hobj->encoding = OBJ_ENCODING_LISTPACK;
    } else if(o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = o->ptr;
        size_t sz = lpBytes(lpt->lp);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, lpt->lp, sz);

        if (lpt->meta.trash == 0)
            *minHashExpire = ebGetMetaExpTime(&lpt->meta);

        listpackTTL *dup = zcalloc(sizeof(*dup));
        dup->lp = new_zl;
        dup->key = newkey;
        dup->meta.trash = 1;
        hobj = createObject(OBJ_HASH, dup);
        hobj->encoding = OBJ_ENCODING_LISTPACK_TTL;
    } else if(o->encoding == OBJ_ENCODING_HT) {
        dictExpireMetadata *dictExpireMetaSrc, *dictExpireMetaDst = NULL;
        dict *d;

        /* If dict doesn't have HFE metadata, then create a new dict without it */
        if (!isDictWithMetaHFE(o->ptr)) {
            d = dictCreate(&mstrHashDictType);
        } else {
            /* Create a new dict with HFE metadata */
            d = dictCreate(&mstrHashDictTypeWithHFE);
            dictExpireMetaSrc = (dictExpireMetadata *) dictMetadata((dict *) o->ptr);
            dictExpireMetaDst = (dictExpireMetadata *) dictMetadata(d);
            dictExpireMetaDst->key = newkey;         /* reference key in keyspace */
            dictExpireMetaDst->hfe = ebCreate();     /* Allocate HFE DS */
            dictExpireMetaDst->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */

            /* Extract the minimum expire time of the source hash (Will be used by caller
             * to register the new hash in the global ebuckets, i.e db->hexpires) */
            if (dictExpireMetaSrc->expireMeta.trash == 0)
                *minHashExpire = ebGetMetaExpTime(&dictExpireMetaSrc->expireMeta);
        }
        dictExpand(d, dictSize((const dict*)o->ptr));

        hi = hashTypeInitIterator(o);
        while (hashTypeNext(hi, 0) != C_ERR) {
            uint64_t expireTime;
            sds newfield, newvalue;
            /* Extract a field-value pair from an original hash object.*/
            char *field, *value;
            size_t fieldLen, valueLen;
            hashTypeCurrentFromHashTable(hi, OBJ_HASH_KEY, &field, &fieldLen, &expireTime);
            if (expireTime == EB_EXPIRE_TIME_INVALID) {
                newfield = hfieldNew(field, fieldLen, 0);
            } else {
                newfield = hfieldNew(field, fieldLen, 1);
                ebAdd(&dictExpireMetaDst->hfe, &hashFieldExpireBucketsType, newfield, expireTime);
            }

            hashTypeCurrentFromHashTable(hi, OBJ_HASH_VALUE, &value, &valueLen, NULL);
            newvalue = sdsnewlen(value, valueLen);

            /* Add a field-value pair to a new hash object. */
            dictUseStoredKeyApi(d, 1);
            dictAdd(d,newfield,newvalue);
            dictUseStoredKeyApi(d, 0);
        }
        hashTypeReleaseIterator(hi);

        hobj = createObject(OBJ_HASH, d);
        hobj->encoding = OBJ_ENCODING_HT;
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
void hashTypeRandomElement(robj *hashobj, unsigned long hashsize, listpackEntry *key, listpackEntry *val) {
    if (hashobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetFairRandomKey(hashobj->ptr);
        hfield field = dictGetKey(de);
        key->sval = (unsigned char*)field;
        key->slen = hfieldlen(field);
        if (val) {
            sds s = dictGetVal(de);
            val->sval = (unsigned char*)s;
            val->slen = sdslen(s);
        }
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK) {
        lpRandomPair(hashobj->ptr, hashsize, key, val, 0);
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        lpRandomPair(hashLpGetListpack(hashobj), hashsize, key, val, 1);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*
 * Active expiration of fields in hash
 *
 * Called by hashTypeDbActiveExpire() for each hash registered in the HFE DB
 * (db->hexpires) with an expiration-time less than or equal current time.
 *
 * This callback performs the following actions for each hash:
 * - Delete expired fields as by calling ebExpire(hash)
 * - If afterward there are future fields to expire, it will update the hash in
 *   HFE DB with the next hash-field minimum expiration time by returning
 *   ACT_UPDATE_EXP_ITEM.
 * - If the hash has no more fields to expire, it is removed from the HFE DB
 *   by returning ACT_REMOVE_EXP_ITEM.
 * - If hash has no more fields afterward, it will remove the hash from keyspace.
 */
static ExpireAction hashTypeActiveExpire(eItem _hashObj, void *ctx) {
    robj *hashObj = (robj *) _hashObj;
    ActiveExpireCtx *activeExpireCtx = (ActiveExpireCtx *) ctx;
    sds keystr = NULL;
    ExpireInfo info = {0};

    /* If no more quota left for this callback, stop */
    if (activeExpireCtx->fieldsToExpireQuota == 0)
        return ACT_STOP_ACTIVE_EXP;

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        info = (ExpireInfo){
                .maxToExpire = activeExpireCtx->fieldsToExpireQuota,
                .ctx = hashObj,
                .now = commandTimeSnapshot(),
                .itemsExpired = 0};

        hashLpExpire(hashObj, &info);
        listpackTTL *lpt = hashObj->ptr;
        keystr = lpt->key;
    } else {
        serverAssert(hashObj->encoding == OBJ_ENCODING_HT);

        dict *d = hashObj->ptr;
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(
                d);

        info = (ExpireInfo){
                .maxToExpire = activeExpireCtx->fieldsToExpireQuota,
                .onExpireItem = onFieldExpire,
                .ctx = hashObj,
                .now = commandTimeSnapshot(),
                .itemsExpired = 0
        };

        ebExpire(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, &info);
        keystr = dictExpireMeta->key;
    }

    /* Update quota left */
    activeExpireCtx->fieldsToExpireQuota -= info.itemsExpired;

    /* If hash has no more fields to expire, remove it from HFE DB */
    if (info.nextExpireTime == 0) {
        if (hashTypeLength(hashObj, 0) == 0) {
            robj *key = createStringObject(keystr, sdslen(keystr));
            dbDelete(activeExpireCtx->db, key);
            //notifyKeyspaceEvent(NOTIFY_HASH,"xxxxxxxxx",c->argv[1],c->db->id);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key, activeExpireCtx->db->id);
            server.dirty++;
            signalModifiedKey(NULL, &server.db[0], key);
            decrRefCount(key);
        }
        return ACT_REMOVE_EXP_ITEM;
    } else {
        /* Hash has more fields to expire. Keep hash to pending items that will
         * be added back to global HFE DS at the end of ebExpire() */
        ExpireMeta *expireMeta = hashGetExpireMeta(hashObj);
        ebSetMetaExpTime(expireMeta, info.nextExpireTime);
        return ACT_UPDATE_EXP_ITEM;
    }
}

/* Return the next/minimum expiry time of the hash-field.
 * If not found, return EB_EXPIRE_TIME_INVALID */
int64_t hashTypeGetMinExpire(robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return EB_EXPIRE_TIME_INVALID;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        return hashLpGetMinExpire(o);
    }

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    dict *d = o->ptr;
    if (!isDictWithMetaHFE(d))
        return EB_EXPIRE_TIME_INVALID;

    ExpireMeta *expireMeta = &((dictExpireMetadata *) dictMetadata(d))->expireMeta;

    /* Keep aside next hash-field expiry before updating HFE DS. Verify it is not trash */
    if (expireMeta->trash == 1)
        return EB_EXPIRE_TIME_INVALID;

    return ebGetMetaExpTime(expireMeta);
}

/* Delete all expired fields in a hash */
void hashTypeDeleteExpiredFields(client *c, robj *hashObj) {
    redisDb *db = c->db;
    sds keystr;
    ExpireInfo info = {0};

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = hashObj->ptr;
        if (lpt->meta.trash)
            return;

        info = (ExpireInfo){
                .maxToExpire = 0xFFFFFFFF,
                .ctx = hashObj,
                .now = commandTimeSnapshot(),
                .itemsExpired = 0};

        hashLpExpire(hashObj, &info);
        keystr = lpt->key;
    } else {
        serverAssert(hashObj->encoding == OBJ_ENCODING_HT);

        dict *d = hashObj->ptr;

        if (!isDictWithMetaHFE(d))
            return;

        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);

        /* If HFE metadata is marked as trash, return */
        if (dictExpireMeta->expireMeta.trash)
            return;

        /* If time of next hash-field to expire is greater than current time, return */
        if (ebGetExpireTime(&hashExpireBucketsType, hashObj) >=(uint64_t)commandTimeSnapshot())
            return;

        /* Remove expired fields as part of lazy-expire */
        info = (ExpireInfo){
                .maxToExpire = 0xFFFFFFFF,
                .onExpireItem = onFieldExpire,
                .ctx = hashObj,
                .now = commandTimeSnapshot(),
                .itemsExpired = 0};

        ebExpire(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, &info);
        keystr = dictExpireMeta->key;
    }
    /* If hash has no more fields to expire, remove it from HFE DB */
    if (info.nextExpireTime == 0) {

        /* Take care to remove hash from global HFE DS before deleting it */
        ebRemove(&c->db->hexpires, &hashExpireBucketsType, hashObj);

        if (hashTypeLength(hashObj, 0) == 0) {
            robj *key = createStringObject(keystr, sdslen(keystr));
            dbDelete(db, key);
            //notifyKeyspaceEvent(NOTIFY_HASH,"xxxxxxxxx",c->argv[1],c->db->id);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key, db->id);
            server.dirty++;
            signalModifiedKey(NULL, &server.db[0], key);
            decrRefCount(key);
        }
    } else {
        /* Update hash expiry based on next time of hash-field to expire in "global" HFE */
        ebRemove(&c->db->hexpires, &hashExpireBucketsType, hashObj);
        ebAdd(&c->db->hexpires, &hashExpireBucketsType, hashObj, info.nextExpireTime);
    }
}

uint64_t hashTypeRemoveFromExpires(ebuckets *hexpires, robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return EB_EXPIRE_TIME_INVALID;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* If dict doesn't holds HFE metadata */
        if (!isDictWithMetaHFE(o->ptr))
            return EB_EXPIRE_TIME_INVALID;
    }

    uint64_t expireTime = ebGetExpireTime(&hashExpireBucketsType, o);

    /* If registered in global HFE DS then remove it (not trash) */
    if (expireTime != EB_EXPIRE_TIME_INVALID)
        ebRemove(hexpires, &hashExpireBucketsType, o);

    return expireTime;
}

/* Precondition: hashObj is a hash object with HFE metadata */
void hashTypeAddToExpires(redisDb *db, robj *keyObj, robj *hashObj, uint64_t expireTime) {
    if (expireTime == EB_EXPIRE_TIME_INVALID)
         return;

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = hashObj->ptr;
        dictEntry *de = dbFind(db, keyObj->ptr);

        lpt->key = dictGetKey(de);
        ebAdd(&db->hexpires, &hashExpireBucketsType, hashObj, expireTime);
        return;
    }
    serverAssert(hashObj->encoding == OBJ_ENCODING_HT);

    serverAssert(isDictWithMetaHFE(hashObj->ptr));

    /* Reuse the sds from the main dict in the expire dict */
    dictEntry *de = dbFind(db, keyObj->ptr);

    serverAssertWithInfo(NULL, keyObj, de != NULL);

    /* Update hash with key for notifications */
    dict *d = hashObj->ptr;
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
    dictExpireMeta->key = dictGetKey(de);

    /* Add hash to global HFE DS */
    ebAdd(&db->hexpires, &hashExpireBucketsType, hashObj, expireTime);
}

/* DB active expire and update hashes with time-expiration on fields.
 *
 * The callback function hashTypeActiveExpire() is invoked for each hash registered
 * in the HFE DB (db->expires) with an expiration-time less than or equal to the
 * current time. This callback performs the following actions for each hash:
 * - If the hash has one or more fields to expire, it will delete those fields.
 * - If there are more fields to expire, it will update the hash with the next
 *   expiration time in HFE DB.
 * - If the hash has no more fields to expire, it is removed from the HFE DB.
 * - If the hash has no more fields, it is removed from the main DB.
 *
 * Returns number of fields active-expired.
 */
uint64_t hashTypeDbActiveExpire(redisDb *db, uint32_t maxFieldsToExpire) {
    ActiveExpireCtx ctx = { .db = db, .fieldsToExpireQuota = maxFieldsToExpire };
    ExpireInfo info = {
            .maxToExpire = UINT64_MAX, /* Only maxFieldsToExpire play a role */
            .onExpireItem = hashTypeActiveExpire,
            .ctx = &ctx,
            .now = commandTimeSnapshot(),
            .itemsExpired = 0};

    ebExpire(&db->hexpires, &hashExpireBucketsType, &info);

    /* Return number of fields active-expired */
    return maxFieldsToExpire - ctx.fieldsToExpireQuota;
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

void hsetnxCommand(client *c) {
    robj *o;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    if (hashTypeExists(o, c->argv[2]->ptr)) {
        addReply(c, shared.czero);
    } else {
        hashTypeTryConversion(o,c->argv,2,3);
        hashTypeSet(c->db, o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
        addReply(c, shared.cone);
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }
}

void hsetCommand(client *c) {
    int i, created = 0;
    robj *o;

    if ((c->argc % 2) == 1) {
        addReplyErrorArity(c);
        return;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    for (i = 2; i < c->argc; i += 2)
        created += !hashTypeSet(c->db, o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        addReply(c, shared.ok);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty += (c->argc - 2)/2;
}

void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
        if (vstr) {
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        value = 0;
    }

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;
    new = sdsfromlonglong(value);
    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    addReplyLongLong(c,value);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if (isnan(incr) || isinf(incr)) {
        addReplyError(c,"value is NaN or Infinity");
        return;
    }
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&ll) == C_OK) {
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            value = (long double)ll;
        }
    } else {
        value = 0;
    }

    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,LD_STR_HUMAN);
    new = sdsnewlen(buf,len);
    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    addReplyBulkCBuffer(c,buf,len);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    robj *newobj;
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,shared.hset);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

static void addHashFieldToReply(client *c, robj *o, sds field) {
    if (o == NULL) {
        addReplyNull(c);
        return;
    }

    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if (hashTypeGetValue(o, field, &vstr, &vlen, &vll) == C_OK) {
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }
    } else {
        addReplyNull(c);
    }
}

void hgetCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c,o,OBJ_HASH)) return;

    addReplyArrayLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            deleted++;
            if (hashTypeLength(o, 0) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

void hlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addReplyLongLong(c,hashTypeLength(o, 1));
}

void hstrlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_LISTPACK || hi->encoding == OBJ_ENCODING_LISTPACK_TTL) {
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
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int length, count = 0;

    robj *emptyResp = (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) ?
        shared.emptymap[c->resp] : shared.emptyarray;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyResp))
        == NULL || checkType(c,o,OBJ_HASH)) return;

    /* We return a map if the user requested keys and values, like in the
     * HGETALL case. Otherwise to use a flat array makes more sense. */
    length = hashTypeLength(o, 1 /*subtractExpiredFields*/);
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) {
        addReplyMapLen(c, length);
    } else {
        addReplyArrayLen(c, length);
    }

    hi = hashTypeInitIterator(o);

    /* Skip expired fields if the hash has an expire time set at global HFE DS. We could
     * set it to constant 1, but then it will make another lookup for each field expiration */
    int skipExpiredFields = (EB_EXPIRE_TIME_INVALID == hashTypeGetMinExpire(o)) ? 0 : 1;

    while (hashTypeNext(hi, skipExpiredFields) != C_ERR) {
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);

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
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addReply(c, hashTypeExists(o,c->argv[2]->ptr) ? shared.cone : shared.czero);
}

void hscanCommand(client *c) {
    robj *o;
    unsigned long long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    scanGenericCommand(c,o,cursor);
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
    robj *hash;

    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,hash,OBJ_HASH)) return;
    /* TODO: Active-expire */
    size = hashTypeLength(hash, 0);

    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

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
                hfield field = dictGetKey(de);
                sds value = dictGetVal(de);
                if (withvalues && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, field, hfieldlen(field));
                if (withvalues)
                    addReplyBulkCBuffer(c, value, sdslen(value));
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
        } else if (hash->encoding == OBJ_ENCODING_LISTPACK ||
                   hash->encoding == OBJ_ENCODING_LISTPACK_TTL) {
            listpackEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            unsigned char *lp = hashLpGetListpack(hash);
            int skip = hash->encoding == OBJ_ENCODING_LISTPACK ? 0 : 1;

            limit = count > HRANDFIELD_RANDOM_SAMPLE_LIMIT ? HRANDFIELD_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(listpackEntry)*limit);
            if (withvalues)
                vals = zmalloc(sizeof(listpackEntry)*limit);
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomPairs(lp, sample_count, keys, vals, skip);
                hrandfieldReplyWithListpack(c, sample_count, keys, vals);
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
            zfree(keys);
            zfree(vals);
        }
        return;
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
        hashTypeIterator *hi = hashTypeInitIterator(hash);
        while (hashTypeNext(hi, 0) != C_ERR) {
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            if (withvalues)
                addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
        }
        hashTypeReleaseIterator(hi);
        return;
    }

    /* CASE 2.5 listpack only. Sampling unique elements, in non-random order.
     * Listpack encoded hashes are meant to be relatively small, so
     * HRANDFIELD_SUB_STRATEGY_MUL isn't necessary and we rather not make
     * copies of the entries. Instead, we emit them directly to the output
     * buffer.
     *
     * And it is inefficient to repeatedly pick one random element from a
     * listpack in CASE 4. So we use this instead. */
    if (hash->encoding == OBJ_ENCODING_LISTPACK || hash->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        unsigned char *lp = hashLpGetListpack(hash);
        int skip = hash->encoding == OBJ_ENCODING_LISTPACK ? 0 : 1;

        listpackEntry *keys, *vals = NULL;
        keys = zmalloc(sizeof(listpackEntry)*count);
        if (withvalues)
            vals = zmalloc(sizeof(listpackEntry)*count);
        serverAssert(lpRandomPairsUnique(lp, count, keys, vals, skip) == count);
        hrandfieldReplyWithListpack(c, count, keys, vals);
        zfree(keys);
        zfree(vals);
        return;
    }

    /* CASE 3:
     * The number of elements inside the hash is not greater than
     * HRANDFIELD_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a hash from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the hash, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count*HRANDFIELD_SUB_STRATEGY_MUL > size) {
        /* Hashtable encoding (generic implementation) */
        dict *d = dictCreate(&sdsReplyDictType);  /* without metadata! */
        dictExpand(d, size);
        hashTypeIterator *hi = hashTypeInitIterator(hash);

        /* Add all the elements into the temporary dictionary. */
        while ((hashTypeNext(hi, 0)) != C_ERR) {
            int ret = DICT_ERR;
            sds key, value = NULL;

            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            if (withvalues)
                value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            ret = dictAdd(d, key, value);

            serverAssert(ret == DICT_OK);
        }
        serverAssert(dictSize(d) == size);
        hashTypeReleaseIterator(hi);

        /* Remove random elements to reach the right count. */
        while (size > count) {
            dictEntry *de;
            de = dictGetFairRandomKey(d);
            dictUseStoredKeyApi(d, 1);
            dictUnlink(d,dictGetKey(de));
            dictUseStoredKeyApi(d, 0);
            sdsfree(dictGetKey(de));
            sdsfree(dictGetVal(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        while ((de = dictNext(di)) != NULL) {
            sds key = dictGetKey(de);
            sds value = dictGetVal(de);
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkSds(c, key);
            if (withvalues)
                addReplyBulkSds(c, value);
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

    /* CASE 4: We have a big hash compared to the requested number of elements.
     * In this case we can simply get random elements from the hash and add
     * to the temporary hash, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        /* Hashtable encoding (generic implementation) */
        unsigned long added = 0;
        listpackEntry key, value;
        dict *d = dictCreate(&hashDictType);
        dictExpand(d, count);
        while(added < count) {
            hashTypeRandomElement(hash, size, &key, withvalues? &value : NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            sds skey = hashSdsFromListpackEntry(&key);
            if (dictAdd(d,skey,NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            added++;

            /* We can reply right away, so that we don't need to store the value in the dict. */
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            hashReplyFromListpackEntry(c, &key);
            if (withvalues)
                hashReplyFromListpackEntry(c, &value);
        }

        /* Release memory */
        dictRelease(d);
    }
}

/* HRANDFIELD key [<count> [WITHVALUES]] */
void hrandfieldCommand(client *c) {
    long l;
    int withvalues = 0;
    robj *hash;
    listpackEntry ele;

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

    hashTypeRandomElement(hash,hashTypeLength(hash, 0),&ele,NULL);
    hashReplyFromListpackEntry(c, &ele);
}

/*-----------------------------------------------------------------------------
 * Hash Field with optional expiry (based on mstr)
 *----------------------------------------------------------------------------*/

/* if expireAt is 0, then expireAt is ignored and no metadata is attached */
hfield hfieldNew(const void *field, size_t fieldlen, int withExpireMeta) {
    if (!withExpireMeta)
        return mstrNew(field, fieldlen);

    hfield hf = mstrNewWithMeta(&mstrFieldKind, field, fieldlen,
                                (mstrFlags) 1 << HFIELD_META_EXPIRE);

    ExpireMeta *expireMeta = mstrMetaRef(hf, &mstrFieldKind, HFIELD_META_EXPIRE);

    /* as long as it is not inside ebuckets, it is considered trash */
    expireMeta->trash = 1;
    return hf;
}

int hfieldIsExpireAttached(hfield field) {
    return mstrIsMetaAttached(field) && mstrGetFlag(field, (int) HFIELD_META_EXPIRE);
}

static ExpireMeta* hfieldGetExpireMeta(const eItem field) {
    /* extract the expireMeta from the field of type mstr */
    return mstrMetaRef(field, &mstrFieldKind, (int) HFIELD_META_EXPIRE);
}

static uint64_t hfieldGetExpireTime(hfield field) {
    if (!hfieldIsExpireAttached(field))
        return EB_EXPIRE_TIME_INVALID;

    ExpireMeta *expireMeta = mstrMetaRef(field, &mstrFieldKind, (int) HFIELD_META_EXPIRE);
    if (expireMeta->trash)
        return EB_EXPIRE_TIME_INVALID;

    return ebGetMetaExpTime(expireMeta);
}

/* Remove TTL from the field. Assumed ExpireMeta is attached and has valid value */
static void hfieldPersist(redisDb *db, robj *hashObj, hfield field) {
    uint64_t fieldExpireTime = hfieldGetExpireTime(field);
    if (fieldExpireTime == EB_EXPIRE_TIME_INVALID)
        return;

    serverAssert(isDictWithMetaHFE(hashObj->ptr));

    dict *d = hashObj->ptr;
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *)dictMetadata(d);

    /* If field has valid expiry then dict should have valid metadata as well */
    serverAssert(dictExpireMeta->expireMeta.trash == 0);

    uint64_t hmin = ebGetMetaExpTime(&dictExpireMeta->expireMeta);

    /* Remove field from private HFE DS */
    ebRemove(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, field);

    /* If the removed field was the minimal to expire, Might need to update the hash at global HFE DS  */
    if (hmin == fieldExpireTime) {
        /* If hnext min expire time of the hash is different than the one removed, then update global HFE DS */
        uint64_t hnext = ebGetNextTimeToExpire(dictExpireMeta->hfe, &hashFieldExpireBucketsType, 1);
        if (hnext != fieldExpireTime) {
            ebRemove(&db->hexpires, &hashExpireBucketsType, hashObj);
            /* If not last field to expire */
            if (hnext != EB_EXPIRE_TIME_INVALID)
                ebAdd(&db->hexpires, &hashExpireBucketsType, hashObj, hnext);
        }
    }
}

int hfieldIsExpired(hfield field) {
    /* Condition remains valid even if hfieldGetExpireTime() returns EB_EXPIRE_TIME_INVALID,
     * as the constant is equivalent to (EB_EXPIRE_TIME_MAX + 1). */
    return ( (mstime_t)hfieldGetExpireTime(field) < commandTimeSnapshot());
}

/*-----------------------------------------------------------------------------
 * Hash Field Expiration (HFE)
 *----------------------------------------------------------------------------*/
/* Called during active expiration of hash-fields */
static ExpireAction onFieldExpire(eItem item, void *ctx) {
    hfield hf = item;
    robj *hashobj = (robj *) ctx;
    dictUseStoredKeyApi((dict *)hashobj->ptr, 1);
    hashTypeDelete(hashobj, hf);
    server.stat_expired_hash_fields++;
    dictUseStoredKeyApi((dict *)hashobj->ptr, 0);
    return ACT_REMOVE_EXP_ITEM;
}

/* Retrieve the ExpireMeta associated with the hash.
 * The caller is responsible for ensuring that it is indeed attached. */
static ExpireMeta *hashGetExpireMeta(const eItem item) {
    robj *hashObj = (robj *)item;
    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = ((robj*)item)->ptr;
        return &lpt->meta;
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
        return &dictExpireMeta->expireMeta;
    }

    serverAssert(0);
}

/* Set time-expiration to hash-field */
SetExpireTimeRes hashTypeSetExpire(ebuckets *eb,
                                   robj *hashObj,
                                   sds field,
                                   uint64_t expireAt,
                                   int flag,
                                   uint64_t *minPrevExp)
{
    dict *d = hashObj->ptr;
    uint64_t prevExpire = EB_EXPIRE_TIME_MAX;

    /* First retrieve the field to check if it exists */
    dictEntry *de = dictFind(d, field);
    if (de == NULL) return HFE_SET_NO_FIELD;

    hfield hf = dictGetKey(de);

    /* If field doesn't have expiry metadata attached */
    if (!hfieldIsExpireAttached(hf)) {
        if (flag & (HFE_XX | HFE_LT | HFE_GT))
            return HFE_SET_NO_CONDITION_MET;

        /* allocate new field with expire metadata */
        hfield hfNew =  hfieldNew(hf, hfieldlen(hf), 1 /*withExpireMeta*/);
        /* Replace the old field with the new one with metadata */
        dictSetKey(d, de, hfNew);
        hfieldFree(hf);
        hf = hfNew;
    } else {
        /* read previous expire time */
        prevExpire = hfieldGetExpireTime(hf);

        if (prevExpire != EB_EXPIRE_TIME_INVALID) {
            if (((flag == HFE_GT) && (prevExpire >= expireAt)) ||
                ((flag == HFE_LT) && (prevExpire <= expireAt)) ||
                (flag == HFE_NX) )
                return HFE_SET_NO_CONDITION_MET;

            ebRemove(eb, &hashFieldExpireBucketsType, hf);

            if (*minPrevExp > prevExpire)
                *minPrevExp = prevExpire;
        } else {
            if (flag & (HFE_XX | HFE_LT | HFE_GT))
                return HFE_SET_NO_CONDITION_MET;
        }
    }

    /* if expiration time is in the past */
    if (checkAlreadyExpired(expireAt)) {
        hashTypeDelete(hashObj, field);
        return HFE_SET_DELETED;
    }
    ebAdd(eb, &hashFieldExpireBucketsType, hf, expireAt);

    // TODO: propagate, rewrite command if needed. See expireGenericCommand() as reference

    return HFE_SET_OK;
}

static void httlGenericCommand(client *c, const char *cmd, long long basetime, int unit) {
    UNUSED(cmd);
    robj *hashObj;
    long numFields = 0, numFieldsAt = 2;

    /* Read the hash object */
    if ((hashObj = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp])) == NULL ||
        checkType(c, hashObj, OBJ_HASH)) return;



    /* Read number of fields */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Parameter `numFileds` should be greater than 0") != C_OK)
        return;

    /* Verify `numFields` is consistent with number of arguments */
    if (numFields > (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "Parameter `numFileds` is more than number of arguments");
        return;
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        void *lp = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[3 + i]->ptr;
            void *fptr = lpFirst(lp);
            if (fptr != NULL)
                fptr = lpFind(lp, fptr, (unsigned char *) field, sdslen(field), 1);

            if (!fptr)
                addReplyLongLong(c, HFE_GET_NO_FIELD);
            else
                addReplyLongLong(c, HFE_GET_NO_TTL);
        }
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        listpackTTL *lpt = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[3+i]->ptr;
            void *fptr = lpFirst(lpt->lp);
            if (fptr != NULL)
                fptr = lpFind(lpt->lp, fptr, (unsigned char *) field, sdslen(field), 2);

            if (!fptr) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            long long expire;
            fptr = lpNext(lpt->lp, fptr);
            fptr = lpNext(lpt->lp, fptr);
            lpGetValue(fptr, NULL, &expire);

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
    }

    dict *d = hashObj->ptr;

    addReplyArrayLen(c, numFields);
    for (int i = 0 ; i < numFields ; i++) {
        sds field = c->argv[3+i]->ptr;
        dictEntry *de = dictFind(d, field);
        if (de == NULL) {
            addReplyLongLong(c, HFE_GET_NO_FIELD);
            continue;
        }

        hfield hf = dictGetKey(de);
        uint64_t expire = hfieldGetExpireTime(hf);
        if (expire == EB_EXPIRE_TIME_INVALID) {
            addReplyLongLong(c, HFE_GET_NO_TTL); /* no ttl */
            continue;
        }

        if ( (long long) expire <= commandTimeSnapshot()) {
            addReplyLongLong(c, HFE_GET_NO_FIELD);
            continue;
        }

        if (unit == UNIT_SECONDS)
            addReplyLongLong(c, (expire + 999 - basetime) / 1000);
        else
            addReplyLongLong(c, (expire - basetime));
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
 * Additional flags are supported and parsed via parseExtendedExpireArguments */
static void hexpireGenericCommand(client *c, const char *cmd, long long basetime, int unit) {
    long numFields = 0, numFieldsAt = 3;
    long long expire; /* unix time in msec */
    int flag = 0;
    robj *hashObj, *keyArg = c->argv[1], *expireArg = c->argv[2];

    /* Read the hash object */
    if ((hashObj = lookupKeyWriteOrReply(c, keyArg, shared.null[c->resp])) == NULL ||
        checkType(c, hashObj, OBJ_HASH)) return;


    /* Read the expiry time from command */
    if (getLongLongFromObjectOrReply(c, expireArg, &expire, NULL) != C_OK)
        return;

    /* Check expire overflow */
    if (expire > (long long) EB_EXPIRE_TIME_MAX) {
        addReplyErrorExpireTime(c);
        return;
    }

    if (unit == UNIT_SECONDS) {
        if (expire > (long long) EB_EXPIRE_TIME_MAX / 1000) {
            addReplyErrorExpireTime(c);
            return;
        }
        expire *= 1000;
    } else {
        if (expire > (long long) EB_EXPIRE_TIME_MAX) {
            addReplyErrorExpireTime(c);
            return;
        }
    }

    if (expire > (long long) EB_EXPIRE_TIME_MAX - basetime) {
        addReplyErrorExpireTime(c);
        return;
    }
    expire += basetime;

    /* Adjust the expiration time to match the configured precision reduction.
     * This optimizes the aggregation of keys into buckets of `ebucetks` down
     * to ~seconds, ~minutes, ~hours, etc, rather than msec resolution */
    if (server.hash_field_expiry_bits) {
        uint64_t tmp = (1 << server.hash_field_expiry_bits)-1;
        expire = (expire + tmp) & ~tmp;

        /* Check expire overflow after rounding */
        if (expire > (long long) EB_EXPIRE_TIME_MAX) {
            addReplyErrorExpireTime(c);
            return;
        }
    }

    /* Read optional flag [NX|XX|GT|LT] */
    char *optArg = c->argv[3]->ptr;
    if (!strcasecmp(optArg, "nx")) {
        flag = HFE_NX; ++numFieldsAt;
    } else if (!strcasecmp(optArg, "xx")) {
        flag = HFE_XX; ++numFieldsAt;
    } else if (!strcasecmp(optArg, "gt")) {
        flag = HFE_GT; ++numFieldsAt;
    } else if (!strcasecmp(optArg, "lt")) {
        flag = HFE_LT; ++numFieldsAt;
    }

    /* Read number of fields */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Parameter `numFields` should be greater than 0") != C_OK)
        return;

    /* Verify `numFields` is consistent with number of arguments */
    if (numFields > (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "Parameter `numFileds` is more than number of arguments");
        return;
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvert(hashObj, OBJ_ENCODING_LISTPACK_TTL, c->db);
        listpackTTL *lpt = hashObj->ptr;

        dictEntry *de = dbFind(c->db, keyArg->ptr);
        serverAssert(de != NULL);
        lpt->key = dictGetKey(de);
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        uint64_t minExpire = EB_EXPIRE_TIME_INVALID;
        listpackTTL *lpt = hashObj->ptr;

        if (lpt->meta.trash == 0)
            minExpire = hashLpGetMinExpire(hashObj);

        /* For each field in command, update dict HFE DS */
        int fieldUpdated=0, fieldDeleted=0;
        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt+i+1]->ptr;

            int res = hashlpSetExpire(hashObj, field, expire, flag);
            addReplyLongLong(c,res);

            if (res == HFE_SET_DELETED)
                ++fieldDeleted;
            else if (res == HFE_SET_OK)
                ++fieldUpdated;
        }

        /* Notify keyspace event, update dirty count and update global HFE DS */
        if (fieldDeleted + fieldUpdated > 0) {
            server.dirty += fieldDeleted + fieldUpdated;
            signalModifiedKey(c, c->db, keyArg);
            notifyKeyspaceEvent(NOTIFY_HASH, cmd, keyArg, c->db->id);
            if (fieldDeleted && hashTypeLength(hashObj, 0) == 0) {
                dbDelete(c->db, keyArg);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", keyArg, c->db->id);
            } else {
                if (minExpire != EB_EXPIRE_TIME_INVALID)
                    ebRemove(&c->db->hexpires, &hashExpireBucketsType, hashObj);

                minExpire = hashLpGetMinExpire(hashObj);
                if (minExpire != EB_EXPIRE_TIME_INVALID)
                    ebAdd(&c->db->hexpires, &hashExpireBucketsType, hashObj, minExpire);
            }
        }

        return;
    }

    dictExpireMetadata *dictExpireMeta;
    uint64_t minExpire = EB_EXPIRE_TIME_INVALID;
    dict *d = hashObj->ptr;

    /* If dict doesn't have metadata attached */
    if (!isDictWithMetaHFE(d)) {
        /* Realloc (only header of dict) with metadata for hash-field expiration */
        dictTypeAddMeta(&d, &mstrHashDictTypeWithHFE);
        dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
        hashObj->ptr = d;

        /* Find the key in the keyspace. Need to keep reference to the key for
         * notifications or even removal of the hash */
        dictEntry *de = dbFind(c->db, keyArg->ptr);
        serverAssert(de != NULL);
        sds key = dictGetKey(de);

        /* Fillup dict HFE metadata */
        dictExpireMeta->key = key;            /* reference key in keyspace */
        dictExpireMeta->hfe = ebCreate();     /* Allocate HFE DS */
        dictExpireMeta->expireMeta.trash = 1; /* mark as trash (as long it wasn't ebAdd()) */
    } else {
        dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
        ExpireMeta *expireMeta = &dictExpireMeta->expireMeta;

        /* Keep aside next hash-field expiry before updating HFE DS. Verify it is not trash */
        if (expireMeta->trash == 0)
            minExpire = ebGetMetaExpTime(&dictExpireMeta->expireMeta);
    }

    /* Figure out from provided set of fields in command, which one has the minimum
     * expiration time, before the modification (Will be used for optimization below) */
    uint64_t minExpireFields = EB_EXPIRE_TIME_INVALID;

    /* For each field in command, update dict HFE DS */
    int fieldUpdated=0, fieldDeleted=0;
    addReplyArrayLen(c, numFields);
    for (int i = 0 ; i < numFields ; i++) {
        sds field = c->argv[numFieldsAt+i+1]->ptr;

        SetExpireTimeRes res = hashTypeSetExpire(&dictExpireMeta->hfe,
                                                 hashObj,
                                                 field,
                                                 expire,
                                                 flag,
                                                 &minExpireFields);
        addReplyLongLong(c,res);
        if (res == HFE_SET_DELETED)
            ++fieldDeleted;
        else if (res == HFE_SET_OK)
            ++fieldUpdated;
    }

    /* Notify keyspace event, update dirty count and update global HFE DS */
    if (fieldDeleted + fieldUpdated > 0) {
        server.dirty += fieldDeleted + fieldUpdated;
        signalModifiedKey(c,c->db,keyArg);
        notifyKeyspaceEvent(NOTIFY_HASH,cmd,keyArg,c->db->id);
        if (fieldDeleted && hashTypeLength(hashObj, 0) == 0) {
            dbDelete(c->db,keyArg);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",keyArg, c->db->id);
        } else {

            /* If minimum HFE of the hash is smaller than expiration time of the
             * specified fields in the command as well as it is smaller or equal
             * than expiration time provided in the command, then the minimum
             * HFE of the hash won't change following this command. */
            if ((minExpire < minExpireFields) && ((long long)minExpire <= expire) )
                return;

            /* retrieve new expired time */
            uint64_t newMinExpire = ebGetNextTimeToExpire(dictExpireMeta->hfe,
                                                          &hashFieldExpireBucketsType,
                                                          0);

            if (minExpire != newMinExpire) {
                if (minExpire != EB_EXPIRE_TIME_INVALID)
                    ebRemove(&c->db->hexpires, &hashExpireBucketsType, hashObj);
                if (newMinExpire != EB_EXPIRE_TIME_INVALID)
                    ebAdd(&c->db->hexpires, &hashExpireBucketsType, hashObj, newMinExpire);
            }
        }
    }
}

/* HPEXPIRE key milliseconds [ NX | XX | GT | LT] numfields <field [field ...]> */
void hpexpireCommand(client *c) {
    hexpireGenericCommand(c,"hpexpire", commandTimeSnapshot(),UNIT_MILLISECONDS);
}

/* HEXPIRE key seconds [NX | XX | GT | LT] numfields <field [field ...]> */
void hexpireCommand(client *c) {
    hexpireGenericCommand(c,"hexpire", commandTimeSnapshot(),UNIT_SECONDS);
}

/* HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] numfields <field [field ...]> */
void hexpireatCommand(client *c) {
    hexpireGenericCommand(c,"hexpireat", 0,UNIT_SECONDS);
}

/* HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] numfields <field [field ...]> */
void hpexpireatCommand(client *c) {
    hexpireGenericCommand(c,"hpexpireat", 0,UNIT_MILLISECONDS);
}

/* for each specified field: get the remaining time to live in seconds*/
/* HTTL key numfields <field [field ...]> */
void httlCommand(client *c) {
    httlGenericCommand(c, "httl", commandTimeSnapshot(), UNIT_SECONDS);
}

/* HPTTL key numfields <field [field ...]> */
void hpttlCommand(client *c) {
    httlGenericCommand(c, "hpttl", commandTimeSnapshot(), UNIT_MILLISECONDS);
}

/* HEXPIRETIME key numFields <field [field ...]> */
void hexpiretimeCommand(client *c) {
    httlGenericCommand(c, "hexpiretime", 0, UNIT_SECONDS);
}

/* HPEXPIRETIME key numFields <field [field ...]> */
void hpexpiretimeCommand(client *c) {
    httlGenericCommand(c, "hexpiretime", 0, UNIT_MILLISECONDS);
}

/* HPERSIST key <FIELDS count field [field ...]> */
void hpersistCommand(client *c) {
    robj *hashObj;
    long numFields = 0, numFieldsAt = 2;

    /* Read the hash object */
    if ((hashObj = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp])) == NULL ||
        checkType(c, hashObj, OBJ_HASH)) return;

    /* Read number of fields */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Parameter `numFileds` should be greater than 0") != C_OK)
        return;

    /* Verify `numFields` is consistent with number of arguments */
    if (numFields > (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "Parameter `numFileds` is more than number of arguments");
        return;
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[3 + i]->ptr;
            unsigned char *fptr, *zl = hashObj->ptr;

            fptr = lpFirst(zl);
            if (fptr != NULL)
                fptr = lpFind(zl, fptr, (unsigned char *) field, sdslen(field), 1);

            if (!fptr)
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
            else
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
        }
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_TTL) {
        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[3 + i]->ptr;
            int ret = hashLpPersist(hashObj, c->db, field, commandTimeSnapshot());
            addReplyLongLong(c, ret);
        }
        return;
    }

    serverAssert(hashObj->encoding == OBJ_ENCODING_HT);

    dict *d = hashObj->ptr;

    addReplyArrayLen(c, numFields);
    for (int i = 0 ; i < numFields ; i++) {
        sds field = c->argv[3+i]->ptr;
        dictEntry *de = dictFind(d, field);
        if (de == NULL) {
            addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
            continue;
        }

        hfield hf = dictGetKey(de);
        uint64_t expire = hfieldGetExpireTime(hf);
        if (expire == EB_EXPIRE_TIME_INVALID) {
            addReplyLongLong(c, HFE_PERSIST_NO_TTL);
            continue;
        }

        /* Already expired. Pretend there is no such field */
        if ( (long long) expire <= commandTimeSnapshot()) {
            addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
            continue;
        }

        hfieldPersist(c->db, hashObj, hf);
        addReplyLongLong(c, HFE_PERSIST_OK);
    }
}
