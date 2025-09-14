#include "redismodule.h"

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <memory.h>
#include <errno.h>

int test_keyslot(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argc);
    long long slot = 0;

    if (RedisModule_StringToLongLong(argv[1],&slot) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid slot");
    }

    RedisModule_ReplyWithLongLong(ctx, RedisModule_ClusterIsMySlot(slot));
    return REDISMODULE_OK;
}

#define MAX_EVENTS 1024
const char *clusterEventLog[MAX_EVENTS];
int numClusterEvents = 0;

const char *clusterTrimEventLog[MAX_EVENTS];
int numClusterTrimEvents = 0;

const char *RedisModuleClusterMigrationInfoToString(RedisModuleClusterMigrationInfo *info, uint64_t sub) {
    /* Generate a string representation of the info struct. */
    /* like "dbnum:1, task_id: sdadsad, slots: 0-100,200-300 */
    char buf[1024] = {0};

    if (sub == REDISMODULE_SUBEVENT_CLUSTER_IMPORT_STARTED)
        snprintf(buf, sizeof(buf), "sub: cluster-import-started, ");
    else  if (sub == REDISMODULE_SUBEVENT_CLUSTER_IMPORT_FAILED)
        snprintf(buf, sizeof(buf), "sub: cluster-import-failed, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_IMPORT_COMPLETED)
        snprintf(buf, sizeof(buf), "sub: cluster-import-completed, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_MIGRATE_STARTED)
        snprintf(buf, sizeof(buf), "sub: cluster-migrate-started, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_MIGRATE_FAILED)
        snprintf(buf, sizeof(buf), "sub: cluster-migrate-failed, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_MIGRATE_COMPLETED)
        snprintf(buf, sizeof(buf), "sub: cluster-migrate-completed, ");
    else {
        RedisModule_Assert(0);
    }


    snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "dbnum:%d, task_id:%s, slots:", info->dbnum, info->task_id);
    for (int i = 0; i < info->slots->num_ranges; i++) {
        RedisModuleSlotRange *sr = &info->slots->ranges[i];
        snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "%d-%d", sr->start, sr->end);
        if (i != info->slots->num_ranges - 1)
            snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), ",");
    }
    return RedisModule_Strdup(buf);
}

const char *RedisModuleClusterTrimInfoToString(RedisModuleClusterTrimInfo *info, uint64_t sub) {
    /* Generate a string representation of the info struct. */
    /* like "dbnum:1, task_id: xEADX, slots:0-100,200-300 */
    RedisModule_Assert(info);
    char buf[1024] = {0};

    if (sub == REDISMODULE_SUBEVENT_CLUSTER_TRIM_BACKGROUND)
        snprintf(buf, sizeof(buf), "sub: cluster-trim-background, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_TRIM_ACTIVE_STARTED)
        snprintf(buf, sizeof(buf), "sub: cluster-trim-active-started, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_TRIM_ACTIVE_COMPLETED)
        snprintf(buf, sizeof(buf), "sub: cluster-trim-active-completed, ");
    else {
        RedisModule_Assert(0);
    }

    snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "dbnum:%d, slots:", info->dbnum);
    for (int i = 0; i < info->slots->num_ranges; i++) {
        RedisModuleSlotRange *sr = &info->slots->ranges[i];
        snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "%d-%d", sr->start, sr->end);
        if (i != info->slots->num_ranges - 1)
            snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), ",");
    }
    return RedisModule_Strdup(buf);
}

void clusterEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data)
{
    REDISMODULE_NOT_USED(ctx);

    if (e.id == REDISMODULE_EVENT_CLUSTER) {
        if (numClusterEvents >= MAX_EVENTS) return;
        RedisModuleClusterMigrationInfo *info = data;
        clusterEventLog[numClusterEvents++] = RedisModuleClusterMigrationInfoToString(info, sub);
    }
}

void clusterTrimEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data)
{
    REDISMODULE_NOT_USED(ctx);

    if (e.id == REDISMODULE_EVENT_CLUSTER_TRIM) {
        if (numClusterEvents >= MAX_EVENTS) return;
        RedisModuleClusterTrimInfo *info = data;
        clusterTrimEventLog[numClusterTrimEvents++] = RedisModuleClusterTrimInfoToString(info, sub);
    }
}


static int keyspaceNotificationTrimmedCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    REDISMODULE_NOT_USED(ctx);

    RedisModule_Assert(type == REDISMODULE_NOTIFY_TRIMMED);
    RedisModule_Assert(strcmp(event, "trimmed") == 0);

    size_t len;
    const char *key_str = RedisModule_StringPtrLen(key, &len);

    char buf[1024] = {0};
    snprintf(buf, sizeof(buf), "keyspace: trimmed, key: %s", key_str);

    if (numClusterTrimEvents >= MAX_EVENTS)
        return REDISMODULE_OK;
    clusterTrimEventLog[numClusterTrimEvents++] = RedisModule_Strdup(buf);

    return REDISMODULE_OK;
}


int clearEventLog(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    for (int i = 0; i < numClusterEvents; i++) {
        RedisModule_Free((void *)clusterEventLog[i]);
    }
    numClusterEvents = 0;

    for (int i = 0; i < numClusterTrimEvents; i++) {
        RedisModule_Free((void *)clusterTrimEventLog[i]);
    }
    numClusterTrimEvents = 0;

    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

int getClusterEventLog(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_ReplyWithArray(ctx, numClusterEvents);
    for (int i = 0; i < numClusterEvents; i++)
        RedisModule_ReplyWithStringBuffer(ctx, clusterEventLog[i], strlen(clusterEventLog[i]));
    return REDISMODULE_OK;
}

int getClusterTrimEventLog(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_ReplyWithArray(ctx, numClusterTrimEvents);
    for (int i = 0; i < numClusterTrimEvents; i++)
        RedisModule_ReplyWithStringBuffer(ctx, clusterTrimEventLog[i], strlen(clusterTrimEventLog[i]));
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "asm", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.is_my_slot", test_keyslot, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.clear_event_log", clearEventLog, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.get_cluster_event_log", getClusterEventLog, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.get_cluster_trim_event_log", getClusterTrimEventLog, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Cluster, clusterEventCallback) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClusterTrim, clusterTrimEventCallback) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_TRIMMED, keyspaceNotificationTrimmedCallback) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
