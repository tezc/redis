#include "redismodule.h"

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <memory.h>
#include <errno.h>


#define MAX_EVENTS 1024
const char *clusterEventLog[MAX_EVENTS];
int numClusterEvents = 0;

const char *clusterTrimEventLog[MAX_EVENTS];
int numClusterTrimEvents = 0;

int replicateModuleCommand = 0;
RedisModuleString *moduleCommandKeyName = NULL;
RedisModuleString *moduleCommandKeyVal = NULL;

int replicate_module_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) {
        RedisModule_ReplyWithError(ctx, "ERR wrong number of arguments");
        return REDISMODULE_OK;
    }

    long long enable = 0;
    if (RedisModule_StringToLongLong(argv[1], &enable) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR enable value");
        return REDISMODULE_OK;
    }
    replicateModuleCommand = (enable != 0);

    if (moduleCommandKeyName) RedisModule_FreeString(ctx, moduleCommandKeyName);
    if (moduleCommandKeyVal) RedisModule_FreeString(ctx, moduleCommandKeyVal);
    moduleCommandKeyName = RedisModule_CreateStringFromString(ctx, argv[1]);
    moduleCommandKeyVal = RedisModule_CreateStringFromString(ctx, argv[2]);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int slotRangeArrayContains(RedisModuleSlotRangeArray *sra, unsigned int slot) {
    for (int i = 0; i < sra->num_ranges; i++)
        if (sra->ranges[i].start <= slot && sra->ranges[i].end >= slot)
            return 1;
    return 0;
}

int sanity(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_Assert(RedisModule_ClusterSlotIsLocal(-1) == 0);
    RedisModule_Assert(RedisModule_ClusterSlotIsLocal(16384) == 0);
    RedisModule_Assert(RedisModule_ClusterSlotIsLocal(100000) == 0);

    RedisModule_Assert(RedisModule_ClusterReplicateForSlotMigration(NULL, NULL, NULL) == REDISMODULE_ERR);
    RedisModule_Assert(RedisModule_ClusterReplicateForSlotMigration(ctx, NULL, NULL) == REDISMODULE_ERR);
    RedisModule_Assert(RedisModule_ClusterReplicateForSlotMigration(NULL, "asm.keyless_cmd1", "") == REDISMODULE_ERR);
    RedisModule_Assert(RedisModule_ClusterReplicateForSlotMigration(ctx, "asm.keyless_cmd1", "") == REDISMODULE_ERR);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int clusterIsSlotLocal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argc);
    long long slot = 0;

    if (RedisModule_StringToLongLong(argv[1],&slot) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid slot");
    }

    RedisModule_ReplyWithLongLong(ctx, RedisModule_ClusterSlotIsLocal(slot));
    return REDISMODULE_OK;
}

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
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_TRIM_STARTED)
        snprintf(buf, sizeof(buf), "sub: cluster-trim-started, ");
    else if (sub == REDISMODULE_SUBEVENT_CLUSTER_TRIM_COMPLETED)
        snprintf(buf, sizeof(buf), "sub: cluster-trim-completed, ");
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

static void tryToReplicateKeyOutsideSlotRange(RedisModuleCtx *ctx, RedisModuleClusterMigrationInfo *info) {
    int slot = 0;
    while (slot >= 0 && slot <= 16383) {
        if (!slotRangeArrayContains(info->slots, slot)) {
            break;
        }
        slot++;
    }

    char buf[128] = {0};
    const char *prefix = RedisModule_ClusterCanonicalKeyNameInSlot(slot);
    snprintf(buf, sizeof(buf), "{%s}%s", prefix, "modulekey");

    int ret = RedisModule_ClusterReplicateForSlotMigration(ctx, "SET", "cc", buf, "value");
    RedisModule_Assert(ret == REDISMODULE_ERR);
}

void clusterEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(ctx);

    if (e.id == REDISMODULE_EVENT_CLUSTER) {
        RedisModuleClusterMigrationInfo *info = data;

        if (sub == REDISMODULE_SUBEVENT_CLUSTER_MIGRATE_MODULE_REPLICATE) {
            if (replicateModuleCommand == 0) return;
            /* Try to replicate a key outside the slot range. */
            tryToReplicateKeyOutsideSlotRange(ctx, info);

            int ret;
            ret = RedisModule_ClusterReplicateForSlotMigration(ctx, "asm.keyless_cmd1", "");
            RedisModule_Assert(ret == REDISMODULE_OK);


            char buf[128] = {0};
            const char *prefix = RedisModule_ClusterCanonicalKeyNameInSlot(info->slots->ranges[0].start);
            snprintf(buf, sizeof(buf), "{%s}%s", prefix, "modulekey");

            ret = RedisModule_ClusterReplicateForSlotMigration(ctx, "SET", "cc", buf, "value");
            RedisModule_Assert(ret == REDISMODULE_OK);
            return;
        } else {
            if (numClusterEvents >= MAX_EVENTS) return;
            clusterEventLog[numClusterEvents++] = RedisModuleClusterMigrationInfoToString(info, sub);
        }
    }
}

void clusterTrimEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
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

int val1 = 0;

int keylessCmd1(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    val1++;
    RedisModule_ReplyWithLongLong(ctx, val1);
    return REDISMODULE_OK;
}

int readVal1(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    RedisModule_ReplyWithLongLong(ctx, val1);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "asm", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.cluster_is_slot_local", clusterIsSlotLocal, "", 0, 0, 0) == REDISMODULE_ERR)
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

    if (RedisModule_CreateCommand(ctx, "asm.keyless_cmd1", keylessCmd1, "write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.read_keyless_cmd1", readVal1, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.sanity", sanity, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.replicate_module_command", replicate_module_command, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


    if (RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_TRIMMED, keyspaceNotificationTrimmedCallback) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
