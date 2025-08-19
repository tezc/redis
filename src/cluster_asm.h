/*
 * Copyright (c) 2025-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef CLUSTER_ASM_H
#define CLUSTER_ASM_H

struct asmTask;
struct slotRangeArray;
struct slotRange;

void clusterAsmInit(void);
void asmBeforeSleep(void);
void asmCron(void);
void asmStartSendBulkAndStream(struct asmTask *task);
void asmCallbackOnFreeClient(client *c);
int asmMigrateInProgress(void);
int asmImportInProgress(void);
void asmFeedMigrationClient(robj **argv, int argc);
int asmDebugSetFailPoint(char * channel, char *state);
int asmDebugSetTrimMethod(const char *method, int delay);

void asmImportIncrAppliedBytes(struct asmTask *task, size_t bytes);
struct slotRangeArray *asmTaskGetSlotRanges(const char *task_id);
int asmNotifyConfigUpdated(struct asmTask *task, struct slotRangeArray *slot_ranges, sds *err);
size_t asmGetPeakSyncBufferSize(void);
int asmKeyBelongsToCurrentNode(kvobj *kv);
size_t asmGetImportingBufferSize(void);
size_t asmGetMigratingBufferSize(void);
void asmTrimSlots(struct slotRangeArray *slots);
int clusterAsmCancel(const char *task_id, const char *reason);
int clusterAsmCancelBySlot(int slot, const char *reason);
int clusterAsmCancelBySlotRangeArray(struct slotRangeArray *slot_ranges, const char *reason);
int clusterAsmCancelByNode(void *node, const char *reason);
int isSlotInAsmTask(int slot);
sds asmCatInfoString(sds info);
void clusterMigrationCommand(client *c);
void clusterSyncSlotsCommand(client *c);


void asmActiveTrimCycle(int type);
int asmActiveTrimIsInProgress(void);
int asmActiveTrimIsInProgressFor(int slot);
int asmActiveTrimOverlaps(struct slotRange *req);
int asmActiveTrimDelIfNeeded(redisDb *db, robj *key, kvobj *kv, long long *key_mem_freed);
int asmTrimSlotsIfNeeded(void);
int asmCanTrimSlots(void);

#endif

