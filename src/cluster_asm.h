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
struct asmTaskManager;

void clusterAsmInit(void);

void clusterMigrationCommand(client *c);
void clusterSyncSlotsCommand(client *c);


/* API for implementation/plugin */

#define ASM_EVENT_IMPORT_STARTED    1
#define ASM_EVENT_IMPORT_FAILED     2
#define ASM_EVENT_IMPORT_COMPLETED  3
#define ASM_EVENT_MIGRATE_STARTED   4
#define ASM_EVENT_MIGRATE_FAILED    5
#define ASM_EVENT_MIGRATE_COMPLETED 6

/* Called when an ASM event occurs to notify implementation/plugin. */
int clusterAsmOnEvent(slotRangeArray *slot_ranges, int event, void *arg);

/* On destination side. Start a new import operation. */
int clusterAsmImport(slotRangeArray *slot_ranges, sds *err);

/* On destination side. Cancels an ongoing import operation that overlaps with
 * the given slot ranges. Returns the number of cancelled operations. */
int clusterAsmCancel(slotRangeArray *slot_ranges, sds *err);

/* Both on source and destination side. Implementation calls when the config
 * is updated */
int clusterAsmConfigUpdated(slotRangeArray *slot_ranges, sds *err);

/* On destination side. Implementation calls when the import is completed.
 * Implementation should change the config and call clusterAsmConfigUpdated() */
int clusterAsmImportCompleted(slotRangeArray *slot_ranges, sds *err);

/* On source side. Implementation calls when the slot writes need to be paused. */
int clusterAsmSlotWritesPause(slotRangeArray *slot_ranges, sds *err);

/* On source side. Implementation calls when the slot writes are paused. */
int clusterAsmSlotWritesPaused(slotRangeArray *slot_ranges, sds *err);

#endif

