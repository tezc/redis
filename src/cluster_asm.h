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
/*
┌───────────────┐            ┌───────────────┐         ┌───────────────┐             ┌───────────────┐
│ Destination   │            │ Destination   │         │    Source     │             │ Source        │
│ Cluster plugin│            │ Master        │         │    Master     │             │ Cluster plugin│
└───────┬───────┘            └───────┬───────┘         └───────┬───────┘             └───────┬───────┘
        │                            │                         │                             │
        │ ASM_REQUEST_IMPORT_START   │                         │                             │
        ├───────────────────────────►│                         │                             │
        │                            │ CLUSTER SYNCSLOTS <arg> │                             │
        │                            ├────────────────────────►│                             │
        │                            │                         │                             │
        │                            │  SNAPSHOT(restore cmds) │                             │
        │                            │◄────────────────────────┤                             │
        │                            │  Repl stream            │                             │
        │                            │◄────────────────────────┤                             │
        │                            │                         │ ASM_EVENT_IMPORT_WAIT_PAUSE │
        │                            │                         ├────────────────────────────►│
        │                            │                         │  ASM_REQUEST_IMPORT_PAUSED  │
        │                            │                         │◄────────────────────────────┤
        │                            │ Drain repl stream       │                             │
        │                            │◄────────────────────────┤                             │
        │ ASM_EVENT_IMPORT_COMPLETED │                         │                             │
        │◄───────────────────────────┤                         │                             │
        │                            │                         │                             │
        │clusterNotifyConfigUpdated()│                         │                             │
        ├───────────────────────────►│                         │ clusterNotifyConfigUpdated()│
        │                            │                         │◄────────────────────────────┤
        │                            │                         │                             │
 */

#define ASM_REQUEST_IMPORT_START      1  /* Start a new import operation (destination side) */
#define ASM_REQUEST_IMPORT_CANCEL     2  /* Cancel an ongoing import operation (destination side) */
#define ASM_REQUEST_IMPORT_PAUSED     3  /* Notify that slot writes are paused (source side) */

/* Called by implementation to request an ASM operation. */
int clusterAsmRequest(slotRangeArray *slot_ranges, int request, void *arg, sds *err);

#define ASM_EVENT_IMPORT_STARTED       1 /* Import started */
#define ASM_EVENT_IMPORT_FAILED        2 /* Import failed */
#define ASM_EVENT_IMPORT_WAIT_PAUSE    3 /* Import waiting for slot writes to be paused */
#define ASM_EVENT_IMPORT_COMPLETED     4 /* Import completed */

#define ASM_EVENT_MIGRATE_STARTED      5 /* Migration started */
#define ASM_EVENT_MIGRATE_FAILED       6 /* Migration failed */
#define ASM_EVENT_MIGRATE_COMPLETED    7 /* Migration completed */

/* Called when an ASM event occurs to notify implementation/plugin. */
int clusterAsmOnEvent(slotRangeArray *slot_ranges, int event, void *arg);

/* Both on source and destination side. Implementation calls when the config
 * is updated */
int clusterNotifyConfigUpdated(slotRangeArray *slot_ranges, sds *err);




#endif

