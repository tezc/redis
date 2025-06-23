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


/* API for implementation/plugin
 *
 * - On destination side, implementation calls clusterAsmRequest(ASM_REQUEST_IMPORT_START)
 *   to start the import operation
 * - Redis calls clusterAsmOnEvent() when an event occurs.
 * - On the source side, Redis will call clusterAsmOnEvent(ASM_EVENT_IMPORT_WAIT_PAUSE)
 *   when the write pause is needed.
 * - Implementation stops the traffic to the slots and calls clusterAsmRequest(ASM_REQUEST_IMPORT_PAUSED)
 * - On the destination side, Redis calls clusterAsmOnEvent(ASM_EVENT_IMPORT_COMPLETED)
 *   when the import is completed.
 * - Plugin calls clusterAsmRequest(ASM_REQUEST_CONFIG_UPDATED) to notify Redis
 *   that the config is updated.
 *
 * Sequence diagram for import:
 *   - Note: shows only the events that plugin needs to react.
 *
 * ┌───────────────┐              ┌───────────────┐         ┌───────────────┐             ┌───────────────┐
 * │ Destination   │              │ Destination   │         │    Source     │             │ Source        │
 * │ Cluster plugin│              │ Master        │         │    Master     │             │ Cluster plugin│
 * └───────┬───────┘              └───────┬───────┘         └───────┬───────┘             └───────┬───────┘
 *         │                              │                         │                             │
 *         │ ASM_REQUEST_IMPORT_START     │                         │                             │
 *         ├─────────────────────────────►│                         │                             │
 *         │                              │CLUSTER SYNCSLOTS <arg>  │                             │
 *         │                              ├────────────────────────►│                             │
 *         │                              │                         │                             │
 *         │                              │  SNAPSHOT(restore cmds) │                             │
 *         │                              │◄────────────────────────┤                             │
 *         │                              │  Repl stream            │                             │
 *         │                              │◄────────────────────────┤                             │
 *         │                              │                         │ASM_EVENT_MIGRATE_WAIT_PAUSE │
 *         │                              │                         ├────────────────────────────►│
 *         │                              │                         │ ASM_REQUEST_MIGRATE_PAUSED  │
 *         │                              │                         │◄────────────────────────────┤
 *         │                              │ Drain repl stream       │                             │
 *         │                              │◄────────────────────────┤                             │
 *         │ASM_EVENT_IMPORT_WAIT_FINALIZE│                         │                             │
 *         │◄─────────────────────────────┤                         │                             │
 *         │                              │                         │                             │
 *         │ ASM_REQUEST_CONFIG_UPDATED   │                         │                             │
 *         ├─────────────────────────────►│                         │ ASM_REQUEST_CONFIG_UPDATED  │
 *         │                              │                         │◄────────────────────────────┤
 *         │                              │                         │                             │
 */

#define ASM_REQUEST_IMPORT_START        1  /* Start a new import operation (destination side) */
#define ASM_REQUEST_IMPORT_CANCEL       2  /* Cancel an ongoing import operation (destination side) */
#define ASM_REQUEST_MIGRATE_PAUSED      3  /* Notify that slot writes are paused (source side) */
#define ASM_REQUEST_CONFIG_UPDATED      4  /* Notify that config is updated (source and destination side) */

/* Called by implementation to request an ASM operation. */
int clusterAsmRequest(slotRangeArray *slot_ranges, int request, void *arg, sds *err);

#define ASM_EVENT_IMPORT_STARTED        1 /* Import started */
#define ASM_EVENT_IMPORT_FAILED         2 /* Import failed */
#define ASM_EVENT_IMPORT_WAIT_FINALIZE  3 /* Import completed, waiting for config change */
#define ASM_EVENT_IMPORT_FINALIZED      4 /* TODO: decide if we need this to trigger when config is updated */

#define ASM_EVENT_MIGRATE_STARTED       5 /* Migration started */
#define ASM_EVENT_MIGRATE_FAILED        6 /* Migration failed */
#define ASM_EVENT_MIGRATE_WAIT_PAUSE    7 /* Migrate operation waiting for slot writes to be paused */
#define ASM_EVENT_MIGRATE_WAIT_FINALIZE 8 /* Migration completed */
#define ASM_EVENT_MIGRATE_FINALIZED     9 /* TODO: decide if we need this to trigger when config is updated */


/* Called when an ASM event occurs to notify implementation/plugin. */
int clusterAsmOnEvent(slotRangeArray *slot_ranges, int event, void *arg);

#endif

