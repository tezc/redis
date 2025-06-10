/*
 * Copyright (c) 2025-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "server.h"
#include "cluster_asm.h"
#include "cluster.h"
#include "cluster_legacy.h"

#define ASM_IMPORT  (1 << 1)
#define ASM_MIGRATE (1 << 2)

typedef struct asmTask {
    list *slot_ranges;            /* List of slot ranges for this migration operation */
    sds state;                    /* Dummy state str */
    char source[CLUSTER_NAMELEN]; /* Source node name */
    char dest[CLUSTER_NAMELEN];   /* Destination node name */
    int operation;                /* Either ASM_IMPORT or ASM_MIGRATE */
    clusterNode *source_node;     /* Source node */
    int main_channel_state;       /* State of the main channel */
    connection *main_channel_con; /* Main channel connection */
    long long main_channel_id;    /* Main channel ID for the task */
    connection *rdb_channel_con;  /* RDB channel connection */
    int rdb_channel_state;        /* State of the RDB channel */
    long long dest_offset;        /* Destination offset */
    long long source_offset;      /* Source offset */
} asmTask;

enum asmState {
    /* Common state */
    ASM_NONE,
    ASM_CONNECTING,
    ASM_AUTH_REPLY,
    ASM_DONE,

    /* Import state */
    ASM_SYNCSLOTS_REPLY,
    ASM_INIT_RDBCHANNEL,
    ASM_BUFFER_STREAM,
    ASM_REPLAY_STREAM,
    ASM_SLOTS_HANDOFF,

    /* Migrate state */
    ASM_WAIT_RDBCHANNEL,
    ASM_WAIT_BGSAVE_START,
    ASM_SEND_BULK_AND_STREAM,
    ASM_SEND_STREAM,
    ASM_STREAM_DONE,

    /* RDB channel state */
    ASM_RDBCHANNEL_REPLY,
    ASM_RDBCHANNEL_TRANSFER,
    ASM_RDBCHANNEL_DONE
};

void asmStartSyncSlots(asmTask *task);

/* Returns C_OK if there is no overlapping import operation in progress for the
 * given slot range. Otherwise, returns C_ERR */
static int checkOverlappingImport(SlotRange *req) {
    listIter li, sli;
    listNode *ln, *sln;

    listRewind(server.cluster->asm_links, &li);
    while ((ln = listNext(&li)) != NULL) {
        asmTask *link = ln->value;

        /* Only import operations on the destination side can be cancelled. */
        if (link->operation != ASM_IMPORT ||
            strcasecmp(link->state, "inprogress") != 0)
        {
            continue;
        }

        listRewind(link->slot_ranges, &sli);
        while ((sln = listNext(&sli)) != NULL) {
            SlotRange *sr = sln->value;
            /* Cancel if any slot range overlaps with the requested range. */
            if (sr->first <= req->last && sr->last >= req->first)
                return C_ERR;
        }
    }

    return C_OK;
}

/* Validates the given slot ranges for a migration operation:
 * - Ensures the current node is a master.
 * - Verifies all slots are in a STABLE state.
 * - Checks that slot ranges are well-formed and non-overlapping.
 * - Confirms all slots belong to a single source node.
 * - Confirms no ongoing import operation that overlaps with the slot ranges.
 *
 * Returns the source node if validation succeeds.
 * Otherwise, returns NULL and sets 'err' variable. */
static clusterNode *validateImportSlotRanges(list *slot_ranges, sds *err) {
    listNode *ln;
    listIter li;
    clusterNode *source = NULL;
    unsigned char *slots = zcalloc(CLUSTER_SLOTS);

    *err = NULL;

    /* Ensure this is a master node */
    if (!clusterNodeIsMaster(server.cluster->myself)) {
        *err = sdsnew("Slot migration not allowed on replica.");
        goto out;
    }

    /* Ensure no manual migration is in progress. */
    for (int i = 0; i < CLUSTER_SLOTS; i++) {
        if (server.cluster->importing_slots_from[i] != NULL ||
            server.cluster->migrating_slots_to[i] != NULL)
        {
            *err = sdsnew("Slot states must be STABLE to start a slot migration operation.");
            goto out;
        }
    }

    listRewind(slot_ranges, &li);
    while ((ln = listNext(&li))) {
        SlotRange *sr = ln->value;

        /* Validate slot boundaries */
        if (sr->first >= CLUSTER_SLOTS || sr->last >= CLUSTER_SLOTS ||
            sr->first > sr->last)
        {
            *err = sdscatprintf(sdsempty(), "invalid slot range: %d-%d",
                                sr->first, sr->last);
            goto out;
        }

        /* Ensure no import operation overlaps with this slot range. */
        if (checkOverlappingImport(sr) != C_OK) {
            *err = sdscatprintf(sdsempty(),
                                "overlapping import exists for slot range: %d-%d",
                                sr->first, sr->last);
            goto out;
        }

        /* Validate if we can start migration operation for this slot range. */
        for (int i = sr->first; i <= sr->last; i++) {
            if (server.cluster->slots[i] == NULL) {
                *err = sdscatprintf(sdsempty(), "slot has no owner: %d", i);
                goto out;
            }

            if (!source) {
                source = server.cluster->slots[i];
                if (source == server.cluster->myself) {
                    *err = sdscatprintf(sdsempty(), "this node is already the owner of the slot: %d", i);
                    goto out;
                }
            } else if (source != server.cluster->slots[i]) {
                *err = sdsnew("slots belong to different source nodes");
                goto out;
            }

            if (slots[i]++ != 0) {
                *err = sdscatprintf(sdsempty(), "slot %d is given twice in different slot ranges", i);
                goto out;
            }
        }
    }

out:
    zfree(slots);
    return *err ? NULL : source;
}

/* CLUSTER MIGRATION IMPORT <start-slot end-slot [start-slot end-slot ...]>
 *
 * Sent by operator to the destination node to start the migration. */
static void clusterCommandMigrationImport(client *c) {
    /* Validate slot range arg count */
    int remaining = c->argc - 3;
    if (remaining == 0 || remaining % 2 != 0) {
        addReplyErrorArity(c);
        return;
    }

    /* Parse slot ranges */
    int first, last;
    list *slot_ranges = listCreate();
    listSetFreeMethod(slot_ranges, zfree);

    for (int i = 3; i < c->argc; i += 2) {
        if ((first = getSlotOrReply(c, c->argv[i])) == -1 ||
            (last = getSlotOrReply(c, c->argv[i + 1])) == -1)
        {
            listRelease(slot_ranges);
            return;
        }

        SlotRange *sr = zmalloc(sizeof(*sr));
        sr->first = first;
        sr->last = last;
        listAddNodeTail(slot_ranges, sr);
    }

    sds err = NULL;
    clusterNode *source;

    /* Validate that the slot ranges are valid and that migration can be
     * initiated for them. */
    source = validateImportSlotRanges(slot_ranges, &err);
    if (!source) {
        addReplyErrorSds(c, err);
        listRelease(slot_ranges);
        return;
    }

    /* Schedule slot migration operation */
    asmTask *link = zcalloc(sizeof(*link));
    link->slot_ranges = slot_ranges;
    link->state = sdsnew("inprogress");
    link->operation = ASM_IMPORT;
    link->source_node = source;
    link->main_channel_con = NULL;
    link->main_channel_state = ASM_NONE;
    link->rdb_channel_con = NULL;
    link->rdb_channel_state = ASM_NONE;
    link->main_channel_id = -1;
    memcpy(link->source, source->name, CLUSTER_NAMELEN);
    memcpy(link->dest, server.cluster->myself->name, CLUSTER_NAMELEN);
    listAddNodeTail(server.cluster->asm_links, link);

    asmStartSyncSlots(link);

    addReply(c, shared.ok);
}

/* Cancel atomic slot migration operations that overlap with the given slot
 * range. Returns the number of cancelled operations. */
static int cancelLinksForSlotRange(SlotRange *req_range) {
    int num_cancelled = 0;
    listIter li, sli;
    listNode *ln, *sln;

    listRewind(server.cluster->asm_links, &li);
    while ((ln = listNext(&li)) != NULL) {
        asmTask *link = ln->value;

        /* Only import operations on the destination side can be cancelled. */
        if (link->operation != ASM_IMPORT ||
            strcasecmp(link->state, "inprogress") != 0)
        {
            continue;
        }

        listRewind(link->slot_ranges, &sli);
        while ((sln = listNext(&sli)) != NULL) {
            SlotRange *sr = sln->value;
            /* Cancel if any slot range overlaps with the requested range. */
            if (sr->first <= req_range->last && sr->last >= req_range->first) {
                link->state = sdscpy(link->state, "cancelled");
                num_cancelled++;
                break;
            }
        }
    }
    return num_cancelled;
}

/* CLUSTER MIGRATION CANCEL <start-slot end-slot [start-slot end-slot ...]>
 *   - Reply: Number of cancelled operations
 *
 * Cancels import operations that overlap with the specified slot ranges.
 * Multiple operations may be cancelled. */
static void clusterCommandMigrationCancel(client *c) {
    int remaining, first, last;
    int num_cancelled = 0;
    listIter li;
    listNode *ln;
    list *slot_ranges;

    /* Validate slot range arg count */
    remaining = c->argc - 3;
    if (remaining == 0 || remaining % 2 != 0) {
        addReplyErrorArity(c);
        return;
    }

    slot_ranges = listCreate();
    listSetFreeMethod(slot_ranges, zfree);

    /* Parse slot ranges into the list */
    for (int i = 3; i < c->argc; i += 2) {
        if ((first = getSlotOrReply(c, c->argv[i])) == -1 ||
            (last = getSlotOrReply(c, c->argv[i + 1])) == -1)
        {
            listRelease(slot_ranges);
            return;
        }

        if (first > last) {
            addReplyErrorFormat(c, "invalid slot range: %d-%d", first, last);
            listRelease(slot_ranges);
            return;
        }

        SlotRange *sr = zmalloc(sizeof(*sr));
        sr->first = first;
        sr->last = last;
        listAddNodeTail(slot_ranges, sr);
    }

    /* Cancel asm operations that overlaps with the slot ranges. */
    listRewind(slot_ranges, &li);
    while ((ln = listNext(&li)) != NULL) {
        num_cancelled += cancelLinksForSlotRange(ln->value);
    }

    addReplyLongLong(c, num_cancelled);
    listRelease(slot_ranges);
}

/* Create a slot range string in the format of: "1000-2000 3000-4000 ..." */
static sds createSlotRangesStr(list *slot_ranges) {
    listNode *ln;
    listIter li;
    sds s = sdsempty();

    listRewind(slot_ranges, &li);
    while ((ln = listNext(&li)) != NULL) {
        SlotRange *range = ln->value;
        s = sdscatprintf(s, "%d-%d ", range->first, range->last);
    }
    sdssetlen(s, sdslen(s) - 1);
    s[sdslen(s)] = '\0';

    return s;
}

/* CLUSTER MIGRATION STATUS
 *  - Reply: Array of atomic slot migration links */
static void clusterCommandMigrationStatus(client *c) {
    listIter li;
    listNode *ln;

    addReplyArrayLen(c, listLength(server.cluster->asm_links));

    listRewind(server.cluster->asm_links, &li);
    while ((ln = listNext(&li)) != NULL) {
        asmTask *link = ln->value;

        addReplyMapLen(c, 5);
        addReplyBulkCString(c, "slots_range");
        addReplyBulkSds(c, createSlotRangesStr(link->slot_ranges));
        addReplyBulkCString(c, "source");
        addReplyBulkCBuffer(c, link->source, CLUSTER_NAMELEN);
        addReplyBulkCString(c, "dest");
        addReplyBulkCBuffer(c, link->dest, CLUSTER_NAMELEN);
        addReplyBulkCString(c, "operation");
        addReplyBulkCString(c, link->operation == ASM_IMPORT ? "importing" : "migrating");
        addReplyBulkCString(c, "state");
        addReplyBulkCString(c, link->state);
    }
}

/* CLUSTER MIGRATION
 *      <IMPORT start-slot end-slot [start-slot end-slot ...] |
 *       STATUS |
 *       CANCEL start-slot end-slot [start-slot end-slot ...]>
 * */
void clusterMigrationCommand(client *c) {
    if (c->argc < 3) {
        addReplyErrorArity(c);
        return;
    }

    if (strcasecmp(c->argv[2]->ptr, "import") == 0) {
        clusterCommandMigrationImport(c);
    } else if (strcasecmp(c->argv[2]->ptr, "status") == 0) {
        clusterCommandMigrationStatus(c);
    } else if (strcasecmp(c->argv[2]->ptr, "cancel") == 0) {
        clusterCommandMigrationCancel(c);
    } else {
        addReplyError(c, "unknown argument");
    }
}

char *sendCommand(connection *conn, ...);
char *receiveSynchronousResponse(connection *conn);
ConnectionType *connTypeOfReplication(void);

void mainChannelBufferStream(connection *conn) {
    UNUSED(conn);
}

void rdbChannelSyncWithSource(connection *conn) {
    asmTask *task = connGetPrivateData(conn);
    char *err = NULL;

    if (task->rdb_channel_state == ASM_CONNECTING) {
        connSetReadHandler(conn, rdbChannelSyncWithSource);
        connSetWriteHandler(conn, NULL);
        task->rdb_channel_state = ASM_RDBCHANNEL_REPLY;
        char cid[LONG_STR_SIZE];
        ull2string(cid, sizeof(cid), task->main_channel_id);
        err = sendCommand(conn, "CLUSTER", "SYNCSLOTS", "RDBCHANNEL", cid, NULL);
        if (err) goto write_error;
        return;
    }

    if (task->rdb_channel_state == ASM_RDBCHANNEL_REPLY) {
        /* Read reply */
        err = receiveSynchronousResponse(conn);
        /* The destination node did not reply */
        if (err == NULL) goto no_response_error;

        /* Check `+SLOTSSNAPSHOT` reply */
        if (!strncmp(err, "+SLOTSSNAPSHOT", strlen("+SLOTSSNAPSHOT"))) {
            task->main_channel_state = ASM_BUFFER_STREAM;
            /* Should buffer pending commands stream */
            // connSetReadHandler(task->main_channel_con, mainChannelBufferStream);

            task->rdb_channel_state = ASM_RDBCHANNEL_TRANSFER;
            client *c = createClient(conn);
            c->flags |= CLIENT_MASTER;
            c->querybuf = sdsempty();
            c->authenticated = 1;
            c->user = NULL;
            c->task = task;
            connSetPrivateData(conn, c);
            connSetReadHandler(conn, readQueryFromClient);
            serverLog(LL_NOTICE,
                "Source replied to SLOTSSNAPSHOT, sync slots snapshot can continue...");
            sdsfree(err);
        } else {
            serverLog(LL_WARNING,"Error reply to CLUSTER SYNCSLOTS RDBCHANNEL from source: '%s'",err);
            sdsfree(err);
            goto error;
        }
    }
    return;

no_response_error:
    serverLog(LL_WARNING, "Source node did not respond to command during RDBCHANNELSYNCSLOTS handshake");
    /* Fall through to regular error handling */
error:
    connClose(conn);
    connClose(task->main_channel_con);
    task->main_channel_con = NULL;
    task->rdb_channel_con = NULL;
    task->main_channel_state = ASM_NONE;
    task->rdb_channel_state = ASM_NONE;
    return;
write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING,"Sending command to source: %s", err);
    sdsfree(err);
    goto error;
}

char *sendCommandArgv(connection *conn, int argc, char **argv, size_t *argv_lens);
void syncWithSource(connection *conn) {
    asmTask *task = connGetPrivateData(conn);
    char *err = NULL;

    if (task->main_channel_state == ASM_CONNECTING) {
        connSetReadHandler(conn, syncWithSource);
        connSetWriteHandler(conn, NULL);
        task->main_channel_state = ASM_SYNCSLOTS_REPLY;
        
        size_t argc = (listLength(task->slot_ranges)*2 + 3);
        char **args = zcalloc(sizeof(char*) * argc);
        size_t *lens = zcalloc(sizeof(size_t) * argc);
        args[0] = "CLUSTER";
        args[1] = "SYNCSLOTS";
        args[2] = "RANGES";
        lens[0] = strlen("CLUSTER");
        lens[1] = strlen("SYNCSLOTS");
        lens[2] = strlen("RANGES");

        size_t i = 3;
        listNode *ln;
        listIter li;
        listRewind(task->slot_ranges, &li);
        while ((ln = listNext(&li)) != NULL) {
            SlotRange *sr = ln->value;
            args[i] = sdscatprintf(sdsempty(), "%d", sr->first);
            lens[i] = sdslen(args[i]);
            args[i+1] = sdscatprintf(sdsempty(), "%d", sr->last);
            lens[i+1] = sdslen(args[i+1]);
            i += 2;
        }
        serverAssert(i == argc);

        /* Send command to source node */
        err = sendCommandArgv(conn, argc, args, lens);
        for (size_t j = 3; j < argc; j++) {
            sdsfree(args[j]);
        }
        zfree(args);
        zfree(lens);
        if (err) goto write_error;

        sds slot_rages_str = createSlotRangesStr(task->slot_ranges);
        serverLog(LL_NOTICE,
            "Sent CLUSTER SYNCSLOTS RANGES command to source node %s, ranges: %s",
            task->source_node->name, slot_rages_str);
        sdsfree(slot_rages_str);
        return;
    }

    if (task->main_channel_state == ASM_SYNCSLOTS_REPLY) {
        /* Read reply */
        err = receiveSynchronousResponse(conn);
        /* The Source node did not reply */
        if (err == NULL) goto no_response_error;

        /* Check `+RDBCHANNELSYNCSLOTS client-id` reply */
        if (!strncmp(err, "+RDBCHANNELSYNCSLOTS", strlen("+RDBCHANNELSYNCSLOTS"))) {
            /* Parse main channel id */
            char *client_id = strchr(err,' ');
            if (client_id) client_id++;
            if (!client_id) {
                serverLog(LL_WARNING,
                            "Source replied with wrong +RDBCHANNELSYNC syntax: %s", err);
                sdsfree(err);
                goto error;
            }
            task->main_channel_id = strtoll(client_id, NULL, 10);
            serverLog(LL_NOTICE,
                "Source replied to RDBCHANNELSYNCSLOTS, sync slots can continue...");
    
            sdsfree(err);
            err = NULL;
            task->main_channel_state = ASM_INIT_RDBCHANNEL ;
        } else {
            serverLog(LL_WARNING,"Error reply to SYNCSLOTS RANGES from Source: '%s'",err);
            sdsfree(err);
            goto error;
        }
    }

    if (task->main_channel_state == ASM_INIT_RDBCHANNEL) {
        /* Create RDB connection */
        task->rdb_channel_con = connCreate(server.el, connTypeOfReplication());
        if (connConnect(task->rdb_channel_con, task->source_node->ip,
                        task->source_node->tcp_port, server.bind_source_addr,
                        rdbChannelSyncWithSource) == C_ERR)
        {
            serverLog(LL_WARNING, "Unable to connect to source node: %s",
                      connGetLastError(task->rdb_channel_con));
            goto error;
        }
        task->rdb_channel_state  = ASM_CONNECTING;
        connSetPrivateData(task->rdb_channel_con, task);

        /* Main channel waits for new events */
        connSetReadHandler(conn, NULL);
        return;
    }
    return;

no_response_error:
    serverLog(LL_WARNING, "Source node did not respond to command during SYNC handshake");
    /* Fall through to regular error handling */

error:
    connClose(conn);
    task->main_channel_con = NULL;
    task->main_channel_state = ASM_NONE;
    return;

write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING,"Sending command to Source: %s", err);
    sdsfree(err);
    goto error;
}

/* CLUSTER SYNCSLOTS SNAPSHOT-EOF
 *
 * This command is sent by the source node to the destination node to indicate
 * that the slots snapshot has ended. */
void clusterSyncSlotsSnapshotEOF(client *c) {
    /* This client is RDB channel connection. */
    asmTask *task = c->task;
    serverAssert(task->rdb_channel_state == ASM_RDBCHANNEL_TRANSFER);
    task->rdb_channel_state = ASM_RDBCHANNEL_DONE;
    serverLog(LL_NOTICE,
        "RDB channel snapshot transfer done for task");

    task->main_channel_state = ASM_REPLAY_STREAM;
  
    client *main_channel_client = createClient(task->main_channel_con);
    main_channel_client->flags |= CLIENT_MASTER;
    main_channel_client->querybuf = sdsempty();
    main_channel_client->authenticated = 1;
    main_channel_client->user = NULL;
    
    /* Replay stream. */
    // replayStream()
    serverLog(LL_NOTICE, "Replaying stream for task is done");

    /* Set the task for the client, and continue to chat */
    main_channel_client->task = task;
    connSetPrivateData(task->main_channel_con, main_channel_client);
    connSetReadHandler(task->main_channel_con, readQueryFromClient);

    /* ACK offset during replaying buffer stream, fake offset. */
    sendCommand(task->main_channel_con, "CLUSTER", "SYNCSLOTS", "ACK", "123", NULL);
    /* Free the RDB channel connection. */
    c->task = NULL;
    c->flags &= ~CLIENT_MASTER;
    freeClientAsync(c); /* Free the client, it is no longer needed. */
}

int clusterNodeSetSlotBit(clusterNode *n, int slot);
void clusterSendUpdate(clusterLink *link, clusterNode *node);
int clusterBumpConfigEpochWithoutConsensus(void);
void clusterSaveConfigOrDie(int do_fsync);

/* CLUSTER SYNCSLOTS STREAM-EOF
 *
 * This command is sent by the source node to the destination node to indicate
 * that the slot sync stream has ended and the slots can be handed off. */
void clusterSyncSlotsStreamEOF(client *c) {
    asmTask *task = c->task;
    if (task->main_channel_state != ASM_REPLAY_STREAM) {
        serverLog(LL_WARNING, "Unexpected CLUSTER SYNCSLOTS STREAM-EOF state: %d",
                               task->main_channel_state);
        return;
    }
    serverLog(LL_NOTICE, "CLUSTER SYNCSLOTS STREAM-EOF received");
    
    /* Iterate task->slot_range, and hand the ownership of slots */
    task->main_channel_state = ASM_SLOTS_HANDOFF;
    listIter li;
    listNode *ln;
    listRewind(task->slot_ranges, &li);
    while ((ln = listNext(&li)) != NULL) {
        SlotRange *sr = ln->value;
        for (int i = sr->first; i <= sr->last; i++) {
            clusterNode *myself = getMyClusterNode();
            server.cluster->slots[i] = myself;
            clusterNodeSetSlotBit(myself, i);
        }
    }
    /* New config and Bump new config */
    clusterBumpConfigEpochWithoutConsensus();
    clusterSendUpdate(task->source_node->link, getMyClusterNode());
    clusterSaveConfigOrDie(1);

    sds slot_ranges_str = createSlotRangesStr(task->slot_ranges);
    serverLog(LL_NOTICE, "Slot ranges: %s handed off", slot_ranges_str);
    sdsfree(slot_ranges_str);
    task->main_channel_state = ASM_DONE;

    /* Free the task */
    listDelNode(server.cluster->asm_links, listSearchKey(server.cluster->asm_links, task));
    listRelease(task->slot_ranges); /* Free the slot ranges list */
    sdsfree(task->state); /* Free the state string */
    zfree(task); /* Free the task itself */
    serverLog(LL_NOTICE, "Slot migration task completed");

    /* Free the main channel connection. */
    c->task = NULL;
    c->flags &= ~CLIENT_MASTER;
    freeClientAsync(c); /* Free the client, it is no longer needed. */
}

/* Start the sync slots task. */
void asmStartSyncSlots(asmTask *task) {
    if (task->main_channel_state != ASM_NONE) {
        return;
    }

   task->main_channel_con = connCreate(server.el, connTypeOfReplication());
   if (connConnect(task->main_channel_con, task->source_node->ip, task->source_node->tcp_port,
                   server.bind_source_addr, syncWithSource) == C_ERR)
    {
        serverLog(LL_WARNING,"Unable to connect to source node: %s",
                    connGetLastError(task->main_channel_con));
        connClose(task->main_channel_con);
        task->main_channel_con = NULL;
        return;
    }
    connSetPrivateData(task->main_channel_con, task);
    task->main_channel_state = ASM_CONNECTING;
}

int startBgsaveForReplication(int mincapa, int req);
void createReplicationBacklogIfNeeded(void);

void clusterSyncSlotsCommand(client *c) {
    if (!strcasecmp(c->argv[2]->ptr, "ranges") && c->argc >= 5) {
        /* CLUSTER SYNCSLOTS RANGES <start-slot> <end-slot> [<start-slot> <end-slot>] */
        int j, first, last;
        if (c->argc % 2 == 0) {
            addReplyErrorArity(c);
            return;
        }

        list *slot_ranges = listCreate();
        listSetFreeMethod(slot_ranges, zfree);

        for (j = 3; j < c->argc; j += 2) {
            if ((first = getSlotOrReply(c, c->argv[j])) == -1 ||
                (last = getSlotOrReply(c, c->argv[j + 1])) == -1) 
            {
                listRelease(slot_ranges);
                return;
            }
            if (first > last) {
                listRelease(slot_ranges);
                addReplyErrorFormat(c, "start slot number %d is greater than end slot number %d", first, last);
                return;
            }
            SlotRange *sr = zmalloc(sizeof(*sr));
            sr->first = first;
            sr->last = last;
            listAddNodeTail(slot_ranges, sr);
        }

        /* Only one slots sync on source node */
        if (listLength(server.cluster->asm_links) != 0) {
            addReplyError(c, "SYNCSLOTS RANGES already in progress");
            listRelease(slot_ranges);
            return;
        }

        /* Create the migrate slots task */
        asmTask *task = zcalloc(sizeof(*task));
        task->slot_ranges = slot_ranges;
        task->main_channel_id = c->id;
        task->state = sdsnew("inprogress");
        task->operation = ASM_MIGRATE;
        listAddNodeTail(server.cluster->asm_links, task);
        addReplyStatusFormat(c, "RDBCHANNELSYNCSLOTS %llu",
                               (unsigned long long) c->id);
    } else if (!strcasecmp(c->argv[2]->ptr, "rdbchannel") && c->argc == 4) {
        /* CLUSTER SYNCSLOTS RDBCHANNEL <client-id> */
        long long client_id;

        if (getLongLongFromObjectOrReply(c, c->argv[3], &client_id, NULL) != C_OK) {
            return;
        }

        
        if (listLength(server.cluster->asm_links) == 0) {
            addReplyError(c, "No migrate slots task in progress");
            return;
        }

        asmTask *task = listNodeValue(listFirst(server.cluster->asm_links));
        serverAssert(task->operation == ASM_MIGRATE);
        if (task->main_channel_id != client_id) {
            addReplyErrorFormat(c, "Export slots task client id mismatch");
            return;
        }

        c->slave_capa |= SLAVE_CAPA_EOF;
        c->slave_req |= SLAVE_REQ_SLOTS_SNAPSHOT;
        c->slave_req |= SLAVE_REQ_RDB_CHANNEL;
        c->flags |= CLIENT_REPL_RDB_CHANNEL;
        c->flags |= CLIENT_REPL_RDBONLY;
        c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
        if (server.repl_disable_tcp_nodelay)
            connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */
        c->repldbfd = -1;
        c->flags |= CLIENT_SLAVE;
        listAddNodeTail(server.slaves, c);
        /* Create the replication backlog if needed. */
        createReplicationBacklogIfNeeded();

        if (!hasActiveChildProcess()) {
            startBgsaveForReplication(c->slave_capa, c->slave_req);
        } else {
            serverLog(LL_NOTICE, "BGSAVE for slots snapshot sync delayed");
        }
    } else if (!strcasecmp(c->argv[2]->ptr, "snapshot-eof") && c->argc == 3) {
        /* CLUSTER SYNCSLOTS SNAPSHOT-EOF */
        clusterSyncSlotsSnapshotEOF(c);
    } else if (!strcasecmp(c->argv[2]->ptr, "stream-eof") && c->argc == 3) {
        /* CLUSTER SYNCSLOTS STREAM-EOF */
        clusterSyncSlotsStreamEOF(c);
    } else if (!strcasecmp(c->argv[2]->ptr, "ack") && c->argc == 4) {
        /* CLUSTER SYNCSLOTS ACK <offset> */
        long long offset;
        if ((getLongLongFromObject(c->argv[3], &offset) != C_OK))
            return;
        serverLog(LL_NOTICE, "CLUSTER SYNCSLOTS ACK received, offset: %lld", offset);
        /* --- For destination ---- */
        if (c->task) {
            /* This is a main channel connection, and we are replaying stream. */
            asmTask *task = c->task;
            if (task->main_channel_state == ASM_REPLAY_STREAM) {
                /* Update the source offset*/
                if (task->source_offset > offset) {
                    serverLog(LL_WARNING, "CLUSTER SYNCSLOTS ACK received, but offset %lld is less than the current source offset %lld",
                              offset, task->source_offset);
                    return;
                }
                task->source_offset = offset;
                serverLog(LL_NOTICE, "CLUSTER SYNCSLOTS ACK received, offset: %lld, updated source offset to %lld",
                          offset, task->source_offset);
            }
            return;
        }

        /* --- For source ---- */
        /* Update the destination offset*/
        if (listLength(server.cluster->asm_links) == 0) {
            serverLog(LL_WARNING, "CLUSTER SYNCSLOTS ACK received, but no task in progress");
            return;
        }
        asmTask *task = listNodeValue(listFirst(server.cluster->asm_links));
        if (task->dest_offset > offset) {
            serverLog(LL_WARNING, "CLUSTER SYNCSLOTS ACK received, but offset %lld is less than the current destination offset %lld",
                      offset, task->dest_offset);
            return;
        }
        task->dest_offset = offset;
        serverLog(LL_NOTICE, "CLUSTER SYNCSLOTS ACK received, offset: %lld, updated destination offset to %lld",
                  offset, task->dest_offset);
        /* Pause write if needed */
        /* Drain all slot ranges command stream */
        /* Send STREAM EOF */
        sendCommand(c->conn, "CLUSTER", "SYNCSLOTS", "STREAM-EOF", NULL);

        listRelease(task->slot_ranges);
        zfree(task->state); /* Free the state string */
        listDelNode(server.cluster->asm_links, listFirst(server.cluster->asm_links));
        freeClientAsync(c); /* Free the client, it is no longer needed. */
    } else if (!strcasecmp(c->argv[2]->ptr, "fail") && c->argc == 4) {
        /* CLUSTER SYNCSLOTS FAIL <err> */
        return; /* This is a no-op, just to handle the command syntax. */
    } else if (!strcasecmp(c->argv[2]->ptr, "conf") && c->argc >= 5) {
        /* CLUSTER SYNCSLOTS CONF <option> <value> [<option> <value>] */
        for (int j = 3; j < c->argc; j += 2) {
            if (j + 1 >= c->argc) {
                addReplyErrorArity(c);
                return;
            }
            /* Handle each option here */
            if (!strcasecmp(c->argv[j]->ptr, "option1")) {
                /* Handle option 'snapshot' */
                long value1 = 0;
                if (getRangeLongFromObjectOrReply(c, c->argv[j+1],
                                    0, 1, &value1, NULL) != C_OK)
                {
                    return;
                }
                addReply(c, shared.ok);
            } else {
                addReplyErrorFormat(c, "Unknown option %s", (char *)c->argv[j]->ptr);
            }
        }
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
    }
}

void createDumpPayload(rio *payload, robj *o, robj *key, int dbid);

int slotRangesSnapshotSaveRio(int req, rio *rdb, int *error) {
    UNUSED(error);
    serverAssert(req & SLAVE_REQ_SLOTS_SNAPSHOT);

    dictEntry *de;
    kvstoreDictIterator *kvs_di = NULL;

    for (int j = 0; j < server.dbnum; j++) {
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        redisDb *db = server.db + j;
        if (kvstoreSize(db->keys) == 0) continue;

        /* SELECT the new DB */
        if (rioWrite(rdb,selectcmd,sizeof(selectcmd)-1) == 0) goto werr;
        if (rioWriteBulkLongLong(rdb, j) == 0) goto werr;

        /* Iterate all slot ranges, and generate the DUMP encoded
         * representation of each key in the DB. */
        listIter li;
        listNode *ln;
        asmTask *task = listNodeValue(listFirst(server.cluster->asm_links));
        listRewind(task->slot_ranges, &li);
        while ((ln = listNext(&li)) != NULL) {
            SlotRange *sr = listNodeValue(ln);
            /* Iterate all keys in the slot range */
            for (int i = sr->first; i <= sr->last; i++) {
                kvs_di = kvstoreGetDictIterator(server.db->keys, i);
                while ((de = kvstoreDictIteratorNext(kvs_di)) != NULL) {
                    /* Get the value object (of type kvobj) */
                    kvobj *o = dictGetKV(de);

                    /* Get the expire time */
                    long long expiretime = kvobjGetExpire(o);

                    /* Set on stack string object for key */
                    robj key;
                    initStaticStringObject(key, kvobjGetKey(o));

                    if (rioWriteBulkCount(rdb, '*', 5) == 0) goto werr;
                    if (rioWriteBulkString(rdb, "RESTORE", 7) == 0) goto werr;
                    if (rioWriteBulkObject(rdb, &key) == 0) goto werr;
                    if (rioWriteBulkLongLong(rdb, expiretime == -1 ? 0 : expiretime) == 0) goto werr;

                    /* Create the DUMP encoded representation. */
                    rio payload;
                    createDumpPayload(&payload, o, &key, j);
                    sds buf = payload.io.buffer.ptr;
                    if (rioWriteBulkString(rdb, buf, sdslen(buf)) == 0) {
                        sdsfree(payload.io.buffer.ptr);
                        goto werr;
                    }
                    sdsfree(payload.io.buffer.ptr);

                    /* Write ABSTTL */
                    if (rioWriteBulkString(rdb, "ABSTTL", 6) == 0) goto werr;
                }
                kvstoreReleaseDictIterator(kvs_di);
                kvs_di = NULL;
            }
        }
    }

    /* Write the end of the snapshot file command */
    if (rioWriteBulkCount(rdb, '*', 3) == 0) goto werr;
    if (rioWriteBulkString(rdb, "CLUSTER", 7) == 0) goto werr;
    if (rioWriteBulkString(rdb, "SYNCSLOTS", 9) == 0) goto werr;
    if (rioWriteBulkString(rdb, "SNAPSHOT-EOF", 12) == 0) goto werr;
    return C_OK;

werr:
    if (kvs_di) kvstoreReleaseDictIterator(kvs_di);
    return C_ERR;
}
