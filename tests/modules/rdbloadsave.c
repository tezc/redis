#include "redismodule.h"
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <memory.h>
#include <errno.h>



int sanity(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_RdbLoad(NULL, 0) == REDISMODULE_OK || errno != EINVAL) {
        RedisModule_ReplyWithError(ctx, "ERR null filename should fail");
        return REDISMODULE_OK;
    }

    if (RedisModule_RdbLoad("dump.rdb", 1) == REDISMODULE_OK || errno != EINVAL) {
        RedisModule_ReplyWithError(ctx, "ERR invalid flags should fail");
        return REDISMODULE_OK;
    }

    if (RedisModule_RdbSave(NULL, 0) == REDISMODULE_OK || errno != EINVAL) {
        RedisModule_ReplyWithError(ctx, "ERR null filename should fail");
        return REDISMODULE_OK;
    }

    if (RedisModule_RdbSave("dump.rdb", 1) == REDISMODULE_OK || errno != EINVAL) {
        RedisModule_ReplyWithError(ctx, "ERR invalid flags should fail");
        return REDISMODULE_OK;
    }

    if (RedisModule_RdbLoad("dump.rdb", 0) == REDISMODULE_OK || errno != ENOENT) {
        RedisModule_ReplyWithError(ctx, "ERR missing file should fail");
        return REDISMODULE_OK;
    }

    if (RedisModule_RdbSave("dump.rdb", 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_ReplyWithError(ctx, "ERR rdbsave failed");
        return REDISMODULE_OK;
    }

    if (RedisModule_RdbLoad("dump.rdb", 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_ReplyWithError(ctx, "ERR rdbload failed");
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int cmd_rdbsave(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleBlockedClient *bc = NULL;
    RedisModuleCtx *reply_ctx = ctx;

    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long blocking = 0;
    if (RedisModule_StringToLongLong(argv[1], &blocking) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Invalid integer value");
        return REDISMODULE_OK;
    }

    size_t len;
    const char *filename = RedisModule_StringPtrLen(argv[2], &len);

    char tmp[len + 1];
    memcpy(tmp, filename, len);
    tmp[len] = '\0';

    if (blocking) {
         bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
         reply_ctx = RedisModule_GetThreadSafeContext(bc);
    }

    if (RedisModule_RdbSave(tmp, 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_ReplyWithError(reply_ctx, "ERR rdbsave failed");
        goto out;
    }

    RedisModule_ReplyWithSimpleString(reply_ctx, "OK");

out:
    if (blocking) {
        RedisModule_FreeThreadSafeContext(reply_ctx);
        RedisModule_UnblockClient(bc, NULL);
    }
    return REDISMODULE_OK;
}

int cmd_rdbload(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleBlockedClient *bc = NULL;
    RedisModuleCtx *reply_ctx = ctx;

    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long blocking = 0;
    if (RedisModule_StringToLongLong(argv[1], &blocking) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Invalid integer value");
        return REDISMODULE_OK;
    }

    size_t len;
    const char *filename = RedisModule_StringPtrLen(argv[2], &len);

    char tmp[len + 1];
    memcpy(tmp, filename, len);
    tmp[len] = '\0';

    printf("blocking %lld\n", blocking);

    if (blocking) {
        bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        reply_ctx = RedisModule_GetThreadSafeContext(bc);
    }

    if (RedisModule_RdbLoad(tmp, 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_ReplyWithError(reply_ctx, "ERR rdbload failed");
        goto out;
    }

    RedisModule_ReplyWithSimpleString(reply_ctx, "OK");

out:
    if (blocking) {
        RedisModule_FreeThreadSafeContext(reply_ctx);
        RedisModule_UnblockClient(bc, NULL);
    }
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"rdbloadsave",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    /* Test basics. */
    if (RedisModule_CreateCommand(ctx, "test.sanity", sanity, "", 0, 0, 0)
                                  == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "test.blocked_client_rdbsave",
                                  cmd_rdbsave, "", 0, 0, 0)
                                  == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "test.blocked_client_rdbload",
                                  cmd_rdbload, "", 0, 0, 0)
                                  == REDISMODULE_ERR) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
