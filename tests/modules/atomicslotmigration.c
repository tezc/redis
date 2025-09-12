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


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "asm", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "asm.ismyslot", test_keyslot, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
