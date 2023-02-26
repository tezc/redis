#include "redismodule.h"

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <memory.h>
#include <errno.h>

/* Sanity tests to verify inputs and return values. */
int sanity(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModuleRdbStream *stream;

    /* NULL filename should fail. */
    if (RedisModule_RdbLoad(NULL, 0) == REDISMODULE_OK || errno != EINVAL) {
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    }

    /* Invalid flags should fail. */
    stream = RedisModule_RdbStreamCreateFromFile("dump.rdb");
    if (RedisModule_RdbLoad(stream, 1) == REDISMODULE_OK || errno != EINVAL) {
        RedisModule_RdbStreamFree(stream);
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    }
    RedisModule_RdbStreamFree(stream);

    /* Missing file should fail. */
    stream = RedisModule_RdbStreamCreateFromFile("dump.rdb");
    if (RedisModule_RdbLoad(stream, 0) == REDISMODULE_OK || errno != EIO) {
        RedisModule_RdbStreamFree(stream);
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    }
    RedisModule_RdbStreamFree(stream);

    /* Save RDB file. */
    stream = RedisModule_RdbStreamCreateFromFile("sanitytest.rdb");
    if (RedisModule_RdbSave(stream, 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_RdbStreamFree(stream);
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    }
    RedisModule_RdbStreamFree(stream);

    /* Load the saved RDB file. */
    stream = RedisModule_RdbStreamCreateFromFile("sanitytest.rdb");
    if (RedisModule_RdbLoad(stream, 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_RdbStreamFree(stream);
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    }
    RedisModule_RdbStreamFree(stream);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int cmd_rdbsave(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t len;
    const char *filename = RedisModule_StringPtrLen(argv[1], &len);

    char tmp[len + 1];
    memcpy(tmp, filename, len);
    tmp[len] = '\0';

    RedisModuleRdbStream *stream = RedisModule_RdbStreamCreateFromFile(tmp);

    if (RedisModule_RdbSave(stream, 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_ReplyWithError(ctx, strerror(errno));
        RedisModule_RdbStreamFree(stream);
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_RdbStreamFree(stream);
    return REDISMODULE_OK;
}

/* Fork before calling RM_RdbSave(). */
int cmd_rdbsave_fork(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t len;
    const char *filename = RedisModule_StringPtrLen(argv[1], &len);

    char tmp[len + 1];
    memcpy(tmp, filename, len);
    tmp[len] = '\0';

    int fork_child_pid = RedisModule_Fork(NULL, NULL);
    if (fork_child_pid < 0) {
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    } else if (fork_child_pid > 0) {
        /* parent */
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        return REDISMODULE_OK;
    }

    RedisModuleRdbStream *stream = RedisModule_RdbStreamCreateFromFile(tmp);

    int ret = 0;
    if (RedisModule_RdbSave(stream, 0) != REDISMODULE_OK) {
        ret = errno;
    }
    RedisModule_RdbStreamFree(stream);

    RedisModule_ExitFromChild(ret);
    return REDISMODULE_OK;
}

int cmd_rdbload(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t len;
    const char *filename = RedisModule_StringPtrLen(argv[1], &len);

    char tmp[len + 1];
    memcpy(tmp, filename, len);
    tmp[len] = '\0';

    RedisModuleRdbStream *stream = RedisModule_RdbStreamCreateFromFile(tmp);

    if (RedisModule_RdbLoad(stream, 0) != REDISMODULE_OK || errno != 0) {
        RedisModule_RdbStreamFree(stream);
        RedisModule_ReplyWithError(ctx, strerror(errno));
        return REDISMODULE_OK;
    }

    RedisModule_RdbStreamFree(stream);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "rdbloadsave", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "test.sanity", sanity, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "test.rdbsave", cmd_rdbsave, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "test.rdbsave_fork", cmd_rdbsave_fork, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "test.rdbload", cmd_rdbload, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
