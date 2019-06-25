package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;

/**
 * A Redis command executor is responsible for executing individual Redis commands. While some commands may only make
 * sense in certain contexts ({@code DISCARD}, for example, only makes sense in the context of a transaction), a
 * command executor imposes no restrictions and is responsible only for sending commands and reporting the results.
 */
public interface RedisCommandExecutor {

    <T> CompletableFuture<T> executeCommand(final RedisCommand<T> command);

    default CompletableFuture<Long> llen(final String key) {
        return executeCommand(RedisCommandFactory.buildLlenCommand(key));
    };

    default CompletableFuture<Long> memoryUsage(final byte[] key) {
        return executeCommand(RedisCommandFactory.buildMemoryUsageCommand(key));
    }
}
