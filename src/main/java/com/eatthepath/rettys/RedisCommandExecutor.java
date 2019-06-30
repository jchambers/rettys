package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;

/**
 * A Redis command executor is responsible for executing individual Redis commands. While some commands may only make
 * sense in certain contexts ({@code DISCARD}, for example, only makes sense in the context of a transaction), a
 * command executor imposes no restrictions and is responsible only for sending commands and reporting the results.
 */
public interface RedisCommandExecutor {

    <T> CompletableFuture<T> executeCommand(RedisCommand<T> command);

    default CompletableFuture<Long> llen(final Object key) {
        return executeCommand(RedisCommandFactory.buildLlenCommand(key));
    };

    default CompletableFuture<Long> memoryUsage(final byte[] key) {
        return executeCommand(RedisCommandFactory.buildMemoryUsageCommand(key));
    }

    default CompletableFuture<Void> multi() {
        return executeCommand(RedisCommandFactory.buildMultiCommand());
    }

    default CompletableFuture<ScanResponse> scan(final Object cursor) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor));
    }

    default CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern));
    }

    default CompletableFuture<ScanResponse> scan(final Object cursor, final long count) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, count));
    }

    default CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern, final long count) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, count));
    }
}
