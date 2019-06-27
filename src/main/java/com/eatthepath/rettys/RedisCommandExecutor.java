package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;

/**
 * A Redis command executor is responsible for executing individual Redis commands. While some commands may only make
 * sense in certain contexts ({@code DISCARD}, for example, only makes sense in the context of a transaction), a
 * command executor imposes no restrictions and is responsible only for sending commands and reporting the results.
 */
public interface RedisCommandExecutor {

    byte[] INITIAL_SCAN_CURSOR = new byte[] { '0' };

    <T> CompletableFuture<T> executeCommand(final RedisCommand<T> command);

    default CompletableFuture<Long> llen(final String key) {
        return executeCommand(RedisCommandFactory.buildLlenCommand(key));
    };

    default CompletableFuture<Long> memoryUsage(final byte[] key) {
        return executeCommand(RedisCommandFactory.buildMemoryUsageCommand(key));
    }

    default CompletableFuture<Void> multi() {
        return executeCommand(RedisCommandFactory.buildMultiCommand());
    }

    default CompletableFuture<ScanResponse> scan(byte[] cursor) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor));
    }

    default CompletableFuture<ScanResponse> scan(byte[] cursor, String matchPattern) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern));
    }

    default CompletableFuture<ScanResponse> scan(byte[] cursor, long count) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, count));
    }

    default CompletableFuture<ScanResponse> scan(byte[] cursor, String matchPattern, long count) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, count));
    }
}
