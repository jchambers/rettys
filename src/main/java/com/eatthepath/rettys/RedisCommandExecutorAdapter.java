package com.eatthepath.rettys;

import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

abstract class RedisCommandExecutorAdapter implements RedisCommandExecutor {

    abstract <T> CompletableFuture<T> executeCommand(RedisCommand<T> command);

    abstract Charset getCharset();

    @Override
    public CompletableFuture<Long> llen(final Object key) {
        return executeCommand(RedisCommandFactory.buildLlenCommand(key));
    };

    @Override
    public CompletableFuture<Long> memoryUsage(final byte[] key) {
        return executeCommand(RedisCommandFactory.buildMemoryUsageCommand(key));
    }

    @Override
    public CompletableFuture<Void> multi() {
        return executeCommand(RedisCommandFactory.buildMultiCommand());
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, getCharset()));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, getCharset()));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor, final long count) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, count, getCharset()));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern, final long count) {
        return executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, count, getCharset()));
    }
}
