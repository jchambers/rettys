package com.eatthepath.rettys;

import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

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

    @Override
    public CompletableFuture<Long> subscribe(final BiConsumer<String, Object> messageHandler, final String... channels) {
        return executeCommand(RedisCommandFactory.buildSubscribeCommand(channels));
    }

    @Override
    public CompletableFuture<Long> unsubscribe(final BiConsumer<String, Object> messageHandler, final String... channels) {
        return executeCommand(RedisCommandFactory.buildUnsubscribeCommand(channels));
    }

    @Override
    public CompletableFuture<Long> psubscribe(final BiConsumer<String, Object> messageHandler, final String... patterns) {
        return executeCommand(RedisCommandFactory.buildPsubscribeCommand(patterns));
    }

    @Override
    public CompletableFuture<Long> punsubscribe(final BiConsumer<String, Object> messageHandler, final String... patterns) {
        return executeCommand(RedisCommandFactory.buildPunsubscribeCommand(patterns));
    }
}
