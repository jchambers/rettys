package com.eatthepath.rettys;

import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

abstract class RedisCommandExecutorAdapter implements RedisCommandExecutor {

    abstract <T> CompletableFuture<T> executeCommand(RedisCommand<T> command);

    abstract Charset getCharset();

    @Override
    public CompletableFuture<Void> auth(final String password) {
        return executeCommand(new RedisCommand<>(RedisResponseConverters.VOID_CONVERTER,
                RedisKeyword.AUTH,
                password));
    }

    @Override
    public CompletableFuture<Long> llen(final Object key) {
        return executeCommand(new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER,
                RedisKeyword.LLEN,
                key));
    };

    @Override
    public CompletableFuture<Long> memoryUsage(final Object key) {
        return executeCommand(new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER,
                RedisKeyword.MEMORY,
                RedisKeyword.USAGE,
                key));
    }

    @Override
    public CompletableFuture<Void> multi() {
        return executeCommand(new RedisCommand<>(RedisResponseConverters.VOID_CONVERTER,
                RedisKeyword.MULTI));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor) {
        return executeCommand(new RedisCommand<>(ScanResponse.scanResponseConverter(getCharset()),
                RedisKeyword.SCAN,
                cursor));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern) {
        return executeCommand(new RedisCommand<>(ScanResponse.scanResponseConverter(getCharset()),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor, final long count) {
        return executeCommand(new RedisCommand<>(ScanResponse.scanResponseConverter(getCharset()),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.COUNT,
                count));
    }

    @Override
    public CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern, final long count) {
        return executeCommand(new RedisCommand<>(ScanResponse.scanResponseConverter(getCharset()),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern,
                RedisKeyword.COUNT,
                count));
    }
}
