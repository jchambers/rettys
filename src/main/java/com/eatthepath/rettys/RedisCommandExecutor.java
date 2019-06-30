package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * A Redis command executor is responsible for executing individual Redis commands. While some commands may only make
 * sense in certain contexts ({@code DISCARD}, for example, only makes sense in the context of a transaction), a
 * command executor imposes no restrictions and is responsible only for sending commands and reporting the results.
 */
public interface RedisCommandExecutor {

    CompletableFuture<Long> llen(Object key);

    CompletableFuture<Long> memoryUsage(byte[] key);

    CompletableFuture<Void> multi();

    CompletableFuture<ScanResponse> scan(Object cursor);

    CompletableFuture<ScanResponse> scan(Object cursor, String matchPattern);

    CompletableFuture<ScanResponse> scan(Object cursor, long count);

    CompletableFuture<ScanResponse> scan(Object cursor, String matchPattern, long count);

    CompletableFuture<Long> subscribe(BiConsumer<String, Object> messageHandler, String... channels);

    CompletableFuture<Long> unsubscribe(BiConsumer<String, Object> messageHandler, String... channels);

    CompletableFuture<Long> psubscribe(BiConsumer<String, Object> messageHandler, String... patterns);

    CompletableFuture<Long> punsubscribe(BiConsumer<String, Object> messageHandler, String... patterns);
}
