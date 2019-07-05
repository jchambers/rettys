package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;

/**
 * A Redis command executor is responsible for executing individual Redis commands. While some commands may only make
 * sense in certain contexts ({@code DISCARD}, for example, only makes sense in the context of a transaction), a
 * command executor imposes no restrictions and is responsible only for sending commands and reporting the results.
 */
public interface RedisCommandExecutor {

    CompletableFuture<Long> llen(final Object key);

    CompletableFuture<Long> memoryUsage(final byte[] key);

    CompletableFuture<Void> multi();

    CompletableFuture<ScanResponse> scan(final Object cursor);

    CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern);

    CompletableFuture<ScanResponse> scan(final Object cursor, final long count);

    CompletableFuture<ScanResponse> scan(final Object cursor, final String matchPattern, final long count);
}
