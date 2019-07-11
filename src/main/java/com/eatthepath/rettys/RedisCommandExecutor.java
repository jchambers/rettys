package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;

/**
 * A Redis command executor is responsible for executing individual Redis commands. While some commands may only make
 * sense in certain contexts ({@code DISCARD}, for example, only makes sense in the context of a transaction), a
 * command executor imposes no restrictions and is responsible only for sending commands and reporting the results.
 */
public interface RedisCommandExecutor {

    CompletableFuture<Void> auth(String password);

    CompletableFuture<Long> llen(Object key);

    CompletableFuture<Long> memoryUsage(Object key);

    CompletableFuture<Void> multi();

    CompletableFuture<ScanResponse> scan(Object cursor);

    CompletableFuture<ScanResponse> scan(Object cursor, String matchPattern);

    CompletableFuture<ScanResponse> scan(Object cursor, long count);

    CompletableFuture<ScanResponse> scan(Object cursor, String matchPattern, long count);
}
