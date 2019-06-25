package com.eatthepath.rettys;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A scan spliterator iterates across the elements of a Redis scan command, issuing new scan commands as needed to get
 * request additional results. A scan spliterator presents the results of a scan command as a single, coherent stream of
 * Redis keys; callers should bear in mind that iterating across an entire key set returned by a scan command may
 * require numerous calls to the Redis server.
 */
class ScanSpliterator implements Spliterator<byte[]> {

    private final Function<byte[], ScanResponse> scanResponseFunction;

    private ScanResponse scanResponse;
    private Deque<byte[]> keys = new ArrayDeque<>();

    /**
     * Constructs a new spliterator that uses the given function to request additional scan results from the Redis
     * server.
     *
     * @param scanResponseFunction a function that takes a Redis cursor position as an argument and retrieves additional
     * scan results from the Redis server
     */
    ScanSpliterator(final Function<byte[], ScanResponse> scanResponseFunction) {
        this.scanResponseFunction = Objects.requireNonNull(scanResponseFunction, "Scan response function must not be null.");
    }

    @Override
    public boolean tryAdvance(final Consumer<? super byte[]> action) {

        while (keys.isEmpty() && (scanResponse == null || !Arrays.equals(scanResponse.getCursor(), RedisCommandExecutor.INITIAL_SCAN_CURSOR))) {
            scanResponse = scanResponseFunction.apply(scanResponse == null ? RedisCommandExecutor.INITIAL_SCAN_CURSOR : scanResponse.getCursor());

            for (final byte[] key : scanResponse.getKeys()) {
                keys.addLast(key);
            }
        }

        if (!keys.isEmpty()) {
            action.accept(keys.pollFirst());
            return true;
        }

        return false;
    }

    @Override
    public Spliterator<byte[]> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return DISTINCT | IMMUTABLE | NONNULL;
    }
}
