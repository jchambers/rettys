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
class ScanSpliterator implements Spliterator<String> {

    private final Function<byte[], ScanResponse> scanResponseFunction;

    private ScanResponse scanResponse;
    private Deque<String> keys = new ArrayDeque<>();

    private static final byte[] INITIAL_SCAN_CURSOR_BYTES = new byte[] { '0' };

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
    public boolean tryAdvance(final Consumer<? super String> action) {

        while (keys.isEmpty() && (scanResponse == null || !Arrays.equals(INITIAL_SCAN_CURSOR_BYTES, scanResponse.getCursor()))) {
            scanResponse = scanResponseFunction.apply(scanResponse == null ? INITIAL_SCAN_CURSOR_BYTES : scanResponse.getCursor());

            for (final String key : scanResponse.getKeys()) {
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
    public Spliterator<String> trySplit() {
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
