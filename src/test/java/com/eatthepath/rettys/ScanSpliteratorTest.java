package com.eatthepath.rettys;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class ScanSpliteratorTest {

    @Test
    void tryAdvance() {
        final List<byte[][]> keys = Arrays.asList(
                new byte[][] { "First key".getBytes() },
                new byte[0][],
                new byte[][] { "Second key".getBytes() }
        );

        final Function<byte[], ScanResponse> scanResponseFunction = (cursor) -> {
            final ScanResponse scanResponse;

            final long cursorAsLong = Long.parseUnsignedLong(new String(cursor, StandardCharsets.US_ASCII), 10);

            if (cursorAsLong < keys.size()) {
                final byte[] nextCursor = String.valueOf(cursorAsLong + 1).getBytes(StandardCharsets.US_ASCII);
                scanResponse = new ScanResponse(nextCursor, keys.get((int) cursorAsLong));
            } else {
                scanResponse = new ScanResponse(RedisCommandExecutor.INITIAL_SCAN_CURSOR, new byte[0][]);
            }

            return scanResponse;
        };

        final ScanSpliterator scanSpliterator = new ScanSpliterator(scanResponseFunction);
        final AtomicInteger i = new AtomicInteger(0);

        final byte[][] expectedKeys = new byte[][] {
                "First key".getBytes(),
                "Second key".getBytes()
        };

        scanSpliterator.forEachRemaining((key) -> {
            assertArrayEquals(expectedKeys[i.getAndIncrement()], key);
        });

        assertEquals(expectedKeys.length, i.get());
    }

    @Test
    void trySplit() {
        assertNull(new ScanSpliterator((cursor -> null)).trySplit());
    }

    @Test
    void estimateSize() {
        assertEquals(Long.MAX_VALUE, new ScanSpliterator((cursor -> null)).estimateSize());
    }

    @Test
    void characteristics() {
        final ScanSpliterator scanSpliterator = new ScanSpliterator((cursor -> null));

        assertTrue((scanSpliterator.characteristics() & Spliterator.CONCURRENT) == 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.DISTINCT) != 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.ORDERED) == 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.SORTED) == 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.SIZED) == 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.NONNULL) != 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.IMMUTABLE) != 0);
        assertTrue((scanSpliterator.characteristics() & Spliterator.SUBSIZED) == 0);
    }
}