package com.eatthepath.rettys;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
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

        final Function<Long, ScanResponse> scanResponseFunction = (cursor) -> {
            final ScanResponse scanResponse;

            if (cursor < keys.size()) {
                scanResponse = new ScanResponse(cursor + 1, keys.get(cursor.intValue()));
            } else {
                scanResponse = new ScanResponse(0, new byte[0][]);
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