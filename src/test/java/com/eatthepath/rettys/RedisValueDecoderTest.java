package com.eatthepath.rettys;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RedisValueDecoderTest {

    private RedisValueDecoder redisValueDecoder;

    @BeforeEach
    void beforeEach() {
        redisValueDecoder = new RedisValueDecoder();
    }

    @ParameterizedTest
    @MethodSource("redisValueProvider")
    void decode(final String frameString, Object expectedValue) throws Exception {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(frameString.getBytes(StandardCharsets.US_ASCII));

        try {
            final List<Object> out = new ArrayList<>();
            redisValueDecoder.decode(null, byteBuf, out);

            assertEquals(1, out.size());

            final Object redisValue = out.get(0);

            // We need to do some special-case comparisons for primitive arrays
            if (expectedValue instanceof byte[]) {
                assertArrayEquals((byte[]) expectedValue, (byte[]) redisValue);
            } else if (expectedValue instanceof Object[]) {
                assertArrayEquals((Object[]) expectedValue, (Object[]) redisValue);
            } else {
                assertEquals(expectedValue, redisValue);
            }
        } finally {
            byteBuf.release();
        }
    }

    static Stream<Arguments> redisValueProvider() {
        return Stream.of(
                arguments("+OK\r\n", "OK"),
                arguments("-Error message\r\n", new RedisException("Error message")),
                arguments(":1000\r\n", 1000L),
                arguments("$6\r\nfoobar\r\n", "foobar".getBytes(StandardCharsets.US_ASCII)),
                arguments("$0\r\n\r\n", new byte[0]),
                arguments("$-1\r\n", null),
                arguments("*0\r\n", new Object[0]),
                arguments("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", new Object[] { "foo".getBytes(StandardCharsets.US_ASCII), "bar".getBytes(StandardCharsets.US_ASCII) }),
                arguments("*-1\r\n", null)
        );
    }
}