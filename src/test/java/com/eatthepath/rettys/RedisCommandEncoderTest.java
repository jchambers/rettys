package com.eatthepath.rettys;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RedisCommandEncoderTest {

    private RedisCommandEncoder redisCommandEncoder;

    @BeforeEach
    void beforeEach() {
        redisCommandEncoder = new RedisCommandEncoder();
    }

    @Test
    void encode() {
        final RedisCommand command = new RedisCommand(RedisCommand.CommandType.LLEN, "mylist");
        final ByteBuf expectedOutput =
                Unpooled.wrappedBuffer("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n".getBytes(StandardCharsets.US_ASCII));

        final ByteBuf out = Unpooled.buffer();

        try {
            redisCommandEncoder.encode(null, command, out);

            assertEquals(expectedOutput, out);
        } finally {
            expectedOutput.release();
            out.release();
        }
    }

    @ParameterizedTest
    @MethodSource("redisValueProvider")
    void getBulkStringBytes(final Object redisValue, final byte[] expectedBulkStringBytes) {
        assertArrayEquals(expectedBulkStringBytes, RedisCommandEncoder.getBulkStringBytes(redisValue));
    }

    static Stream<Arguments> redisValueProvider() {
        return Stream.of(
                arguments(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }),
                arguments("Test!", new byte[] { 'T', 'e', 's', 't', '!' }),
                arguments(12, new byte[] { '1', '2' }),
                arguments(3.5, new byte[] { '3', '.', '5' }));
    }
}