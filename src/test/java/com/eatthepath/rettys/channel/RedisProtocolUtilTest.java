package com.eatthepath.rettys.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RedisProtocolUtilTest {

    @ParameterizedTest
    @MethodSource("argumentsForTestReadInteger")
    void testReadInteger(final String redisIntegerString, final long expectedValue) {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(redisIntegerString.getBytes(StandardCharsets.US_ASCII));

        try {
            assertEquals(expectedValue, RedisProtocolUtil.readInteger(byteBuf));
        } finally {
            byteBuf.release();
        }
    }

    static Stream<Arguments> argumentsForTestReadInteger() {
        return Stream.of(
                arguments("0\r\n", 0),
                arguments("1\r\n", 1),
                arguments("-1\r\n", -1),
                arguments("9223372036854775807\r\n", Long.MAX_VALUE),
                arguments("-9223372036854775808\r\n", Long.MIN_VALUE));
    }
}