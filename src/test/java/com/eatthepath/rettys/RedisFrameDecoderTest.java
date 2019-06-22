package com.eatthepath.rettys;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RedisFrameDecoderTest {

    private RedisFrameDecoder redisFrameDecoder;

    @BeforeEach
    void beforeEach() {
        redisFrameDecoder = new RedisFrameDecoder();
    }

    @Test
    void decode() throws Exception {
        final ByteBuf in = Unpooled.wrappedBuffer("+OK\r\n-Error message\r\n".getBytes(StandardCharsets.US_ASCII));
        final List<Object> out = new ArrayList<>();

        final List<ByteBuf> expectedOutput = Arrays.asList(
                Unpooled.wrappedBuffer("+OK\r\n".getBytes(StandardCharsets.US_ASCII)),
                Unpooled.wrappedBuffer("-Error message\r\n".getBytes(StandardCharsets.US_ASCII)));

        try {
            redisFrameDecoder.decode(null, in, out);

            assertEquals(expectedOutput, out);
        } finally {
            in.release();

            for (final ByteBuf byteBuf : expectedOutput) {
                byteBuf.release();
            }
        }
    }

    @Test
    void decodeInsufficientData() throws Exception {
        final ByteBuf in = Unpooled.wrappedBuffer("+OK".getBytes(StandardCharsets.US_ASCII));
        final List<Object> out = new ArrayList<>();

        try {
            redisFrameDecoder.decode(null, in, out);
            assertTrue(out.isEmpty());
        } finally {
            in.release();
        }
    }

    @Test
    void decodeBogusData() {
        final ByteBuf in = Unpooled.wrappedBuffer("Illegal Redis frame".getBytes(StandardCharsets.US_ASCII));
        final List<Object> out = new ArrayList<>();

        try {
            assertThrows(IOException.class, () -> redisFrameDecoder.decode(null, in, out));
        } finally {
            in.release();
        }
    }

    @ParameterizedTest
    @MethodSource("redisFrameStringProvider")
    void getLengthOfNextFrame(final String frameString, final int expectedFrameLength) throws IOException {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(frameString.getBytes(StandardCharsets.US_ASCII));

        try {
            assertEquals(expectedFrameLength, redisFrameDecoder.getLengthOfNextFrame(byteBuf));
        } finally {
            byteBuf.release();
        }
    }

    static Stream<Arguments> redisFrameStringProvider() {
        return Stream.of(
                arguments("+OK\r\n", 5),
                arguments("-Error message\r\n", 16),
                arguments(":1000\r\n", 7),
                arguments("$6\r\nfoobar\r\n", 12),
                arguments("$0\r\n\r\n", 6),
                arguments("$-1\r\n", 5),
                arguments("*0\r\n", 4),
                arguments("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", 22),
                arguments("*-1\r\n", 5)
        );
    }

    @Test
    void getLengthOfNextFrameInsufficientData() {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer("+OK".getBytes(StandardCharsets.US_ASCII));

        try {
            assertThrows(IndexOutOfBoundsException.class, () -> redisFrameDecoder.getLengthOfNextFrame(byteBuf));
        } finally {
            byteBuf.release();
        }
    }

    @Test
    void getLengthOfNextFrameBogusData() {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer("Illegal Redis frame".getBytes(StandardCharsets.US_ASCII));

        try {
            assertThrows(IOException.class, () -> redisFrameDecoder.getLengthOfNextFrame(byteBuf));
        } finally {
            byteBuf.release();
        }
    }
}