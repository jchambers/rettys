package com.eatthepath.rettys;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class RedisRequestResponseHandlerTest {

    private RedisRequestResponseHandler requestResponseHandler;

    @BeforeEach
    void beforeEach() {
        requestResponseHandler = new RedisRequestResponseHandler();
    }

    @Test
    void testHandleRequestResponse() {
        final RedisCommand<Long> command = new RedisCommand<>(RedisResponseConverters.integerConverter(), RedisKeyword.LLEN, "test");

        final Channel channel = new EmbeddedChannel();
        final MockChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);

        final long redisResponse = 17;

        requestResponseHandler.write(channelHandlerContext, command, channel.newPromise());
        requestResponseHandler.channelRead(channelHandlerContext, redisResponse);

        final long futureResult = assertTimeoutPreemptively(Duration.ofSeconds(1), command.getFuture()::join);
        assertEquals(redisResponse, futureResult);
    }

    @Test
    void testHandleRequestErrorResponse() {
        final RedisCommand<Long> command = new RedisCommand<>(RedisResponseConverters.integerConverter(), RedisKeyword.LLEN, "test");

        final Channel channel = new EmbeddedChannel();
        final MockChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);

        final RedisException redisException = new RedisException("TEST Test exception");

        requestResponseHandler.write(channelHandlerContext, command, channel.newPromise());
        requestResponseHandler.channelRead(channelHandlerContext, redisException);

        final CompletionException completionException =
                assertThrows(CompletionException.class, () -> assertTimeoutPreemptively(Duration.ofSeconds(1), command.getFuture()::join));

        assertEquals(redisException, completionException.getCause());
    }

    @Test
    void testHandleRequestWriteFailure() {
        final RedisCommand<Long> command = new RedisCommand<>(RedisResponseConverters.integerConverter(), RedisKeyword.LLEN, "test");

        final Channel channel = new EmbeddedChannel();
        final MockChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);

        final ChannelPromise writePromise = channel.newPromise();

        requestResponseHandler.write(channelHandlerContext, command, writePromise);

        final IOException ioException = new IOException("A horribleness has befelsterred the children's academy.");

        writePromise.setFailure(ioException);

        final CompletionException completionException =
                assertThrows(CompletionException.class, () -> assertTimeoutPreemptively(Duration.ofSeconds(1), command.getFuture()::join));

        assertEquals(ioException, completionException.getCause());
    }
}