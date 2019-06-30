package com.eatthepath.rettys;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RedisRequestResponseHandlerTest {

    private RedisRequestResponseHandler requestResponseHandler;

    @BeforeEach
    void beforeEach() {
        requestResponseHandler = new RedisRequestResponseHandler(StandardCharsets.UTF_8);
    }

    @Test
    void handleRequestResponse() {
        final RedisCommand<Long> command = RedisCommandFactory.buildLlenCommand("Test");

        final Channel channel = new EmbeddedChannel();
        final MockChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);

        final long redisResponse = 17;

        requestResponseHandler.write(channelHandlerContext, command, channel.newPromise());
        requestResponseHandler.channelRead(channelHandlerContext, redisResponse);

        final long futureResult = assertTimeoutPreemptively(Duration.ofSeconds(1), command.getFuture()::join);
        assertEquals(redisResponse, futureResult);
    }

    @Test
    void handleRequestErrorResponse() {
        final RedisCommand<Long> command = RedisCommandFactory.buildLlenCommand("Test");

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
    void handleRequestWriteFailure() {
        final RedisCommand<Long> command = RedisCommandFactory.buildLlenCommand("Test");

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

    @Test
    void channelInactiveBeforeReply() {
        final RedisCommand<Long> command = RedisCommandFactory.buildLlenCommand("Test");

        final Channel channel = new EmbeddedChannel();
        final MockChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);

        final ChannelPromise writePromise = channel.newPromise();

        requestResponseHandler.write(channelHandlerContext, command, writePromise);
        requestResponseHandler.channelInactive(channelHandlerContext);

        final CompletionException completionException =
                assertThrows(CompletionException.class, () -> assertTimeoutPreemptively(Duration.ofSeconds(1), command.getFuture()::join));

        assertTrue(completionException.getCause() instanceof IOException);
    }

    @Test
    void queuedResponse() {
        final RedisCommand<Long> llenCommand = RedisCommandFactory.buildLlenCommand("Test");
        final RedisCommand<Long> memoryUsageCommand = RedisCommandFactory.buildMemoryUsageCommand(new byte[] { 'T', 'e', 's', 't' });

        final Channel channel = new EmbeddedChannel();
        final MockChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);

        requestResponseHandler.write(channelHandlerContext, llenCommand, channel.newPromise());
        requestResponseHandler.write(channelHandlerContext, memoryUsageCommand, channel.newPromise());

        final long expectedMemoryUsageResponse = 128;

        requestResponseHandler.channelRead(channelHandlerContext, "QUEUED");
        requestResponseHandler.channelRead(channelHandlerContext, expectedMemoryUsageResponse);

        assertEquals(expectedMemoryUsageResponse, assertTimeoutPreemptively(Duration.ofSeconds(1), memoryUsageCommand.getFuture()::join));
        assertFalse(llenCommand.getFuture().isDone());
    }

    @ParameterizedTest
    @MethodSource("subscriptionChangeResponseSource")
    void isSubscriptionChangeResponse(final Object response, final boolean expectIsSubscriptionChangeResponse) {
        assertEquals(expectIsSubscriptionChangeResponse, RedisRequestResponseHandler.isSubscriptionChangeResponse(response));
    }

    static Stream<Arguments> subscriptionChangeResponseSource() {
        return Stream.of(
                arguments("Test!", false),
                arguments(12, false),
                arguments(new Object[] { "subscribe".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), 1, 17 }, false),
                arguments(new Object[] { "subscribe".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), "1".getBytes(StandardCharsets.US_ASCII) }, false),
                arguments(new Object[] { "not-a-subscription-message".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), 1 }, false),
                arguments(new Object[] { "subscribe".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), 1 }, true)
        );
    }

    @ParameterizedTest
    @MethodSource("publishedMessageResponseSource")
    void isPublishedMessageResponse(final Object response, final boolean expectIsPublishedMessageResponse) {
        assertEquals(expectIsPublishedMessageResponse, RedisRequestResponseHandler.isPublishedMessageResponse(response));
    }

    static Stream<Arguments> publishedMessageResponseSource() {
        return Stream.of(
                arguments("Test!", false),
                arguments(12, false),
                arguments(new Object[] { "message".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), new byte[0], 17 }, false),
                arguments(new Object[] { "message".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), 1 }, false),
                arguments(new Object[] { "not-a-message".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), new byte[0] }, false),
                arguments(new Object[] { "message".getBytes(StandardCharsets.US_ASCII), "channel".getBytes(StandardCharsets.US_ASCII), new byte[0] }, true),
                arguments(new Object[] { "pmessage".getBytes(StandardCharsets.US_ASCII), "pattern.*".getBytes(StandardCharsets.US_ASCII), new byte[0] }, true)
        );
    }
}
