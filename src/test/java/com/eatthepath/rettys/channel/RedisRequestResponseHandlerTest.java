package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.*;
import com.eatthepath.rettys.channel.RedisRequestResponseHandler;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisRequestResponseHandlerTest {

    private RedisResponseConsumer responseConsumer;
    private RedisRequestResponseHandler requestResponseHandler;

    private ChannelHandlerContext context;

    @BeforeEach
    void beforeEach() {
        responseConsumer = mock(RedisResponseConsumer.class);
        requestResponseHandler = new RedisRequestResponseHandler(responseConsumer);

        final Channel channel = mock(Channel.class);

        context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);

        when(context.newPromise()).thenAnswer(
                invocationOnMock -> new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE));
    }

    @Test
    void handleRequestResponse() {
        final RedisCommand command = new RedisCommand("LLEN", "Test");

        final long redisResponse = 17;

        final ChannelPromise writePromise = context.newPromise();

        requestResponseHandler.write(context, command, writePromise);
        writePromise.setSuccess();

        requestResponseHandler.channelRead(context, redisResponse);

        verify(responseConsumer).consumeResponse(command, redisResponse);
    }

    @Test
    void handleRequestRedisErrorResponse() {
        final RedisCommand command = new RedisCommand("LLEN", "Test");

        final RedisException redisException = new RedisException("TEST Test exception");

        final ChannelPromise writePromise = context.newPromise();

        requestResponseHandler.write(context, command, writePromise);
        writePromise.setSuccess();

        requestResponseHandler.channelRead(context, redisException);

        verify(responseConsumer).consumeResponse(command, redisException);
    }

    @Test
    void handleRequestWriteFailure() {
        final RedisCommand command = new RedisCommand("LLEN", "Test");

        final ChannelPromise writePromise = context.newPromise();

        requestResponseHandler.write(context, command, writePromise);

        final IOException ioException = new IOException("A horribleness has befelsterred the children's academy.");

        writePromise.setFailure(ioException);

        verify(responseConsumer).handleCommandFailure(command, ioException);
    }

    @Test
    void channelInactiveBeforeReply() {
        final RedisCommand command = new RedisCommand("LLEN", "Test");

        final ChannelPromise writePromise = context.newPromise();

        requestResponseHandler.write(context, command, writePromise);
        writePromise.setSuccess();

        requestResponseHandler.channelInactive(context);

        verify(responseConsumer).handleCommandFailure(command, RedisRequestResponseHandler.CHANNEL_CLOSED_EXCEPTION);
    }
}
