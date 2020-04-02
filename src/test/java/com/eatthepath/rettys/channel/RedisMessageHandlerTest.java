package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.*;
import io.netty.channel.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executor;

import static org.mockito.Mockito.*;

class RedisMessageHandlerTest {

    private RedisMessageConsumer messageConsumer;
    private RedisMessageHandler messageHandler;

    private ChannelHandlerContext context;

    @BeforeEach
    void beforeEach() {
        final Executor immediateExecutor = mock(Executor.class);

        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(0, Runnable.class).run();
            return null;
        }).when(immediateExecutor).execute(any(Runnable.class));

        messageConsumer = mock(RedisMessageConsumer.class);
        messageHandler = new RedisMessageHandler(messageConsumer);

        final Channel channel = mock(Channel.class);

        context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
    }

    @Test
    void handleRequestResponse() {
        final long redisResponse = 17;

        messageHandler.channelRead(context, redisResponse);
        verify(messageConsumer).consumeMessage(context.channel(), redisResponse);
    }

    @Test
    void channelInactiveBeforeReply() {
        messageHandler.channelInactive(context);
        verify(messageConsumer).handleChannelClosure(context.channel());
    }
}
