package com.eatthepath.rettys;

import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class CommandResponseConsumerTest {

    private CommandResponseConsumer commandResponseConsumer;

    @BeforeEach
    void setUp() {
        final Executor handlerExecutor = mock(Executor.class);

        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(0, Runnable.class).run();
            return null;
        }).when(handlerExecutor).execute(any(Runnable.class));

        commandResponseConsumer = new CommandResponseConsumer(handlerExecutor);
    }

    @Test
    void testConsumeMessage() {
        final CompletableFuture<Object> pendingFuture = new CompletableFuture<>();
        final String message = "Test!";

        commandResponseConsumer.addPendingFuture(pendingFuture);
        commandResponseConsumer.consumeMessage(mock(Channel.class), message);

        assertEquals(message, pendingFuture.join());
    }

    @Test
    void testConsumeMessageRedisException() {
        final CompletableFuture<Object> pendingFuture = new CompletableFuture<>();
        final RedisException redisException = new RedisException("TEST Test exception");

        commandResponseConsumer.addPendingFuture(pendingFuture);
        commandResponseConsumer.consumeMessage(mock(Channel.class), redisException);

        final CompletionException completionException = assertThrows(CompletionException.class, pendingFuture::join);
        assertEquals(redisException, completionException.getCause());
    }

    @Test
    void testHandleChannelClosure() {
        final CompletableFuture<Object> pendingFuture = new CompletableFuture<>();

        commandResponseConsumer.addPendingFuture(pendingFuture);
        commandResponseConsumer.handleChannelClosure(mock(Channel.class));

        final CompletionException completionException = assertThrows(CompletionException.class, pendingFuture::join);
        assertTrue(completionException.getCause() instanceof IOException);
    }
}
