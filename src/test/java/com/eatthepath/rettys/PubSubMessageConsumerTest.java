package com.eatthepath.rettys;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class PubSubMessageConsumerTest {

    private PubSubMessageConsumer pubSubMessageConsumer;

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final String CHANNEL_SUBSCRIBE_MESSAGE_TYPE = "subscribe";
    private static final String CHANNEL_UNSUBSCRIBE_MESSAGE_TYPE = "unsubscribe";
    private static final String PATTERN_SUBSCRIBE_MESSAGE_TYPE = "psubscribe";
    private static final String PATTERN_UNSUBSCRIBE_MESSAGE_TYPE = "punsubscribe";

    @BeforeEach
    void setUp() {
        final Executor handlerExecutor = mock(Executor.class);

        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(0, Runnable.class).run();
            return null;
        }).when(handlerExecutor).execute(any(Runnable.class));

        pubSubMessageConsumer = new PubSubMessageConsumer(handlerExecutor, CHARSET);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void addPendingChannelSubscriptionFuture() {
    }

    @Test
    void addPendingPatternSubscriptionFuture() {
    }

    @Test
    void addPendingUnsubscriptionFuture() {
    }

    @Test
    void addPendingPatternUnsubscriptionFuture() {
    }

    @Test
    void addListener() {
    }

    @Test
    void addPatternListener() {
    }

    @Test
    void removeChannelListener() {
    }

    @Test
    void removePatternListener() {
    }

    @Test
    void testConsumeMessage() {
        final CompletableFuture<Object> pingFuture = new CompletableFuture<>();
        pubSubMessageConsumer.addPendingFuture(pingFuture);

        pubSubMessageConsumer.consumeMessage("OK");
        assertEquals("OK", pingFuture.join());
    }

    @Test
    void testConsumeMessageArrayWithNull() {
        assertDoesNotThrow(() -> pubSubMessageConsumer.consumeMessage(new Object[] { null, "OK", 4 }));
    }

    @Test
    void consumeSubscribeMessage() {
        final CompletableFuture<Object> subscribeFuture = new CompletableFuture<>();
        pubSubMessageConsumer.addPendingChannelSubscriptionFuture(subscribeFuture, 2);

        // Make sure unrelated messages don't trigger the future
        pubSubMessageConsumer.consumeMessage("OK");
        pubSubMessageConsumer.consumeMessage("OK");
        assertFalse(subscribeFuture.isDone());

        // Make sure insufficient messages of the expected type don't trigger the future
        pubSubMessageConsumer.consumeMessage(buildSubscriptionChangeMessage(CHANNEL_SUBSCRIBE_MESSAGE_TYPE, "first", 1));
        assertFalse(subscribeFuture.isDone());

        pubSubMessageConsumer.consumeMessage(buildSubscriptionChangeMessage(CHANNEL_SUBSCRIBE_MESSAGE_TYPE, "second", 2));
        assertTrue(subscribeFuture.isDone());
    }

    @Test
    void handleChannelMessage() {
    }

    @Test
    void handlePatternMessage() {
    }

    private static Object[] buildSubscriptionChangeMessage(final String messageType, final String topic, final long activeSubscriptionCount) {
        return new Object[] {
                messageType.getBytes(StandardCharsets.US_ASCII),
                topic.getBytes(CHARSET),
                activeSubscriptionCount
        };
    }

    private static Object[] buildPublishedChannelMessage(final String channel, final String message) {
        return new Object[] {
                "message".getBytes(StandardCharsets.US_ASCII),
                channel.getBytes(CHARSET),
                message.getBytes(CHARSET)
        };
    }

    private static Object[] buildPublishedPatternMessage(final String pattern, final String channel, final String message) {
        return new Object[] {
                "pmessage".getBytes(StandardCharsets.US_ASCII),
                pattern.getBytes(CHARSET),
                channel.getBytes(CHARSET),
                message.getBytes(CHARSET)
        };
    }
}
