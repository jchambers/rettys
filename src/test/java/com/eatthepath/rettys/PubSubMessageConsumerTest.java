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
import static org.mockito.Mockito.*;

class PubSubMessageConsumerTest {

    private PubSubMessageConsumer pubSubMessageConsumer;

    private static final Charset CHARSET = StandardCharsets.UTF_8;

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
    void testAddPendingSubscriptionFuture() {
        assertThrows(IllegalArgumentException.class,
                () -> pubSubMessageConsumer.addPendingSubscriptionFuture(new CompletableFuture<>(), 0));
    }

    @Test
    void testAddPendingUnsubscriptionFuture() {
        assertDoesNotThrow(
                () -> pubSubMessageConsumer.addPendingUnsubscriptionFuture(new CompletableFuture<>(), 0));
    }

    @Test
    void testAddChannelListener() {
        assertThrows(IllegalArgumentException.class,
                () -> pubSubMessageConsumer.addChannelListener(mock(PubSubListener.class)));
    }

    @Test
    void testAddPatternListener() {
        assertThrows(IllegalArgumentException.class,
                () -> pubSubMessageConsumer.addPatternListener(mock(PubSubListener.class)));
    }

    @Test
    void testRemoveChannelListener() {
        final String channel = "channel";
        final String message = "Message!";

        {
            final PubSubListener firstListener = mock(PubSubListener.class);
            final PubSubListener secondListener = mock(PubSubListener.class);

            pubSubMessageConsumer.addChannelListener(firstListener, channel);
            pubSubMessageConsumer.addChannelListener(secondListener, channel);
            pubSubMessageConsumer.removeChannelListener(firstListener, channel);

            pubSubMessageConsumer.consumeMessage(buildPublishedChannelMessage(channel, message));

            verifyNoInteractions(firstListener);
            verify(secondListener).handlePublishedMessage(channel, message.getBytes(CHARSET));
        }

        {
            final PubSubListener firstListener = mock(PubSubListener.class);
            final PubSubListener secondListener = mock(PubSubListener.class);

            pubSubMessageConsumer.addChannelListener(firstListener, channel);
            pubSubMessageConsumer.addChannelListener(secondListener, channel);
            pubSubMessageConsumer.removeChannelListener(firstListener);

            pubSubMessageConsumer.consumeMessage(buildPublishedChannelMessage(channel, message));

            verifyNoInteractions(firstListener);
            verify(secondListener).handlePublishedMessage(channel, message.getBytes(CHARSET));
        }
    }

    @Test
    void testRemovePatternListener() {
        final String pattern = "c*";
        final String channel = "channel";
        final String message = "Message!";

        {
            final PubSubListener firstListener = mock(PubSubListener.class);
            final PubSubListener secondListener = mock(PubSubListener.class);

            pubSubMessageConsumer.addPatternListener(firstListener, pattern);
            pubSubMessageConsumer.addPatternListener(secondListener, pattern);
            pubSubMessageConsumer.removePatternListener(firstListener, pattern);

            pubSubMessageConsumer.consumeMessage(buildPublishedPatternMessage(pattern, channel, message));

            verifyNoInteractions(firstListener);
            verify(secondListener).handlePublishedMessage(channel, message.getBytes(CHARSET));
        }

        {
            final PubSubListener firstListener = mock(PubSubListener.class);
            final PubSubListener secondListener = mock(PubSubListener.class);

            pubSubMessageConsumer.addPatternListener(firstListener, pattern);
            pubSubMessageConsumer.addPatternListener(secondListener, pattern);
            pubSubMessageConsumer.removePatternListener(firstListener);

            pubSubMessageConsumer.consumeMessage(buildPublishedPatternMessage(pattern, channel, message));

            verifyNoInteractions(firstListener);
            verify(secondListener).handlePublishedMessage(channel, message.getBytes(CHARSET));
        }
    }

    @Test
    void testConsumeMessage() {
        {
            final CompletableFuture<Object> pingFuture = new CompletableFuture<>();
            pubSubMessageConsumer.addPendingFuture(pingFuture);

            pubSubMessageConsumer.consumeMessage("OK");
            assertEquals("OK", pingFuture.join());
        }

        {
            final CompletableFuture<Object> lrangeFuture = new CompletableFuture<>();
            pubSubMessageConsumer.addPendingFuture(lrangeFuture);

            final Object[] message = { "subscribe".getBytes(StandardCharsets.US_ASCII), new byte[1], new byte[2] };

            pubSubMessageConsumer.consumeMessage(message);

            assertArrayEquals(message, (Object[]) lrangeFuture.join());
        }

        {
            final CompletableFuture<Object> lrangeFuture = new CompletableFuture<>();
            pubSubMessageConsumer.addPendingFuture(lrangeFuture);

            final Object[] message = { "message".getBytes(StandardCharsets.US_ASCII), new byte[1], 12 };

            pubSubMessageConsumer.consumeMessage(message);

            assertArrayEquals(message, (Object[]) lrangeFuture.join());
        }

        {
            final CompletableFuture<Object> lrangeFuture = new CompletableFuture<>();
            pubSubMessageConsumer.addPendingFuture(lrangeFuture);

            final Object[] message = { "pmessage".getBytes(StandardCharsets.US_ASCII), new byte[1], 12 };

            pubSubMessageConsumer.consumeMessage(message);

            assertArrayEquals(message, (Object[]) lrangeFuture.join());
        }

        {
            final CompletableFuture<Object> lrangeFuture = new CompletableFuture<>();
            pubSubMessageConsumer.addPendingFuture(lrangeFuture);

            final Object[] message = { "test".getBytes(StandardCharsets.US_ASCII), new byte[1], 12 };

            pubSubMessageConsumer.consumeMessage(message);

            assertArrayEquals(message, (Object[]) lrangeFuture.join());
        }
    }

    @Test
    void testConsumeMessageArrayWithNull() {
        assertDoesNotThrow(() -> pubSubMessageConsumer.consumeMessage(new Object[] { null, "OK", 4 }));
    }

    @Test
    void testConsumeSubscriptionChangeMessage() {
        final CompletableFuture<Object> subscribeFuture = new CompletableFuture<>();
        pubSubMessageConsumer.addPendingSubscriptionFuture(subscribeFuture, 2);

        pubSubMessageConsumer.consumeMessage(buildSubscriptionMessage("first", 1));
        assertFalse(subscribeFuture.isDone());

        pubSubMessageConsumer.consumeMessage(buildSubscriptionMessage("second", 2));
        assertTrue(subscribeFuture.isDone());
    }

    @Test
    void testHandleChannelMessage() {
        final String firstChannel = "first";
        final String secondChannel = "second";

        final String firstMessage = "First message";
        final String secondMessage = "Second message";

        final PubSubListener listener = mock(PubSubListener.class);
        pubSubMessageConsumer.addChannelListener(listener, firstChannel, secondChannel);

        pubSubMessageConsumer.consumeMessage(buildPublishedChannelMessage(firstChannel, firstMessage));
        pubSubMessageConsumer.consumeMessage(buildPublishedChannelMessage(secondChannel, secondMessage));

        verify(listener).handlePublishedMessage(firstChannel, firstMessage.getBytes(CHARSET));
        verify(listener).handlePublishedMessage(secondChannel, secondMessage.getBytes(CHARSET));
    }

    @Test
    void testHandlePatternMessage() {
        final String firstPattern = "f*";
        final String secondPattern = "s*";

        final String firstChannel = "first";
        final String secondChannel = "second";

        final String firstMessage = "First message";
        final String secondMessage = "Second message";

        final PubSubListener listener = mock(PubSubListener.class);
        pubSubMessageConsumer.addPatternListener(listener, firstPattern, secondPattern);

        pubSubMessageConsumer.consumeMessage(buildPublishedPatternMessage(firstPattern, firstChannel, firstMessage));
        pubSubMessageConsumer.consumeMessage(buildPublishedPatternMessage(secondPattern, secondChannel, secondMessage));

        verify(listener).handlePublishedMessage(firstChannel, firstMessage.getBytes(CHARSET));
        verify(listener).handlePublishedMessage(secondChannel, secondMessage.getBytes(CHARSET));
    }

    private static Object[] buildSubscriptionMessage(final String topic, final long activeSubscriptionCount) {
        return new Object[] {
                "subscribe".getBytes(StandardCharsets.US_ASCII),
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
