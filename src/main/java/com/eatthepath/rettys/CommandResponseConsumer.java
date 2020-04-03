package com.eatthepath.rettys;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A message consumer that pairs responses from a Redis server to pending futures. A {@code CommandResponseConsumer} is
 * assumed to be paired with a single Redis {@link Channel}. {@code CommandResponseConsumers} complete futures via an
 * {@link Executor} provided at construction time to avoid blocking (or bogging down) IO threads.
 */
class CommandResponseConsumer implements RedisMessageConsumer {
    private final Deque<CompletableFuture<Object>> pendingFutures = new ArrayDeque<>();
    private final Executor handlerExecutor;

    private static final IOException CHANNEL_CLOSED_EXCEPTION =
            new IOException("Channel closed before the server sent a reply.");

    private static final Logger log = LoggerFactory.getLogger(CommandResponseConsumer.class);

    /**
     * Constructs a new command response consumer that completes pending futures via the given {@code Executor}.
     *
     * @param handlerExecutor the {@code Executor} on which future completions will be executed
     */
    CommandResponseConsumer(final Executor handlerExecutor) {
        this.handlerExecutor = handlerExecutor;
    }

    /**
     * Adds a future that expects a response from the Redis {@link Channel} associated with this consumer. Futures will
     * be completed normally with the value received from the server unless the server sends an error response, in which
     * case futures will be completed exceptionally with a {@link RedisException}.
     *
     * @param pendingFuture the future to be completed when a response arrives from the server
     */
    public void addPendingFuture(final CompletableFuture<Object> pendingFuture) {
        this.pendingFutures.addLast(pendingFuture);
    }

    /**
     * Completes the next pending future with the given message from the server. Futures are completed normally with the
     * message received from the server unless the server sends an error response, in which case futures are completed
     * exceptionally with a {@link RedisException}. In either case, completion of the future takes place via the
     * {@link Executor} provided at construction time.
     *
     * @param source the channel from which the message was received
     * @param message the message sent by the server
     */
    @Override
    public void consumeMessage(final Channel source, final Object message) {
        try {
            final CompletableFuture<Object> pendingFuture = pendingFutures.removeFirst();
            handlerExecutor.execute(() -> {
                if (message instanceof RedisException) {
                    pendingFuture.completeExceptionally((RedisException) message);
                } else {
                    pendingFuture.complete(message);
                }
            });
        } catch (final NoSuchElementException e) {
            log.error("Received a message with no pending command: {}", message);
        }
    }

    /**
     * Completes all pending futures exceptionally with the understanding that they will never receive a reply from the
     * now-closed channel.
     *
     * @param channel the channel that closed
     */
    @Override
    public void handleChannelClosure(final Channel channel) {
        pendingFutures.forEach(future ->
                handlerExecutor.execute(() -> future.completeExceptionally(CHANNEL_CLOSED_EXCEPTION)));

        pendingFutures.clear();
    }
}
