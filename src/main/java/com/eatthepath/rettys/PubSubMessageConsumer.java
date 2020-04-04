package com.eatthepath.rettys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * <p>A pub/sub message consumer consumes messages from a single channel in pub/sub mode. Pub/sub channels are different
 * from "command/response" channels in that, while they have at least one active subscription, they accept only a
 * limited subset of commands ({@code SUBSCRIBE}, {@code UNSUBSCRIBE}, {@code PSUBSCRIBE}, {@code PUNSUBSCRIBE},
 * {@code PING}, and {@code QUIT}), and subscription/unsubscription commands may trigger multiple responses for the same
 * command.</p>
 *
 * <p>A pub/sub message consumer is responsible for pairing groups of responses with a future associated with a command.
 * It also dispatches published messages to registered listeners.</p>
 */
class PubSubMessageConsumer extends CommandResponseConsumer {

    private final Charset charset;

    private final Map<String, Set<PubSubListener>> channelSubscriptions = new HashMap<>();
    private final Map<String, Set<PubSubListener>> patternSubscriptions = new HashMap<>();

    private final Deque<Integer> pendingEventCounts = new ArrayDeque<>();
    private int subscriptionEventCount = 0;

    private static final int UNSUBSCRIBE_ALL = -1;

    private static final Logger log = LoggerFactory.getLogger(PubSubMessageConsumer.class);

    private enum PubSubMessageType {
        MESSAGE("message"),
        PATTERN_MESSAGE("pmessage"),
        SUBSCRIBE("subscribe"),
        UNSUBSCRIBE("unsubscribe"),
        PATTERN_SUBSCRIBE("psubscribe"),
        PATTERN_UNSUBSCRIBE("punsubscribe");

        private final byte[] messageTypeBytes;

        PubSubMessageType(final String messageTypeString) {
            this.messageTypeBytes = messageTypeString.getBytes(StandardCharsets.US_ASCII);
        }

        static PubSubMessageType fromBytes(final byte[] messageTypeBytes) {
            for (final PubSubMessageType messageType : PubSubMessageType.values()) {
                if (Arrays.equals(messageType.messageTypeBytes, messageTypeBytes)) {
                    return messageType;
                }
            }

            throw new IllegalArgumentException("No message type found for byte array: " + Arrays.toString(messageTypeBytes));
        }
    }

    /**
     * Constructs a new pub/sub message consumer that completes futures and calls listeners via the given
     * {@code Executor} and decodes channel names and patterns using the given character set.
     *
     * @param handlerExecutor the {@code Executor} on which future completions will be executed and listeners will be
     *                        notified of published messages
     * @param charset the character set to be used when decoding channel names and patterns
     */
    public PubSubMessageConsumer(final Executor handlerExecutor, final Charset charset) {
        super(handlerExecutor);

        this.charset = charset;
    }

    /**
     * Adds a future that expects one or more subscription messages from the Redis server. Futures will be completed
     * when the given number of subscription messages have been received from the server.
     *
     * @param subscriptionFuture the future to be completed when {@code channelCount} subscription messages have been
     *                           received from the server
     * @param topicCount the number of subscription messages required to complete the given future; must be positive
     */
    public void addPendingSubscriptionFuture(final CompletableFuture<Object> subscriptionFuture, final int topicCount) {
        if (topicCount > 0) {
            pendingEventCounts.addLast(topicCount);
            addPendingFuture(subscriptionFuture);
        } else {
            throw new IllegalArgumentException("Number of topics must be positive, but was actually " + topicCount);
        }
    }

    /**
     * Adds a future that expects one or more unsubscription messages from the Redis server. Futures will be completed
     * when the given number of unsubscription messages have been received from the server.
     *
     * @param unsubscriptionFuture the future to be completed when {@code channelCount} unsubscription messages have
     *                             been received from the server
     * @param topicCount the number of unsubscription messages required to complete the given future; may be zero, in
     *                   which case the given future will be notified when the server sends a message reporting zero
     *                   active subscriptions
     */
    public void addPendingUnsubscriptionFuture(final CompletableFuture<Object> unsubscriptionFuture, final int topicCount) {
        pendingEventCounts.addLast(topicCount > 0 ? topicCount : UNSUBSCRIBE_ALL);
        addPendingFuture(unsubscriptionFuture);
    }

    public void addChannelListener(final PubSubListener listener, final String... channelNames) {
        addListener(listener, channelSubscriptions, channelNames);
    }

    public void addPatternListener(final PubSubListener listener, final String... patterns) {
        addListener(listener, patternSubscriptions, patterns);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void addListener(final PubSubListener listener, final Map<String, Set<PubSubListener>> listenerMap, final String... topics) {
        if (Objects.requireNonNull(topics, "List of topics must not be null").length > 0) {
            synchronized (listenerMap) {
                for (final String topic : topics) {
                    listenerMap.computeIfAbsent(topic, c -> new HashSet<>()).add(listener);
                }
            }
        } else {
            throw new IllegalArgumentException("List of topics must not be empty");
        }
    }

    public void removeChannelListener(final PubSubListener listener, final String... channelNames) {
        removeListener(listener, channelSubscriptions, channelNames);
    }

    public void removePatternListener(final PubSubListener listener, final String... patterns) {
        removeListener(listener, patternSubscriptions, patterns);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void removeListener(final PubSubListener listener, final Map<String, Set<PubSubListener>> listenerMap, final String... topics) {
        if (topics == null || topics.length == 0) {
            synchronized (listenerMap) {
                // No topics were specified, so remove the given listener from ALL topics
                listenerMap.values().forEach(listenerSet -> listenerSet.remove(listener));
            }
        } else {
            synchronized (listenerMap) {
                for (final String topic : topics) {
                    listenerMap.getOrDefault(topic, Collections.emptySet()).remove(listener);
                }
            }
        }
    }

    @Override
    public void consumeMessage(final Object message) {
        // The message could be a message array, a subscribe/unsubscribe array, or a String response to a PING or QUIT
        // command. In theory, it could also be an exception response to a spurious non-pub/sub command.
        if (message instanceof Object[]) {
            final Object[] messageArray = (Object[]) message;

            if (messageArray.length >= 3 && messageArray[0] instanceof byte[] && messageArray[1] instanceof byte[]) {
                try {
                    final PubSubMessageType messageType = PubSubMessageType.fromBytes((byte[]) messageArray[0]);

                    switch (messageType) {
                        case SUBSCRIBE:
                        case UNSUBSCRIBE:
                        case PATTERN_SUBSCRIBE:
                        case PATTERN_UNSUBSCRIBE: {
                            if (messageArray.length == 3 && messageArray[2] instanceof Long) {
                                handleSubscriptionChangeEvent((Long) messageArray[2]);
                            } else {
                                // This isn't the "shape" of message we'd expect; it's possible this is a response to
                                // another command (even though that would be really weird).
                                super.consumeMessage(message);
                            }

                            break;
                        }

                        case MESSAGE: {
                            if (messageArray.length == 3 && messageArray[2] instanceof byte[]) {
                                handleChannelMessage(new String((byte[]) messageArray[1], charset), (byte[]) messageArray[2]);
                            } else {
                                // This isn't the "shape" of message we'd expect; it's possible this is a response to
                                // another command (even though that would be really weird).
                                super.consumeMessage(message);
                            }

                            break;
                        }

                        case PATTERN_MESSAGE: {
                            if (messageArray.length == 4 && messageArray[2] instanceof byte[] && messageArray[3] instanceof byte[]) {
                                handlePatternMessage(new String((byte[]) messageArray[1], charset),
                                        new String((byte[]) messageArray[2], charset),
                                        (byte[]) messageArray[3]);
                            } else {
                                // This isn't the "shape" of message we'd expect; it's possible this is a response to
                                // another command (even though that would be really weird).
                                super.consumeMessage(message);
                            }

                            break;
                        }

                        default: {
                            // This should never happen; this means that we've added something to the PubSubMessageType
                            // enum, but haven't updated this switch statement.
                            throw new RuntimeException("Unexpected pub/sub message type: " + messageType);
                        }
                    }
                } catch (final IllegalArgumentException e) {
                    // The first element of the array didn't contain a known pub/sub message type
                    super.consumeMessage(message);
                }
            }
        } else {
            super.consumeMessage(message);
        }
    }

    private void handleSubscriptionChangeEvent(final long subscriptionCount) {
        if (!pendingEventCounts.isEmpty()) {
            final int expectedCount = pendingEventCounts.peek();
            subscriptionEventCount += 1;

            if (subscriptionEventCount == expectedCount || (expectedCount == UNSUBSCRIBE_ALL && subscriptionCount == 0)) {
                // We've received as many subscription events as we were expecting and should fulfill the next pending
                // future.
                pendingEventCounts.removeFirst();
                subscriptionEventCount = 0;

                consumeMessage(subscriptionCount);
            }
        } else {
            log.error("Received unexpected subscription change event.");
        }
    }

    private void handleChannelMessage(final String channelName, final byte[] messageBytes) {
        // This may seem like too much dispatching to the executor, but this method may be called by an IO thread. We
        // want to make sure we're NOT synchronizing in IO threads, so we dispatch the dispatcher.
        getHandlerExecutor().execute(() -> {
            synchronized (channelSubscriptions) {
                channelSubscriptions.getOrDefault(channelName, Collections.emptySet())
                        .forEach(pubSubListener ->
                                getHandlerExecutor().execute(() ->
                                        pubSubListener.handlePublishedMessage(channelName, messageBytes)));
            }
        });
    }

    private void handlePatternMessage(final String pattern, final String channelName, final byte[] messageBytes) {
        // This may seem like too much dispatching to the executor, but this method may be called by an IO thread. We
        // want to make sure we're NOT synchronizing in IO threads, so we dispatch the dispatcher.
        getHandlerExecutor().execute(() -> {
            synchronized (patternSubscriptions) {
                patternSubscriptions.getOrDefault(pattern, Collections.emptySet())
                        .forEach(pubSubListener ->
                                getHandlerExecutor().execute(() ->
                                        pubSubListener.handlePublishedMessage(channelName, messageBytes)));
            }
        });
    }
}
