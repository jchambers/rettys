package com.eatthepath.rettys;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * A Redis request/response handler tracks pending commands and matches them to responses from the Redis server.
 */
class RedisRequestResponseHandler extends ChannelInboundOutboundHandlerAdapter {

    private final Charset charset;

    private final Deque<RedisCommand> pendingCommands = new ArrayDeque<>();

    private Map<String, BiConsumer<String, Object>> subscribedChannelHandlers = new HashMap<>();
    private Map<String, BiConsumer<String, Object>> subscribedPatternHandlers = new HashMap<>();

    private int subscribedChannels = 0;
    private int subscribedPatterns = 0;

    private Map<byte[], List<Object[]>> collectedSubscriptionChangeMessages;

    private static final String QUEUED_RESPONSE = "QUEUED";

    private static final byte[] SUBSCRIBE_BULK_STRING = "subscribe".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PSUBSCRIBE_BULK_STRING = "psubscribe".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] UNSUBSCRIBE_BULK_STRING = "unsubscribe".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PUNSUBSCRIBE_BULK_STRING = "punsubscribe".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] MESSAGE_BULK_STRING = "message".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PMESSAGE_BULK_STRING = "pmessage".getBytes(StandardCharsets.US_ASCII);

    private static final IOException CHANNEL_CLOSED_EXCEPTION = new IOException("Channel closed before the Redis server could respond.");

    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private static final Logger log = LoggerFactory.getLogger(RedisRequestResponseHandler.class);

    RedisRequestResponseHandler(final Charset charset) {
        this.charset = charset;

        final Map<byte[], List<Object[]>> collectedSubscriptionChangeMessages = new HashMap<>();
        collectedSubscriptionChangeMessages.put(SUBSCRIBE_BULK_STRING, new ArrayList<>());
        collectedSubscriptionChangeMessages.put(PSUBSCRIBE_BULK_STRING, new ArrayList<>());
        collectedSubscriptionChangeMessages.put(UNSUBSCRIBE_BULK_STRING, new ArrayList<>());
        collectedSubscriptionChangeMessages.put(PUNSUBSCRIBE_BULK_STRING, new ArrayList<>());

        this.collectedSubscriptionChangeMessages = Collections.unmodifiableMap(collectedSubscriptionChangeMessages);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (subscribedChannels + subscribedPatterns > 0 && isPublishedMessageResponse(msg)) {
            final Object[] responseArray = (Object[]) msg;

            final byte[] action = (byte[]) responseArray[0];
            final String topic = new String((byte[]) responseArray[1], charset);
            final byte[] message = (byte[]) responseArray[2];

            final BiConsumer<String, Object> messageHandler = Arrays.equals(MESSAGE_BULK_STRING, action) ?
                    subscribedChannelHandlers.get(topic) : subscribedPatternHandlers.get(topic);

            if (messageHandler != null) {
                messageHandler.accept(topic, message);
            } else {
                log.error("No message handler found for channel or pattern {}", topic);
            }
        } else if (isSubscriptionChangeResponse(msg)) {
            final Object[] responseArray = (Object[]) msg;

            final byte[] action = (byte[]) responseArray[0];
            final String topic = new String((byte[]) responseArray[1], charset);
            final long activeSubscriptionCount = (Long) responseArray[2];

            final List<Object[]> messages = collectedSubscriptionChangeMessages.get(action);

            if (messages == null) {
                throw new IllegalArgumentException("Unexpected subscription change action: " + new String(action, charset));
            }

            messages.add(responseArray);

            final RedisSubscriptionChangeCommand subscriptionChangeCommand;
            {
                final RedisCommand pendingCommand = pendingCommands.peekFirst();

                if (pendingCommand == null) {
                    throw new IllegalStateException("Received subscription change message with no pending subscription change command.");
                } else if (!(pendingCommand instanceof RedisSubscriptionChangeCommand)) {
                    throw new IllegalStateException("Received subscription change message, but pending command was {}" + pendingCommand.getComponents()[0]);
                }

                subscriptionChangeCommand = (RedisSubscriptionChangeCommand) pendingCommand;
            }

            if (Arrays.equals(SUBSCRIBE_BULK_STRING, action)) {
                subscribedChannels = (int) activeSubscriptionCount;
                subscribedChannelHandlers.put(topic, subscriptionChangeCommand.getHandler());
            } else if (Arrays.equals(PSUBSCRIBE_BULK_STRING, action)) {
                subscribedPatterns = (int) activeSubscriptionCount;
                subscribedPatternHandlers.put(topic, subscriptionChangeCommand.getHandler());
            } else if (Arrays.equals(UNSUBSCRIBE_BULK_STRING, action)) {
                subscribedChannels = (int) activeSubscriptionCount;
                subscribedChannelHandlers.remove(topic);
            } else if (Arrays.equals(PUNSUBSCRIBE_BULK_STRING, action)) {
                subscribedPatterns = (int) activeSubscriptionCount;
                subscribedPatternHandlers.remove(topic);
            }

            // Do we have as many subscription change messages as we need to call the command "done?"
            final boolean shouldCompleteCommand;

            final RedisKeyword commandType = (RedisKeyword) subscriptionChangeCommand.getComponents()[0];

            // In most cases, we know that we're trying to subscribe to (or unsubscribe from) N channels/topics, and so
            // know to expect N subscription change messages. In the case of an unsubscribe command with no arguments,
            // though, we're actually waiting for the number of active subscriptions to reach zero.
            if ((RedisKeyword.UNSUBSCRIBE.equals(commandType) || RedisKeyword.PUNSUBSCRIBE.equals(commandType)) &&
                    subscriptionChangeCommand.getComponents().length == 1) {

                shouldCompleteCommand = activeSubscriptionCount == 0;
            } else {
                shouldCompleteCommand = messages.size() == (subscriptionChangeCommand.getComponents().length - 1);
            }

            if (shouldCompleteCommand) {
                pendingCommands.removeFirst();
                subscriptionChangeCommand.getFuture().complete(
                        subscriptionChangeCommand.getResponseConverter().apply(messages.toArray(EMPTY_OBJECT_ARRAY)));
            }
        } else {
            final RedisCommand pendingCommand = pendingCommands.pollFirst();

            if (pendingCommand != null) {
                if (msg instanceof RedisException) {
                    pendingCommand.getFuture().completeExceptionally((RedisException) msg);
                } else if (!QUEUED_RESPONSE.equals(msg)) {
                    // We DO want to continue to move things through the queue, but do NOT want to actually complete
                    // the futures for commands that are part of a transaction. Commands queued as part of a transaction
                    // will pile their responses into an array returned for the EXEC command at the end of the
                    // transaction, and we'll rely on the transaction to dispatch those responses to its constituent
                    // commands.

                    //noinspection unchecked
                    pendingCommand.getFuture().complete(pendingCommand.getResponseConverter().apply(msg));
                }
            } else {
                log.error("Received a Redis response, but have no pending commands.");
            }
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise writePromise) {
        if (msg instanceof RedisTransaction) {
            final RedisTransaction transaction = (RedisTransaction) msg;

            write(ctx, transaction.getMultiCommand(), ctx.newPromise());
            transaction.getCommands().forEach((command) -> write(ctx, command, ctx.newPromise()));
            write(ctx, transaction.getExecCommand(), ctx.newPromise());
        } else if (msg instanceof RedisCommand) {
            final RedisCommand command = (RedisCommand) msg;

            pendingCommands.addLast(command);

            writePromise.addListener((GenericFutureListener<Future<Void>>) future -> {
                if (!future.isSuccess()) {
                    pendingCommands.remove(command);
                    command.getFuture().completeExceptionally(future.cause());
                }
            });
        }

        ctx.write(msg, writePromise);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
        for (final RedisCommand pendingCommand : pendingCommands) {
            pendingCommand.getFuture().completeExceptionally(CHANNEL_CLOSED_EXCEPTION);
        }

        pendingCommands.clear();

        ctx.fireChannelActive();
    }

    static boolean isSubscriptionChangeResponse(final Object msg) {
        if (msg instanceof Object[]) {
            final Object[] responseArray = (Object[]) msg;

            if (responseArray.length == 3 && responseArray[0] instanceof byte[] && responseArray[1] instanceof byte[] && responseArray[2] instanceof Number) {
                final byte[] actionBulkString = (byte[]) responseArray[0];

                return  Arrays.equals(SUBSCRIBE_BULK_STRING, actionBulkString) ||
                        Arrays.equals(PSUBSCRIBE_BULK_STRING, actionBulkString) ||
                        Arrays.equals(UNSUBSCRIBE_BULK_STRING, actionBulkString) ||
                        Arrays.equals(PUNSUBSCRIBE_BULK_STRING, actionBulkString);
            }
        }

        return false;
    }

    static boolean isPublishedMessageResponse(final Object msg) {
        if (msg instanceof Object[]) {
            final Object[] responseArray = (Object[]) msg;

            if (responseArray.length == 3 && responseArray[0] instanceof byte[] && responseArray[1] instanceof byte[] && responseArray[2] instanceof byte[]) {
                final byte[] action = (byte[]) responseArray[0];

                return Arrays.equals(MESSAGE_BULK_STRING, action) || Arrays.equals(PMESSAGE_BULK_STRING, action);
            }
        }

        return false;
    }
}
