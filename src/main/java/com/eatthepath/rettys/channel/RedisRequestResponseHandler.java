package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisCommand;
import com.eatthepath.rettys.RedisResponseConsumer;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A Redis request/response handler tracks pending commands and matches them to responses from the Redis server.
 */
class RedisRequestResponseHandler extends ChannelDuplexHandler {

    private final Deque<RedisCommand> pendingCommands = new ArrayDeque<>();
    private final RedisResponseConsumer responseConsumer;

    static final IOException CHANNEL_CLOSED_EXCEPTION =
            new IOException("Channel closed before the Redis server could respond.");

    private static final Logger log = LoggerFactory.getLogger(RedisRequestResponseHandler.class);

    RedisRequestResponseHandler(final RedisResponseConsumer responseConsumer) {
        this.responseConsumer = responseConsumer;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        final RedisCommand pendingCommand = pendingCommands.pollFirst();

        if (pendingCommand != null) {
            responseConsumer.consumeResponse(pendingCommand, msg);
        } else {
            log.error("Received a Redis message, but have no pending commands.");
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise writePromise) {
        if (msg instanceof RedisCommand) {
            final RedisCommand command = (RedisCommand) msg;

            writePromise.addListener(future -> {
                if (future.isSuccess()) {
                    pendingCommands.addLast(command);
                } else {
                    responseConsumer.handleCommandFailure(command, future.cause());
                }
            });
        }

        ctx.write(msg, writePromise);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
        pendingCommands.forEach(command -> responseConsumer.handleCommandFailure(command, CHANNEL_CLOSED_EXCEPTION));
        pendingCommands.clear();

        ctx.fireChannelInactive();
    }
}
