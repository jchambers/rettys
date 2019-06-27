package com.eatthepath.rettys;

import io.netty.channel.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A Redis request/response handler tracks pending commands and matches them to responses from the Redis server.
 */
class RedisRequestResponseHandler extends ChannelHandlerAdapter implements ChannelInboundHandler, ChannelOutboundHandler {

    private final Deque<RedisCommand> pendingCommands = new ArrayDeque<>();

    private static final String QUEUED_RESPONSE = "QUEUED";

    private static final IOException CHANNEL_CLOSED_EXCEPTION = new IOException("Channel closed before the Redis server could respond.");

    private static final Logger log = LoggerFactory.getLogger(RedisRequestResponseHandler.class);

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        final RedisCommand pendingCommand = pendingCommands.pollFirst();

        if (pendingCommand != null) {
            if (msg instanceof RedisException) {
                pendingCommand.getFuture().completeExceptionally((RedisException) msg);
            } else if (!QUEUED_RESPONSE.equals(msg)) {
                // We DO want to continue to move things through the queue, but do NOT want to actually complete the
                // futures for commands that are part of a transaction. Commands queued as part of a transaction will
                // pile their responses into an array returned for the EXEC command at the end of the transaction, and
                // we'll rely on the transaction to dispatch those responses to its constituent commands.

                //noinspection unchecked
                pendingCommand.getFuture().complete(pendingCommand.getResponseConverter().convertRedisResponse(msg));
            }
        } else {
            log.error("Received a Redis message, but have no pending commands.");
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

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) {
        ctx.fireChannelRegistered();
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) {
        ctx.fireChannelUnregistered();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        ctx.fireChannelActive();
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        ctx.fireChannelReadComplete();
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object event) {
        ctx.fireUserEventTriggered(event);
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void bind(final ChannelHandlerContext ctx, final SocketAddress socketAddress, final ChannelPromise channelPromise) {
        ctx.bind(socketAddress, channelPromise);
    }

    @Override
    public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise channelPromise) {
        ctx.connect(remoteAddress, localAddress, channelPromise);
    }

    @Override
    public void disconnect(final ChannelHandlerContext ctx, final ChannelPromise channelPromise) {
        ctx.disconnect(channelPromise);
    }

    @Override
    public void close(final ChannelHandlerContext ctx, final ChannelPromise channelPromise) {
        ctx.close(channelPromise);
    }

    @Override
    public void deregister(final ChannelHandlerContext ctx, final ChannelPromise channelPromise) {
        ctx.deregister(channelPromise);
    }

    @Override
    public void read(final ChannelHandlerContext ctx) {
        ctx.read();
    }

    @Override
    public void flush(final ChannelHandlerContext ctx) {
        ctx.flush();
    }
}
