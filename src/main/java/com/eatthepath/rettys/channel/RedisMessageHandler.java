package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisMessageConsumer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.Executor;

/**
 * A handler that dispatches messages from Redis to an out-of-pipeline message consumer on a non-IO thread.
 */
class RedisMessageHandler extends ChannelInboundHandlerAdapter {

    private final RedisMessageConsumer messageConsumer;
    private final Executor executor;

    RedisMessageHandler(final RedisMessageConsumer messageConsumer, final Executor executor) {
        this.messageConsumer = messageConsumer;
        this.executor = executor;
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        executor.execute(() -> messageConsumer.consumeMessage(context.channel(), message));
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) {
        executor.execute(() -> messageConsumer.handleChannelClosure(context.channel()));
        context.fireChannelInactive();
    }
}
