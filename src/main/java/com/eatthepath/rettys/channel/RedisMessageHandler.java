package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisMessageConsumer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.Executor;

/**
 * A handler that dispatches messages from Redis to an out-of-pipeline message consumer.
 */
class RedisMessageHandler extends ChannelInboundHandlerAdapter {

    private final RedisMessageConsumer messageConsumer;

    RedisMessageHandler(final RedisMessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        messageConsumer.consumeMessage(context.channel(), message);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) {
        messageConsumer.handleChannelClosure(context.channel());
        context.fireChannelInactive();
    }
}
