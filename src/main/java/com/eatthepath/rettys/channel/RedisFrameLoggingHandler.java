package com.eatthepath.rettys.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

class RedisFrameLoggingHandler extends ChannelDuplexHandler {

    private final Charset charset;

    private static final Logger log = LoggerFactory.getLogger(RedisFrameLoggingHandler.class);

    RedisFrameLoggingHandler(final Charset charset) {
        this.charset = charset;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (log.isTraceEnabled() && msg instanceof ByteBuf) {
            log.trace("READ:  {}", redisMessageByteBufToString((ByteBuf) msg, charset));
        }

        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise writePromise) {
        if (log.isTraceEnabled() && msg instanceof ByteBuf) {
            log.trace("WRITE: {}", redisMessageByteBufToString((ByteBuf) msg, charset));
        }

        ctx.write(msg, writePromise);
    }

    private static String redisMessageByteBufToString(final ByteBuf byteBuf, final Charset charset) {
        return byteBuf.toString(charset)
                .replaceAll("\r", "\\\\r")
                .replaceAll("\n", "\\\\n");
    }
}
