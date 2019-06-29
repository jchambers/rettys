package com.eatthepath.rettys;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RedisFrameLoggingHandler extends ChannelInboundOutboundHandlerAdapter {

    private final Charset charset;

    private static final Logger log = LoggerFactory.getLogger(RedisFrameLoggingHandler.class);

    RedisFrameLoggingHandler() {
        // TODO Make this configurable
        this.charset = StandardCharsets.UTF_8;
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
