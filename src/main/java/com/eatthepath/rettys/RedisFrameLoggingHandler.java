package com.eatthepath.rettys;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RedisFrameLoggingHandler extends ChannelHandlerAdapter implements ChannelInboundHandler, ChannelOutboundHandler {

    private final Charset charset;

    private static final Logger log = LoggerFactory.getLogger(RedisFrameLoggingHandler.class);

    public RedisFrameLoggingHandler() {
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

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
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
