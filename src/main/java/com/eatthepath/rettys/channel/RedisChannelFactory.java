package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisResponseConsumer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import java.net.SocketAddress;
import java.nio.charset.Charset;

public class RedisChannelFactory {

    private final EventLoopGroup eventLoopGroup;

    private final Charset charset;
    private final boolean useSsl;

    public RedisChannelFactory(final EventLoopGroup eventLoopGroup, final Charset charset, final boolean useSsl) {
        this.eventLoopGroup = eventLoopGroup;

        this.charset = charset;
        this.useSsl = useSsl;
    }

    public ChannelFuture createChannel(final SocketAddress inetSocketAddress, final RedisResponseConsumer responseConsumer) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.group(eventLoopGroup);

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(final SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();

                if (useSsl) {
                    pipeline.addLast(new SslHandler(SslContextBuilder.forClient().build().newEngine(channel.alloc())));
                }

                // TODO Tune parameters
                pipeline.addLast(new FlushConsolidationHandler());

                pipeline.addLast(new RedisFrameDecoder());
                // TODO Make this configurable
                pipeline.addLast(new RedisFrameLoggingHandler(charset));
                pipeline.addLast(new RedisValueDecoder());
                pipeline.addLast(new RedisCommandEncoder(charset));
                pipeline.addLast(new RedisRequestResponseHandler(responseConsumer));
            }
        });

        return bootstrap.connect(inetSocketAddress);
    }
}
