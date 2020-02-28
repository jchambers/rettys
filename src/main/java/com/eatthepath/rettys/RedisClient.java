package com.eatthepath.rettys;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RedisClient extends RedisCommandExecutorAdapter {

    private final Charset charset;

    private final Channel channel;

    // TODO Add mechanisms to configure and gracefully shut down this event loop group
    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);

    RedisClient(final SocketAddress inetSocketAddress, final Charset charset, final boolean useSsl) throws InterruptedException {
        this.charset = charset;

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
                pipeline.addLast(new RedisRequestResponseHandler());
            }
        });

        final ChannelFuture connectFuture = bootstrap.connect(inetSocketAddress).await();
        channel = connectFuture.channel();
    }

    @Override
    public Charset getCharset() {
        return charset;
    }

    @Override
    public <T> CompletableFuture<T> executeCommand(final RedisCommand<T> command) {
        channel.writeAndFlush(command);
        return command.getFuture();
    }

    public Stream<String> scan() {
        return StreamSupport.stream(new ScanSpliterator(cursor -> scan(cursor).join()), false);
    }

    public Stream<String> scan(final String matchPattern) {
        return StreamSupport.stream(new ScanSpliterator(cursor -> scan(cursor, matchPattern).join()), false);
    }

    public Stream<String> scan(final long count) {
        return StreamSupport.stream(new ScanSpliterator(cursor -> scan(cursor, count).join()), false);
    }

    public Stream<String> scan(final String matchPattern, final long count) {
        return StreamSupport.stream(new ScanSpliterator(cursor -> scan(cursor, matchPattern, count).join()), false);
    }
}
