package com.eatthepath.rettys;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class RedisClient {

    private final Channel channel;

    // TODO Add mechanisms to configure and gracefully shut down this event loop group
    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);

    public RedisClient(final InetSocketAddress inetSocketAddress) throws InterruptedException {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.group(eventLoopGroup);

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(final SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();

                pipeline.addLast(new RedisFrameDecoder());
                pipeline.addLast(new RedisValueDecoder());
                pipeline.addLast(new RedisCommandEncoder());
                pipeline.addLast(new RedisRequestResponseHandler());
            }
        });

        final ChannelFuture connectFuture = bootstrap.connect(inetSocketAddress).await();
        channel = connectFuture.channel();
    }

    public CompletableFuture<Long> llen(final String key) {
        final RedisCommand<Long> llenCommand = new RedisCommand<>(
                RedisResponseConverters.integerConverter(),
                RedisKeyword.LLEN,
                key);

        channel.writeAndFlush(llenCommand);

        return llenCommand.getFuture();
    }

    public CompletableFuture<Long> memoryUsage(final byte[] key) {
        final RedisCommand<Long> memoryUsageCommand = new RedisCommand<>(
                RedisResponseConverters.integerConverter(),
                RedisKeyword.MEMORY,
                RedisKeyword.USAGE,
                key);

        channel.writeAndFlush(memoryUsageCommand);

        return memoryUsageCommand.getFuture();
    }

    public CompletableFuture<ScanResponse> scan(final long cursor) {
        final RedisCommand<ScanResponse> scanCommand = new RedisCommand<>(
                RedisResponseConverters.scanResponseConverter(),
                RedisKeyword.SCAN,
                Long.toUnsignedString(cursor).getBytes(StandardCharsets.US_ASCII));

        channel.writeAndFlush(scanCommand);

        return scanCommand.getFuture();
    }
}
