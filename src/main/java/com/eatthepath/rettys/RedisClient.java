package com.eatthepath.rettys;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    public <T> CompletableFuture<T> executeCommand(final RedisCommand<T> command) {
        channel.writeAndFlush(command);
        return command.getFuture();
    }

    public CompletableFuture<Long> llen(final String key) {
        return executeCommand(new RedisCommand<>(RedisResponseConverters.integerConverter(),
                RedisKeyword.LLEN,
                key));
    }

    public CompletableFuture<Long> memoryUsage(final byte[] key) {
        return executeCommand(new RedisCommand<>(RedisResponseConverters.integerConverter(),
                RedisKeyword.MEMORY,
                RedisKeyword.USAGE,
                key));
    }

    public Stream<byte[]> scan() {
        return StreamSupport.stream(new ScanSpliterator((cursor) -> executeCommand(
                new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                        RedisKeyword.SCAN,
                        Long.toUnsignedString(cursor).getBytes(StandardCharsets.US_ASCII))).join()), false);
    }

    public Stream<byte[]> scan(final String matchPattern) {
        return StreamSupport.stream(new ScanSpliterator((cursor) -> executeCommand(
                new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                        RedisKeyword.SCAN,
                        Long.toUnsignedString(cursor).getBytes(StandardCharsets.US_ASCII),
                        RedisKeyword.MATCH,
                        matchPattern)).join()), false);
    }

    public Stream<byte[]> scan(final long count) {
        return StreamSupport.stream(new ScanSpliterator((cursor) -> executeCommand(
                new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                        RedisKeyword.SCAN,
                        Long.toUnsignedString(cursor).getBytes(StandardCharsets.US_ASCII),
                        RedisKeyword.COUNT,
                        count)).join()), false);
    }

    public Stream<byte[]> scan(final String matchPattern, final long count) {
        return StreamSupport.stream(new ScanSpliterator((cursor) -> executeCommand(
                new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                        RedisKeyword.SCAN,
                        Long.toUnsignedString(cursor).getBytes(StandardCharsets.US_ASCII),
                        RedisKeyword.MATCH,
                        matchPattern,
                        RedisKeyword.COUNT,
                        count)).join()), false);
    }
}
