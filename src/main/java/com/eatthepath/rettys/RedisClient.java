package com.eatthepath.rettys;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RedisClient implements RedisCommandExecutor {

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

    /**
     * Executes the given commands within a Redis transaction. The given commands <em>must</em> be called via the
     * provided {@link RedisCommandExecutor}; commands called directly via a {@code RedisClient} will be executed
     * outside the scope of the transaction.
     *
     * @param commands the actions to be performed within the transaction
     */
    public void doInTransaction(final Consumer<RedisCommandExecutor> commands) {
        final RedisTransaction transaction = new RedisTransaction();

        commands.accept(transaction);
        channel.writeAndFlush(transaction);
    };

    public Stream<byte[]> scan() {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor)).join()), false);
    }

    public Stream<byte[]> scan(final String matchPattern) {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern)).join()), false);
    }

    public Stream<byte[]> scan(final long count) {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, count)).join()), false);
    }

    public Stream<byte[]> scan(final String matchPattern, final long count) {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, count)).join()), false);
    }
}
