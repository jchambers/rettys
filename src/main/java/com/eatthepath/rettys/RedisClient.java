package com.eatthepath.rettys;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
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

    public RedisClient(final InetSocketAddress inetSocketAddress) throws InterruptedException {
        this(inetSocketAddress, Charset.defaultCharset());
    }

    public RedisClient(final InetSocketAddress inetSocketAddress, final Charset charset) throws InterruptedException {
        this.charset = charset;

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.group(eventLoopGroup);

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(final SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();

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

    /**
     * Executes the given commands within a Redis transaction. The given commands <em>must</em> be called via the
     * provided {@link RedisCommandExecutor}; commands called directly via a {@code RedisClient} will be executed
     * outside the scope of the transaction.
     *
     * @param commands the actions to be performed within the transaction
     */
    public void doInTransaction(final Consumer<RedisCommandExecutor> commands) {
        final RedisTransaction transaction = new RedisTransaction(getCharset());

        commands.accept(transaction);
        channel.writeAndFlush(transaction);
    };

    public Stream<String> scan() {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, getCharset())).join()), false);
    }

    public Stream<String> scan(final String matchPattern) {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, getCharset())).join()), false);
    }

    public Stream<String> scan(final long count) {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, count, getCharset())).join()), false);
    }

    public Stream<String> scan(final String matchPattern, final long count) {
        return StreamSupport.stream(new ScanSpliterator((cursor) ->
                executeCommand(RedisCommandFactory.buildScanCommand(cursor, matchPattern, count, getCharset())).join()), false);
    }
}
