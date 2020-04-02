package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisMessageConsumer;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Tag("integration")
@Testcontainers
public class RedisChannelFactoryTest {

    @SuppressWarnings("rawtypes")
    @Container
    private GenericContainer redis = new GenericContainer<>("redis:5.0.3-alpine")
            .withExposedPorts(6379);

    private RedisChannelFactory redisChannelFactory;

    private static EventLoopGroup eventLoopGroup;
    private static ExecutorService handlerExecutor;

    @BeforeAll
    static void setUpBeforeClass() {
        eventLoopGroup = new NioEventLoopGroup(1);
        handlerExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    static void tearDownAfterClass() {
        eventLoopGroup.shutdownGracefully();
        handlerExecutor.shutdown();
    }

    @BeforeEach
    void setUp() {
        redisChannelFactory = new RedisChannelFactory(eventLoopGroup, handlerExecutor, StandardCharsets.UTF_8, false);
    }

    @Test
    void testCreateChannel() throws InterruptedException {
        final InetSocketAddress socketAddress = new InetSocketAddress(redis.getContainerIpAddress(), redis.getFirstMappedPort());
        final RedisMessageConsumer consumer = mock(RedisMessageConsumer.class);

        final ChannelFuture createChannelFuture = redisChannelFactory.createChannel(socketAddress, consumer).await();

        assertTrue(createChannelFuture.isSuccess());
        assertTrue(createChannelFuture.channel().close().await().isSuccess());
    }
}
