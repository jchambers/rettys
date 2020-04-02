package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisResponseConsumer;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

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

    @BeforeAll
    static void setUpBeforeClass() {
        eventLoopGroup = new NioEventLoopGroup(1);
    }

    @AfterAll
    static void tearDownAfterClass() {
        eventLoopGroup.shutdownGracefully();
    }

    @BeforeEach
    void setUp() {
        redisChannelFactory = new RedisChannelFactory(eventLoopGroup, StandardCharsets.UTF_8, false);
    }

    @Test
    void testCreateChannel() throws InterruptedException {
        final InetSocketAddress socketAddress = new InetSocketAddress(redis.getContainerIpAddress(), redis.getFirstMappedPort());
        final RedisResponseConsumer consumer = mock(RedisResponseConsumer.class);

        final ChannelFuture createChannelFuture = redisChannelFactory.createChannel(socketAddress, consumer).await();

        assertTrue(createChannelFuture.isSuccess());
        assertTrue(createChannelFuture.channel().close().await().isSuccess());
    }
}
