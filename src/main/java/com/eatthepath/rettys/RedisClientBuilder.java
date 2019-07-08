package com.eatthepath.rettys;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Objects;

public class RedisClientBuilder {

    private SocketAddress socketAddress;

    private Charset charset = Charset.defaultCharset();

    private boolean useSsl;

    public RedisClientBuilder setSocketAddress(final SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
        return this;
    }

    public RedisClientBuilder setCharset(final Charset charset) {
        this.charset = Objects.requireNonNull(charset, "Charset must not be null.");
        return this;
    }

    public RedisClientBuilder setUseSsl(final boolean useSsl) {
        this.useSsl = useSsl;
        return this;
    }

    public RedisClient buildClient() throws InterruptedException {
        if (socketAddress == null) {
            throw new IllegalStateException("Socket address must be set before building a client.");
        }

        return new RedisClient(socketAddress, charset, useSsl);
    }
}
