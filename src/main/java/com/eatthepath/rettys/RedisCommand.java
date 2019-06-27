package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * <p>A Redis command is sent by a client to the Redis server to perform some action. Commands always have a command
 * type (e.g. "PING") and may optionally have one or more arguments (e.g. "LLEN mylist").</p>
 *
 * <p>A {@code RedisCommand} also has a {@link CompletableFuture} that will be notified when the Redis server sends a
 * reply. The given {@code responseConverter} converts the response from an {@link Object} to a more specific type
 * (or potentially even an entirely different type) expected by the given future.</p>
 */
public class RedisCommand<T> {

    private final Object[] components;

    private final Function<Object, T> responseConverter;
    private final CompletableFuture<T> future;

    /**
     * Constructs a Redis command of the given type with the given arguments and response converter.
     *
     * @param responseConverter a function to be used to interpret the response from the Redis server
     * @param components TODO
     */
    public RedisCommand(final Function<Object, T> responseConverter, final Object... components) {
        this.components = components;

        this.future = new CompletableFuture<>();
        this.responseConverter = responseConverter;
    }

    /**
     * Returns the future to be notified when the Redis server response to this command.
     *
     * @return the future to be notified when the Redis server response to this command
     */
    CompletableFuture<T> getFuture() {
        return future;
    }

    /**
     * Returns the function to be used to interpret the response from the Redis server.
     *
     * @return the function to be used to interpret the response from the Redis server
     */
    Function<Object, T> getResponseConverter() {
        return responseConverter;
    }

    /**
     * Returns the list of "components" that comprise this command. The list must always have the command type as its
     * first component, and arguments as subsequent elements.
     *
     * @return the list of components to be sent to the Redis server to execute this command
     */
    Object[] getComponents() {
        return components;
    }
}
