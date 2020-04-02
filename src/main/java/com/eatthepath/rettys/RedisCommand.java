package com.eatthepath.rettys;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * <p>A Redis command is sent by a client to the Redis server to perform some action. Commands always have a command
 * type (e.g. "PING") and may optionally have one or more arguments (e.g. "LLEN mylist").</p>
 */
public class RedisCommand {

    private final Object[] components;

    /**
     * Constructs a Redis command of the given type with the given arguments and response converter.
     *
     * @param components TODO
     */
    public RedisCommand(final Object... components) {
        this.components = components;
    }

    /**
     * Returns the list of "components" that comprise this command. The list must always have the command type as its
     * first component, and arguments as subsequent elements.
     *
     * @return the list of components to be sent to the Redis server to execute this command
     */
    public Object[] getComponents() {
        return components;
    }
}
