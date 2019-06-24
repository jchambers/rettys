package com.eatthepath.rettys;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * <p>A Redis command is sent by a client to the Redis server to perform some action. Commands always have a command
 * type (e.g. "PING") and may optionally have one or more arguments (e.g. "LLEN mylist").</p>
 *
 * <p>A {@code RedisCommand} also has a {@link CompletableFuture} that will be notified when the Redis server sends a
 * reply. The given {@link RedisResponseConverter} converts the response from an {@link Object} to a more specific
 * type (or potentially even an entirely different type) expected by the given future.</p>
 */
class RedisCommand<T> {

    enum CommandType {
        LLEN("LLEN");

        private final byte[] bytes;

        CommandType(final String commandName) {
            this.bytes = commandName.getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] getBulkStringBytes() {
            return bytes;
        }
    }

    private final RedisResponseConverter<T> responseConverter;
    private final CompletableFuture<T> future;

    private final Object[] components;

    /**
     * Constructs a Redis command of the given type with the given arguments.
     *
     * @param future the future to be notified when the Redis server replies to this command
     * @param responseConverter the converter to be used to interpret the response from the Redis server
     * @param commandType the type of command to execute
     * @param arguments the arguments to pass as part of the command
     */
    RedisCommand(final CompletableFuture<T> future, final RedisResponseConverter<T> responseConverter, final CommandType commandType, final Object... arguments) {
        components = new Object[arguments.length + 1];

        components[0] = commandType.getBulkStringBytes();
        System.arraycopy(arguments, 0, components, 1, arguments.length);

        this.future = future;
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
     * Returns the converter to be used to interpret the response from the Redis server.
     *
     * @return the converter to be used to interpret the response from the Redis server
     */
    RedisResponseConverter<T> getResponseConverter() {
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
