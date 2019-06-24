package com.eatthepath.rettys;

import java.nio.charset.StandardCharsets;

/**
 * A Redis command is sent by a client to the Redis server to perform some action. Commands always have a command type
 * (e.g. "PING") and may optionally have one or more arguments (e.g. "LLEN mylist").
 */
class RedisCommand {

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

    private final Object[] components;

    /**
     * Constructs a Redis command of the given type with the given arguments.
     *
     * @param commandType the type of command to execute
     * @param arguments the arguments to pass as part of the command
     */
    RedisCommand(final CommandType commandType, final Object... arguments) {
        components = new Object[arguments.length + 1];

        components[0] = commandType.getBulkStringBytes();
        System.arraycopy(arguments, 0, components, 1, arguments.length);
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
