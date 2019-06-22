package com.eatthepath.rettys;

import java.util.Objects;

/**
 * A Redis exception represents an error reported by the Redis server in response to a command. Redis exceptions have
 * an "error prefix" that identifies the type of error that occurred (e.g. {@code "WRONGTYPE"}) and a human-readable,
 * explanatory message.
 */
public class RedisException extends Exception {
    private final String errorPrefix;
    private final String message;

    /**
     * Constructs a new Redis exception from the given Redis error string (less the leading '-' and trailing '\r\n').
     *
     * @param errorString the error string from which to construct a Redis exception
     */
    public RedisException(final String errorString) {
        this.message = Objects.requireNonNull(errorString, "Error string must not be null.");

        final int indexOfFirstSpace = errorString.indexOf(' ');
        this.errorPrefix = indexOfFirstSpace >= 0 ? errorString.substring(0, indexOfFirstSpace) : errorString;
    }

    /**
     * Returns the full message (including error prefix) reported by the Redis server.
     *
     * @return the full message (including error prefix) reported by the Redis server
     */
    @Override
    public String getMessage() {
        return message;
    }

    /**
     * Returns the "error prefix" reported by the Redis server. The error prefix is the first word (up to the first
     * space or to the end of the error string, whichever comes first) in the error message and indicates the specific
     * type of error reported by the Redis server.
     *
     * @return the "error prefix" reported by the Redis server
     */
    public String getErrorPrefix() {
        return errorPrefix;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RedisException that = (RedisException) o;
        return message.equals(that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }
}
