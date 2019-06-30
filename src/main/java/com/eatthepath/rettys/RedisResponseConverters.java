package com.eatthepath.rettys;

import java.util.function.Function;

/**
 * This class provides static methods for getting singleton instances of common Redis response converters.
 */
class RedisResponseConverters {

    /**
     * Disallow construction.
     */
    private RedisResponseConverters() {}

    /**
     * A response converter that discards Redis responses and always returns {@code null}. This is intended for commands
     * where Redis unconditionally returns an "OK" string as a response.
     */
    public static final Function<Object, Void> VOID_CONVERTER = redisResponse -> null;

    /**
     * A response converter that interprets Redis responses as {@link Long} values.
     */
    public static final Function<Object, Long> INTEGER_CONVERTER = redisResponse -> {
        if (!(redisResponse instanceof Number)) {
            throw new IllegalArgumentException("Could not convert Redis response to long: " + redisResponse.getClass());
        }

        return ((Number) redisResponse).longValue();
    };

    /**
     * A response converter that interprets Redis responses as arrays of objects.
     */
    public static final Function<Object, Object[]> OBJECT_ARRAY_CONVERTER = redisResponse -> {
        if (!(redisResponse instanceof Object[])) {
            throw new IllegalArgumentException("Could not convert Redis response to object array: " + redisResponse.getClass());
        }

        return (Object[]) redisResponse;
    };
}
