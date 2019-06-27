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

    /**
     * A response converter that interprets Redis responses as structured {@link ScanResponse} instances.
     */
    public static final Function<Object, ScanResponse> SCAN_RESPONSE_CONVERTER = redisResponse -> {
        if (!(redisResponse instanceof Object[])) {
            throw new IllegalArgumentException("Could not convert Redis response to scan response: " + redisResponse.getClass());
        }

        final Object[] responseArray = (Object[]) redisResponse;

        if (responseArray.length != 2) {
            throw new IllegalArgumentException("Unexpected array length for cursor response: " + responseArray.length);
        }

        if (!(responseArray[0] instanceof byte[])) {
            throw new IllegalArgumentException("Could not convert array element to cursor value: " + responseArray[0].getClass());
        }

        final byte[] cursor = (byte[]) responseArray[0];

        if (!(responseArray[1] instanceof Object[])) {
            throw new IllegalArgumentException("Could not convert array element to list of keys: " + responseArray[1].getClass());
        }

        final Object[] keyArray = (Object[]) responseArray[1];
        final byte[][] keys = new byte[keyArray.length][];

        for (int i = 0; i < keyArray.length; i++) {
            if (!(keyArray[i] instanceof byte[])) {
                throw new IllegalArgumentException("Could not convert array element to bulk string: " + keyArray[i].getClass());
            }

            keys[i] = (byte[]) keyArray[i];
        }

        return new ScanResponse(cursor, keys);
    };
}
