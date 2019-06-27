package com.eatthepath.rettys;

/**
 * This class provides static methods for getting singleton instances of common Redis response converters.
 */
class RedisResponseConverters {

    private static final RedisResponseConverter<Void> VOID_CONVERTER = redisResponse -> null;

    private static final RedisResponseConverter<Long> INTEGER_CONVERTER = redisResponse -> {
        if (!(redisResponse instanceof Number)) {
            throw new IllegalArgumentException("Could not convert Redis response to long: " + redisResponse.getClass());
        }

        return ((Number) redisResponse).longValue();
    };

    private static final RedisResponseConverter<Object[]> OBJECT_ARRAY_CONVERTER = redisResponse -> {
        if (!(redisResponse instanceof Object[])) {
            throw new IllegalArgumentException("Could not convert Redis response to object array: " + redisResponse.getClass());
        }

        return (Object[]) redisResponse;
    };

    private static final RedisResponseConverter<ScanResponse> SCAN_RESPONSE_CONVERTER = redisResponse -> {
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

    /**
     * Returns a response converter that discards Redis responses and always returns {@code null}. This is intended for
     * commands where Redis unconditionally returns an "OK" string as a response.
     *
     * @return a response converter that always returns {@code null}
     */
    static RedisResponseConverter<Void> voidConverter() {
        return VOID_CONVERTER;
    }

    /**
     * Returns a response converter that interprets Redis responses as {@link Long} values.
     *
     * @return a response converter that interprets Redis responses as Long values
     */
    static RedisResponseConverter<Long> integerConverter() {
        return INTEGER_CONVERTER;
    }

    /**
     * Returns a response converter that interprets Redis responses as arrays of objects.
     *
     * @return a response converter that interprets Redis responses as arrays of objects
     */
    static RedisResponseConverter<Object[]> objectArrayConverter() {
        return OBJECT_ARRAY_CONVERTER;
    }

    static RedisResponseConverter<ScanResponse> scanResponseConverter() {
        return SCAN_RESPONSE_CONVERTER;
    }
}
