package com.eatthepath.rettys;

/**
 * This class provides static methods for getting singleton instances of common Redis response converters.
 */
class RedisResponseConverters {

    private static final RedisResponseConverter<Long> INTEGER_CONVERTER = new RedisResponseConverter<Long>() {
        @Override
        public Long convertRedisResponse(final Object redisResponse) {
            if (redisResponse instanceof Number) {
                return ((Number) redisResponse).longValue();
            }

            throw new IllegalArgumentException("Could not convert Redis response to long: " + redisResponse.getClass());
        }
    };

    /**
     * Returns a response converter that interprets Redis responses as {@link Long} values.
     *
     * @return a response converter that interprets Redis responses as Long values
     */
    static RedisResponseConverter<Long> integerConverter() {
        return INTEGER_CONVERTER;
    }
}
