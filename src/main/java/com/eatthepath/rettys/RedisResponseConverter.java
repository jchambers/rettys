package com.eatthepath.rettys;

/**
 * A Redis response converter converts a response from the Redis server to a more specific (or entirely different) type
 * expected by the future associated with a {@link RedisCommand}.
 *
 * @param <T> the type to which the Redis response will be converted
 */
interface RedisResponseConverter<T> {

    /**
     * Converts the given object sent by the Redis server to a more specific type. The given object will always be one
     * of a {@link String}, {@link Long}, {@code byte[]}, or {@code Object[]} (which may have any of the legal Redis
     * types as elements).
     *
     * @param redisResponse the response to be converted to a more specific type
     *
     * @return the converted response from the Redis server
     */
    T convertRedisResponse(Object redisResponse);
}
