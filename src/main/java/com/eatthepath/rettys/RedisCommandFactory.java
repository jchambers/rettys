package com.eatthepath.rettys;

class RedisCommandFactory {

    static RedisCommand<Long> buildLlenCommand(final String key) {
        return new RedisCommand<>(RedisResponseConverters.integerConverter(),
                RedisKeyword.LLEN,
                key);
    }

    static RedisCommand<Long> buildMemoryUsageCommand(final byte[] key) {
        return new RedisCommand<>(RedisResponseConverters.integerConverter(),
                RedisKeyword.MEMORY,
                RedisKeyword.USAGE,
                key);
    }
}
