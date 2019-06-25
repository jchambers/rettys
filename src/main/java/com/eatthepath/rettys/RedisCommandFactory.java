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

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor) {
        return new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                RedisKeyword.SCAN,
                cursor);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor, final String matchPattern) {
        return new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor, final long count) {
        return new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.COUNT,
                count);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor, final String matchPattern, final long count) {
        return new RedisCommand<>(RedisResponseConverters.scanResponseConverter(),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern,
                RedisKeyword.COUNT,
                count);
    }
}
