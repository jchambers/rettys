package com.eatthepath.rettys;

class RedisCommandFactory {

    static RedisCommand<Object[]> buildExecCommand() {
        return new RedisCommand<>(RedisResponseConverters.OBJECT_ARRAY_CONVERTER,
                RedisKeyword.EXEC);
    }

    static RedisCommand<Long> buildLlenCommand(final String key) {
        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER,
                RedisKeyword.LLEN,
                key);
    }

    static RedisCommand<Long> buildMemoryUsageCommand(final byte[] key) {
        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER,
                RedisKeyword.MEMORY,
                RedisKeyword.USAGE,
                key);
    }

    static RedisCommand<Void> buildMultiCommand() {
        return new RedisCommand<>(RedisResponseConverters.VOID_CONVERTER,
                RedisKeyword.MULTI);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor) {
        return new RedisCommand<>(RedisResponseConverters.SCAN_RESPONSE_CONVERTER,
                RedisKeyword.SCAN,
                cursor);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor, final String matchPattern) {
        return new RedisCommand<>(RedisResponseConverters.SCAN_RESPONSE_CONVERTER,
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor, final long count) {
        return new RedisCommand<>(RedisResponseConverters.SCAN_RESPONSE_CONVERTER,
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.COUNT,
                count);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final byte[] cursor, final String matchPattern, final long count) {
        return new RedisCommand<>(RedisResponseConverters.SCAN_RESPONSE_CONVERTER,
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern,
                RedisKeyword.COUNT,
                count);
    }
}
