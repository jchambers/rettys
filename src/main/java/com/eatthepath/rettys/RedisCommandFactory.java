package com.eatthepath.rettys;

import java.nio.charset.Charset;

class RedisCommandFactory {

    static RedisCommand<Object[]> buildExecCommand() {
        return new RedisCommand<>(RedisResponseConverters.OBJECT_ARRAY_CONVERTER,
                RedisKeyword.EXEC);
    }

    static RedisCommand<Long> buildLlenCommand(final Object key) {
        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER,
                RedisKeyword.LLEN,
                key);
    }

    static RedisCommand<Long> buildMemoryUsageCommand(final Object key) {
        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER,
                RedisKeyword.MEMORY,
                RedisKeyword.USAGE,
                key);
    }

    static RedisCommand<Void> buildMultiCommand() {
        return new RedisCommand<>(RedisResponseConverters.VOID_CONVERTER,
                RedisKeyword.MULTI);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final Object cursor, final Charset charset) {
        return new RedisCommand<>(ScanResponse.scanResponseConverter(charset),
                RedisKeyword.SCAN,
                cursor);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final Object cursor, final String matchPattern, final Charset charset) {
        return new RedisCommand<>(ScanResponse.scanResponseConverter(charset),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final Object cursor, final long count, final Charset charset) {
        return new RedisCommand<>(ScanResponse.scanResponseConverter(charset),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.COUNT,
                count);
    }

    static RedisCommand<ScanResponse> buildScanCommand(final Object cursor, final String matchPattern, final long count, final Charset charset) {
        return new RedisCommand<>(ScanResponse.scanResponseConverter(charset),
                RedisKeyword.SCAN,
                cursor,
                RedisKeyword.MATCH,
                matchPattern,
                RedisKeyword.COUNT,
                count);
    }

    static RedisCommand<Long> buildSubscribeCommand(final String... channels) {
        final Object[] components = new Object[channels.length + 1];

        components[0] = RedisKeyword.SUBSCRIBE;
        System.arraycopy(channels, 0, components, 1, channels.length);

        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER, components);
    }

    static RedisCommand<Long> buildUnsubscribeCommand(final String... channels) {
        final Object[] components = new Object[channels.length + 1];

        components[0] = RedisKeyword.UNSUBSCRIBE;
        System.arraycopy(channels, 0, components, 1, channels.length);

        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER, components);
    }

    static RedisCommand<Long> buildPsubscribeCommand(final String... patterns) {
        final Object[] components = new Object[patterns.length + 1];

        components[0] = RedisKeyword.PSUBSCRIBE;
        System.arraycopy(patterns, 0, components, 1, patterns.length);

        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER, components);
    }

    static RedisCommand<Long> buildPunsubscribeCommand(final String... patterns) {
        final Object[] components = new Object[patterns.length + 1];

        components[0] = RedisKeyword.PUNSUBSCRIBE;
        System.arraycopy(patterns, 0, components, 1, patterns.length);

        return new RedisCommand<>(RedisResponseConverters.INTEGER_CONVERTER, components);
    }
}
