package com.eatthepath.rettys;

import java.nio.charset.StandardCharsets;

/**
 * An enumeration of Redis keywords (commands, subcommands, and options).
 */
enum RedisKeyword {
    LLEN("LLEN"),
    MEMORY("MEMORY"),
    SCAN("SCAN"),
    USAGE("USAGE");

    private final byte[] bytes;

    RedisKeyword(final String keyword) {
        this.bytes = keyword.getBytes(StandardCharsets.US_ASCII);
    }

    byte[] getBulkStringBytes() {
        return bytes;
    }
}
