package com.eatthepath.rettys;

import java.util.function.Function;

class ScanResponse {
    private final byte[] cursor;
    private final byte[][] keys;

    static final Function<Object, ScanResponse> SCAN_RESPONSE_CONVERTER = redisResponse -> {
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

    ScanResponse(final byte[] cursor, final byte[][] keys) {
        this.cursor = cursor;
        this.keys = keys;
    }

    byte[] getCursor() {
        return cursor;
    }

    byte[][] getKeys() {
        return keys;
    }
}
