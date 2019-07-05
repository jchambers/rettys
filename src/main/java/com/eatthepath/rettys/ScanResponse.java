package com.eatthepath.rettys;

import java.nio.charset.Charset;
import java.util.function.Function;

public class ScanResponse {
    private final byte[] cursor;
    private final String[] keys;

    static Function<Object, ScanResponse> scanResponseConverter(final Charset charset) {
        return redisResponse -> {
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
            final String[] keys = new String[keyArray.length];

            for (int i = 0; i < keyArray.length; i++) {
                if (!(keyArray[i] instanceof byte[])) {
                    throw new IllegalArgumentException("Could not convert array element to bulk string: " + keyArray[i].getClass());
                }

                keys[i] = new String((byte[]) keyArray[i], charset);
            }

            return new ScanResponse(cursor, keys);
        };
    }

    ScanResponse(final byte[] cursor, final String[] keys) {
        this.cursor = cursor;
        this.keys = keys;
    }

    public byte[] getCursor() {
        return cursor;
    }

    public String[] getKeys() {
        return keys;
    }
}
