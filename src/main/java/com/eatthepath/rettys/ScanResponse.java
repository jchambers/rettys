package com.eatthepath.rettys;

public class ScanResponse {
    private final byte[] cursor;
    private final byte[][] keys;

    public ScanResponse(final byte[] cursor, final byte[][] keys) {
        this.cursor = cursor;
        this.keys = keys;
    }

    public byte[] getCursor() {
        return cursor;
    }

    public byte[][] getKeys() {
        return keys;
    }
}
