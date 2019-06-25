package com.eatthepath.rettys;

public class ScanResponse {
    private final long cursor;
    private final byte[][] keys;

    public ScanResponse(final long cursor, final byte[][] keys) {
        this.cursor = cursor;
        this.keys = keys;
    }

    public long getCursor() {
        return cursor;
    }

    public byte[][] getKeys() {
        return keys;
    }
}
