package com.eatthepath.rettys.channel;

import io.netty.buffer.ByteBuf;

class RedisProtocolUtil {

    static final byte SIMPLE_STRING_PREFIX = '+';
    static final byte ERROR_STRING_PREFIX = '-';
    static final byte INTEGER_PREFIX = ':';
    static final byte BULK_STRING_PREFIX = '$';
    static final byte ARRAY_PREFIX = '*';

    /**
     * Reads the next integer value from the given byte buffer. This method consumes the trailing '\r\n'.
     *
     * @param byteBuf the buffer from which to read the next '\r\n'-terminated integer
     *
     * @return the next '\r\n'-terminated integer in the given byte buffer
     */
    static long readInteger(final ByteBuf byteBuf) {
        final boolean negate;

        if (byteBuf.getByte(0) == '-') {
            negate = true;
            byteBuf.skipBytes(1);
        } else {
            negate = false;
        }

        long l = 0;

        for (byte b = byteBuf.readByte(); b != '\r'; b = byteBuf.readByte()) {
            l *= 10;
            l += b - '0';
        }

        // We've already read the trailing '\r' in the loop above; skip the following '\n', too.
        byteBuf.skipBytes(1);

        return negate ? -l : l;
    }
}
