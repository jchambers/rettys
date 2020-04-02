package com.eatthepath.rettys.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

/**
 * A Redis frame decoder breaks a stream of bytes from a Redis server into complete "frames," each of which represents
 * a complete Redis value.
 */
class RedisFrameDecoder extends ByteToMessageDecoder {

    private static final byte SIMPLE_STRING_PREFIX = '+';
    private static final byte ERROR_STRING_PREFIX = '-';
    private static final byte INTEGER_PREFIX = ':';
    private static final byte BULK_STRING_PREFIX = '$';
    private static final byte ARRAY_PREFIX = '*';

    private static final IndexOutOfBoundsException BUFFER_UNDERRUN_EXCEPTION = new IndexOutOfBoundsException();

    @Override
    protected void decode(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out) throws Exception {
        while (in.readableBytes() > 0) {
            try {
                out.add(in.readRetainedSlice(getLengthOfNextFrame(in)));
            } catch (final IndexOutOfBoundsException e) {
                // We don't have enough data for a complete Redis frame; just wait for more data to arrive
                return;
            }
        }
    }

    /**
     * Returns the length of the next Redis frame in bytes. The returned length includes any prefixes and trailing line
     * feeds and newline characters.
     *
     * @param byteBuf the buffer from which
     * @return the length of the next redis frame in the given buffer in bytes, including any prefixes and trailing line
     * feeds and newline characters
     *
     * @throws IOException if the data in the given buffer could not be parsed as a legal Redis value
     * @throws IndexOutOfBoundsException if the given buffer does not (yet) contain enough data to determine the length
     * of the next frame
     */
    int getLengthOfNextFrame(final ByteBuf byteBuf) throws IOException {
        byteBuf.markReaderIndex();

        final int frameLength;

        try {
            final byte prefixByte = byteBuf.readByte();

            switch (prefixByte) {
                case SIMPLE_STRING_PREFIX:
                case ERROR_STRING_PREFIX:
                case INTEGER_PREFIX: {
                    final int bytesBeforeNewline = byteBuf.bytesBefore((byte) '\n');

                    if (bytesBeforeNewline < 0) {
                        throw BUFFER_UNDERRUN_EXCEPTION;
                    }

                    // We have one additional byte for the prefix and one for the newline, since bytesBefore doesn't
                    // include the newline itself.
                    frameLength = bytesBeforeNewline + 2;

                    break;
                }

                case BULK_STRING_PREFIX: {
                    final int bytesBeforeCarriageReturn = byteBuf.bytesBefore((byte) '\r');

                    if (bytesBeforeCarriageReturn < 0) {
                        throw BUFFER_UNDERRUN_EXCEPTION;
                    }

                    final int bulkStringLength = (int) readInteger(byteBuf);

                    // At this point, the read index is just after the '\r\n' after the bulk string length section.
                    if (bulkStringLength >= 0) {
                        // In total, we have:
                        //
                        // - One byte for the prefix
                        // - The number of bytes used to tell us about the length of the rest of the bulk string
                        // - A CRLF between the length and the payload
                        // - The payload itself
                        // - A closing CRLF
                        frameLength = 1 + bytesBeforeCarriageReturn + 2 + bulkStringLength + 2;
                    } else {
                        // Special case: Redis represents null bulk string values as "$-1\r\n", so there's one less CRLF
                        // than we'd otherwise expect (and, obviously, no payload).
                        frameLength = 1 + bytesBeforeCarriageReturn + 2;
                    }

                    break;
                }

                case ARRAY_PREFIX: {
                    final int bytesBeforeCarriageReturn = byteBuf.bytesBefore((byte) '\r');

                    if (bytesBeforeCarriageReturn < 0) {
                        throw BUFFER_UNDERRUN_EXCEPTION;
                    }

                    final int arrayLength = (int) readInteger(byteBuf);

                    int totalArrayElementLength = 0;

                    for (int i = 0; i < arrayLength; i++) {
                        totalArrayElementLength += getLengthOfNextFrame(byteBuf.slice(
                                byteBuf.readerIndex() + totalArrayElementLength,
                                byteBuf.readableBytes() - totalArrayElementLength));
                    }

                    frameLength = 1 + bytesBeforeCarriageReturn + 2 + totalArrayElementLength;

                    break;
                }

                default: {
                    throw new IOException(String.format("Unexpected prefix: %x", prefixByte));
                }
            }
        } finally {
            byteBuf.resetReaderIndex();
        }

        return frameLength;
    }

    /**
     * Reads the next integer value from the given byte buffer. This method consumes the trailing '\r\n'.
     *
     * @param byteBuf the buffer from which to read the next '\r\n'-terminated integer
     *
     * @return the next '\r\n'-terminated integer in the given byte buffer
     */
    private long readInteger(final ByteBuf byteBuf) {
        long l = 0;

        for (byte b = byteBuf.readByte(); b != '\r'; b = byteBuf.readByte()) {
            l *= 10;
            l += b - '0';
        }

        // We've already read the trailing '\r' in the loop above; skip the following '\n', too.
        byteBuf.skipBytes(1);

        return l;
    }
}
