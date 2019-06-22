package com.eatthepath.rettys;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

                    final int bulkStringLength;
                    {
                        final byte[] bulkStringLengthBytes = new byte[bytesBeforeCarriageReturn];
                        byteBuf.readBytes(bulkStringLengthBytes);

                        bulkStringLength = Integer.parseInt(
                                new String(bulkStringLengthBytes, StandardCharsets.US_ASCII), 10);
                    }

                    // At this point, the read index is just before the "\r" after the bulk string length section. We
                    // want to make sure all the rest of the data is there

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

                    final int arrayLength;
                    {
                        final byte[] arrayLengthBytes = new byte[bytesBeforeCarriageReturn];
                        byteBuf.readBytes(arrayLengthBytes);

                        arrayLength = Integer.parseInt(
                                new String(arrayLengthBytes, StandardCharsets.US_ASCII), 10);
                    }

                    // Skip the CRLF after the array length to position the read index just before the first value (if
                    // any)
                    byteBuf.skipBytes(2);

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
}
