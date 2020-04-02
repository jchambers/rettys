package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A Redis value decoder converts complete Redis value frames and parses them as Java types. Redis value types are
 * converted to Java types as follows:
 *
 * <table>
 *     <thead>
 *         <tr>
 *             <th>Redis type</th>
 *             <th>Java type</th>
 *         </tr>
 *     </thead>
 *
 *     <tbody>
 *         <tr>
 *             <td>Simple string</td>
 *             <td>{@link String}</td>
 *         </tr>
 *
 *         <tr>
 *             <td>Error</td>
 *             <td>{@link RedisException}</td>
 *         </tr>
 *
 *         <tr>
 *             <td>Integer</td>
 *             <td>{@link Long}</td>
 *         </tr>
 *
 *         <tr>
 *             <td>Bulk string</td>
 *             <td>byte[]</td>
 *         </tr>
 *
 *         <tr>
 *             <td>Array</td>
 *             <td>Object[]</td>
 *         </tr>
 *     </tbody>
 * </table>
 *
 * @see RedisFrameDecoder
 */
class RedisValueDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out) throws Exception {
        // We can assume that the byte buffer we're processing represents a complete Redis value because there's a
        // RedisFrameDecoder earlier in the pipeline.
        out.add(parseNextValue(in));
    }

    /**
     * Parses the next Redis value from the given byte buffer and advances the write buffer's read index accordingly.
     *
     * @param byteBuf the buffer from which to extract a Redis value; must contain at least one complete Redis value
     *
     * @return a Java representation of the next Redis value in the given byte buffer
     *
     * @throws IOException if a Redis value could not be extracted from the given byte buffer
     */
    private Object parseNextValue(final ByteBuf byteBuf) throws IOException {
        final byte prefixByte = byteBuf.readByte();

        switch (prefixByte) {
            case RedisProtocolUtil.SIMPLE_STRING_PREFIX: {
                return readString(byteBuf);
            }

            case RedisProtocolUtil.ERROR_STRING_PREFIX: {
                return new RedisException(readString(byteBuf));
            }

            case RedisProtocolUtil.INTEGER_PREFIX: {
                return RedisProtocolUtil.readInteger(byteBuf);
            }

            case RedisProtocolUtil.BULK_STRING_PREFIX: {
                final int bulkStringLength = (int) RedisProtocolUtil.readInteger(byteBuf);

                if (bulkStringLength < 0) {
                    return null;
                } else {
                    final byte[] bulkStringBytes = new byte[bulkStringLength];
                    byteBuf.readBytes(bulkStringBytes);

                    // Skip the trailing CRLF
                    byteBuf.skipBytes(2);

                    return bulkStringBytes;
                }
            }

            case RedisProtocolUtil.ARRAY_PREFIX: {
                final int arrayLength = (int) RedisProtocolUtil.readInteger(byteBuf);

                if (arrayLength < 0) {
                    return null;
                } else {
                    final Object[] array = new Object[arrayLength];

                    for (int i = 0; i < arrayLength; i++) {
                        array[i] = parseNextValue(byteBuf);
                    }

                    return array;
                }
            }

            default: {
                throw new IOException(String.format("Unexpected prefix: %x", prefixByte));
            }
        }
    }

    /**
     * Reads the next Redis string from the given byte buffer. This method consumes the trailing '\r\n', but does not
     * include it in the returned string.
     *
     * @param byteBuf the buffer from which to read the next '\r\n'-terminated string
     *
     * @return the next '\r\n'-terminated string in the given byte buffer, excluding the trailing '\r\n'
     */
    private String readString(final ByteBuf byteBuf) {
        final byte[] stringBytes = new byte[byteBuf.bytesBefore((byte) '\r')];
        byteBuf.readBytes(stringBytes);

        final String string = new String(stringBytes, StandardCharsets.US_ASCII);

        // Skip the trailing CRLF
        byteBuf.skipBytes(2);

        return string;
    }
}
