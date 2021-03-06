package com.eatthepath.rettys.channel;

import com.eatthepath.rettys.RedisCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A Redis command encoder serializes Redis commands to arrays of "bulk strings" to send to a Redis server.
 */
class RedisCommandEncoder extends MessageToByteEncoder<RedisCommand> {

    private final Charset charset;

    private static final byte ARRAY_PREFIX = '*';
    private static final byte BULK_STRING_PREFIX = '$';

    private static final byte[] CRLF = new byte[] { '\r', '\n' };

    private static final byte[] NULL_BULK_STRING = new byte[] { BULK_STRING_PREFIX, '-', '1', '\r', '\n' };

    RedisCommandEncoder(final Charset charset) {
        this.charset = charset;
    }

    @Override
    protected void encode(final ChannelHandlerContext context, final RedisCommand command, final ByteBuf out) {
        // All Redis commands are sent as an array of bulk strings. Start with the array header.
        out.writeByte(ARRAY_PREFIX);
        writeUnsignedIntAsRedisIntegerBytes(command.getComponents().length, out);
        out.writeBytes(CRLF);

        // Each argument (including the name of the command) is written as a Redis bulk string
        for (final Object component : command.getComponents()) {
            if (component == null) {
                out.writeBytes(NULL_BULK_STRING);
            } else {
                final byte[] bulkStringBytes = getBulkStringBytes(component);

                out.writeByte(BULK_STRING_PREFIX);
                writeUnsignedIntAsRedisIntegerBytes(bulkStringBytes.length, out);
                out.writeBytes(CRLF);
                out.writeBytes(bulkStringBytes);
                out.writeBytes(CRLF);
            }
        }
    }

    byte[] getBulkStringBytes(final Object redisValue) {
        assert redisValue != null;

        final byte[] bulkStringBytes;

        if (redisValue instanceof byte[]) {
            bulkStringBytes = (byte[]) redisValue;
        } else if (redisValue instanceof String) {
            bulkStringBytes = ((String) redisValue).getBytes(charset);
        } else if (redisValue instanceof Number) {
            bulkStringBytes = redisValue.toString().getBytes(StandardCharsets.US_ASCII);
        } else {
            throw new IllegalArgumentException("Unexpected argument type: " + redisValue.getClass());
        }

        return bulkStringBytes;
    }

    private static void writeUnsignedIntAsRedisIntegerBytes(final int i, final ByteBuf out) {
        out.writeBytes(String.valueOf(i).getBytes(StandardCharsets.US_ASCII));
    }
}
