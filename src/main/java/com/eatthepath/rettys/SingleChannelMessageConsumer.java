package com.eatthepath.rettys;

/**
 * A message consumer that receives consumes messages from a single Redis {@link io.netty.channel.Channel}.
 *
 * @see RedisMessageConsumer
 */
public interface SingleChannelMessageConsumer {

    void consumeMessage(Object message);

    void handleChannelClosure();
}
