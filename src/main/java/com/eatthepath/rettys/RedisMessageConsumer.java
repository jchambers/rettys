package com.eatthepath.rettys;

import io.netty.channel.Channel;

public interface RedisMessageConsumer {

    void consumeMessage(Channel source, Object message);

    void handleChannelClosure(Channel channel);
}
