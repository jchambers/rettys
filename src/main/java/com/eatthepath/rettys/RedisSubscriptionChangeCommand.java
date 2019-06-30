package com.eatthepath.rettys;

import java.util.function.BiConsumer;
import java.util.function.Function;

public class RedisSubscriptionChangeCommand extends RedisCommand<Long> {

    final BiConsumer<String, Object> handler;

    public RedisSubscriptionChangeCommand(final BiConsumer<String, Object> handler, final Object... components) {
        super(RedisResponseConverters.INTEGER_CONVERTER, components);

        this.handler = handler;
    }

    public BiConsumer<String, Object> getHandler() {
        return handler;
    }
}
