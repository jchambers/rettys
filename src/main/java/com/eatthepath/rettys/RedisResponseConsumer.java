package com.eatthepath.rettys;

public interface RedisResponseConsumer {

    void consumeResponse(RedisCommand command, Object response);

    void handleCommandFailure(RedisCommand command, Throwable cause);
}
