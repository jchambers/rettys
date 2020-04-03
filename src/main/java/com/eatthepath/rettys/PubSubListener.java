package com.eatthepath.rettys;

public interface PubSubListener {

    void handlePublishedMessage(String topic, byte[] message);
}
