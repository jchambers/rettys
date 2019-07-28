package com.eatthepath.rettys;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RedisClusterTest {

    @Test
    void crc16() {
        assertEquals(0x31c3, RedisCluster.crc16("123456789".getBytes(StandardCharsets.US_ASCII)));
    }
}
