package com.eatthepath.rettys;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class RedisTransactionTest {

    private RedisTransaction transaction;

    @BeforeEach
    void beforeEach() {
        transaction = new RedisTransaction();
    }

    @Test
    void executeCommand() {
        assertTrue(transaction.getCommands().isEmpty());

        transaction.llen("Test");

        assertEquals(1, transaction.getCommands().size());
        assertEquals(RedisKeyword.LLEN, transaction.getCommands().get(0).getComponents()[0]);
    }

    @Test
    void execSuccess() {
        final CompletableFuture<Long> llenFuture = transaction.llen("Test");

        final long expectedLlen = 12;
        transaction.getExecCommand().getFuture().complete(new Object[] { expectedLlen });

        assertEquals(expectedLlen, assertTimeoutPreemptively(Duration.ofSeconds(1), llenFuture::join));
    }

    @Test
    void execFailure() {
        final CompletableFuture<Long> llenFuture = transaction.llen("Test");

        final RedisException redisException = new RedisException("ERR OH NO");
        transaction.getExecCommand().getFuture().completeExceptionally(redisException);

        final CompletionException completionException =
                assertThrows(CompletionException.class, () -> assertTimeoutPreemptively(Duration.ofSeconds(1), llenFuture::join));

        assertEquals(redisException, completionException.getCause());
    }

    @Test
    void multi() {
        assertThrows(UnsupportedOperationException.class, () -> transaction.multi());
    }
}
