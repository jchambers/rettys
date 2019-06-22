package com.eatthepath.rettys;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RedisExceptionTest {

    @Test
    void redisExceptionNullMessage() {
        //noinspection ThrowableNotThrown
        assertThrows(NullPointerException.class, () -> new RedisException(null));
    }

    @Test
    void getMessage() {
        final String message = "TEST This is an test error message";

        assertEquals(message, new RedisException(message).getMessage());
    }

    @ParameterizedTest
    @MethodSource("errorMessageProvider")
    void getErrorPrefix(final String errorMessage, final String expectedErrorPrefix) {
        assertEquals(expectedErrorPrefix, new RedisException(errorMessage).getErrorPrefix());
    }

    static Stream<Arguments> errorMessageProvider() {
        return Stream.of(
                arguments("TEST A test error with an explanatory message", "TEST"),
                arguments("TEST", "TEST"));
    }
}