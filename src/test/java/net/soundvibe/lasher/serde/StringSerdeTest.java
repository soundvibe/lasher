package net.soundvibe.lasher.serde;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.Random;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StringSerdeTest {

    private static final StringSerde sut = new StringSerde();

    @ParameterizedTest
    @NullSource
    @MethodSource("stringProvider")
    void should_read_write_strings(String value) {
        var bytes = sut.toBytes(value);
        var actual = sut.fromBytes(bytes);
        assertEquals(value, actual);
    }

    static Stream<String> stringProvider() {
        var random = new Random();
        return IntStream.range(0, 100)
                .mapToObj(byte[]::new)
                .map(bytes -> {
                    random.nextBytes(bytes);
                    return new String(bytes);
                });
    }
}