package net.soundvibe.lasher.serde;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.Random;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class BytesSerdeTest {

    private static final BytesSerde sut = new BytesSerde();

    @ParameterizedTest
    @NullSource
    @MethodSource("bytesProvider")
    void should_read_write_bytes(byte[] value) {
        var bytes = sut.toBytes(value);
        var actual = sut.fromBytes(bytes);
        assertEquals(value, actual);
    }

    static Stream<byte[]> bytesProvider() {
        var random = new Random();
        return IntStream.range(0, 100)
                .mapToObj(byte[]::new)
                .peek(random::nextBytes);
    }
}