package net.soundvibe.lasher.serde;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LongSerdeTest {

    private static final LongSerde sut = new LongSerde();

    @ParameterizedTest
    @NullSource
    @MethodSource("longProvider")
    void should_read_write_longs(Long value) {
        var bytes = sut.toBytes(value);
        var actual = sut.fromBytes(bytes);
        assertEquals(value, actual);
    }

    static Stream<Long> longProvider() {
        var random = new Random();
        return IntStream.range(0, 100)
                .mapToObj(i -> random.nextLong());
    }
}