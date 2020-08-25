package net.soundvibe.lasher.serde;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.Random;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class IntegerSerdeTest {

    private static final IntegerSerde sut = new IntegerSerde();

    @ParameterizedTest
    @NullSource
    @MethodSource("integerProvider")
    void should_read_write_ints(Integer value) {
        var bytes = sut.toBytes(value);
        var actual = sut.fromBytes(bytes);
        assertEquals(value, actual);
    }

    static Stream<Integer> integerProvider() {
        var random = new Random();
        return IntStream.range(0, 100)
                .mapToObj(i -> random.nextInt());
    }
}