package net.soundvibe.lasher.serde;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.UUID;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class UUIDSerdeTest {

    private static final UUIDSerde sut = new UUIDSerde();

    @ParameterizedTest
    @NullSource
    @MethodSource("uuidProvider")
    void should_read_write_uuids(UUID uuid) {
        var bytes = sut.toBytes(uuid);
        var actual = sut.fromBytes(bytes);
        assertEquals(uuid, actual);
    }

    static Stream<UUID> uuidProvider() {
        return IntStream.range(0, 100)
                .mapToObj(i -> UUID.randomUUID());
    }
}