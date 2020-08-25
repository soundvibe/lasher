package net.soundvibe.lasher.map;

import net.soundvibe.lasher.serde.Serdes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

import static java.util.stream.Collectors.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TimeLashTest {

    @Test
    void should_stream_keys_and_values(@TempDir Path tmpDir) {
        try (var sut = new TimeLash<>(tmpDir, Duration.ofHours(24), Serdes.STRING, Serdes.STRING)) {
            assertNull(sut.put("foo1", "bar1", Instant.parse("2020-02-03T06:10:00Z").getEpochSecond()));
            assertNull(sut.put("foo2", "bar2", Instant.parse("2020-02-03T11:00:00Z").getEpochSecond()));
            assertNull(sut.put("foo3", "bar3", Instant.parse("2020-02-03T12:23:00Z").getEpochSecond()));
            assertNull(sut.put("foo4", "bar4", Instant.parse("2020-02-03T14:00:00Z").getEpochSecond()));

            assertEquals(4L, sut.size());
            assertEquals(4L, sut.stream().count());

            var entries = sut.stream().collect(toSet());
            assertEquals(Set.of(
                    new SimpleEntry<>("foo1", "bar1"),
                    new SimpleEntry<>("foo2", "bar2"),
                    new SimpleEntry<>("foo3", "bar3"),
                    new SimpleEntry<>("foo4", "bar4")
            ), entries);

            var keys = sut.streamKeys().collect(toSet());
            assertEquals(Set.of("foo1", "foo2", "foo3", "foo4"), keys);
        }
    }

    @Test
    void should_expire_old_entries(@TempDir Path tmpDir) {
        try (var sut = new TimeLash<>(tmpDir, Duration.ofHours(6), Serdes.STRING, Serdes.STRING)) {
            assertNull(sut.put("foo1", "bar1", Instant.parse("2020-02-03T06:10:00Z").getEpochSecond()));
            assertEquals("bar1", sut.get("foo1", Instant.parse("2020-02-03T06:11:00Z").getEpochSecond()));

            assertNull(sut.put("foo2", "bar2", Instant.parse("2020-02-03T11:00:00Z").getEpochSecond()));
            assertEquals("bar2", sut.get("foo2", Instant.parse("2020-02-03T11:11:00Z").getEpochSecond()));

            assertNull(sut.put("foo3", "bar3", Instant.parse("2020-02-03T12:23:00Z").getEpochSecond()));
            assertEquals("bar3", sut.get("foo3", Instant.parse("2020-02-03T12:11:00Z").getEpochSecond()));

            assertNull(sut.put("foo4", "bar4", Instant.parse("2020-02-03T14:00:00Z").getEpochSecond()));
            assertNull(sut.get("foo1", Instant.parse("2020-02-03T06:10:00Z").getEpochSecond()));
            assertEquals("bar4", sut.get("foo4", Instant.parse("2020-02-03T14:00:00Z").getEpochSecond()));
            assertEquals("bar2", sut.get("foo2", Instant.parse("2020-02-03T11:11:00Z").getEpochSecond()));
            assertEquals("bar3", sut.get("foo3", Instant.parse("2020-02-03T12:11:00Z").getEpochSecond()));
            assertEquals(3L, sut.size());
        }
    }

    @Test
    void should_not_allow_adding_expired_keys(@TempDir Path tmpDir) {
        try (var sut = new TimeLash<>(tmpDir, Duration.ofHours(1), Serdes.STRING, Serdes.STRING)) {
            assertNull(sut.put("foo1", "bar1", Instant.parse("2020-02-03T06:10:10Z")));
            assertNull(sut.put("foo2", "bar2", Instant.parse("2020-02-03T05:10:00Z")));

            assertNull(sut.get("foo2", Instant.parse("2020-02-03T05:10:00Z")));
            assertEquals("bar1", sut.get("foo1", Instant.parse("2020-02-03T06:10:10Z")));
        }
    }

    @Test
    void should_remove_entries(@TempDir Path tmpDir) {
        try (var sut = new TimeLash<>(tmpDir, Duration.ofHours(1), Serdes.LONG, Serdes.STRING)) {
            assertNull(sut.put(1L, "test1", Instant.now()));
            assertEquals("test1", sut.get(1L, Instant.now()));
            assertTrue(sut.containsKey(1L, Instant.now()));
            assertEquals("test1", sut.remove(1L, Instant.now()));
        }
    }
}