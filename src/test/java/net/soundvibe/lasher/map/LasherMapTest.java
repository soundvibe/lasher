package net.soundvibe.lasher.map;

import net.soundvibe.lasher.serde.Serdes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LasherMapTest {

    @Test
    void should_do_basic_operations(@TempDir Path tmpPath) {
        try (var sut = new LasherMap<>(new LasherDB(tmpPath), Serdes.STRING, Serdes.STRING)) {
            assertNull(sut.get("foo"));

            assertNull(sut.put("foo", "value"));

            assertEquals("value", sut.get("foo"));

            assertEquals("value", sut.remove("foo" ));

            assertNull(sut.put("foo", "valueUpdated" ));

            assertEquals("valueUpdated" ,sut.get("foo" ));

            assertEquals("valueUpdated" , sut.put("foo" , "valueUpdatedAgain" ));

            assertEquals("valueUpdatedAgain" , sut.putIfAbsent("foo" , "valueUpdatedAgain" ));
            assertNull(sut.putIfAbsent("foo2" , "value" ));

            assertTrue(sut.containsKey("foo2" ));
            assertEquals("value" , sut.remove("foo2" ));
            assertFalse(sut.containsKey("foo2" ));


            assertNull(sut.putIfAbsent("foo2" , "value" ));
            assertTrue(sut.remove("foo2" , "value" ));

            assertNull(sut.putIfAbsent("foo2" , "value" ));
            assertEquals("value" , sut.replace("foo2" , "value2" ));

            assertTrue(sut.replace("foo2" , "value2" , "newValue" ));

            sut.clear();
            assertEquals(0L, sut.sizeLong());
            assertTrue(sut.isEmpty());

            assertFalse(sut.containsKey("foo"));
            assertFalse(sut.containsKey("foo2"));
        }
    }

    @Test
    void should_iterate(@TempDir Path tmpPath) {
        try (var sut = new LasherMap<>(new LasherDB(tmpPath), Serdes.LONG, Serdes.LONG)) {
            var rng = new Random();
            var m = new ConcurrentHashMap<Long, Long>(1000);
            long nInserts = 1000;

            for(long k=0; k<nInserts; k++){
                final long v = rng.nextLong();
                sut.put(k, v);
                m.put(k, v);
            }

            var counter = new AtomicLong(0L);
            sut.forEach((k,v) -> {
                counter.incrementAndGet();
                assertEquals(m.get(k).longValue(), v);
                m.remove(k);
            });

            assertEquals(nInserts, counter.get() );
            assertTrue(m.isEmpty());
            assertEquals(nInserts, sut.size());
            for(long i=0; i<nInserts; i++){
                assertTrue(sut.containsKey(i));
            }
        }
    }
}
