package net.soundvibe.lasher.map.core;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.soundvibe.lasher.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LasherTest {

    private static final long MB_32 = (long) Math.pow(2, 25L);
    private static final long MB_64 = (long) Math.pow(2, 26L);
    private final MeterRegistry registry = new SimpleMeterRegistry();

	@BeforeEach
	void setUp() {
		Metrics.addRegistry(registry);
	}

	@AfterEach
	void tearDown() {
		Metrics.removeRegistry(registry);
	}

	@Test
    void should_do_basic_operations(@TempDir Path tmpPath) {
        try (var sut = new Lasher(tmpPath, MB_32, MB_32)) {
            assertNull(sut.get("foo".getBytes()));

            assertNull(sut.put("foo".getBytes(), "value".getBytes()));

            assertArrayEquals("value".getBytes(),sut.get("foo".getBytes()));

            assertArrayEquals("value".getBytes(),sut.remove("foo".getBytes()));

            assertNull(sut.put("foo".getBytes(), "valueUpdated".getBytes()));

            assertArrayEquals("valueUpdated".getBytes(),sut.get("foo".getBytes()));

            assertArrayEquals("valueUpdated".getBytes(), sut.put("foo".getBytes(), "valueUpdatedAgain".getBytes()));

            assertEquals(MB_64, sut.nextPowerOf2(60 * 1024 * 1000));

            assertArrayEquals("valueUpdatedAgain".getBytes(), sut.putIfAbsent("foo".getBytes(), "valueUpdatedAgain".getBytes()));
            assertNull(sut.putIfAbsent("foo2".getBytes(), "value".getBytes()));

            assertTrue(sut.containsKey("foo2".getBytes()));
            assertArrayEquals("value".getBytes(), sut.remove("foo2".getBytes()));
            assertFalse(sut.containsKey("foo2".getBytes()));


            assertNull(sut.putIfAbsent("foo2".getBytes(), "value".getBytes()));
            assertTrue(sut.remove("foo2".getBytes(), "value".getBytes()));

            assertNull(sut.putIfAbsent("foo2".getBytes(), "value".getBytes()));
            assertArrayEquals("value".getBytes(), sut.replace("foo2".getBytes(), "value2".getBytes()));

            assertTrue(sut.replace("foo2".getBytes(), "value2".getBytes(), "newValue".getBytes()));

            sut.clear();
            assertEquals(0L, sut.size());

            assertFalse(sut.containsKey("foo".getBytes()));
            assertFalse(sut.containsKey("foo2".getBytes()));

            assertThrows(NullPointerException.class, () -> sut.put(null, "value".getBytes()));
            assertThrows(NullPointerException.class, () -> sut.put("key".getBytes(), null));
        }
    }

    @Test
    void should_rehash(@TempDir Path tmpPath) {
        long fileSize = (long) Math.pow(2, 8L);
        try (var sut = new Lasher(tmpPath, fileSize, fileSize)) {
            assertEquals(0L, sut.rehashIndex.get());
            long count = 15_000_000;
            var bytes = new byte[32];
            Arrays.fill(bytes, (byte)1);

            for (long i = 0; i < count; i++) {
                sut.put(BytesSupport.longToBytes(i), bytes);
            }

            assertNotEquals(0L, sut.rehashIndex.get());

            for (long i = 0; i < count; i++) {
                assertArrayEquals(bytes, sut.get(BytesSupport.longToBytes(i)));
            }

            var rehashCount = ((long) sut.metrics.rehashCounter().count());
            assertTrue(rehashCount > 0L);
        }
    }

    @Test
    void should_read_from_store(@TempDir Path tmpPath) {
        long fileSize = (long) Math.pow(2, 8L);
        long count = 15_000_000;
        var bytes = new byte[24];
        Arrays.fill(bytes, (byte)1);
        try (var sut = new Lasher(tmpPath, fileSize, fileSize)) {
            assertEquals(0L, sut.rehashIndex.get());

            for (long i = 0; i < count; i++) {
                sut.put(BytesSupport.longToBytes(i), bytes);
            }

            assertNotEquals(0L, sut.rehashIndex.get());

            for (long i = 0; i < count; i++) {
                assertArrayEquals(bytes, sut.get(BytesSupport.longToBytes(i)));
            }

            assertEquals(count, sut.size());
        }

        try (var sut = new Lasher(tmpPath, fileSize, fileSize)) {
            assertEquals(count, sut.size());
            for (long i = 0; i < count; i++) {
                assertArrayEquals(bytes, sut.get(BytesSupport.longToBytes(i)));
            }
        }
    }

    @Test
    void should_iterate(@TempDir Path tmpPath) {
        try (var sut = new Lasher(tmpPath, MB_32, MB_32)) {
            var m = new ConcurrentHashMap<Long, Long>(1000);
            var rng = new Random();
            long nInserts = 1000;

            for(long k=0; k<nInserts; k++){
                final byte[] kBytes = BytesSupport.longToBytes(k);
                final long v = rng.nextLong();
                final byte[] vBytes = BytesSupport.longToBytes(v);
                m.put(k, v);
                sut.put(kBytes, vBytes);
            }
            var it = sut.iterator();
            for(long i=0; i<nInserts; i++){
                var e = it.next();
                final long k = BytesSupport.bytesToLong(e.getKey());
                final long v = BytesSupport.bytesToLong(e.getValue());
                //Does iterator value match what was inserted?
                assertEquals(m.get(k).longValue(), v);
                m.remove(k);
            }
            //Does iteration hit all elements, and no more?
            assertFalse(it.hasNext());
            assertTrue(m.isEmpty());
            assertEquals(nInserts, sut.size());
            for(long i=0; i<nInserts; i++){
                final byte[] kBytes = BytesSupport.longToBytes(i);
                assertTrue(sut.containsKey(kBytes));
            }
        }
    }

    @Test
    void should_work_when_collisions_occur(@TempDir Path tmpPath) {
        try (var sut = new Lasher(tmpPath, MB_32, MB_32)) {
            final long[] collisions = new long[500];
            collisions[0] = 1;
            for(int idx = 1; idx<collisions.length; idx++){
                collisions[idx] = Hash.findCollision(collisions[idx - 1], 16);
            }

            for(int i=0; i<collisions.length; i++){
                final long v = collisions[i]+1;
                final byte[] kBytes = BytesSupport.longToBytes(collisions[i]);
                final byte[] vBytes = BytesSupport.longToBytes(v);
                sut.put(kBytes, vBytes);
                final byte[] vOutBytes = sut.get(kBytes);
                final long vOut = BytesSupport.bytesToLong(vOutBytes);

                assertEquals(v, vOut, "Insert/retrieval failure on index " + i);
            }
            for(int idx=0; idx<collisions.length; idx++){
                final long i = collisions[idx];
                final byte[] k = BytesSupport.longToBytes(i);
                final byte[] v = sut.get(k);
                assertEquals(i+1
                        , BytesSupport.bytesToLong(v)
                        , "Unexpected inequality on index " + idx + ".");
            }
        }
    }
}
