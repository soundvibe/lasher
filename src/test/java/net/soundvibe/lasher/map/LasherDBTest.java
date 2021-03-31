package net.soundvibe.lasher.map;

import net.soundvibe.lasher.util.BytesSupport;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LasherDBTest {

    @Test
    void should_do_basic_operations(@TempDir Path tmpPath) {
        try (var sut = new LasherDB(tmpPath)) {
            assertNull(sut.get("foo".getBytes()));

            assertNull(sut.put("foo".getBytes(), "value".getBytes()));

            assertArrayEquals("value".getBytes(),sut.get("foo".getBytes()));

            assertArrayEquals("value".getBytes(),sut.remove("foo".getBytes()));

            assertNull(sut.put("foo".getBytes(), "valueUpdated".getBytes()));

            assertArrayEquals("valueUpdated".getBytes(),sut.get("foo".getBytes()));

            assertArrayEquals("valueUpdated".getBytes(), sut.put("foo".getBytes(), "valueUpdatedAgain".getBytes()));

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
    void should_iterate(@TempDir Path tmpPath) {
        try (var sut = new LasherDB(tmpPath)) {
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
    void should_allow_concurrent_inserts(@TempDir Path tmpPath) throws Exception {
        try (var sut = new LasherDB(tmpPath, 4)) {
            final int recsPerThread = 750000;

            var t1 = new Thread(() -> {
                for(long i=0; i<recsPerThread; i++){
                    final byte[] k = BytesSupport.longToBytes(i);
                    final byte[] v = BytesSupport.longToBytes(i+1);
                    sut.put(k, v);
                }
            });
            var t2 = new Thread(() -> {
                for(long i=recsPerThread; i<recsPerThread*2; i++){
                    final byte[] k = BytesSupport.longToBytes(i);
                    final byte[] v = BytesSupport.longToBytes(i+1);
                    sut.put(k, v);
                }
            });
            var t3 = new Thread(() -> {
                for(long i=recsPerThread*2; i<recsPerThread*3; i++){
                    final byte[] k = BytesSupport.longToBytes(i);
                    final byte[] v = BytesSupport.longToBytes(i+1);
                    sut.put(k, v);
                }
            });
            var t4 = new Thread(() -> {
                for(long i=recsPerThread*3; i<recsPerThread*4; i++){
                    final byte[] k = BytesSupport.longToBytes(i);
                    final byte[] v = BytesSupport.longToBytes(i+1);
                    sut.put(k, v);
                }
            });
            var t5 = new Thread(() -> {
                for(long i=recsPerThread; i<recsPerThread*4; i++){
                    final byte[] k = BytesSupport.longToBytes(i);
                    final byte[] v = BytesSupport.longToBytes(i+1);
                    sut.put(k, v);
                }
            });

            t1.start(); t2.start(); t3.start(); t4.start(); t5.start();
            t1.join();  t2.join();  t3.join();  t4.join(); t5.join();
            for(long i=0; i<recsPerThread*4; i++){
                assertEquals(BytesSupport.bytesToLong(sut.get(BytesSupport.longToBytes(i))), i+1);
            }
        }
    }
}