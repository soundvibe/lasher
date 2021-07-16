package net.soundvibe.lasher.db;

import net.soundvibe.lasher.map.LasherMap;
import net.soundvibe.lasher.serde.Serde;

import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;

public class TimeLash<K,V> implements AutoCloseable, Iterable<Map.Entry<K,V>> {

    private final Path baseDir;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final long retentionSecs;
    private final AtomicLong watermark = new AtomicLong();
    private final Map<Long, LasherMap<K,V>> buckets;
    private final long bucketSizeSeconds;

    private static final Duration DEFAULT_BUCKET_WINDOW = Duration.ofHours(1);

    public TimeLash(Path baseDir, Duration retention, Duration bucketWindow, Serde<K> keySerde, Serde<V> valSerde) {
        this.baseDir = baseDir;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.retentionSecs = retention.toSeconds();
        this.buckets = new ConcurrentHashMap<>(Math.max(10, (int)retention.toHours()));
        this.bucketSizeSeconds = bucketWindow.toSeconds();
    }

    public TimeLash(Path baseDir, Duration retention, Serde<K> keySerde, Serde<V> valSerde) {
        this(baseDir, retention, DEFAULT_BUCKET_WINDOW, keySerde, valSerde);
    }

    public V get(K key, Instant timestamp) {
        return get(key, timestamp.getEpochSecond());
    }

    public V get(K key, long timestamp) {
        long idx = idxFromTimestamp(timestamp);
        var map = buckets.get(idx);
        if (map != null) {
            return map.get(key);
        }
        return null;
    }

    public V put(K key, V value, Instant timestamp) {
        return put(key, value, timestamp.getEpochSecond());
    }

    /**
     * Puts key value to the map based on given timestamp
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param timestamp Number of seconds from the Java epoch of 1970-01-01T00:00:00Z.
     * @return the previous value associated with key, or null if there was no mapping for key.
     */
    public V put(K key, V value, long timestamp) {
        var maxBucketTime = watermark.updateAndGet(prev -> Math.max(prev, idxFromTimestamp(timestamp)));
        long bucketTimestampStart = idxFromTimestamp(timestamp);
        var map = buckets.compute(bucketTimestampStart, (k, oldMap) -> {
            if (oldMap != null) return oldMap;
            if (bucketInRange(bucketTimestampStart, maxBucketTime)) {
                return createNewMap(bucketTimestampStart);
            }
            return null;
        });
        if (map == null) {
            return null;
        }

        if (map.isEmpty()) {
            //check for expired entries
            var expiredEntries = buckets.entrySet().stream()
                    .filter(not(e -> bucketInRange(e.getKey(), maxBucketTime)))
                    .collect(toList());

            expiredEntries.forEach(e -> {
                deleteExpired(e.getValue());
                buckets.remove(e.getKey());
            });
        }

        return map.put(key, value);
    }

    public V remove(K key, Instant timestamp) {
        return remove(key, timestamp.getEpochSecond());
    }

    public V remove(K key, long timestamp) {
        long idx = idxFromTimestamp(timestamp);
        var map = buckets.get(idx);
        if (map != null) {
            return map.remove(key);
        }
        return null;
    }

    public long size() {
        return buckets.values().stream()
                .mapToLong(LasherMap::sizeLong)
                .sum();
    }

    public boolean containsKey(K key, Instant timestamp) {
        return containsKey(key, timestamp.getEpochSecond());
    }

    public boolean containsKey(K key, long timestamp) {
        return get(key, timestamp) != null;
    }

    public Stream<K> streamKeys() {
        return buckets.values().stream()
                .flatMap(map -> map.keySet().stream());
    }

    public Stream<Map.Entry<K,V>> stream() {
        return buckets.values().stream()
                .flatMap(map -> map.entrySet().stream());
    }

    @Override
    public void close() {
        buckets.forEach((k, m) -> m.close());
        buckets.clear();
    }

    private boolean bucketInRange(long bucket, long maxWatermark) {
        // retention 6 hours
        // e.g. 6 hour bucket size
        // max = 06:10 -> [06:00,12:00)
        // new item 17:59 -> [12:00,18:00), max = 12:00
        // allowedStart = max - retention = 12:00 - 6h -> 06:00
        var allowedStart = maxWatermark - retentionSecs;
        return bucket >= allowedStart;
    }

    private LasherMap<K,V> createNewMap(long bucket) {
        var lasher = new LasherDB(baseDir.resolve(Long.toString(bucket)));
        return new LasherMap<>(lasher, keySerde, valSerde);
    }

    private synchronized void deleteExpired(LasherMap<K,V> map) {
        map.delete();
    }

    protected long idxFromTimestamp(long timestamp) {
        return timestamp - (timestamp % bucketSizeSeconds);
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return stream().iterator();
    }
}
