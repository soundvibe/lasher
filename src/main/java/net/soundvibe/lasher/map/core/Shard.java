package net.soundvibe.lasher.map.core;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.*;
import net.soundvibe.lasher.map.sync.*;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class Shard implements AutoCloseable, Iterable<Map.Entry<byte[], byte[]>> {

    private final int id;
    private final Lasher lasher;
    private final Locker rwLock;
    private final ShardMetrics metrics;

	record ShardMetrics(Timer getLatency, Timer putLatency) {}

    public Shard(int id, Path path, long indexFileLength, long dataFileLength, Tags tags) {
		this.id = id;
		var shardTags = tags.and(Tag.of("shard", String.valueOf(id)));
		this.lasher = Lasher.forShard(path, indexFileLength, dataFileLength, shardTags);
		this.rwLock = new RWLocker(new ReentrantReadWriteLock());
		Metrics.gauge("shard-size", shardTags, this, Shard::size);
		this.metrics = new ShardMetrics(
				Metrics.timer("shard-get-latency", shardTags),
				Metrics.timer("shard-put-latency", shardTags)
		);
	}

    public byte[] get(byte[] key, long hash) {
    	return metrics.getLatency.record(() -> {
    		try (var ignored = rwLock.readLock()) {
				return lasher.get(key, hash);
			}
		});
    }

    public byte[] put(byte[] key, long hash, byte[] value) {
    	return metrics.putLatency.record(() -> {
			try (var ignored = rwLock.writeLock()) {
				return lasher.put(key, value, hash);
			}
		});
    }

    public byte[] putIfAbsent(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.putIfAbsent(key, value, hash);
        }
    }

    public byte[] remove(byte[] key, long hash) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.remove(key, hash);
        }
    }

    public boolean remove(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.remove(key, value, hash);
        }
    }

    public boolean replace(byte[] key, long hash, byte[] prevVal, byte[] newVal) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.replace(key, hash, prevVal, newVal);
        }
    }

    public byte[] replace(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.replace(key, value, hash);
        }
    }

    public long size() {
        return lasher.size();
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        return lasher.iterator(rwLock);
    }

    public void clear() {
        try (var ignored = rwLock.writeLock()) {
            lasher.clear();
        }
    }

	public void delete() {
		try (var ignored = rwLock.writeLock()) {
			lasher.delete();
		}
	}

	@Override
    public void close() {
        try (var ignored = rwLock.writeLock()) {
            lasher.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return id == shard.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return "Shard{" +
                "id=" + id +
                ", lasher=" + lasher +
                '}';
    }
}
