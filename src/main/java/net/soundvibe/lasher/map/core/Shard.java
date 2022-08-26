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
            rwLock.readLock();
    		try {
				return lasher.get(key, hash);
			} finally {
                rwLock.readUnlock();
            }
		});
    }

    public byte[] put(byte[] key, long hash, byte[] value) {
    	return metrics.putLatency.record(() -> {
            rwLock.writeLock();
			try {
				return lasher.put(key, value, hash);
			} finally {
                rwLock.writeUnlock();
            }
		});
    }

    public byte[] putIfAbsent(byte[] key, long hash, byte[] value) {
        rwLock.writeLock();
        try {
            return lasher.putIfAbsent(key, value, hash);
        } finally {
            rwLock.writeUnlock();
        }
    }

    public byte[] remove(byte[] key, long hash) {
        rwLock.writeLock();
        try {
            return lasher.remove(key, hash);
        } finally {
            rwLock.writeUnlock();
        }
    }

    public boolean remove(byte[] key, long hash, byte[] value) {
        rwLock.writeLock();
        try {
            return lasher.remove(key, value, hash);
        } finally {
            rwLock.writeUnlock();
        }
    }

    public boolean replace(byte[] key, long hash, byte[] prevVal, byte[] newVal) {
        rwLock.writeLock();
        try {
            return lasher.replace(key, hash, prevVal, newVal);
        } finally {
            rwLock.writeUnlock();
        }
    }

    public byte[] replace(byte[] key, long hash, byte[] value) {
        rwLock.writeLock();
        try {
            return lasher.replace(key, value, hash);
        } finally {
            rwLock.writeUnlock();
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
        rwLock.writeLock();
        try {
            lasher.clear();
        } finally {
            rwLock.writeUnlock();
        }
    }

	public void delete() {
        rwLock.writeLock();
		try {
			lasher.delete();
		} finally {
            rwLock.writeUnlock();
        }
	}

	@Override
    public void close() {
        rwLock.writeLock();
        try {
            lasher.close();
        } finally {
            rwLock.writeUnlock();
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
