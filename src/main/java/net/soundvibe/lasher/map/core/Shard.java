package net.soundvibe.lasher.map.core;

import java.util.*;
import java.util.concurrent.locks.*;

public final class Shard implements AutoCloseable, Iterable<Map.Entry<byte[], byte[]>> {

    private final int id;
    private final Lasher lasher;
    private final Locker rwLock;

    public Shard(int id, Lasher lasher) {
        this.id = id;
        this.lasher = lasher;
        this.rwLock = new RWLocker(new ReentrantReadWriteLock());
    }

    public byte[] get(byte[] key, long hash) {
        try (var ignored = rwLock.readLock()) {
           return lasher.get(key, hash);
        }
    }

    public byte[] put(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.put(key, hash, value, true);
        }
    }

    public byte[] putIfAbsent(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.putIfAbsent(key, hash, value);
        }
    }

    public byte[] remove(byte[] key, long hash) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.remove(key, hash);
        }
    }

    public boolean remove(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.remove(key, hash, value);
        }
    }

    public boolean replace(byte[] key, long hash, byte[] prevVal, byte[] newVal) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.replace(key, hash, prevVal, newVal);
        }
    }

    public byte[] replace(byte[] key, long hash, byte[] value) {
        try (var ignored = rwLock.writeLock()) {
            return lasher.replace(key, hash, value);
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
