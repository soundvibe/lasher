package net.soundvibe.lasher.map.core;

import java.util.concurrent.locks.*;

public final class Shard implements AutoCloseable {

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
