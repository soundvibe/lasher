package net.soundvibe.lasher.map.sync;

import java.util.concurrent.locks.*;

public record RWLocker(ReadWriteLock rwLock) implements Locker {

	@Override
    public void readLock() {
        rwLock.readLock().lock();
    }

    @Override
    public void readUnlock() {
        rwLock.readLock().unlock();
    }

    @Override
    public void writeLock() {
        rwLock.writeLock().lock();
    }

    @Override
    public void writeUnlock() {
        rwLock.writeLock().unlock();
    }
}
