package net.soundvibe.lasher.map.sync;

import java.util.concurrent.locks.*;

public final class RWLocker implements Locker {

    private final ReadWriteLock rwLock;

    public RWLocker(ReadWriteLock rwLock) {
        this.rwLock = rwLock;
    }

    @Override
    public Lock readLock() {
        rwLock.readLock().lock();
        return () -> rwLock.readLock().unlock();
    }

    @Override
    public Lock writeLock() {
        rwLock.writeLock().lock();
        return () -> rwLock.writeLock().unlock();
    }
}
