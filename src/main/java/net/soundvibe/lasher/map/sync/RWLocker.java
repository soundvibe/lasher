package net.soundvibe.lasher.map.sync;

import java.util.concurrent.locks.*;

public record RWLocker(ReadWriteLock rwLock) implements Locker {

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
