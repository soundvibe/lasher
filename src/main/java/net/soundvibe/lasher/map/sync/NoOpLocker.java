package net.soundvibe.lasher.map.sync;

public final class NoOpLocker implements Locker {

    @Override
    public void readLock() {
        //
    }

    @Override
    public void readUnlock() {
        //
    }

    @Override
    public void writeLock() {
        //
    }

    @Override
    public void writeUnlock() {
        //
    }
}
