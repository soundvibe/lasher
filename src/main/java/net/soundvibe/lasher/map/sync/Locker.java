package net.soundvibe.lasher.map.sync;

public interface Locker {

    void readLock();

    void readUnlock();

    void writeLock();

    void writeUnlock();

}
