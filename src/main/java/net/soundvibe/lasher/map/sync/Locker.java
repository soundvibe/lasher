package net.soundvibe.lasher.map.sync;

public interface Locker {

    Lock readLock();

    Lock writeLock();

}
