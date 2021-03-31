package net.soundvibe.lasher.map.core;

public interface Locker {

    Lock readLock();

    Lock writeLock();

}
