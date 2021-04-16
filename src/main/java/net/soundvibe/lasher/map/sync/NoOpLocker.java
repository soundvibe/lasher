package net.soundvibe.lasher.map.sync;

public final class NoOpLocker implements Locker {

    private static final Lock NO_OP = () -> {};

    @Override
    public Lock readLock() {
        return NO_OP;
    }

    @Override
    public Lock writeLock() {
        return NO_OP;
    }
}
