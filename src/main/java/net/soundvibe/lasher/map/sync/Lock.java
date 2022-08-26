package net.soundvibe.lasher.map.sync;

public interface Lock extends AutoCloseable {

    default void unlock() {
        close();
    }

    @Override
    void close();
}
