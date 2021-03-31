package net.soundvibe.lasher.map.core;

public interface Lock extends AutoCloseable {

    default void unlock() {
        close();
    }

    @Override
    void close();
}
