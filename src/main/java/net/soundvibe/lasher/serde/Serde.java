package net.soundvibe.lasher.serde;

public interface Serde<T> {

    byte[] toBytes(T value);

    T fromBytes(byte[] bytes);

}
