package net.soundvibe.lasher.serde;

public final class BytesSerde implements Serde<byte[]> {
    @Override
    public byte[] toBytes(byte[] value) {
        return value;
    }

    @Override
    public byte[] fromBytes(byte[] bytes) {
        return bytes;
    }
}
