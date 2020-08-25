package net.soundvibe.lasher.serde;

import net.soundvibe.lasher.util.BytesSupport;

public final class LongSerde implements Serde<Long> {

    @Override
    public byte[] toBytes(Long value) {
        if (value == null) return null;
        return BytesSupport.longToBytes(value);
    }

    @Override
    public Long fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        return BytesSupport.bytesToLong(bytes);
    }
}
