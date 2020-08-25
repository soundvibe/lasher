package net.soundvibe.lasher.serde;

import static net.soundvibe.lasher.util.BytesSupport.*;

public final class IntegerSerde implements Serde<Integer> {
    @Override
    public byte[] toBytes(Integer value) {
        if (value == null) return null;
        return intToBytes(value);
    }

    @Override
    public Integer fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        return bytesToInt(bytes);
    }
}
