package net.soundvibe.lasher.serde;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class UUIDSerde implements Serde<UUID> {
    @Override
    public byte[] toBytes(UUID value) {
        if (value == null) return null;
        var byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(value.getLeastSignificantBits());
        byteBuffer.putLong(value.getMostSignificantBits());
        return byteBuffer.array();
    }

    @Override
    public UUID fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        var byteBuffer = ByteBuffer.wrap(bytes);
        var leastBits = byteBuffer.getLong();
        var mostBits = byteBuffer.getLong();
        return new UUID(mostBits, leastBits);
    }
}
