package net.soundvibe.lasher.serde;

import java.nio.charset.StandardCharsets;

public final class StringSerde implements Serde<String> {
    @Override
    public byte[] toBytes(String value) {
        if (value == null) return null;
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String fromBytes(byte[] bytes) {
        if (bytes == null) return null;
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
