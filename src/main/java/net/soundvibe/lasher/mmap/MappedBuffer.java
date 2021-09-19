package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.map.model.UnsafeAccess;
import net.soundvibe.lasher.util.BytesSupport;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;

import static java.util.Objects.requireNonNull;

public final class MappedBuffer implements AutoCloseable {

    private final MappedByteBuffer buffer;
    private final Unsafe unsafe;
    private final Field addressField;

    public MappedBuffer(MappedByteBuffer buffer, Unsafe unsafe, Field addressField) {
        requireNonNull(buffer, "buffer cannot be null");
        this.buffer = buffer;
        this.buffer.order(BytesSupport.BYTE_ORDER);
        this.unsafe = unsafe;
        this.addressField = addressField;
    }

    public int capacity() {
        return buffer.capacity();
    }

    public void flush() {
		buffer.force();
    }

    public int getInt(int pos) {
		return buffer.getInt(pos);
    }

    public void putInt(int pos, int value) {
		buffer.putInt(pos, value);
    }

    public long getLong(int pos) {
		return buffer.getLong(pos);
    }

    public void putLong(int pos, long value) {
		buffer.putLong(pos, value);
    }

    public void get(int pos, byte[] dst, int offset, int length) {
		buffer.get(pos, dst, offset, length);
    }

    public void put(int pos, byte[] dst, int offset, int length) {
		buffer.put(pos, dst, offset, length);
    }

    @Override
    public void close() {
        if (buffer == null || !buffer.isDirect()) return;
		unsafe.invokeCleaner(buffer);
    }

    public void clear() {
        try {
            var address = addressField.getLong(buffer);
            unsafe.setMemory(address, buffer.capacity(), (byte) 0);
        } catch (IllegalAccessException e) {
            throw new UnsafeAccess(e);
        }
    }
}
