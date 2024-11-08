package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.util.BytesSupport;

import java.nio.MappedByteBuffer;

import static java.util.Objects.requireNonNull;
import static net.soundvibe.lasher.mmap.MemoryMapped.JAVA_NIO_ACCESS;
import static net.soundvibe.lasher.mmap.MemoryMapped.UNSAFE;

public final class MappedBuffer implements AutoCloseable {

    private final MappedByteBuffer buffer;

    public MappedBuffer(MappedByteBuffer buffer) {
        requireNonNull(buffer, "buffer cannot be null");
        this.buffer = buffer;
        this.buffer.order(BytesSupport.BYTE_ORDER);
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
		UNSAFE.invokeCleaner(buffer);
    }

    public void clear() {
        var address = JAVA_NIO_ACCESS.getBufferAddress(buffer);
        UNSAFE.setMemory(address, buffer.capacity(), (byte) 0);
    }
}
