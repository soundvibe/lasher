package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.map.model.UnsafeAccess;
import net.soundvibe.lasher.util.BytesSupport;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public final class MappedBuffer implements AutoCloseable {

    private final MappedByteBuffer buffer;
    private final ReentrantReadWriteLock rwLock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final Unsafe unsafe;
    private final Field addressField;

    public MappedBuffer(MappedByteBuffer buffer, Unsafe unsafe, Field addressField) {
        requireNonNull(buffer, "buffer cannot be null");
        this.buffer = buffer;
        this.buffer.order(BytesSupport.BYTE_ORDER);
        this.unsafe = unsafe;
        this.addressField = addressField;
        this.rwLock = new ReentrantReadWriteLock(false);
        this.readLock = this.rwLock.readLock();
        this.writeLock = this.rwLock.writeLock();
    }

    public int capacity() {
        return buffer.capacity();
    }

    public void flush() {
        writeLock.lock();
        try {
            buffer.force();
        } finally {
            writeLock.unlock();
        }
    }

    public int getInt(int pos) {
       // readLock.lock();
        try {
            return buffer.getInt(pos);
        } finally {
       //     readLock.unlock();
        }
    }

    public void putInt(int pos, int value) {
       // writeLock.lock();
        try {
            buffer.putInt(pos, value);
        } finally {
      //      writeLock.unlock();
        }
    }

    public long getLong(int pos) {
       // readLock.lock();
        try {
            return buffer.getLong(pos);
        } finally {
        //    readLock.unlock();
        }
    }

    public void putLong(int pos, long value) {
       // writeLock.lock();
        try {
            buffer.putLong(pos, value);
        } finally {
       //     writeLock.unlock();
        }
    }

    public void get(int pos, byte[] dst, int offset, int length) {
        //readLock.lock();
        try {
            buffer.get(pos, dst, offset, length);
        } finally {
           // readLock.unlock();
        }
    }

    public void put(int pos, byte[] dst, int offset, int length) {
       // writeLock.lock();
        try {
            buffer.put(pos, dst, offset, length);
        } finally {
        //    writeLock.unlock();
        }
    }

    @Override
    public void close() {
        if (buffer == null || !buffer.isDirect()) return;
        writeLock.lock();
        try {
            unsafe.invokeCleaner(buffer);
        } finally {
            writeLock.unlock();
        }
    }

    public void clear() {
        writeLock.lock();
        try {
            var address = addressField.getLong(buffer);
            unsafe.setMemory(address, buffer.capacity(), (byte) 0);
        } catch (IllegalAccessException e) {
            throw new UnsafeAccess(e);
        } finally {
            writeLock.unlock();
        }
    }
}
