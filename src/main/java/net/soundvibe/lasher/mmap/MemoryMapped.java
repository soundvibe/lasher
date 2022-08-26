package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.map.model.*;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static net.soundvibe.lasher.util.BytesSupport.BYTE_ORDER;

public abstract class MemoryMapped implements Closeable {

	private static final int MIN_CHUNK_SIZE = 32 * 1024 * 1024; // 32 MB
	private static final int MAX_CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB

    private final int chunkSize;
    private final FileType fileType;
    protected MappedBuffer[] buffers;
    protected long size;
    private final Path baseDir;
    protected final ReadWriteLock rwLock;

    private static final Class<?> UNSAFE_CLASS = resolveUnsafeClass();
    private static final Unsafe UNSAFE = resolveUnsafe();
    private static final Field ADDRESS_FIELD = resolveAddressField();

    protected MemoryMapped(final Path baseDir, FileType fileType, long defaultLength) {
        Objects.requireNonNull(baseDir, "baseDir is null");
        this.fileType = fileType;
        this.baseDir = baseDir;
        this.rwLock = new ReentrantReadWriteLock(true);
        this.chunkSize = Math.max(MIN_CHUNK_SIZE, Math.min(MAX_CHUNK_SIZE, (int) roundTo4096(defaultLength)));
        final long length = Math.max(this.chunkSize, roundTo4096(defaultLength));

        var fileStats = readFileStats(baseDir, fileType, length);
        this.size = Math.max(length, fileStats.totalSize);
        this.buffers = fileStats.buffers;
        if (fileStats.buffers.length == 0) {
            mapAndResize(this.size);
        }
    }

    public abstract long getLong(long pos);
    public abstract void putLong(long pos, long val);

	private record FileStats(long totalSize, MappedBuffer[] buffers) {}

    private FileStats readFileStats(final Path baseDir, FileType fileType, long defaultLength) {
        var path = baseDir.resolve(fileType.filename);
        if (Files.notExists(path)) {
            return new FileStats(defaultLength, new MappedBuffer[0]);
        }

        var file = path.toFile();
        var fileSize = file.length();

        try {
            try (var rf = new RandomAccessFile(file, "rw");
                var fc = rf.getChannel()) {

                var totalBuffersSize = (int) Math.ceil(Math.max(1d, (double) fileSize / chunkSize));
                var totalBuffers =  new MappedBuffer[totalBuffersSize];
                int index = 0;
                for (long i = 0; i < fileSize; i+= chunkSize) {
                    totalBuffers[index] = new MappedBuffer(
                            fc.map(FileChannel.MapMode.READ_WRITE, i, chunkSize), UNSAFE, ADDRESS_FIELD);
                    index++;
                }
                return new FileStats(fileSize, totalBuffers);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long size() {
        return this.size;
    }

    private void remap(long newLength) {
        var newSize = roundTo4096(newLength);
        mapAndResize(newSize);
        this.size = newSize;
    }

    public void doubleGrow() {
        rwLock.writeLock().lock();
        try {
            doubleGrowNoLock();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void doubleGrowNoLock() {
        remap(this.size * 2);
    }

    @Override
    public void close() {
        for (var buffer : buffers) {
            if (buffer != null) {
                buffer.flush();
                buffer.close();
            }
        }
    }

    public void clear() {
        for (var buffer : buffers) {
            if (buffer != null) {
                buffer.clear();
            }
        }
    }

    private static Class<?> resolveUnsafeClass() {
        try {
            return Class.forName("sun.misc.Unsafe");
        } catch (Exception ex) {
            try {
                return Class.forName("jdk.internal.misc.Unsafe");
            } catch (ClassNotFoundException e) {
                return null;
            }
        }
    }

    private static Unsafe resolveUnsafe() {
        if (UNSAFE_CLASS == null) throw new UnsupportedOperationException("Unsafe not supported on this platform");
        try {
            var theUnsafeField = UNSAFE_CLASS.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            return (Unsafe) theUnsafeField.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new UnsafeAccess(e);
        }
    }

    private static Field resolveAddressField() {
        try {
            var field = Buffer.class.getDeclaredField("address");
            field.setAccessible(true);
            return field;
        } catch (Exception e) {
            throw new UnsafeAccess(e);
        }
    }

    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    private void mapAndResize(long newSize) {
        var bufferIndex = findBufferIndex(newSize);
        expandBuffers(bufferIndex, newSize);
    }

    private MappedByteBuffer mapBuffer(FileChannel fc, int bufferIndex, int bufferSize) {
        try {
            var pos = resolveBufferPos(bufferIndex);
            var buffer = fc.map(FileChannel.MapMode.READ_WRITE, pos, bufferSize);
            buffer.order(BYTE_ORDER);
            return buffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long resolveBufferPos(int bufferIndex) {
        long result = 0L;
        for (int i = 0; i < bufferIndex; i++) {
            var buffer = buffers[i];
            if (buffer != null) {
                result += buffer.capacity();
            }
        }
        return result;
    }

    protected int convertPos(long absolutePos, int bufferIndex) {
        long startPos = (long) chunkSize * (long) bufferIndex;
        int bufferPos = (int) (absolutePos - startPos);
        if (bufferPos < 0 || bufferPos > chunkSize) {
            throw new IndexOutOfBoundsException("Buffer pos " + bufferPos + " is out of bounds: " + chunkSize + " for pos: " + absolutePos + " and buffer index: " + bufferIndex);
        }
        return bufferPos;
    }

    protected int findBufferIndex(long pos) {
        return (int) Math.floorDiv(pos, chunkSize);
    }

    protected int resolveBufferIndex(long pos) {
        int ix = (int) Math.floorDiv(pos, chunkSize);
        if (ix < 0 || ix >= buffers.length) {
            throw new IndexOutOfBoundsException("Buffer index " + ix + " is out of total length: " + buffers.length + " for pos " + pos);
        }
        return ix;
    }

    protected void expandBuffers(int newPartition, long newSize) {
        if (newPartition + 1 > buffers.length) {
            int oldLength = buffers.length;
            buffers = Arrays.copyOf(buffers, newPartition + 1);
            try (var f = new RandomAccessFile(baseDir.resolve(fileType.filename).toFile(), "rw");
                 var fc = f.getChannel()) {
                if (f.length() < newSize) {
                    f.setLength(newSize);
                    this.size = newSize;
                }

                for (int i = oldLength; i < buffers.length; i++) {
                    var buffer = buffers[i];
                    if (buffer != null) {
                        buffer.close();
                    }
                    buffers[i] = new MappedBuffer(mapBuffer(fc, i, chunkSize), UNSAFE, ADDRESS_FIELD);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
