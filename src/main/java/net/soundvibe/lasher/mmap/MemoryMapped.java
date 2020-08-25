package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.map.model.*;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

import static net.soundvibe.lasher.util.BytesSupport.BYTE_ORDER;

public abstract class MemoryMapped implements Closeable {

    private static final int CHUNK_SIZE = 256 * 1024 * 1024; //256 MB
    private final FileType fileType;
    protected MappedByteBuffer[] buffers;
    protected long size;
    private final Path baseDir;
    private final long defaultLength;

    private static final Class<?> UNSAFE_CLASS = resolveUnsafeClass();
    private static final Unsafe UNSAFE = resolveUnsafe();
    private static final Field ADDRESS_FIELD = resolveAddressField();

    protected MemoryMapped(final Path baseDir, FileType fileType, long defaultLength) {
        Objects.requireNonNull(baseDir, "baseDir is null");
        this.fileType = fileType;
        this.baseDir = baseDir;
        this.defaultLength = roundTo4096(defaultLength);

        var fileStats = readFileStats(baseDir, fileType, this.defaultLength);
        this.size = Math.max(this.defaultLength, fileStats.totalSize);
        this.buffers = fileStats.buffers;
        if (fileStats.buffers.length == 0) {
            mapAndResize(this.size);
        }
    }

    public abstract long getLong(long pos);
    public abstract void putLong(long pos, long val);

    private static final class FileStats {
        final long totalSize;
        final MappedByteBuffer[] buffers;

        private FileStats(long totalSize, MappedByteBuffer[] buffers) {
            this.totalSize = totalSize;
            this.buffers = buffers;
        }
    }

    private FileStats readFileStats(final Path baseDir, FileType fileType, long defaultLength) {
        var path = baseDir.resolve(fileType.filename);
        if (Files.notExists(path)) {
            return new FileStats(defaultLength, new MappedByteBuffer[0]);
        }

        var file = path.toFile();
        var fileSize = file.length();

        try {
            try (var rf = new RandomAccessFile(file, "rw");
                var fc = rf.getChannel()) {

                var totalBuffersSize = (int) Math.ceil(Math.max(1d, (double) fileSize / CHUNK_SIZE));
                var totalBuffers = new MappedByteBuffer[totalBuffersSize];
                int index = 0;
                for (long i = 0; i < fileSize; i+=CHUNK_SIZE) {
                    var remaining = fileSize - i;
                    var bufferSize = remaining < CHUNK_SIZE ? remaining : CHUNK_SIZE;
                    totalBuffers[index] = fc.map(FileChannel.MapMode.READ_WRITE, i, bufferSize);
                    totalBuffers[index].order(BYTE_ORDER);
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

    public void remap(long newLength) {
        var newSize = roundTo4096(newLength);
        mapAndResize(newSize);
        this.size = newSize;
    }

    public void doubleGrow() {
        remap(this.size * 2);
    }

    @Override
    public void close() {
        for (var buffer : buffers) {
            if (buffer != null) {
                buffer.force();
                unmap(buffer);
            }
        }
    }

    public void clear() {
        for (var buffer : buffers) {
            if (buffer != null) {
                try {
                    var addr = ADDRESS_FIELD.getLong(buffer);
                    UNSAFE.setMemory(addr, buffer.capacity(), (byte) 0);
                } catch (IllegalAccessException e) {
                    throw new UnsafeAccess(e);
                }
            }
        }
    }

    private static void unmap(ByteBuffer byteBuffer) {
        if (byteBuffer == null || !byteBuffer.isDirect()) return;
        if (UNSAFE == null) {
            throw new UnsupportedOperationException("Unsafe not supported on this platform");
        }
        UNSAFE.invokeCleaner(byteBuffer);
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
        resizeOlderBuffersIfNeeded(bufferIndex);

        long sizeToSet = bufferIndex == 0 ? newSize : resolveSize(newSize, bufferIndex);
        buffers[bufferIndex] = mapBuffer(bufferIndex, (int)sizeToSet, newSize);
    }

    private MappedByteBuffer mapBuffer(int bufferIndex, int bufferSize, long fileSize) {
        try (var f = new RandomAccessFile(baseDir.resolve(fileType.filename).toFile(), "rw");
             var fc = f.getChannel()) {
            if (f.length() < fileSize) {
                f.setLength(fileSize);
                this.size = fileSize;
            }

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

    private void resizeOlderBuffersIfNeeded(int bufferIndex) {
        if (bufferIndex > 0) {
            for (int i = 0; i < bufferIndex; i++) {
                if (buffers[i] == null || buffers[i].capacity() < CHUNK_SIZE) {
                    remapTo(i, CHUNK_SIZE);
                }
            }
        }
    }

    private void remapTo(int bufferIndex, long newSize) {
        unmap(buffers[bufferIndex]);
        try {
            try (var f = new RandomAccessFile(baseDir.resolve(fileType.filename).toFile(), "rw");
                 var fc = f.getChannel()) {
                var pos = resolveBufferPos(bufferIndex);
                buffers[bufferIndex] = fc.map(FileChannel.MapMode.READ_WRITE, pos, newSize);
                buffers[bufferIndex].order(BYTE_ORDER);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long resolveSize(long size, int bufferIndex) {
        long newSize = size;
        for (int i = 0; i < bufferIndex - 1; i++) {
            if (buffers[i] != null) {
                newSize -= buffers[i].capacity();
            }
        }

        if (newSize < defaultLength) {
            return defaultLength;
        }

        return Math.min(CHUNK_SIZE, newSize);
    }

    protected int convertPos(long absolutePos, int bufferIndex) {
        long startPos = (long) CHUNK_SIZE * (long) bufferIndex;
        int bufferPos = (int) (absolutePos - startPos);
        if (bufferPos < 0 || bufferPos > CHUNK_SIZE) {
            throw new IndexOutOfBoundsException("Buffer pos " + bufferPos + " is out of bounds: " + CHUNK_SIZE + " for pos: " + absolutePos + " and buffer index: " + bufferIndex);
        }
        return bufferPos;
    }

    protected int findBufferIndex(long pos) {
        return (int) Math.floorDiv(pos, CHUNK_SIZE);
    }

    protected int resolveBufferIndex(long pos) {
        int ix = (int) Math.floorDiv(pos, CHUNK_SIZE);
        if (ix < 0 || ix >= buffers.length) {
            throw new IndexOutOfBoundsException("Buffer index " + ix + " is out of total length: " + buffers.length + " for pos " + pos);
        }
        return ix;
    }

    protected void expandBuffers(int newPartition, long newSize) {
        if (newPartition + 1 > buffers.length) {
            int oldLength = buffers.length;
            buffers = Arrays.copyOf(buffers, newPartition + 1);
            for (int i = oldLength; i < buffers.length - 1; i++) {
                unmap(buffers[i]);
                buffers[i] = mapBuffer(i, CHUNK_SIZE, newSize);
            }
        }
    }
}
