package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.map.model.FileType;

import java.nio.file.Path;

public final class IndexNode extends MemoryMapped {

    public IndexNode(Path baseDir, long len) {
        super(baseDir, FileType.INDEX, len);
    }

    public long getDataAddress(long pos) {
        return getLong(pos);
    }

    public void putDataAddress(long pos, long dataAddress) {
        putLong(pos, dataAddress);
    }

    @Override
    public long getLong(long pos) {
        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);
        if (posBuffer + Long.BYTES > buffer.capacity()) {
            throw new IllegalStateException(String.format("Pos in buffer exceeds it's capacity: %d > %d",
                    posBuffer + Long.BYTES, buffer.capacity()));
        }
        return buffer.getLong(posBuffer);
    }

    @Override
    public void putLong(long pos, long val) {
        if ((pos + Long.BYTES) > size) {
            throw new IllegalStateException(String.format("pos [%d] larger than total size [%d]", pos + Long.BYTES, size));
        }

        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);

        if (posBuffer + Long.BYTES > buffer.capacity()) {
            throw new IllegalStateException(String.format("Pos in buffer exceeds it's capacity: %d > %d",
                    posBuffer + Long.BYTES, buffer.capacity()));
        }

        buffer.putLong(posBuffer, val);
    }
}
