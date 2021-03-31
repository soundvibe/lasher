package net.soundvibe.lasher.mmap;

import net.soundvibe.lasher.map.model.*;
import net.soundvibe.lasher.util.BytesSupport;

import java.nio.file.Path;
import java.util.Arrays;

import static net.soundvibe.lasher.util.BytesSupport.*;

public final class DataNode extends MemoryMapped {

    public DataNode(Path baseDir, long len) {
        super(baseDir, FileType.DATA, len);
    }

    private static final int DATA_HEADER_SIZE = 16;

    public int headerSize() {
        return DATA_HEADER_SIZE;
    }

    public RecordNode readRecord(long pos) {
        if (pos >= size) {
            throw new IndexOutOfBoundsException("Record pos: " + pos + " is out of total size range: " + size);
        }
        var header = new byte[DATA_HEADER_SIZE];
        getBytes(pos, header);

        var nextRecordPos = longFromBytes(header);
        final int keyLen = intFromBytes(header,8);
        final int valLen = intFromBytes(header, 12);

        if (keyLen < 0) {
            throw new IndexOutOfBoundsException("KeyLen: " + keyLen + " for pos: " + pos + " and size:" + size);
        }

        var dataLen = keyLen + Math.max(0, valLen);
        var data = new byte[dataLen];
        getBytes(pos + DATA_HEADER_SIZE, data);

        var key = Arrays.copyOfRange(data, 0, keyLen);
        byte[] val = null;
        if (valLen != -1) {
            val = Arrays.copyOfRange(data, keyLen, data.length);
        }
        return new RecordNode(pos, nextRecordPos, key, val);
    }

/*    public void writeRecord(byte[] key, byte[] value, long pos, long nextRecPos) {
        int valueLength = value == null ? 0 : value.length;
        var buffer = ByteBuffer.allocate(DATA_HEADER_SIZE + key.length + valueLength);
        buffer.order(BYTE_ORDER);

        buffer.putLong(nextRecPos);
        buffer.putInt(key.length);
        buffer.putInt(value == null ? -1 : value.length);
        buffer.put(key);
        if (value != null) {
            buffer.put(value);
        }

        putBytes(pos, buffer.array());
    }*/

    public void writeRecord(byte[] key, byte[] value, long pos, long nextRecPos) {
        writeNextRecordPos(pos, nextRecPos);
        putInt(pos + 8, key.length);
        putInt(pos + 12, value == null ? -1 : value.length);
        putBytes(pos + 16, key);
        if (value != null) {
            putBytes(pos + 16 + key.length, value);
        }
    }

    public void writeNextRecordPos(long pos, long nextRecordPos) {
        putLong(pos, nextRecordPos);
    }

    public int getInt(long pos) {
        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);

        if (posBuffer + Integer.BYTES > buffer.capacity()) {
            var valBytes = new byte[Integer.BYTES];
            getBytes(pos, valBytes);
            return BytesSupport.bytesToInt(valBytes);
        } else {
            return buffer.getInt(posBuffer);
        }
    }

    public void putInt(long pos, int val) {
        if ((pos + Integer.BYTES) > size) {
            throw new IllegalStateException(String.format("pos [%d] larger than total size [%d]",
                    pos + Integer.BYTES, size));
        }

        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);

        if (posBuffer + Integer.BYTES > buffer.capacity()) {
            var valBytes = BytesSupport.intToBytes(val);
            putBytes(pos, valBytes);
        } else {
            buffer.putInt(posBuffer, val);
        }
    }

    public void getBytes(long pos, byte[] data) {
        if (pos + data.length > size) return;

        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);
        var offset = 0;
        var length = data.length;

        while (posBuffer + length > buffer.capacity()) {
            var remaining = buffer.capacity() - posBuffer;
           //buffer.order(BYTE_ORDER);
            buffer.get(posBuffer, data, offset, remaining);
            bufferIndex++;
            buffer = buffers[bufferIndex];
            posBuffer = 0;
            offset += remaining;
            length -= remaining;
        }
        //buffer.order(BYTE_ORDER);
        buffer.get(posBuffer, data, offset, length);
    }

    public void putBytes(long pos, byte[] data) {
        putBytes(pos, data, data.length);
    }

    public void putBytes(long pos, byte[] data, int len) {
        if ((pos + len) > size) {
            throw new IllegalStateException(String.format("pos [%d] larger than total size [%d]", pos + len, size));
        }

        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);
        var offset = 0;
        var length = len;

        while (posBuffer + length > buffer.capacity()) {
            var remaining = buffer.capacity() - posBuffer;
           // buffer.order(BYTE_ORDER);
            buffer.put(posBuffer, data, offset, remaining);
            bufferIndex++;
            buffer = buffers[bufferIndex];
            posBuffer = 0;
            offset += remaining;
            length -= remaining;
        }

        //buffer.order(BYTE_ORDER);
        buffer.put(posBuffer, data, offset, length);
    }

    @Override
    public long getLong(long pos) {
        var bufferIndex = resolveBufferIndex(pos);
        var buffer = buffers[bufferIndex];
        var posBuffer = convertPos(pos, bufferIndex);

        if (posBuffer + Long.BYTES > buffer.capacity()) {
            var valBytes = new byte[Long.BYTES];
            getBytes(pos, valBytes);
            return BytesSupport.bytesToLong(valBytes);
        } else {
            return buffer.getLong(posBuffer);
        }
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
            var valBytes = BytesSupport.longToBytes(val);
            putBytes(pos, valBytes);
        } else {
            buffer.putLong(posBuffer, val);
        }
    }
}
