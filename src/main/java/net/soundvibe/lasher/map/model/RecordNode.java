package net.soundvibe.lasher.map.model;

import net.soundvibe.lasher.mmap.DataNode;

import java.util.*;

public final class RecordNode {
    public final long pos;
    private long nextRecordPos;
    public final byte[] key;
    public final byte[] val;

    public RecordNode(long pos, long nextRecordPos, byte[] key, byte[] val) {
        this.pos = pos;
        this.nextRecordPos = nextRecordPos;
        this.key = key;
        this.val = val;
    }

    public long getNextRecordPos() {
        return this.nextRecordPos;
    }

    public void setNextRecordPos(long nRecPos) {
        this.nextRecordPos = nRecPos;
    }

    public void setNextRecordPos(long nRecPos, DataNode dataNode) {
        dataNode.writeNextRecordPos(pos, nRecPos);
        this.nextRecordPos = nRecPos;
    }

    public boolean keyEquals(byte[] k) {
        return Arrays.equals(k, this.key);
    }

    public boolean keyValueEquals(byte[] k, byte[] v) {
        return keyEquals(k) && Arrays.equals(v, this.val);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RecordNode that = (RecordNode) o;
        return pos == that.pos &&
               nextRecordPos == that.nextRecordPos &&
               Arrays.equals(key, that.key) &&
               Arrays.equals(val, that.val);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(pos, nextRecordPos);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(val);
        return result;
    }

    @Override
    public String toString() {
        return "RecordNode{" +
               "pos=" + pos +
               ", nextRecordPos=" + nextRecordPos +
               ", keyLength=" + key.length +
               ", valLength=" + (val == null ? -1 : val.length) +
               '}';
    }
}
