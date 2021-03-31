package net.soundvibe.lasher.map.core;

import net.soundvibe.lasher.map.model.RecordNode;
import net.soundvibe.lasher.util.Hash;

import java.nio.file.Path;
import java.util.*;

import static java.util.Objects.requireNonNull;
import static net.soundvibe.lasher.util.Constants.*;

public class Lasher extends BaseLinearHashMap {

    public static final long DEFAULT_FILE_LENGTH = 1L << 28;

    public static final long MB_32 = (long) Math.pow(2, 25L);
    public static final long MB_128 = (long) Math.pow(2, 27L);

    private static final int INDEX_REC_SIZE = Long.BYTES;

    public Lasher(Path baseDir) {
        this(baseDir, MB_128, MB_32);
    }

    public Lasher(Path baseDir, long indexFileLength) {
        super(baseDir, indexFileLength, DEFAULT_FILE_LENGTH);
    }

    public Lasher(Path baseDir, long indexFileLength, long dataFileLength) {
        super(baseDir, indexFileLength, dataFileLength);
    }

    public Lasher(Path baseDir, long indexFileLength, long dataFileLength, boolean locker) {
        super(baseDir, indexFileLength, dataFileLength, locker);
    }

    public static Lasher forShard(Path baseDir) {
        return new Lasher(baseDir, MB_128, DEFAULT_FILE_LENGTH, false);
    }

    public static Lasher forShard(Path baseDir, long indexFileLength, long dataFileLength) {
        return new Lasher(baseDir, indexFileLength, dataFileLength, false);
    }

    @Override
    public byte[] get(byte[] key) {
        requireNonNull(key, KEY_NOT_NULL);
        final long hash = Hash.hashBytes(key);
        var lock = lockForHash(hash);
        lock.lock();
        try {
            return get(key, hash);
        } finally {
            lock.unlock();
        }
    }

    public byte[] get(byte[] key, long hash) {
        final long indexPos = indexPos(hash);
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) return null;

        var record = readDataRecord(adr);
        while (true) {
            if (record.keyEquals(key)) {
                return record.val;
            } else if (record.getNextRecordPos() != 0L) {
                record = readDataRecord(record.getNextRecordPos());
            } else
                return null;
        }
    }

    public byte[] putIfAbsent(byte[] key, byte[] value) {
        requireNonNull(key, KEY_NOT_NULL);
        requireNonNull(value, VALUE_NOT_NULL);
        if (load() > LOAD_FACTOR) rehash();
        final long hash = Hash.hashBytes(key);
        var lock = lockForHash(hash);
        lock.lock();
        try {
            return putIfAbsent(key, hash, value);
        } finally {
            lock.unlock();
        }
    }

    public byte[] putIfAbsent(byte[] key, long hash, byte[] value) {
        final long indexPos = indexPos(hash);
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) {
            insertNewRecord(indexPos, key, value);
            return null;
        }
        var bucket = readDataRecord(adr);
        while (true) {
            if (bucket.keyEquals(key)) {
                return bucket.val;
            } else if (bucket.getNextRecordPos() != 0L) {
                bucket = readDataRecord(bucket.getNextRecordPos());
            } else {
                insertNewRecordInChain(0L, bucket.pos, key, value);
                return null;
            }
        }
    }

    public byte[] put(byte[] key, byte[] value) {
        requireNonNull(key, KEY_NOT_NULL);
        requireNonNull(value, VALUE_NOT_NULL);
        rehash();
        final long hash = Hash.hashBytes(key);
        var lock = lockForHash(hash);
        lock.lock();
        try {
            return put(key, hash, value, false);
        } finally {
            lock.unlock();
        }
    }

    public byte[] put(byte[] key, long hash, byte[] value, boolean shouldRehash) {
        if (shouldRehash) {
            rehash();
        }

        final long indexPos = indexPos(hash);
        if (indexPos >= index.size()) {
            throw new IndexOutOfBoundsException("Pos: " + indexPos + " index size: " + index.size());
        }
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) {
            insertNewRecord(indexPos, key, value);
            return null;
        }
        var bucket = readDataRecord(adr);
        RecordNode prev = null;
        while (true) {
            long nextPos = bucket.getNextRecordPos();
            if (bucket.keyEquals(key)) {
                updateRecord(indexPos, nextPos, key, value, prev);
                return bucket.val;
            } else if (nextPos != 0L) {
                prev = bucket;
                bucket = readDataRecord(nextPos);
            } else {
                insertNewRecordInChain(nextPos, bucket.pos, key, value);
                return null;
            }
        }
    }

    public byte[] remove(byte[] key) {
        requireNonNull(key, KEY_NOT_NULL);
        final long hash = Hash.hashBytes(key);

        var lock = lockForHash(hash);
        lock.lock();
        try {
            return remove(key, hash);
        } finally {
            lock.unlock();
        }
    }

    public byte[] remove(byte[] key, long hash) {
        final long indexPos = indexPos(hash);
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) return null;

        var bucket = readDataRecord(adr);
        RecordNode prev = null;
        while (true) {
            if (bucket.keyEquals(key)) {
                removeRecord(indexPos, bucket.getNextRecordPos(), prev);
                return bucket.val;
            } else if (bucket.getNextRecordPos() != 0L) {
                prev = bucket;
                bucket = readDataRecord(bucket.getNextRecordPos());
            } else {
                return null;
            }
        }
    }

    public boolean remove(byte[] key, byte[] value) {
        requireNonNull(key, KEY_NOT_NULL);
        requireNonNull(value, VALUE_NOT_NULL);
        final long hash = Hash.hashBytes(key);

        var lock = lockForHash(hash);
        lock.lock();
        try {
            return remove(key, hash, value);
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(byte[] key, long hash, byte[] value) {
        final long indexPos = indexPos(hash);
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) return false;

        var bucket = readDataRecord(adr);
        RecordNode prev = null;
        while (true) {
            if (bucket.keyValueEquals(key, value)) {
                removeRecord(indexPos, bucket.getNextRecordPos(), prev);
                return true;
            } else if (bucket.getNextRecordPos() != 0L) {
                prev = bucket;
                bucket = readDataRecord(bucket.getNextRecordPos());
            } else return false;
        }
    }

    public boolean replace(byte[] key, byte[] prevVal, byte[] newVal) {
        requireNonNull(key, KEY_NOT_NULL);
        requireNonNull(prevVal, PREV_VALUE_NOT_NULL);
        requireNonNull(newVal, NEW_VALUE_NOT_NULL);

        final long hash = Hash.hashBytes(key);
        var lock = lockForHash(hash);
        lock.lock();
        try {
            return replace(key, hash, prevVal, newVal);
        } finally {
            lock.unlock();
        }
    }

    public boolean replace(byte[] key, long hash, byte[] prevVal, byte[] newVal) {
        final long indexPos = indexPos(hash);
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) return false;

        var bucket = readDataRecord(adr);
        RecordNode prev = null;
        while (true) {
            if (bucket.keyValueEquals(key, prevVal)) {
                updateRecord(indexPos, bucket.getNextRecordPos(), key, newVal, prev);
                return true;
            } else if (bucket.getNextRecordPos() != 0L) {
                prev = bucket;
                bucket = readDataRecord(bucket.getNextRecordPos());
            } else {
                return false;
            }
        }
    }

    public byte[] replace(byte[] key, byte[] value) {
        requireNonNull(key, KEY_NOT_NULL);
        requireNonNull(value, VALUE_NOT_NULL);
        final long hash = Hash.hashBytes(key);

        var lock = lockForHash(hash);
        lock.lock();
        try {
            final long indexPos = indexPos(hash);
            final long adr = index.getDataAddress(indexPos);
            if (adr == 0L) return null;

            var bucket = readDataRecord(adr);
            RecordNode prev = null;
            while (true) {
                if (bucket.keyEquals(key)) {
                    updateRecord(indexPos, bucket.getNextRecordPos(),  key, value, prev);
                    return bucket.val;
                } else if (bucket.getNextRecordPos() != 0L) {
                    prev = bucket;
                    bucket = readDataRecord(bucket.getNextRecordPos());
                } else {
                    return null;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public byte[] replace(byte[] key, long hash, byte[] value) {
        final long indexPos = indexPos(hash);
        final long adr = index.getDataAddress(indexPos);
        if (adr == 0L) return null;

        var bucket = readDataRecord(adr);
        RecordNode prev = null;
        while (true) {
            if (bucket.keyEquals(key)) {
                updateRecord(indexPos, bucket.getNextRecordPos(),  key, value, prev);
                return bucket.val;
            } else if (bucket.getNextRecordPos() != 0L) {
                prev = bucket;
                bucket = readDataRecord(bucket.getNextRecordPos());
            } else {
                return null;
            }
        }
    }

    @Override
    protected void readHeader() {
        try (var ignored = dataLock.readLock()) {
            final long size = data.getLong(0L);
            final long bucketsInMap = data.getLong(8L);
            final long lastSecondaryPos = data.getLong(16L);
            final int rehashComplete = data.getInt(24L);
            this.size.set(size);
            this.tableLength = bucketsInMap == 0L ? (index.size() / INDEX_REC_SIZE) : bucketsInMap;
            this.dataWritePos.set(lastSecondaryPos == 0L ? getHeaderSize() : lastSecondaryPos);
            this.rehashIndex.set(rehashComplete);
        }
    }

    private void insertNewRecord(long indexPos, byte[] key, byte[] value) {
        final long insertPos = allocateNewRecord(key, value);
        data.writeRecord(key, value, insertPos, 0L);
        index.putDataAddress(indexPos, insertPos);
        size.incrementAndGet();
    }

    private void insertNewRecordInChain(long nextRecordPos, long prevPos, byte[] key, byte[] value) {
        final long insertPos = allocateNewRecord(key, value);
        data.writeRecord(key, value, insertPos, nextRecordPos);
        data.writeNextRecordPos(prevPos, insertPos);
        size.incrementAndGet();
    }

    private void updateRecord(long indexPos, long nextRecordPos, byte[] key, byte[] value, RecordNode prevRecordNode) {
        final long insertPos = allocateNewRecord(key, value);
        data.writeRecord(key, value, insertPos, nextRecordPos);
        if (prevRecordNode == null) {
            index.putDataAddress(indexPos, insertPos);
        } else {
            data.writeNextRecordPos(prevRecordNode.pos, insertPos);
        }
    }

    private void removeRecord(long indexPos, long nextRecordPos, RecordNode prevRecordNode) {
        if (prevRecordNode == null) {
            index.putDataAddress(indexPos, nextRecordPos);
        } else {
            data.writeNextRecordPos(prevRecordNode.pos, nextRecordPos);
        }
        size.decrementAndGet();
    }

    protected long idxToPos(long idx) {
        return idx * INDEX_REC_SIZE;
    }

    protected long indexPos(long hash) {
        return idxToPos(idxForHash(hash));
    }

    protected RecordNode readDataRecord(long pos) {
        try (var ignored = dataLock.readLock()) {
            return data.readRecord(pos);
        }
    }

    protected long allocateNewRecord(byte[] key, byte[] value) {
        final long recordSize = data.headerSize() + key.length + (value == null ? 0L : value.length);
        return allocateData(recordSize);
    }

    @Override
    protected void rehashIdx(long idx, long tableLength) {
        final long indexPos = idxToPos(idx);
        final long addr = index.getDataAddress(indexPos);
        if (addr == 0L) return;
        final long moveIdx = idx + tableLength;
        final long moveIndexPos = idxToPos(moveIdx);

        var keepBuckets = new ArrayList<RecordNode>();
        var moveBuckets = new ArrayList<RecordNode>();
        var bucket = readDataRecord(addr);
        while (true) {
            long hash = Hash.hashBytes(bucket.key);
            final long newIdx = hash & (tableLength + tableLength - 1L);
            if (newIdx == idx) {
                keepBuckets.add(bucket);
            } else if (newIdx == moveIdx) {
                moveBuckets.add(bucket);
            } else {
                throw new IllegalStateException("hash:" + hash +
                                                ", idx:" + idx +
                                                ", newIdx:" + newIdx +
                                                ", tableLength:" + tableLength +
                                                ", moveIdx=" + moveIdx +
                                                ", primaryPos=" + indexPos +
                                                ", bucket=" + bucket);
            }

            if (bucket.getNextRecordPos() != 0L) {
                bucket = readDataRecord(bucket.getNextRecordPos());
            } else {
                break;
            }
        }
        //Adjust chains
        var keepInitialPos = updateChain(keepBuckets);
        var moveInitialPos = updateChain(moveBuckets);
        index.putDataAddress(indexPos, keepInitialPos);
        index.putDataAddress(moveIndexPos, moveInitialPos);
    }

    /**
     * Cause each bucket to point to the subsequent one.  Returns address of original,
     * or 0 if the list was empty.
     */
    protected long updateChain(List<RecordNode> buckets) {
        if (buckets.isEmpty()) return 0L;
        for (int i = 0; i < buckets.size() - 1; i++) {
            buckets.get(i)
                    .writeAndSetNextRecordPos(buckets.get(i + 1).pos, data);
        }
        buckets.get(buckets.size() - 1)
                .writeAndSetNextRecordPos(0L, data);
        return buckets.get(0).pos;
    }

    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        return new LashIterator(dataLock);
    }

    public Iterator<Map.Entry<byte[], byte[]>> iterator(Locker locker) {
        return new LashIterator(locker);
    }

    public final class LashIterator implements Iterator<Map.Entry<byte[], byte[]>> {
        private long nextIdx = 0L;
        private long nextAddr = 0L;
        private boolean finished = true;
        private final long length;
        private final Locker locker;

        public LashIterator(Locker locker) {
            this.locker = locker;
            this.length = rehashIndex.get() == 0L ? tableLength : tableLength * 2L;
            try (var ignored = locker.readLock()) {
                for (nextIdx = 0L; nextIdx < length; nextIdx++) {
                    nextAddr = index.getDataAddress(idxToPos(nextIdx));
                    if (nextAddr != 0L) {
                        finished = false;
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (finished) throw new NoSuchElementException();
            final var node = readDataRecord(nextAddr);
            advance(node);
            return new AbstractMap.SimpleEntry<>(node.key, node.val);
        }

        private void advance(RecordNode bucket) {
            if (bucket.getNextRecordPos() != 0L) {
                nextAddr = bucket.getNextRecordPos();
                finished = false;
                return;
            }
            for (nextIdx = nextIdx + 1L; nextIdx < length; nextIdx++) {
                final long pos = idxToPos(nextIdx);
                try (var ignored = locker.readLock()) {
                    nextAddr = index.getDataAddress(pos);
                }
                if (nextAddr != 0L) {
                    finished = false;
                    return;
                }
            }
            finished = true;
        }
    }

}
