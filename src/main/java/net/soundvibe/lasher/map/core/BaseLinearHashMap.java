package net.soundvibe.lasher.map.core;

import net.soundvibe.lasher.mmap.*;

import java.nio.file.Path;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static net.soundvibe.lasher.util.FileSupport.deleteDirectory;

public abstract class BaseLinearHashMap implements AutoCloseable {

    static final int STRIPES = (int) Math.pow(2,8);
    static final double LOAD_FACTOR = 0.75;

    private final long defaultFileLength /*1L << 28*/;

    final IndexNode index;
    final DataNode data;
    final Path baseDir;

    final AtomicLong dataWritePos = new AtomicLong(0L);

    final ReentrantReadWriteLock dataLock = new ReentrantReadWriteLock();

    final Lock[] locks = new Lock[STRIPES];

    final AtomicLong size = new AtomicLong(0L);

    long tableLength;

    private static final long HEADER_SIZE = 28L;

    final AtomicInteger rehashIndex = new AtomicInteger(0);

    private static final class Lock {}

    public BaseLinearHashMap(Path baseDir, long indexFileLength, long dataFileLength) {
        this.baseDir = baseDir;
        this.defaultFileLength = nextPowerOf2(dataFileLength);
        for (int i = 0; i < STRIPES; i++) locks[i] = new Lock();
        baseDir.toFile().mkdirs();
        index = new IndexNode(baseDir, nextPowerOf2(indexFileLength));
        data = new DataNode(baseDir, this.defaultFileLength);
        readHeader();
    }

    protected abstract void readHeader();

    public abstract byte[] get(byte[] key);

    protected long getHeaderSize() {
        return HEADER_SIZE;
    }

    protected void writeHeader() {
        dataLock.writeLock().lock();
        try {
            data.putLong(0L, size());
            data.putLong(8L, tableLength);
            data.putLong(16L, dataWritePos.get());
            data.putInt(24L, rehashIndex.get());
        } finally {
            dataLock.writeLock().unlock();
        }
    }

    public long nextPowerOf2(long i) {
        if (i < defaultFileLength) return (defaultFileLength);
        if ((i & (i - 1)) == 0) return i;
        return (1 << (64 - (Long.numberOfLeadingZeros(i))));
    }

    /**
     * Returns the lock for the stripe for the given hash.  Synchronize of this
     * object before mutating the map.
     */
    protected Object lockForHash(long hash) {
        return locks[(int) (hash & (STRIPES - 1L))];
    }

    /**
     * Returns the bucket index for the given hash.
     * This doesn't lock - because it depends on tableLength, callers should
     * establish some lock that precludes a full rehash (read or write lock on
     * any of the locks).
     */
    protected long idxForHash(long hash) {
        return (hash & (STRIPES - 1L)) < rehashIndex.get()
                ? hash & (tableLength + tableLength - 1L)
                : hash & (tableLength - 1L);
    }

    /**
     * Recursively locks all stripes, and doubles the size of the primary mapper.
     * On Linux your filesystem probably makes this expansion a sparse operation.
     */
    protected void completeExpansion(int idx) {
        if (idx == STRIPES) {
            rehashIndex.set(0);
            tableLength *= 2;
        } else {
            synchronized (locks[idx]) {
                completeExpansion(idx + 1);
            }
        }
    }

    /**
     * Perform incremental rehashing to keep the load under the threshold.
     */
    protected void rehash() {
        while (load() > LOAD_FACTOR) {
            //If we've completed all rehashing, we need to expand the table & reset
            //the counters.
            if (rehashIndex.compareAndSet(STRIPES, STRIPES + 1)) {
                completeExpansion(0);
                return;
            }

            //Otherwise, we attempt to grab the next index to process
            int stripeToRehash;
            while (true) {
                stripeToRehash = rehashIndex.getAndIncrement();
                if (stripeToRehash == 0) {
                    index.doubleGrow();
                }
                //If it's in the valid table range, we conceptually acquired a valid ticket
                if (stripeToRehash < STRIPES) break;
                //Otherwise we're in the middle of a reset - spin until it has completed.
                while (rehashIndex.get() >= STRIPES) {
                    Thread.yield();
                    if (load() < LOAD_FACTOR) return;
                }
            }
            //We now have a valid ticket - we rehash all the indexes in the given stripe
            synchronized (locks[stripeToRehash]) {
                for (long idx = stripeToRehash; idx < tableLength; idx += STRIPES) {
                    rehashIdx(idx);
                }
            }
        }
    }

    /**
     * Allocates the given amount of space in secondary storage, and returns a
     * pointer to it.  Expands secondary storage if necessary.
     */
    protected long allocateData(long size) {
        dataLock.readLock().lock();
        try {
            while (true) {
                final long out = dataWritePos.get();
                final long newDataPos = out + size;
                if (newDataPos >= data.size()) {
                    //Goes to reallocation section
                    break;
                } else {
                    if (dataWritePos.compareAndSet(out, newDataPos)) {
                        return out;
                    }
                }
            }
        } finally {
            dataLock.readLock().unlock();
        }

        dataLock.writeLock().lock();
        try {
            if (dataWritePos.get() + size >= data.size()) {
                data.doubleGrow();
            }
        } finally {
            dataLock.writeLock().unlock();
        }
        return allocateData(size);
    }

    /**
     * Because all records in a bucket hash to their position or position + tableLength,
     * we can incrementally rehash one bucket at a time.
     * This does not need to acquire a lock; the calling rehash() method handles it.
     */
    protected abstract void rehashIdx(long idx);

    private void clear(int i) {
        if (i == STRIPES) {
            this.dataLock.writeLock().lock();
            try {
                this.index.clear();
                this.dataWritePos.set(getHeaderSize());
                this.size.set(0);
                this.rehashIndex.set(0);
            } finally {
                this.dataLock.writeLock().unlock();
            }
        } else {
            synchronized (locks[i]) {
                clear(i + 1);
            }
        }
    }

    /**
     * Removes all entries from the map, zeroing the primary file and marking
     * the current position in the secondary as immediately after the header.
     * Data is not actually removed from the secondary, but it will be
     * overwritten on subsequent writes.
     */
    public void clear() {
        clear(0);
    }

    /**
     * Writes all header metadata and unmaps the backing mmap'd files.
     */
    @Override
    public void close() {
        writeHeader();
        index.close();
        data.close();
    }

    public void delete() {
        close();
        deleteDirectory(baseDir);
    }

    public long size() {
        return size.get();
    }

    /**
     * "Fullness" of the table.  Some implementations may wish to override this
     * to account for multiple records per bucket.
     */
    public double load() {
        return size.doubleValue() / (tableLength + ((double) tableLength / (STRIPES)) * rehashIndex.get());
    }

    public boolean containsKey(byte[] k) {
        return get(k) != null;
    }

}
