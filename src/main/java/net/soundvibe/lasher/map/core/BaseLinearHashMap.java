package net.soundvibe.lasher.map.core;

import com.google.common.util.concurrent.Striped;
import io.micrometer.core.instrument.*;
import net.soundvibe.lasher.map.sync.*;
import net.soundvibe.lasher.mmap.*;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.*;

import static net.soundvibe.lasher.util.FileSupport.deleteDirectory;

public abstract class BaseLinearHashMap implements AutoCloseable {

	static final int STRIPES = (int) Math.pow(2, 8);
	static final double LOAD_FACTOR = 0.75;

	private final long defaultFileLength /*1L << 28*/;

	final IndexNode index;
	final DataNode data;
	final Path baseDir;
	final LinerHashMapMetrics metrics;

	final AtomicLong dataWritePos = new AtomicLong(0L);

	final Locker dataLock;

	final Striped<Lock> striped = Striped.lock(STRIPES);

	final AtomicLong size = new AtomicLong(0L);

	long tableLength;

	private static final long HEADER_SIZE = 28L;

	final AtomicInteger rehashIndex = new AtomicInteger(0);

	protected BaseLinearHashMap(Path baseDir, long indexFileLength, long dataFileLength) {
		this(baseDir, indexFileLength, dataFileLength, true, Tags.empty());
	}

	protected BaseLinearHashMap(Path baseDir, long indexFileLength, long dataFileLength, boolean locker, Tags tags) {
		this.baseDir = baseDir;
		this.defaultFileLength = nextPowerOf2(dataFileLength);
		this.dataLock = locker ? new RWLocker(new ReentrantReadWriteLock()) : new NoOpLocker();
		baseDir.toFile().mkdirs();
		this.index = new IndexNode(baseDir, nextPowerOf2(indexFileLength));
		this.data = new DataNode(baseDir, this.defaultFileLength);
		readHeader();
		Metrics.gauge("index-size-bytes", tags, this.index, MemoryMapped::size);
		Metrics.gauge("data-size-bytes", tags, this.data, MemoryMapped::size);
		this.metrics = new LinerHashMapMetrics(
				Metrics.timer("rehash-duration", tags),
				Metrics.gauge("rehashing", tags, new AtomicLong(0L)));
	}

	record LinerHashMapMetrics(Timer rehashDuration, AtomicLong rehashInProgress) {
	}

	protected abstract void readHeader();

	public abstract byte[] get(byte[] key);

	protected long getHeaderSize() {
		return HEADER_SIZE;
	}

	protected void writeHeader() {
		try (var ignored = dataLock.writeLock()) {
			data.putLong(0L, size());
			data.putLong(8L, tableLength);
			data.putLong(16L, dataWritePos.get());
			data.putInt(24L, rehashIndex.get());
		}
	}

	public long nextPowerOf2(long i) {
		if (i < defaultFileLength) return (defaultFileLength);
		if ((i & (i - 1)) == 0) return i;
		return (1L << (64 - (Long.numberOfLeadingZeros(i))));
	}

	/**
	 * Returns the lock for the stripe for the given hash.  Synchronize of this
	 * object before mutating the map.
	 */
	protected Lock lockForHash(long hash) {
		return striped.getAt((int) (hash & (STRIPES - 1L)));
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
	 * Perform incremental rehashing to keep the load under the threshold.
	 */
	protected void rehash() {
		boolean wasRehashed = false;
		long rehashStarted = 0L;
		while (load() > LOAD_FACTOR) {
			if (!wasRehashed) {
				metrics.rehashInProgress.set(1L);
				rehashStarted = System.currentTimeMillis();
			}
			wasRehashed = true;
			int stripeToRehash = 0;
			try (var ignored = dataLock.writeLock()) {
				stripeToRehash = rehashIndex.getAndIncrement();
				if (stripeToRehash == 0) {
					index.doubleGrowNoLock();
				} else if (stripeToRehash == STRIPES) {
					rehashIndex.set(0);
					tableLength *= 2;
					break;
				}
			}

			var lock = dataLock.readLock();
			var currentLength = tableLength;
			lock.unlock();

			var stripedLock = striped.getAt(stripeToRehash);
			stripedLock.lock();
			try {
				for (long idx = stripeToRehash; idx < currentLength; idx += STRIPES) {
					rehashIdx(idx, currentLength);
				}
			} finally {
				stripedLock.unlock();
			}
		}
		if (wasRehashed) {
			metrics.rehashInProgress.set(0L);
			metrics.rehashDuration.record(System.currentTimeMillis() - rehashStarted, TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Allocates the given amount of space in secondary storage, and returns a
	 * pointer to it.  Expands secondary storage if necessary.
	 */
	protected long allocateData(long size) {
		try (var ignored = dataLock.readLock()) {
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
		}

		try (var ignored = dataLock.writeLock()) {
			if (dataWritePos.get() + size >= data.size()) {
				data.doubleGrow();
			}
		}
		return allocateData(size);
	}

	/**
	 * Because all records in a bucket hash to their position or position + tableLength,
	 * we can incrementally rehash one bucket at a time.
	 * This does not need to acquire a lock; the calling rehash() method handles it.
	 */
	protected abstract void rehashIdx(long idx, long tableLength);

	/**
	 * Removes all entries from the map, zeroing the primary file and marking
	 * the current position in the secondary as immediately after the header.
	 * Data is not actually removed from the secondary, but it will be
	 * overwritten on subsequent writes.
	 */
	public void clear() {
		var lock = striped.getAt(STRIPES - 1);
		lock.lock();
		try {
			try (var ignored = dataLock.writeLock()) {
				this.index.clear();
				this.dataWritePos.set(getHeaderSize());
				this.size.set(0);
				this.rehashIndex.set(0);
			}
		} finally {
			lock.unlock();
		}
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
		return load(rehashIndex.get());
	}

	public double load(int rehashIndex) {
		try (var ignored = dataLock.readLock()) {
			return size.doubleValue() / (tableLength + ((double) tableLength / (STRIPES)) * rehashIndex);
		}
	}

	public boolean containsKey(byte[] k) {
		return get(k) != null;
	}
}
