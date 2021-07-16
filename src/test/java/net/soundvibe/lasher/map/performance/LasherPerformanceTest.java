package net.soundvibe.lasher.map.performance;

import net.soundvibe.lasher.db.LasherDB;
import net.soundvibe.lasher.map.core.Lasher;
import net.soundvibe.lasher.util.BytesSupport;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@Tag("performance")
@Disabled
class LasherPerformanceTest {

	@RepeatedTest(1)
	void performance_huge_sequential(@TempDir Path tmpPath) {
		long recs = 100_000_000;
		long counter = 0;
		System.gc();
		var usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		var startMs = System.currentTimeMillis();
		try (var sut = new Lasher(tmpPath, 64L * 1024 * 1024)) {
			System.out.println("Folder size before in MB: " + getFolderSize(tmpPath.toFile()) / 1024 / 1024);
			printFiles(tmpPath);

			var startPutMs = System.currentTimeMillis();
			for (long i = 0; i < recs; i++) {
				final byte[] k = BytesSupport.toBytes(i);
				final byte[] v = BytesSupport.toBytes(i + 1);
				sut.put(k, v);
				final byte[] vOut = sut.get(k);
				counter++;
				assertEquals(i + 1
						, BytesSupport.longFromBytes(vOut)
						, "Insert & retrieval mismatch: " + i);
			}
			var elapsedPutMs = System.currentTimeMillis() - startPutMs;
			System.out.printf("Inserted and retrieved %d rows in %d ms, %s rec/s%n",
					counter, elapsedPutMs, counter / (elapsedPutMs / 1000d));

			for (long i = 0; i < recs; i++) {
				final byte[] k = BytesSupport.toBytes(i);
				final byte[] vOut = sut.get(k);
				assertEquals(i + 1
						, BytesSupport.longFromBytes(vOut)
						, "Insert & retrieval mismatch: " + i);
			}
			System.out.println("All found");
			var startGetMs = System.currentTimeMillis();

			var iterCount = new AtomicLong(0L);
			sut.iterator().forEachRemaining(entry -> {
				var v = sut.get(entry.getKey());
				assertArrayEquals(entry.getValue(), v);
				iterCount.incrementAndGet();
			});
			assertEquals(recs, iterCount.get());
			var elapsedGetMs = System.currentTimeMillis() - startGetMs;
			System.out.printf("Iterated and retrieved %d rows in %d ms, %s rec/s%n",
					iterCount.get(), elapsedGetMs, iterCount.get() / (elapsedGetMs / 1000d));
		} catch (Exception e) {
			System.err.println("Got error on " + counter + " \n" + e);
			throw e;
		} finally {
			var endMs = System.currentTimeMillis();
			var elapsedMs = endMs - startMs;
			System.out.printf("Total %d rows in %d ms, %s rec/s%n",
					counter, elapsedMs, counter / (elapsedMs / 1000d));
			var folderSize = getFolderSize(tmpPath.toFile());
			var used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.gc();
			System.out.println("Folder size after in MB: " + folderSize / 1024 / 1024d);
			printFiles(tmpPath);
			var usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.out.printf("Before: %s MB, after: %s MB, afterGC: %s MB, used: %s MB%n",
					toMB(usedBefore), toMB(used), toMB(usedAfter), toMB(usedAfter - usedBefore));
		}


	}

	@RepeatedTest(1)
	void performance_seq_concurrent(@TempDir Path tmpPath) {
		long recs = 20_000_000;
		var counterFound = new AtomicLong(0L);
		var counterInserted = new AtomicLong(0L);

		var rnd = new Random();
		var entries = LongStream.range(0, recs)
				.mapToObj(k -> {
					var bytes = new byte[Math.max(16, rnd.nextInt(256))];
					rnd.nextBytes(bytes);
					return new AbstractMap.SimpleEntry<>(BytesSupport.toBytes(k), bytes);
				})
				.collect(Collectors.toList());

		System.gc();
		var usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		try (var sut = new Lasher(tmpPath)) {
			var elapsedPutMs = measure(() -> entries.parallelStream().forEach(e -> {
				sut.put(e.getKey(), e.getValue());
				counterInserted.incrementAndGet();
			}));
			System.out.printf("Inserted %d rows in %d ms, %s ops/s%n",
					counterInserted.get(), elapsedPutMs, counterInserted.get() / (elapsedPutMs / 1000d));

			var elapsedGetAllMs = measure(() -> entries.parallelStream().forEach(e -> {
				var expected = sut.get(e.getKey());
				if (expected != null) {
					counterFound.incrementAndGet();
				}
			}));
			System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
					recs, counterFound.get(), elapsedGetAllMs, recs / (elapsedGetAllMs / 1000d));

			counterFound.set(0L);
			var elapsedGetSomeRandomMs = measure(() -> entries.parallelStream().forEach(e -> {
				var expected = sut.get(BytesSupport.toBytes(rnd.nextLong()));
				if (expected != null) {
					counterFound.incrementAndGet();
				}
			}));
			System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
					recs, counterFound.get(), elapsedGetSomeRandomMs, recs / (elapsedGetSomeRandomMs / 1000d));
			var used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.gc();
			var usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.out.printf("Before: %s MB, after: %s MB, afterGC: %s MB, used: %s MB%n",
					toMB(usedBefore), toMB(used), toMB(usedAfter), toMB(usedAfter - usedBefore));
			printGCStats();
			System.out.printf("folder size %d MB%n", toMB(getFolderSize(tmpPath.toFile())));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	void performance_lasher_db_concurrent(@TempDir Path tmpPath) {
		long recs = 10_000_000;
		var counterFound = new AtomicLong(0L);
		var counterInserted = new AtomicLong(0L);

		var rnd = new Random();
		var entries = LongStream.range(0, recs)
				.mapToObj(k -> {
					var bytes = new byte[Math.max(16, rnd.nextInt(256))];
					rnd.nextBytes(bytes);
					return new AbstractMap.SimpleEntry<>(BytesSupport.toBytes(k), bytes);
				})
				.collect(Collectors.toList());

		System.gc();
		var usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		System.out.println("starting test");
		try (var sut = new LasherDB(tmpPath, 32)) {
			var elapsedPutMs = measure(() -> entries.parallelStream().forEach(e -> {
				sut.put(e.getKey(), e.getValue());
				counterInserted.incrementAndGet();
			}));
			System.out.printf("Inserted %d rows in %d ms, %s ops/s%n",
					counterInserted.get(), elapsedPutMs, counterInserted.get() / (elapsedPutMs / 1000d));

			var elapsedGetAllMs = measure(() -> entries.parallelStream().forEach(e -> {
				var expected = sut.get(e.getKey());
				if (expected != null) {
					counterFound.incrementAndGet();
				}
			}));
			System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
					recs, counterFound.get(), elapsedGetAllMs, recs / (elapsedGetAllMs / 1000d));

			counterFound.set(0L);
			var elapsedGetSomeRandomMs = measure(() -> entries.parallelStream().forEach(e -> {
				var expected = sut.get(BytesSupport.toBytes(rnd.nextLong()));
				if (expected != null) {
					counterFound.incrementAndGet();
				}
			}));
			System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
					recs, counterFound.get(), elapsedGetSomeRandomMs, recs / (elapsedGetSomeRandomMs / 1000d));
			var used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.gc();
			var usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.out.printf("Before: %s MB, after: %s MB, afterGC: %s MB, used: %s MB%n",
					toMB(usedBefore), toMB(used), toMB(usedAfter), toMB(usedAfter - usedBefore));
			printGCStats();
			System.out.printf("folder size %d MB%n", toMB(getFolderSize(tmpPath.toFile())));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@RepeatedTest(3)
	void performance_random(@TempDir Path tmpPath) {
		long recs = 10_000_000;
		var counterFound = new AtomicLong(0L);
		var counterInserted = new AtomicLong(0L);
		var rnd = new Random();
		var entries = rnd.longs(recs)
				.mapToObj(k -> {
					var bytes = new byte[Math.max(16, rnd.nextInt(256))];
					rnd.nextBytes(bytes);
					return new AbstractMap.SimpleEntry<>(BytesSupport.toBytes(k), bytes);
				})
				.collect(Collectors.toList());
		System.gc();
		var usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		try (var sut = new Lasher(tmpPath, 64L * 1024 * 1024)) {
			var elapsedPutRandomMs = measure(() -> entries.forEach(e -> {
				sut.put(e.getKey(), e.getValue());
				counterInserted.incrementAndGet();
			}));
			System.out.printf("Inserted %d rows in %d ms, %s ops/s%n",
					counterInserted.get(), elapsedPutRandomMs, counterInserted.get() / (elapsedPutRandomMs / 1000d));

			var elapsedGetAllRandomMs = measure(() -> entries.forEach(e -> {
				var expected = sut.get(e.getKey());
				if (expected != null) {
					counterFound.incrementAndGet();
				}
			}));
			System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
					recs, counterFound.get(), elapsedGetAllRandomMs, recs / (elapsedGetAllRandomMs / 1000d));

			counterFound.set(0L);
			var elapsedGetSomeRandomMs = measure(() -> entries.forEach(e -> {
				var expected = sut.get(BytesSupport.toBytes(rnd.nextLong()));
				if (expected != null) {
					counterFound.incrementAndGet();
				}
			}));
			System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
					recs, counterFound.get(), elapsedGetSomeRandomMs, recs / (elapsedGetSomeRandomMs / 1000d));
			var used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.gc();
			var usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			System.out.printf("Before: %s MB, after: %s MB, afterGC: %s MB, used: %s MB%n",
					toMB(usedBefore), toMB(used), toMB(usedAfter), toMB(usedAfter - usedBefore));
		}
	}

	private long measure(Runnable runnable) {
		var startGetMs = System.currentTimeMillis();
		runnable.run();
		return System.currentTimeMillis() - startGetMs;
	}

	private static long toMB(long sizeinBytes) {
		return sizeinBytes / 1024 / 1024;
	}

	@RepeatedTest(3)
	void performance_test_java_hashmap_random() {
		long recs = 10_000_000;
		var counterFound = new AtomicLong(0L);
		var counterInserted = new AtomicLong(0L);
		var rnd = new Random();
		var entries = rnd.longs(recs)
				.mapToObj(k -> {
					var bytes = new byte[Math.max(16, rnd.nextInt(256))];
					rnd.nextBytes(bytes);
					return new AbstractMap.SimpleEntry<>(BytesSupport.toBytes(k), bytes);
				})
				.collect(Collectors.toList());
		System.gc();
		var usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		var sut = new HashMap<byte[], byte[]>(1_000_000);
		var elapsedPutRandomMs = measure(() -> entries.forEach(e -> {
			sut.put(e.getKey(), e.getValue());
			counterInserted.incrementAndGet();
		}));
		System.out.printf("Inserted %d rows in %d ms, %s ops/s%n",
				counterInserted.get(), elapsedPutRandomMs, counterInserted.get() / (elapsedPutRandomMs / 1000d));

		var elapsedGetAllRandomMs = measure(() -> entries.forEach(e -> {
			var expected = sut.get(e.getKey());
			if (expected != null) {
				counterFound.incrementAndGet();
			}
		}));
		System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
				recs, counterFound.get(), elapsedGetAllRandomMs, recs / (elapsedGetAllRandomMs / 1000d));

		counterFound.set(0L);
		var elapsedGetSomeRandomMs = measure(() -> entries.forEach(e -> {
			var expected = sut.get(BytesSupport.toBytes(rnd.nextLong()));
			if (expected != null) {
				counterFound.incrementAndGet();
			}
		}));
		System.out.printf("Iterated %d and found %d rows in %d ms, %s rec/s%n",
				recs, counterFound.get(), elapsedGetSomeRandomMs, recs / (elapsedGetSomeRandomMs / 1000d));
		var used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		System.gc();
		var usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		System.out.printf("Before: %s MB, after: %s MB, afterGC: %s MB, used: %s MB%n",
				toMB(usedBefore), toMB(used), toMB(usedAfter), toMB(usedAfter - usedBefore));

	}

	private long getFolderSize(File folder) {
		long length = 0;
		File[] files = folder.listFiles();
		if (files == null) return length;

		for (var file : files) {
			if (file.isFile()) {
				length += file.length();
			} else {
				length += getFolderSize(file);
			}
		}
		return length;
	}

	private void printFiles(Path path) {
		try (var fileStream = Files.list(path)) {
			fileStream.forEach(p -> {
				try {
					System.out.printf("%s : %d KB%n", p, Files.size(p) / 1024);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public void printGCStats() {
		long totalGarbageCollections = 0;
		long garbageCollectionTime = 0;

		for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
			long count = gc.getCollectionCount();

			if (count >= 0) {
				totalGarbageCollections += count;
			}

			long time = gc.getCollectionTime();

			if (time >= 0) {
				garbageCollectionTime += time;
			}
		}

		System.out.println("Total Garbage Collections: " + totalGarbageCollections);
		System.out.println("Total Garbage Collection Time (ms): " + garbageCollectionTime);
	}

}
