package net.soundvibe.lasher.map;

import io.micrometer.core.instrument.*;
import net.soundvibe.lasher.map.core.*;
import net.soundvibe.lasher.util.Hash;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.*;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static net.soundvibe.lasher.util.Constants.*;

public final class LasherDB implements AutoCloseable {

	private final UUID id;
	private final List<Shard> shards;

	public LasherDB(Path baseDir) {
		this(baseDir, Math.max(2, Runtime.getRuntime().availableProcessors()));
	}

	public LasherDB(Path baseDir, int shards) {
		this(baseDir, shards, Lasher.MB_32, Lasher.MB_32);
	}

	public LasherDB(Path baseDir, int shards, long indexFileLength, long dataFileLength) {
		this.id = UUID.randomUUID();
		var tags = Tags.of(Tag.of("lasherId", id.toString()));
		this.shards = IntStream.range(0, shards)
				.mapToObj(i -> new Shard(i, baseDir.resolve("shard_" + i), indexFileLength, dataFileLength, tags))
				.collect(toList());
		Metrics.gauge("shards", tags, shards);
	}

	public byte[] get(byte[] key) {
		requireNonNull(key, KEY_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.get(key, hash);
	}

	public byte[] put(byte[] key, byte[] value) {
		requireNonNull(key, KEY_NOT_NULL);
		requireNonNull(value, VALUE_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.put(key, hash, value);
	}

	public byte[] putIfAbsent(byte[] key, byte[] value) {
		requireNonNull(key, KEY_NOT_NULL);
		requireNonNull(value, VALUE_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.putIfAbsent(key, hash, value);
	}

	public byte[] remove(byte[] key) {
		requireNonNull(key, KEY_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.remove(key, hash);
	}

	public boolean remove(byte[] key, byte[] value) {
		requireNonNull(key, KEY_NOT_NULL);
		requireNonNull(value, VALUE_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.remove(key, hash, value);
	}

	public boolean replace(byte[] key, byte[] prevVal, byte[] newVal) {
		requireNonNull(key, KEY_NOT_NULL);
		requireNonNull(prevVal, PREV_VALUE_NOT_NULL);
		requireNonNull(newVal, NEW_VALUE_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.replace(key, hash, prevVal, newVal);
	}

	public byte[] replace(byte[] key, byte[] value) {
		requireNonNull(key, KEY_NOT_NULL);
		requireNonNull(value, VALUE_NOT_NULL);
		final long hash = Hash.hashBytes(key);
		var shard = shardForHash(hash);
		return shard.replace(key, hash, value);
	}

	public boolean containsKey(byte[] k) {
		return get(k) != null;
	}

	public long size() {
		return shards.stream()
				.mapToLong(Shard::size)
				.sum();
	}

	public void clear() {
		for (var shard : shards) {
			shard.clear();
		}
	}

	public Iterator<Map.Entry<byte[], byte[]>> iterator() {
		return shards.stream()
				.flatMap(shard -> StreamSupport.stream(shard.spliterator(), false))
				.iterator();
	}

	private Shard shardForHash(long hash) {
		return shards.get(Math.floorMod(hash, shards.size()));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		LasherDB lasherDB = (LasherDB) o;
		return id.equals(lasherDB.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public void close() {
		for (var shard : shards) {
			shard.close();
		}
	}

	public void delete() {
		for (var shard : shards) {
			shard.delete();
		}
	}
}
