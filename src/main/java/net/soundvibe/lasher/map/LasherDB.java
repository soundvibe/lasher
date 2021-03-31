package net.soundvibe.lasher.map;

import net.soundvibe.lasher.map.core.*;
import net.soundvibe.lasher.util.Hash;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.*;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class LasherDB implements AutoCloseable {

    private final List<Shard> shards;

    public LasherDB(Path baseDir, int shards) {
        this(baseDir, shards, Lasher.MB_32, Lasher.MB_32);
    }
    public LasherDB(Path baseDir, int shards, long indexFileLength, long dataFileLength) {
        this.shards = IntStream.range(0, shards)
                .mapToObj(i -> new Shard(i, Lasher.forShard(baseDir.resolve("shard_" + i), indexFileLength, dataFileLength)))
                .collect(toList());
    }

    public byte[] get(byte[] key) {
        requireNonNull(key, "key cannot be null");
        final long hash = Hash.hashBytes(key);
        var shard = shardForHash(hash);
        return shard.get(key, hash);
    }

    public byte[] put(byte[] key, byte[] value) {
        requireNonNull(key, "key cannot be null");
        requireNonNull(value, "value cannot be null");
        final long hash = Hash.hashBytes(key);
        var shard = shardForHash(hash);
        return shard.put(key, hash, value);
    }

    public byte[] putIfAbsent(byte[] key, byte[] value) {
        requireNonNull(key, "key cannot be null");
        requireNonNull(value, "value cannot be null");
        final long hash = Hash.hashBytes(key);
        var shard = shardForHash(hash);
        return shard.putIfAbsent(key, hash, value);
    }

    private Shard shardForHash(long hash) {
        return shards.get(Math.floorMod(hash, shards.size()));
    }

    @Override
    public void close() {
        for (var shard : shards) {
            shard.close();
        }
    }
}
