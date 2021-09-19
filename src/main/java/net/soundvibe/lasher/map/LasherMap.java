package net.soundvibe.lasher.map;

import net.soundvibe.lasher.db.LasherDB;
import net.soundvibe.lasher.serde.Serde;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class LasherMap<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K, V>, AutoCloseable {

    private final LasherDB lasherDB;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public LasherMap(LasherDB lasherDB, Serde<K> keySerde, Serde<V> valSerde) {
        this.lasherDB = lasherDB;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
    }

    @Override
    public void close() {
        lasherDB.close();
    }

    public void delete() {
        lasherDB.delete();
    }

    @Override
    public int size() {
        return (int) lasherDB.size();
    }

    public long sizeLong() {
        return lasherDB.size();
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public V get(Object key) {
        var kBytes = keySerde.toBytes( (K)key);
        var out = lasherDB.get(kBytes);
        if (out == null) return null;
        return valSerde.fromBytes(out);
    }

    @Override
    public V put(K key, V value) {
        var old = lasherDB.put(keySerde.toBytes(key), valSerde.toBytes(value));
        if (old == null) return null;
        return valSerde.fromBytes(old);
    }

    @Override
    public V remove(Object key) {
        var kBytes = keySerde.toBytes((K)key);
        var old = lasherDB.remove(kBytes);
        if (old == null) return null;
        return valSerde.fromBytes(old);
    }

    @Override
    public void clear() {
        lasherDB.clear();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        var old = lasherDB.putIfAbsent(keySerde.toBytes(key), valSerde.toBytes(value));
        if (old == null) return null;
        return valSerde.fromBytes(old);
    }

    @Override
    public boolean remove(Object key, Object value) {
        var kBytes = keySerde.toBytes((K)key);
        var vBytes = valSerde.toBytes((V)value);
        return lasherDB.remove(kBytes, vBytes);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        var kBytes = keySerde.toBytes(key);
        var oldValBytes = valSerde.toBytes(oldValue);
        var newValBytes = valSerde.toBytes(newValue);
        return lasherDB.replace(kBytes, oldValBytes, newValBytes);
    }

    @Override
    public V replace(K key, V value) {
        var kBytes = keySerde.toBytes(key);
        var vBytes = valSerde.toBytes(value);
        var out = lasherDB.replace(kBytes, vBytes);
        if (out == null) return null;
        return valSerde.fromBytes(out);
    }

    protected class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public int size() {
            return LasherMap.this.size();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new Iterator<>() {
                final Iterator<Map.Entry<byte[], byte[]>> backingIt = lasherDB.iterator();

                @Override
                public boolean hasNext() {
                    return backingIt.hasNext();
                }

                @Override
                public java.util.Map.Entry<K, V> next() {
                    final Map.Entry<byte[], byte[]> e = backingIt.next();
                    var k = keySerde.fromBytes(e.getKey());
                    var v = valSerde.fromBytes(e.getValue());
                    return new AbstractMap.SimpleImmutableEntry<>(k, v);
                }

                @Override
                public void remove() {
                    backingIt.remove();
                }
            };
        }

        @Override
        public boolean contains(Object o) {
			if (!(o instanceof Map.Entry e)) {
				return false;
			}
			final V v = LasherMap.this.get(e.getKey());
			return v == null ? e.getValue() == null : v.equals(e.getValue());
        }

        @Override
        public void clear() {
            LasherMap.this.clear();
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry e)) return false;
            return LasherMap.this.remove(e.getKey(), e.getValue());
        }
    }
}
