package net.soundvibe.lasher.map;

import net.soundvibe.lasher.map.core.Lasher;
import net.soundvibe.lasher.serde.Serde;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class LasherMap<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K, V>, AutoCloseable {

    private final Lasher map;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public LasherMap(Lasher map, Serde<K> keySerde, Serde<V> valSerde) {
        this.map = map;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
    }

    @Override
    public void close() {
        map.close();
    }

    public void delete() {
        map.delete();
    }

    @Override
    public int size() {
        return (int) map.size();
    }

    public long sizeLong() {
        return map.size();
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public V get(Object key) {
        var kBytes = keySerde.toBytes( (K)key);
        var out = map.get(kBytes);
        if (out == null) return null;
        return valSerde.fromBytes(out);
    }

    @Override
    public V put(K key, V value) {
        var old = map.put(keySerde.toBytes(key), valSerde.toBytes(value));
        if (old == null) return null;
        return valSerde.fromBytes(old);
    }

    @Override
    public V remove(Object key) {
        var kBytes = keySerde.toBytes((K)key);
        var old = map.remove(kBytes);
        if (old == null) return null;
        return valSerde.fromBytes(old);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        var old = map.putIfAbsent(keySerde.toBytes(key), valSerde.toBytes(value));
        if (old == null) return null;
        return valSerde.fromBytes(old);
    }

    @Override
    public boolean remove(Object key, Object value) {
        var kBytes = keySerde.toBytes((K)key);
        var vBytes = valSerde.toBytes((V)value);
        return map.remove(kBytes, vBytes);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        var kBytes = keySerde.toBytes(key);
        var oldValBytes = valSerde.toBytes(oldValue);
        var newValBytes = valSerde.toBytes(newValue);
        return map.replace(kBytes, oldValBytes, newValBytes);
    }

    @Override
    public V replace(K key, V value) {
        var kBytes = keySerde.toBytes(key);
        var vBytes = valSerde.toBytes(value);
        var out = map.replace(kBytes, vBytes);
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
                final Iterator<Map.Entry<byte[], byte[]>> backingIt = map.iterator();

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
            if (!(o instanceof Map.Entry)) return false;
            var e = (Map.Entry<K, V>) o;
            final V v = get(e.getKey());
            return v == null ? e.getValue() == null : v.equals(e.getValue());
        }

        @Override
        public void clear() {
            LasherMap.this.clear();
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) return false;
            var e = (Map.Entry<K, V>) o;
            return LasherMap.this.remove(e.getKey(), e.getValue());
        }
    }
}
