package net.soundvibe.lasher.util;

public final class Hash {

    private Hash() {
    }

    private static final int HASH_SEED = 0xe17a1465;

    /**
     * Return a number greater than i whose bottom n bits collide when hashed.
     */
    public static long findCollision(long i, long nBits) {
        final long mask = (1L << nBits) - 1;
        final long targetHash = murmurHash(i) & mask;

        for (i = i + 1; i < Long.MAX_VALUE; i++) {
            if ((murmurHash(i) & mask) == targetHash) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Utility to hash a single value
     */
    public static long murmurHash(long k) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (HASH_SEED & 0xffffffffL) ^ (8 * m);

        k *= m;
        k ^= k >>> r;
        k *= m;
        h ^= k;
        h *= m;
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h & 0x7fffffffffffffffL;
    }

    public static long murmurHash(long[] data) {
        final int length = data.length;
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (HASH_SEED & 0xffffffffL) ^ (length * 8 * m);

        for (final long datum : data) {
            long k = datum;
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h & 0x7fffffffffffffffL;
    }

    public static long murmurHash(byte[] data) {
        return murmurHash(HASH_SEED, data);
    }

    /**
     * Modified to always return positive number. Murmurhash2 with 64-bit output.
     */
    public static long murmurHash(long hashSeed, byte[] data) {
        final int length = data.length;
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (hashSeed & 0xffffffffL) ^ (length * m);

        int length8 = length / 8;

        for (int i = 0; i < length8; i++) {
            final int i8 = i * 8;
            long k = ((long) data[i8] & 0xff) + (((long) data[i8 + 1] & 0xff) << 8)
                     + (((long) data[i8 + 2] & 0xff) << 16) + (((long) data[i8 + 3] & 0xff) << 24)
                     + (((long) data[i8 + 4] & 0xff) << 32) + (((long) data[i8 + 5] & 0xff) << 40)
                     + (((long) data[i8 + 6] & 0xff) << 48) + (((long) data[i8 + 7] & 0xff) << 56);

            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
        }

        switch (length % 8) {
            case 7:
                h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
            case 6:
                h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
            case 5:
                h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
            case 4:
                h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
            case 3:
                h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
            case 2:
                h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
            case 1:
                h ^= (long) (data[length & ~7] & 0xff);
                h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        //Clear sign bit
        return h & 0x7fffffffffffffffL;
    }
}
