package net.soundvibe.lasher.util;

import net.openhft.hashing.LongHashFunction;

public final class Hash {

    private Hash() {
    }

    private static final int HASH_SEED = 0xe17a1465;

    private static final LongHashFunction XX_HASH = LongHashFunction.xx(HASH_SEED);

    /**
     * Return a number greater than i whose bottom n bits collide when hashed.
     */
    public static long findCollision(long i, long nBits) {
        final long mask = (1L << nBits) - 1;
        final long targetHash = hashLong(i) & mask;

        for (i = i + 1; i < Long.MAX_VALUE; i++) {
            if ((hashLong(i) & mask) == targetHash) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Utility to hash a single value
     */
    public static long hashLong(long k) {
        return XX_HASH.hashLong(k);
    }

    public static long hashBytes(byte[] data) {
        return XX_HASH.hashBytes(data);
    }
}
